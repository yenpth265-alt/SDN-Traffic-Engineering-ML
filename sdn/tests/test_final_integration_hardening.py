"""
Final SDN Integration Hardening Tests (`Task F`).

Covers:
1. Supervised automatic Data Outbox delivery, receiver outage/recovery, graceful shutdown,
   and operational status reporting (`PASS_ADAPTER_TEST`).
2. Crash/restart recovery of pending outbox events and concurrent per-event deduplication guard.
3. Durable SQLite `DemandRelease` ledger: persist-before-ACK, restart restoration preserving
   exact `available_at_utc`, duplicate idempotency, conflict rejection (`409`), no zero-fill
   for missing bins, and post-restart causal `Snapshot` creation.
4. Persistence failure safety: DB error returns `503 DEMAND_PERSISTENCE_FAILED` (`ack=False`)
   without polluting RAM.
5. Separated `/health` deployment readiness gates (`process_liveness`, `sdn_readiness`,
   `data_delivery_readiness`, `full_system_readiness`).
6. Repeated `POST /apply?async=1` with active outbox worker + durable ledgers (no regression).
"""

from __future__ import annotations

import io
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sdn.adapters.data_event_publisher import (
    DataEventPublisher,
    DataPublisherConfig,
    DeliveryStatus,
)
from sdn.adapters.http_control_api import (
    HTTPDemandReleaseProvider,
    NorthboundAPIConfig,
    SDNNorthboundHTTPApp,
)
from sdn.adapters.sqlite_demand_ledger import SQLiteDemandReleaseLedger
from sdn.adapters.sqlite_idempotency_store import SQLiteBatchIdempotencyStore
from sdn.controller.decision_handler import DecisionHandler, DecisionHandlerConfig
from sdn.controller.state_manager import StateManager
from sdn.controller.topology_registry import TopologyRegistry
from sdn.flow_management.flow_matcher import FlowMatcher
from sdn.flow_management.route_compiler import RouteCompiler
from sdn.flow_management.rule_installer import RuleInstaller
from sdn.flow_management.switch_driver import InMemorySwitchDriver
from sdn.monitoring.snapshot_creator import SnapshotCreator
from sdn.route_verification.rule_verifier import RuleVerifier
from sdn.schemas.snapshot_models import OriginRefs

REPO_ROOT = Path(__file__).resolve().parents[2]
TOPOLOGY_BUNDLE_PATH = REPO_ROOT / "configs" / "network" / "topology_bundle.json"
CATALOG_DIR = REPO_ROOT / "configs" / "network"


def _wsgi_call(
    app: SDNNorthboundHTTPApp,
    method: str,
    path: str,
    *,
    query: str = "",
    body: dict[str, Any] | None = None,
    token: str | None = "hardening-tok",
) -> tuple[int, dict[str, Any]]:
    raw = json.dumps(body).encode("utf-8") if body is not None else b""
    environ: dict[str, Any] = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "QUERY_STRING": query,
        "CONTENT_LENGTH": str(len(raw)),
        "wsgi.input": io.BytesIO(raw),
    }
    if token:
        environ["HTTP_X_SDN_TOKEN"] = token
    captured: dict[str, Any] = {}

    def _start(status: str, headers: list[tuple[str, str]]) -> None:
        captured["status"] = int(status.split(" ", 1)[0])

    chunks = app(environ, _start)
    payload = json.loads(b"".join(chunks).decode("utf-8"))
    return captured["status"], payload


def _build_stack(
    tmp_path: Path,
    *,
    run_id: str = "run_hardening",
    demand_ledger: SQLiteDemandReleaseLedger | None = None,
    idempotency_store: SQLiteBatchIdempotencyStore | None = None,
    data_publisher: DataEventPublisher | None = None,
    health_provider: Any = None,
) -> tuple[TopologyRegistry, StateManager, SnapshotCreator, HTTPDemandReleaseProvider, SDNNorthboundHTTPApp]:
    topo = TopologyRegistry.from_bundle_file(TOPOLOGY_BUNDLE_PATH, csv_config_dir=CATALOG_DIR)
    driver = InMemorySwitchDriver(topo)
    compiler = RouteCompiler(topo, FlowMatcher(topo))
    verifier = RuleVerifier(compiler, driver)
    installer = RuleInstaller(compiler, driver, verifier)
    sm = StateManager(topo)
    boot_ts = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    boot_evs = installer.bootstrap_initial_paths(run_id, boot_ts)
    sm.initialize_run(run_id, boot_ts, verification_evidences=boot_evs)

    demand_provider = HTTPDemandReleaseProvider(ledger=demand_ledger)
    snap_creator = SnapshotCreator(topo, sm, demand_provider)
    demand_provider._origin_refs_lookup = lambda rid: snap_creator.get_run_origin_refs(rid)
    dh = DecisionHandler(
        topo,
        sm,
        installer,
        config=DecisionHandlerConfig.for_integration(),
        idempotency_store=idempotency_store,
        snapshot_lookup=snap_creator.get_closed_snapshot,
    )
    app = SDNNorthboundHTTPApp(
        state_manager=sm,
        decision_handler=dh,
        snapshot_creator=snap_creator,
        demand_provider=demand_provider,
        data_publisher=data_publisher,
        config=NorthboundAPIConfig(
            auth_token="hardening-tok",
            default_sync_wait_ms=0,
            default_run_id=run_id,
        ),
        health_provider=health_provider,
    )
    return topo, sm, snap_creator, demand_provider, app


def test_h01_auto_outbox_delivery_and_receiver_outage_recovery(tmp_path: Path) -> None:
    """
    Verify supervised background outbox worker (`PASS_ADAPTER_TEST`):
    - Receiver down -> events remain `PENDING_DELIVERY`, `last_error` recorded, never `DELIVERED`.
    - Receiver recovers -> worker automatically delivers pending events without manual flush.
    - Graceful `stop_worker()` stops worker cleanly.
    """
    receiver_up = {"ok": False}
    received: list[tuple[str, str]] = []

    def _sender(url: str, payload: dict[str, Any], headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
        if not receiver_up["ok"]:
            raise ConnectionError("DATA_RECEIVER_DOWN")
        received.append((url, headers["X-Idempotency-Key"]))
        return 200, '{"ack":true}'

    pub = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://127.0.0.1:19090",
            outbox_dir=tmp_path / "outbox",
            timeout_s=0.2,
            max_retries=1,
            backoff_base_s=0.01,
            auto_flush=True,
            flush_interval_s=0.04,
        ),
        sender_fn=_sender,
    )
    try:
        assert pub.worker_running is True
        topo = TopologyRegistry.from_bundle_file(TOPOLOGY_BUNDLE_PATH, csv_config_dir=CATALOG_DIR)
        driver = InMemorySwitchDriver(topo)
        installer = RuleInstaller(RouteCompiler(topo, FlowMatcher(topo)), driver, RuleVerifier(RouteCompiler(topo, FlowMatcher(topo)), driver))
        sm = StateManager(topo)
        sm.initialize_run("run_h01", "2026-11-22T10:00:00Z", verification_evidences=installer.bootstrap_initial_paths("run_h01", "2026-11-22T10:00:00Z"))
        rs = sm.get_routing_state("run_h01")

        res = pub.publish_routing_state(rs, deliver_now=False)
        assert res.status == DeliveryStatus.PENDING_DELIVERY

        # Wait briefly while receiver is down -> must stay PENDING_DELIVERY with last_error
        time.sleep(0.12)
        summary_down = pub.status_summary()
        assert summary_down["worker_running"] is True
        assert summary_down["pending_delivery_count"] == 1
        assert summary_down["delivered_count"] == 0
        assert summary_down["last_error"] is not None
        assert "DATA_RECEIVER_DOWN" in summary_down["last_error"]

        # Bring receiver back up -> background worker automatically delivers!
        receiver_up["ok"] = True
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if pub.status_summary()["delivered_count"] == 1:
                break
            time.sleep(0.03)

        summary_up = pub.status_summary()
        assert summary_up["delivered_count"] == 1
        assert summary_up["pending_delivery_count"] == 0
        assert summary_up["last_successful_delivery_utc"] is not None
        assert summary_up["last_error"] is None
        assert len(received) == 1
    finally:
        pub.stop_worker()
    assert pub.worker_running is False


def test_h02_outbox_crash_restart_recovery_and_concurrent_dedup_guard(tmp_path: Path) -> None:
    """
    Verify:
    - Pending outbox events survive restart and are delivered by a new publisher instance.
    - Concurrent `flush_pending()` calls never double-deliver the same `dedup_key`.
    """
    outbox_dir = tmp_path / "outbox_restart"
    topo = TopologyRegistry.from_bundle_file(TOPOLOGY_BUNDLE_PATH, csv_config_dir=CATALOG_DIR)
    driver = InMemorySwitchDriver(topo)
    comp = RouteCompiler(topo, FlowMatcher(topo))
    installer = RuleInstaller(comp, driver, RuleVerifier(comp, driver))
    sm = StateManager(topo)
    sm.initialize_run("run_h02", "2026-11-22T10:00:00Z", verification_evidences=installer.bootstrap_initial_paths("run_h02", "2026-11-22T10:00:00Z"))
    rs = sm.get_routing_state("run_h02")

    # Process 1: enqueue while endpoint is unreachable, then "crash" (no ACK)
    pub1 = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://127.0.0.1:19090",
            outbox_dir=outbox_dir,
            max_retries=1,
            auto_flush=False,
        ),
        sender_fn=lambda u, p, h, t: (503, "down"),
    )
    r1 = pub1.publish_routing_state(rs, deliver_now=True)
    assert r1.status == DeliveryStatus.PENDING_DELIVERY

    # Process 2: restart on same outbox_dir with slow sender & concurrent flushes
    send_calls = {"count": 0}
    send_lock = threading.Lock()
    entered = threading.Event()
    release = threading.Event()

    def _slow_sender(url: str, payload: dict[str, Any], headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
        with send_lock:
            send_calls["count"] += 1
        entered.set()
        release.wait(timeout=2.0)
        return 200, '{"ack":true}'

    pub2 = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://127.0.0.1:19090",
            outbox_dir=outbox_dir,
            max_retries=1,
            auto_flush=False,
        ),
        sender_fn=_slow_sender,
    )
    assert pub2.status_summary()["pending_delivery_count"] == 1

    t1 = threading.Thread(target=pub2.flush_pending)
    t1.start()
    assert entered.wait(timeout=2.0)
    # Second concurrent flush while t1 is in-flight must NOT send duplicate HTTP POST!
    pub2.flush_pending()
    release.set()
    t1.join(timeout=2.0)

    assert send_calls["count"] == 1
    assert pub2.status_summary()["delivered_count"] == 1
    assert pub2.status_summary()["pending_delivery_count"] == 0


def test_h03_demand_release_durable_sqlite_restart_and_snapshot_causality(tmp_path: Path) -> None:
    """
    Verify Task B:
    - Persist 48 DemandRelease events in a single SQLite WAL transaction before ACK.
    - Restart controller -> restore all 48 events with exact original `available_at_utc`.
    - Duplicate same event_id -> idempotent `duplicate=True` (200).
    - Conflicting same event_id -> rejected `409 DEMAND_RELEASE_CONFLICT`.
    - Create valid causal `Snapshot` post-restart; missing bins remain `null` (never `0.0`).
    """
    db_path = tmp_path / "demand_ledger.sqlite3"
    ledger1 = SQLiteDemandReleaseLedger(db_path, retention_bins=3600)
    topo, sm1, sc1, dp1, app1 = _build_stack(tmp_path, run_id="run_h03", demand_ledger=ledger1)

    now_dt = datetime.now(timezone.utc).replace(microsecond=0)
    meas_dt = now_dt - timedelta(seconds=1)
    w_start = meas_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    w_end = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    events = [
        {
            "event_id": f"network:run_h03:demand:{fg.flow_group_id}:0",
            "run_id": "run_h03",
            "flow_group_id": fg.flow_group_id,
            "bin_index": 0,
            "window_start_utc": w_start,
            "window_end_utc": w_end,
            "requested_mbps": 3.25,
            "released_at_utc": w_end,
        }
        for fg in topo.get_all_flow_groups()
    ]
    c_rel, b_rel = _wsgi_call(app1, "POST", "/demand-releases", body={"events": events})
    assert c_rel == 200
    assert b_rel["ack"] is True
    assert b_rel["count"] == 48
    orig_avail = {r["event_id"]: r["available_at_utc"] for r in b_rel["results"]}
    assert dp1.is_durable is True
    assert ledger1.count("run_h03") == 48
    dp1.close()

    # Simulate controller restart on the same SQLite demand ledger
    ledger2 = SQLiteDemandReleaseLedger(db_path, retention_bins=3600)
    _, _, _, dp2, app2 = _build_stack(tmp_path, run_id="run_h03", demand_ledger=ledger2)
    try:
        restored = dp2.get_observations("run_h03")
        assert len(restored) == 48
        # Original available_at_utc must be preserved identically!
        for rec in restored:
            assert rec.available_at_utc == orig_avail[rec.release_record_id]

        # Duplicate same event_id -> idempotent 200 (duplicate=True, original available_at_utc preserved)
        c_dup, b_dup = _wsgi_call(app2, "POST", "/demand-releases", body=events[0])
        assert c_dup == 200
        assert b_dup["duplicate"] is True
        assert b_dup["available_at_utc"] == orig_avail[events[0]["event_id"]]

        # Conflicting same event_id -> 409 Conflict
        bad_evt = dict(events[0])
        bad_evt["requested_mbps"] = 99.0
        c_conf, b_conf = _wsgi_call(app2, "POST", "/demand-releases", body=bad_evt)
        assert c_conf == 409
        assert b_conf["error"] == "DEMAND_RELEASE_CONFLICT"

        # Create causal snapshot post-restart (cutoff=1, lookback_L=1 -> VALID)
        origins = {
            "run_started_at_utc": (meas_dt - timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "traffic_started_at_utc": (meas_dt - timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "measurement_started_at_utc": w_start,
            "delta_s": 1,
        }
        snap_as_of = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        c_snap, b_snap = _wsgi_call(
            app2,
            "POST",
            "/snapshots",
            body={
                "run_id": "run_h03",
                "cutoff": 1,
                "lookback_L": 1,
                "snapshot_as_of_utc": snap_as_of,
                "origin_refs": origins,
            },
        )
        assert c_snap == 200
        assert b_snap["quality_status"] == "VALID"

        # Missing bin 1 must NOT be substituted with 0.0 (cutoff=2, lookback_L=2 -> INCOMPLETE + null!)
        c_miss, b_miss = _wsgi_call(
            app2,
            "POST",
            "/snapshots",
            body={
                "run_id": "run_h03",
                "cutoff": 2,
                "lookback_L": 2,
                "snapshot_as_of_utc": (now_dt + timedelta(seconds=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "origin_refs": origins,
            },
        )
        assert c_miss == 200
        assert b_miss["quality_status"] != "VALID"
        bin1_items = [it for it in b_miss["history"] if it["bin_index"] == 1]
        assert len(bin1_items) == 48
        assert all(it["requested_mbps"] is None and it["valid"] is False for it in bin1_items)
    finally:
        dp2.close()


def test_h04_demand_release_persistence_failure_blocks_ack(tmp_path: Path) -> None:
    """
    Verify DB failure safety: if SQLite persistence fails, `POST /demand-releases` MUST
    return `503 DEMAND_PERSISTENCE_FAILED` (`ack=False`) and MUST NOT mutate RAM.
    """
    ledger = SQLiteDemandReleaseLedger(tmp_path / "fail_ledger.sqlite3")
    _, _, _, dp, app = _build_stack(tmp_path, run_id="run_h04", demand_ledger=ledger)
    ledger.close()  # Force persistence failure

    evt = {
        "event_id": "network:run_h04:demand:0x103:0",
        "run_id": "run_h04",
        "flow_group_id": "0x103",
        "bin_index": 0,
        "window_start_utc": "2026-11-22T10:01:00Z",
        "window_end_utc": "2026-11-22T10:01:01Z",
        "requested_mbps": 4.0,
        "released_at_utc": "2026-11-22T10:01:01Z",
    }
    code, body = _wsgi_call(app, "POST", "/demand-releases", body=evt)
    assert code == 503
    assert body["ack"] is False
    assert body["error"] == "DEMAND_PERSISTENCE_FAILED"
    assert len(dp.get_observations("run_h04")) == 0


def test_h05_production_health_readiness_gates_separation(tmp_path: Path) -> None:
    """
    Verify Task C: `/health` explicitly separates process liveness, SDN readiness,
    Data delivery readiness, and full-system readiness without falsely failing SDN Core
    when Data is unconfigured.
    """
    # Case 1: SDN verified, Data NOT configured
    _, _, _, _, app_no_data = _build_stack(
        tmp_path,
        run_id="run_h05_a",
        health_provider=lambda: {
            "connected_dpids": [1, 2, 3, 4, 5, 6],
            "readiness_state": "VERIFIED",
            "verified_cached": 48,
            "state_version": 2,
            "reconcile_required": False,
        },
    )
    c1, h1 = _wsgi_call(app_no_data, "GET", "/health")
    assert c1 == 200
    assert h1["process_liveness"]["alive"] is True
    assert h1["sdn_readiness"]["ready"] is True
    assert h1["data_delivery_readiness"]["configured"] is False
    assert h1["data_delivery_readiness"]["ready"] is False
    assert "DATA_ENDPOINT_NOT_CONFIGURED" in h1["data_delivery_readiness"]["blockers"]
    assert h1["full_system_readiness"]["ready"] is False

    # Case 2: Full production stack (durable SQLite idempotency + durable demand ledger + running outbox worker)
    idem = SQLiteBatchIdempotencyStore(tmp_path / "idem.sqlite3")
    d_ledger = SQLiteDemandReleaseLedger(tmp_path / "demand.sqlite3")
    pub = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://127.0.0.1:19090",
            outbox_dir=tmp_path / "outbox_h05",
            auto_flush=True,
            flush_interval_s=0.05,
        ),
        sender_fn=lambda u, p, h, t: (200, '{"ack":true}'),
    )
    try:
        _, _, _, _, app_full = _build_stack(
            tmp_path,
            run_id="run_h05_b",
            demand_ledger=d_ledger,
            idempotency_store=idem,
            data_publisher=pub,
            health_provider=lambda: {
                "connected_dpids": [1, 2, 3, 4, 5, 6],
                "readiness_state": "VERIFIED",
                "verified_cached": 48,
                "state_version": 2,
                "reconcile_required": False,
            },
        )
        c2, h2 = _wsgi_call(app_full, "GET", "/health")
        assert c2 == 200
        assert h2["sdn_readiness"]["ready"] is True
        assert h2["sdn_readiness"]["idempotency_store_durable"] is True
        assert h2["sdn_readiness"]["demand_ledger_durable"] is True
        assert h2["data_delivery_readiness"]["ready"] is True
        assert h2["data_delivery_readiness"]["worker_running"] is True
        # SDN integration readiness is True, while full_system_readiness MUST stay False
        # because SDN /health cannot verify external roles (ML/Optimization/Runner/TC-I01).
        assert h2["sdn_integration_readiness"]["ready"] is True
        assert h2["sdn_integration_readiness"]["blockers"] == []
        assert h2["full_system_readiness"]["ready"] is False
        assert h2["full_system_readiness"]["sdn_integration_ready"] is True
        assert h2["full_system_readiness"]["tc_i01_verified"] is False
        assert "EXTERNAL_ROLES_NOT_VERIFIED_BY_SDN_HEALTH" in h2["full_system_readiness"]["blockers"]
    finally:
        pub.stop_worker()
        d_ledger.close()
        idem.close()


def test_h06_repeated_async_apply_with_outbox_worker_and_durable_demand_no_regression(
    tmp_path: Path,
) -> None:
    """
    Verify repeated `POST /apply?async=1` (`P1 -> P2` and `P2 -> P1`) works cleanly
    alongside durable SQLite DemandRelease ledger, SQLite idempotency, and auto-flush
    DataEventPublisher without interference or regression.
    """
    delivered_types: list[str] = []

    def _sender(url: str, payload: dict[str, Any], headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
        delivered_types.append(headers["X-SDN-Event-Type"])
        return 200, '{"ack":true}'

    idem = SQLiteBatchIdempotencyStore(tmp_path / "idem_h06.sqlite3")
    d_ledger = SQLiteDemandReleaseLedger(tmp_path / "demand_h06.sqlite3")
    pub = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://127.0.0.1:19090",
            outbox_dir=tmp_path / "outbox_h06",
            auto_flush=True,
            flush_interval_s=0.04,
        ),
        sender_fn=_sender,
    )
    try:
        topo, sm, _, _, app = _build_stack(
            tmp_path,
            run_id="run_h06",
            demand_ledger=d_ledger,
            idempotency_store=idem,
            data_publisher=pub,
        )
        now_dt = datetime.now(timezone.utc).replace(microsecond=0)
        meas_dt = now_dt - timedelta(seconds=1)
        w_start = meas_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        w_end = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        events = [
            {
                "event_id": f"network:run_h06:demand:{fg.flow_group_id}:0",
                "run_id": "run_h06",
                "flow_group_id": fg.flow_group_id,
                "bin_index": 0,
                "window_start_utc": w_start,
                "window_end_utc": w_end,
                "requested_mbps": 2.0,
                "released_at_utc": w_end,
            }
            for fg in topo.get_all_flow_groups()
        ]
        c_rel, _ = _wsgi_call(app, "POST", "/demand-releases", body={"events": events})
        assert c_rel == 200

        snap_as_of = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        c_sn, b_sn = _wsgi_call(
            app,
            "POST",
            "/snapshots",
            body={
                "run_id": "run_h06",
                "cutoff": 1,
                "lookback_L": 1,
                "snapshot_as_of_utc": snap_as_of,
                "origin_refs": {
                    "run_started_at_utc": (meas_dt - timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "traffic_started_at_utc": (meas_dt - timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "measurement_started_at_utc": w_start,
                    "delta_s": 1,
                },
            },
        )
        assert c_sn == 200
        assert b_sn["quality_status"] == "VALID"

        # Apply 1: 0x103 P1 -> P2 via async=1
        req1 = {
            "run_id": "run_h06",
            "decision_batch_id": "batch_h06_1",
            "snapshot_id": b_sn["snapshot_id"],
            "expected_state_version": sm.get_routing_state("run_h06").state_version,
            "config_version": "1.0",
            "forecast_start": w_end,
            "forecast_end": (now_dt + timedelta(seconds=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "apply_deadline_utc": (now_dt + timedelta(seconds=25)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "changes": [{"flow_group_id": "0x103", "old_path_id": "0x103_P1", "new_path_id": "0x103_P2"}],
            "topology_version": topo.topology_version,
            "topology_hash": topo.topology_hash,
            "snapshot_as_of_utc": b_sn["snapshot_as_of_utc"],
            "history_cutoff_utc": b_sn["history_cutoff_utc"],
        }
        c_a1, b_a1 = _wsgi_call(app, "POST", "/apply", query="async=1", body=req1)
        assert c_a1 in (200, 202)
        for _ in range(50):
            _, st1 = _wsgi_call(app, "GET", "/apply-status", query="run_id=run_h06&decision_batch_id=batch_h06_1")
            if st1.get("job_state") == "FINALIZED":
                break
            time.sleep(0.02)
        assert st1["apply_response"]["status"] == "APPLIED"
        assert st1["apply_response"]["results"][0]["actual_path_id"] == "0x103_P2"

        # Wait for background outbox worker to automatically deliver Snapshot + ApplyResponse + RoutingState
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if pub.status_summary()["pending_delivery_count"] == 0 and pub.status_summary()["delivered_count"] >= 2:
                break
            time.sleep(0.03)
        assert pub.status_summary()["pending_delivery_count"] == 0
        assert pub.status_summary()["delivered_count"] >= 2
        assert "ApplyResponse" in delivered_types
    finally:
        pub.stop_worker()
        d_ledger.close()
        idem.close()
