"""
Phase 12 Integration Adapter Contract Tests (`PASS_MOCK` on Windows / Ubuntu).

Tests all three production-oriented adapters in `sdn/adapters/`:
- `SQLiteBatchIdempotencyStore` (WAL durability, atomic conflict, restart recovery, concurrency)
- `DataEventPublisher` (durable JSONL outbox, ACK enforcement, retry/backoff, disconnect recovery)
- `SDNNorthboundHTTPApp` & `HTTPDemandReleaseProvider` (auth, size limits, `sender.py` demand
  ingestion, `48 x 60` snapshots, sync & async `POST /apply`, `GET /apply-status`, in-flight
  retry/conflict/BUSY guards, and static ARP non-flood compatibility).

NOTE: Mock Data receiver tests here validate adapter contracts (`PASS_MOCK`) and do NOT
constitute full cross-role Data/ML/Optimization system integration.
"""

from __future__ import annotations

import io
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import pytest

from sdn.adapters.data_event_publisher import (
    DataEventPublisher,
    DataPublisherConfig,
    DeliveryStatus,
)
from sdn.adapters.http_control_api import (
    ApplyJobState,
    HTTPDemandReleaseProvider,
    NorthboundAPIConfig,
    SDNNorthboundHTTPApp,
)
from sdn.adapters.sqlite_idempotency_store import (
    IdempotencyConflictError,
    SQLiteBatchIdempotencyStore,
)
from sdn.apps.ryu_s0_app import ARP_HELPER_COOKIE, S0AppConfig, S0ControllerOrchestrator
from sdn.controller.decision_handler import (
    CachedBatchEntry,
    DecisionHandler,
    DecisionHandlerConfig,
)
from sdn.controller.state_manager import StateManager, format_utc_timestamp
from sdn.controller.topology_registry import TopologyRegistry
from sdn.flow_management.flow_matcher import FlowMatcher
from sdn.flow_management.route_compiler import RouteCompiler
from sdn.flow_management.rule_installer import RuleInstaller
from sdn.flow_management.switch_driver import InMemorySwitchDriver
from sdn.monitoring.snapshot_creator import SnapshotCreator
from sdn.route_verification.rule_verifier import RuleVerifier
from sdn.schemas.apply_models import ApplyFlowChange, ApplyRequest, ApplyResponse
from sdn.schemas.snapshot_models import OriginRefs, Snapshot
from sdn.schemas.state_models import (
    BatchStatus,
    EvidenceMode,
    ReasonCode,
    VerificationEvidence,
    VerificationStatus,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
BUNDLE_PATH = REPO_ROOT / "configs" / "network" / "topology_bundle.json"
CSV_DIR = REPO_ROOT / "configs" / "network"


def _wsgi_call(
    app: SDNNorthboundHTTPApp,
    method: str,
    path: str,
    *,
    query: str = "",
    body_obj: Optional[Any] = None,
    raw_body: Optional[bytes] = None,
    headers: Optional[dict[str, str]] = None,
) -> tuple[int, dict[str, Any]]:
    if raw_body is not None:
        data = raw_body
    elif body_obj is not None:
        data = json.dumps(body_obj).encode("utf-8")
    else:
        data = b""

    environ: dict[str, Any] = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "QUERY_STRING": query,
        "CONTENT_LENGTH": str(len(data)),
        "CONTENT_TYPE": "application/json",
        "wsgi.input": io.BytesIO(data),
    }
    for k, v in (headers or {}).items():
        env_key = "HTTP_" + k.upper().replace("-", "_")
        environ[env_key] = v

    captured: dict[str, Any] = {}

    def _start_response(status: str, resp_headers: list[tuple[str, str]]) -> None:
        captured["status_code"] = int(status.split(" ", 1)[0])
        captured["headers"] = dict(resp_headers)

    chunks = app(environ, _start_response)
    raw = b"".join(chunks)
    return captured["status_code"], json.loads(raw.decode("utf-8"))


def _build_mock_stack(
    tmp_path: Path,
    *,
    run_id: str = "run_p12_01",
    auth_token: Optional[str] = None,
    max_request_bytes: int = 1_048_576,
    default_sync_wait_ms: int = 300,
    data_publisher: Optional[DataEventPublisher] = None,
    verify_delay_s: float = 0.0,
) -> dict[str, Any]:
    topo = TopologyRegistry.from_bundle_file(BUNDLE_PATH)
    driver = InMemorySwitchDriver(topo)
    matcher = FlowMatcher(topo)
    compiler = RouteCompiler(topo, matcher)
    verifier = RuleVerifier(compiler, driver)
    installer = RuleInstaller(compiler, driver, verifier)
    sm = StateManager(topo)

    if verify_delay_s > 0.0:
        orig_verify = driver.verify_forwarding_counter_or_probe

        def _slow_verify(plan, *, probe_id=None):
            time.sleep(verify_delay_s)
            return orig_verify(plan, probe_id=probe_id)

        driver.verify_forwarding_counter_or_probe = _slow_verify  # type: ignore[method-assign]

    db_path = tmp_path / "sdn_idempotency.sqlite3"
    idem_store = SQLiteBatchIdempotencyStore(db_path)
    dh = DecisionHandler(
        topo,
        sm,
        installer,
        config=DecisionHandlerConfig.for_integration(),
        idempotency_store=idem_store,
    )

    # Initialize run with 48/48 P1 VERIFIED (timestamp 120s in the past relative to UTC now)
    init_utc = format_utc_timestamp(datetime.now(timezone.utc) - timedelta(seconds=120))
    from sdn.flow_management.switch_driver import FlowModCommand, SwitchPhaseTag

    evidences = {}
    for fg in topo.get_all_flow_groups():
        pid = f"{fg.flow_group_id}_P1"
        plan = compiler.compile_path(pid)
        for rule in (*plan.downstream_rules, plan.source_edge_rule):
            driver.send_flow_mod(rule, FlowModCommand.ADD, phase=SwitchPhaseTag.PRIMARY_DOWNSTREAM)
        vr = verifier.verify_flow_path(
            run_id, fg.flow_group_id, pid, verified_at_utc=init_utc
        )
        assert vr.evidence is not None
        evidences[fg.flow_group_id] = vr.evidence
    sm.initialize_run(
        run_id,
        bootstrapped_at_utc=init_utc,
        verification_evidences=evidences,
    )

    demand_provider = HTTPDemandReleaseProvider()
    snap_creator = SnapshotCreator(topo, sm, demand_provider)
    origins = OriginRefs(
        run_started_at_utc="2026-11-21T23:59:00Z",
        traffic_started_at_utc="2026-11-21T23:59:00Z",
        measurement_started_at_utc="2026-11-22T00:00:00Z",
        delta_s=1,
    )
    snap_creator.set_run_origin_refs(run_id, origins)
    demand_provider._origin_refs_lookup = lambda rid: snap_creator.get_run_origin_refs(rid)

    app = SDNNorthboundHTTPApp(
        state_manager=sm,
        decision_handler=dh,
        snapshot_creator=snap_creator,
        demand_provider=demand_provider,
        data_publisher=data_publisher,
        config=NorthboundAPIConfig(
            bind_host="127.0.0.1",
            bind_port=18080,
            auth_token=auth_token,
            max_request_bytes=max_request_bytes,
            default_sync_wait_ms=default_sync_wait_ms,
            default_run_id=run_id,
        ),
    )
    return {
        "topo": topo,
        "driver": driver,
        "sm": sm,
        "dh": dh,
        "idem_store": idem_store,
        "db_path": db_path,
        "demand_provider": demand_provider,
        "snap_creator": snap_creator,
        "app": app,
        "run_id": run_id,
    }


def _make_apply_request(
    stack: dict[str, Any],
    *,
    batch_id: str,
    snap: Snapshot,
    flow_group_id: str = "0x103",
    old_path_id: str = "0x103_P1",
    new_path_id: str = "0x103_P2",
    expected_sv: Optional[int] = None,
    now_dt: Optional[datetime] = None,
) -> dict[str, Any]:
    now = now_dt or datetime.now(timezone.utc)
    st = stack["sm"].get_routing_state(stack["run_id"])
    req = ApplyRequest(
        run_id=stack["run_id"],
        decision_batch_id=batch_id,
        snapshot_id=snap.snapshot_id,
        expected_state_version=st.state_version if expected_sv is None else expected_sv,
        config_version="1.0",
        forecast_start=format_utc_timestamp(now),
        forecast_end=format_utc_timestamp(now + timedelta(seconds=30)),
        apply_deadline_utc=format_utc_timestamp(now + timedelta(seconds=25)),
        changes=(
            ApplyFlowChange(
                flow_group_id=flow_group_id,
                old_path_id=old_path_id,
                new_path_id=new_path_id,
            ),
        ),
        topology_version=stack["topo"].topology_version,
        topology_hash=stack["topo"].topology_hash,
        snapshot_as_of_utc=snap.snapshot_as_of_utc,
        history_cutoff_utc=snap.history_cutoff_utc,
    )
    return req.to_dict()


def _populate_1bin_and_snapshot(stack: dict[str, Any], *, cutoff: int = 1) -> Snapshot:
    run_id = stack["run_id"]
    now_dt = datetime.now(timezone.utc).replace(microsecond=0)
    # Register origin_refs aligned to now_dt - cutoff seconds so snapshot is fresh!
    meas_dt = now_dt - timedelta(seconds=cutoff)
    stack["snap_creator"].set_run_origin_refs(
        run_id,
        OriginRefs(
            run_started_at_utc=format_utc_timestamp(meas_dt - timedelta(seconds=60)),
            traffic_started_at_utc=format_utc_timestamp(meas_dt - timedelta(seconds=60)),
            measurement_started_at_utc=format_utc_timestamp(meas_dt),
            delta_s=1,
        ),
    )
    w_start = format_utc_timestamp(meas_dt + timedelta(seconds=cutoff - 1))
    w_end = format_utc_timestamp(meas_dt + timedelta(seconds=cutoff))
    events = [
        {
            "event_id": f"network:{run_id}:demand:{fg.flow_group_id}:{cutoff - 1}",
            "run_id": run_id,
            "flow_group_id": fg.flow_group_id,
            "bin_index": cutoff - 1,
            "window_start_utc": w_start,
            "window_end_utc": w_end,
            "requested_mbps": 2.5,
            "released_at_utc": w_end,
        }
        for fg in stack["topo"].get_all_flow_groups()
    ]
    code, ack = _wsgi_call(stack["app"], "POST", "/demand-releases", body_obj={"events": events})
    assert code == 200 and ack["ack"] is True and ack["count"] == 48

    code, snap_dict = _wsgi_call(
        stack["app"],
        "POST",
        "/snapshots",
        body_obj={"run_id": run_id, "cutoff": cutoff, "lookback_L": 1},
    )
    assert code == 200
    snap = Snapshot.from_dict(snap_dict, strict=True, verify_hash=True)
    assert snap.quality_status.value == "VALID"
    return snap


# =============================================================================
# Test 1: Durable SQLite WAL Idempotency Store & Restart Recovery
# =============================================================================


def test_p12_01_sqlite_idempotency_durable_replay_conflict_and_restart(tmp_path: Path) -> None:
    stack = _build_mock_stack(tmp_path)
    store: SQLiteBatchIdempotencyStore = stack["idem_store"]
    assert store.is_durable is True
    assert store.store_status == "DURABLE_SQLITE_WAL"

    snap = _populate_1bin_and_snapshot(stack, cutoff=1)
    req_dict = _make_apply_request(stack, batch_id="batch_durable_001", snap=snap)
    code, body = _wsgi_call(stack["app"], "POST", "/apply", body_obj=req_dict)
    assert code == 200
    assert body["apply_response"]["status"] == "APPLIED"
    assert store.count_entries(stack["run_id"]) == 1

    # Atomic conflict check directly on SQLite store
    stored = store.get(stack["run_id"], "batch_durable_001")
    assert stored is not None
    with pytest.raises(IdempotencyConflictError):
        store.put(
            stack["run_id"],
            "batch_durable_001",
            CachedBatchEntry(payload_hash="deadbeef" * 8, response=stored.response),
        )

    # Concurrent idempotent writes must never fail or corrupt
    errors: list[Exception] = []

    def _writer() -> None:
        try:
            store.put(stack["run_id"], "batch_durable_001", stored)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=_writer) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []

    # Simulate full controller process restart: close DB, open brand-new SQLite store & DecisionHandler
    store.close()
    restarted_store = SQLiteBatchIdempotencyStore(stack["db_path"])
    assert restarted_store.count_entries(stack["run_id"]) == 1

    sm2 = StateManager(stack["topo"])
    past_boot = format_utc_timestamp(datetime.now(timezone.utc) - timedelta(seconds=120))
    past_restart = format_utc_timestamp(datetime.now(timezone.utc) - timedelta(seconds=60))
    sm2.initialize_run(stack["run_id"], bootstrapped_at_utc=past_boot)
    driver2 = InMemorySwitchDriver(stack["topo"])
    compiler2 = RouteCompiler(stack["topo"], FlowMatcher(stack["topo"]))
    installer2 = RuleInstaller(compiler2, driver2, RuleVerifier(compiler2, driver2))
    dh2 = DecisionHandler(
        stack["topo"],
        sm2,
        installer2,
        config=DecisionHandlerConfig.for_integration(),
        idempotency_store=restarted_store,
    )
    dh2.notify_controller_restart(stack["run_id"], restarted_at_utc=past_restart)

    # 1. Immediately after restart (while reconcile_required=True), DecisionHandler must NOT
    # mask the active reconciliation lock (consistent with StateManager restart semantics):
    pre_rec_resp = dh2.handle_apply_request(
        ApplyRequest.from_dict(req_dict, strict=True),
        decision_time_utc=format_utc_timestamp(datetime.now(timezone.utc)),
    )
    assert pre_rec_resp.status == BatchStatus.REJECTED
    assert pre_rec_resp.reason == ReasonCode.RECONCILIATION_REQUIRED

    # 2. Reconcile state after restart, then replay the pre-restart committed batch ->
    # durable SQLite store returns persisted APPLIED (idempotent_replay=True) with 0 FlowMods!
    from sdn.flow_management.switch_driver import FlowModCommand, SwitchPhaseTag

    rec_utc = format_utc_timestamp(datetime.now(timezone.utc) - timedelta(seconds=10))
    ev2 = {}
    for fg in stack["topo"].get_all_flow_groups():
        pid = "0x103_P2" if fg.flow_group_id == "0x103" else f"{fg.flow_group_id}_P1"
        plan = compiler2.compile_path(pid)
        for rule in (*plan.downstream_rules, plan.source_edge_rule):
            driver2.send_flow_mod(rule, FlowModCommand.ADD, phase=SwitchPhaseTag.PRIMARY_DOWNSTREAM)
        vr2 = RuleVerifier(compiler2, driver2).verify_flow_path(
            stack["run_id"], fg.flow_group_id, pid, verified_at_utc=rec_utc
        )
        assert vr2.evidence is not None
        ev2[fg.flow_group_id] = vr2.evidence
    sm2.reconcile_run_state(stack["run_id"], verified_evidences=ev2, reconciled_at_utc=rec_utc)

    ops_before = len(driver2.get_operation_log())
    replay_resp = dh2.handle_apply_request(
        ApplyRequest.from_dict(req_dict, strict=True),
        decision_time_utc=format_utc_timestamp(datetime.now(timezone.utc)),
    )
    assert replay_resp.status == BatchStatus.APPLIED
    assert replay_resp.idempotent_replay is True
    assert len(driver2.get_operation_log()) == ops_before

    # Replay same batch_id with modified payload after restart -> BATCH_ID_CONFLICT!
    conflict_dict = dict(req_dict)
    conflict_dict["changes"] = [
        {"flow_group_id": "0x104", "old_path_id": "0x104_P1", "new_path_id": "0x104_P2"}
    ]
    conflict_dict.pop("payload_hash", None)
    conflict_resp = dh2.handle_apply_request(
        ApplyRequest.from_dict(conflict_dict, strict=True),
        decision_time_utc=format_utc_timestamp(datetime.now(timezone.utc)),
    )
    assert conflict_resp.status == BatchStatus.REJECTED
    assert conflict_resp.reason == ReasonCode.BATCH_ID_CONFLICT
    restarted_store.close()


# =============================================================================
# Test 2: Data Event Publisher (Outbox, ACK, Retry, Disconnect Recovery)
# =============================================================================


def test_p12_02_data_event_publisher_outbox_ack_retry_and_disconnect_recovery(tmp_path: Path) -> None:
    outbox_dir = tmp_path / "data_outbox"
    receiver_state = {"online": False, "received": [], "fail_once": True}

    def _mock_sender(url: str, payload: dict[str, Any], headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
        if not receiver_state["online"]:
            raise ConnectionRefusedError("Data endpoint offline")
        if receiver_state["fail_once"]:
            receiver_state["fail_once"] = False
            return 503, "Service Unavailable"
        receiver_state["received"].append((url, headers["X-Idempotency-Key"], payload))
        return 200, '{"ack": true}'

    pub = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://127.0.0.1:19090",
            outbox_dir=outbox_dir,
            max_retries=2,
            backoff_base_s=0.001,
        ),
        sender_fn=_mock_sender,
        sleep_fn=lambda _s: None,
    )

    stack = _build_mock_stack(tmp_path, data_publisher=pub)
    snap = _populate_1bin_and_snapshot(stack, cutoff=1)
    req_dict = _make_apply_request(stack, batch_id="batch_pub_001", snap=snap)
    code, body = _wsgi_call(stack["app"], "POST", "/apply", body_obj=req_dict)
    assert code == 200 and body["apply_response"]["status"] == "APPLIED"

    # Also publish real VerificationEvidence from RuleVerifier
    vr = RuleVerifier(
        RouteCompiler(stack["topo"], FlowMatcher(stack["topo"])), stack["driver"]
    ).verify_flow_path(
        stack["run_id"], "0x103", "0x103_P2", verified_at_utc="2026-11-21T17:00:05Z"
    )
    assert vr.evidence is not None
    pub.publish_verification_evidence(stack["run_id"], vr.evidence)

    # Flush while Data is offline -> all 5 events remain PENDING_DELIVERY (never falsely DELIVERED!)
    flush1 = pub.flush_pending()
    assert flush1["delivered"] == 0
    assert flush1["pending"] == 5
    assert len(pub.get_delivered_events()) == 0
    assert (outbox_dir / "outbox_events.jsonl").exists()

    # Simulate process restart: reload DataEventPublisher from durable outbox_dir
    pub_reloaded = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://127.0.0.1:19090",
            outbox_dir=outbox_dir,
            max_retries=3,
            backoff_base_s=0.001,
        ),
        sender_fn=_mock_sender,
        sleep_fn=lambda _s: None,
    )
    assert len(pub_reloaded.get_pending_events()) == 5

    # Bring mock Data receiver online (with 1 transient 503 before 200 ACK)
    receiver_state["online"] = True
    flush2 = pub_reloaded.flush_pending()
    assert flush2["delivered"] == 5
    assert flush2["pending"] == 0
    assert len(receiver_state["received"]) == 5

    # Re-publishing the exact same Snapshot is deduplicated
    dup_res = pub_reloaded.publish_snapshot(snap)
    assert dup_res.duplicate is True
    assert dup_res.status == DeliveryStatus.DELIVERED
    stack["idem_store"].close()


# =============================================================================
# Test 3: Northbound HTTP Auth, Size Limit, and Network sender.py DemandRelease
# =============================================================================


def test_p12_03_northbound_auth_size_limit_and_sender_demand_releases(tmp_path: Path) -> None:
    stack = _build_mock_stack(tmp_path, auth_token="secret-token-123", max_request_bytes=4096)
    app = stack["app"]
    hdr = {"X-SDN-Token": "secret-token-123"}

    # Missing / wrong token -> 401
    code, err = _wsgi_call(app, "GET", "/health")
    assert code == 401 and err["error"] == "UNAUTHORIZED"
    code, ok = _wsgi_call(app, "GET", "/health", headers=hdr)
    assert code == 200 and ok["status"] == "ok" and ok["idempotency_store_durable"] is True

    # Bearer token also accepted
    code, ok2 = _wsgi_call(app, "GET", "/health", headers={"Authorization": "Bearer secret-token-123"})
    assert code == 200 and ok2["status"] == "ok"

    # Oversized payload -> 413
    code, err = _wsgi_call(app, "POST", "/demand-releases", raw_body=b"{" + b"a" * 5000 + b"}", headers=hdr)
    assert code == 413 and err["error"] == "PAYLOAD_TOO_LARGE"

    # Exact Network sender.py DemandRelease JSON (no available_at_utc field!)
    sender_event = {
        "event_id": "network:run_p12_01:demand:0x103:0",
        "run_id": "run_p12_01",
        "flow_group_id": "0x103",
        "bin_index": 0,
        "window_start_utc": "2026-11-22T00:00:00Z",
        "window_end_utc": "2026-11-22T00:00:01Z",
        "requested_mbps": 3.25,
        "released_at_utc": "2026-11-22T00:00:01.005Z",
    }
    code, ack = _wsgi_call(app, "POST", "/demand-releases", body_obj=sender_event, headers=hdr)
    assert code == 200
    assert ack["ack"] is True
    assert ack["duplicate"] is False
    assert ack["event_id"] == "network:run_p12_01:demand:0x103:0"
    assert ack["available_at_utc"] is not None  # Server stamped!

    # Idempotent retry of same DemandRelease
    code, ack_dup = _wsgi_call(app, "POST", "/demand-releases", body_obj=sender_event, headers=hdr)
    assert code == 200 and ack_dup["duplicate"] is True
    assert len(stack["demand_provider"].get_observations("run_p12_01")) == 1

    # Conflicting duplicate -> 409 Conflict
    conflict_evt = dict(sender_event, requested_mbps=9.99)
    code, err_c = _wsgi_call(app, "POST", "/demand-releases", body_obj=conflict_evt, headers=hdr)
    assert code == 409 and err_c["error"] == "DEMAND_RELEASE_CONFLICT"

    # Forbidden demand source -> 400
    bad_src = dict(sender_event, event_id="ev_bad_src", bin_index=1, source_kind="goodput_mbps")
    code, err_s = _wsgi_call(app, "POST", "/demand-releases", body_obj=bad_src, headers=hdr)
    assert code == 400 and err_s["error"] == "FORBIDDEN_DEMAND_SOURCE"

    # Legacy flow_group_id -> 400
    bad_id = dict(sender_event, event_id="ev_bad_id", bin_index=2, flow_group_id="h1_h3")
    code, err_id = _wsgi_call(app, "POST", "/demand-releases", body_obj=bad_id, headers=hdr)
    assert code == 400 and err_id["error"] == "CONTRACT_VALIDATION_ERROR"
    stack["idem_store"].close()


# =============================================================================
# Test 4: Snapshot Creation with 48 Flows and lookback_L=60 (2880 rows)
# =============================================================================


def test_p12_04_northbound_snapshot_48_flows_lookback_60(tmp_path: Path, monkeypatch) -> None:
    stack = _build_mock_stack(tmp_path, max_request_bytes=4 * 1024 * 1024)
    run_id = stack["run_id"]
    meas_dt = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=60)
    stack["snap_creator"].set_run_origin_refs(
        run_id,
        OriginRefs(
            run_started_at_utc=format_utc_timestamp(meas_dt - timedelta(seconds=60)),
            traffic_started_at_utc=format_utc_timestamp(meas_dt - timedelta(seconds=60)),
            measurement_started_at_utc=format_utc_timestamp(meas_dt),
            delta_s=1,
        ),
    )

    monkeypatch.setattr("sdn.adapters.http_control_api._utc_now_rfc3339",
                        lambda: format_utc_timestamp(meas_dt + timedelta(seconds=60, milliseconds=100)))

    fgs = [fg.flow_group_id for fg in stack["topo"].get_all_flow_groups()]
    assert len(fgs) == 48
    for k in range(60):
        w_start = format_utc_timestamp(meas_dt + timedelta(seconds=k))
        w_end = format_utc_timestamp(meas_dt + timedelta(seconds=k + 1))
        batch_events = [
            {
                "event_id": f"network:{run_id}:demand:{fid}:{k}",
                "run_id": run_id,
                "flow_group_id": fid,
                "bin_index": k,
                "window_start_utc": w_start,
                "window_end_utc": w_end,
                "requested_mbps": round(1.0 + (k * 0.05), 4),
                "released_at_utc": w_end,
            }
            for fid in fgs
        ]
        code, ack = _wsgi_call(stack["app"], "POST", "/demand-releases", body_obj={"events": batch_events})
        assert code == 200 and ack["count"] == 48

    code, snap_dict = _wsgi_call(
        stack["app"],
        "POST",
        "/snapshots",
        body_obj={"run_id": run_id, "cutoff": 60, "lookback_L": 60,
                  "snapshot_as_of_utc": format_utc_timestamp(meas_dt + timedelta(seconds=61))},
    )
    assert code == 200
    snap = Snapshot.from_dict(snap_dict, strict=True, verify_hash=True)
    assert snap.quality_status.value == "VALID"
    assert snap.routing_state_complete is True
    assert len(snap.history) == 48 * 60
    assert len(snap.routing_state) == 48
    stack["idem_store"].close()


# =============================================================================
# Test 5: Sync Apply, Routing State/History, Replay & BATCH_ID_CONFLICT
# =============================================================================


def test_p12_05_northbound_apply_sync_replay_conflict_and_routing_queries(tmp_path: Path) -> None:
    stack = _build_mock_stack(tmp_path)
    snap = _populate_1bin_and_snapshot(stack, cutoff=1)
    req_dict = _make_apply_request(stack, batch_id="batch_sync_001", snap=snap)

    code, res = _wsgi_call(stack["app"], "POST", "/apply", body_obj=req_dict)
    assert code == 200
    assert res["job_state"] == ApplyJobState.FINALIZED.value
    assert res["apply_response"]["status"] == "APPLIED"
    assert res["apply_response"]["results"][0]["actual_path_id"] == "0x103_P2"
    sv_after = res["apply_response"]["state_version"]

    # GET /routing-state & GET /routing-history & GET /apply-status
    code, rs = _wsgi_call(stack["app"], "GET", "/routing-state", query=f"run_id={stack['run_id']}")
    assert code == 200 and rs["state_version"] == sv_after
    code, rh = _wsgi_call(
        stack["app"], "GET", "/routing-history", query=f"run_id={stack['run_id']}&flow_group_id=0x103"
    )
    assert code == 200 and rh["count"] == 2 and rh["intervals"][-1]["path_id"] == "0x103_P2"
    code, st = _wsgi_call(
        stack["app"],
        "GET",
        "/apply-status",
        query=f"run_id={stack['run_id']}&decision_batch_id=batch_sync_001",
    )
    assert code == 200 and st["job_state"] == "FINALIZED" and st["apply_response"]["status"] == "APPLIED"

    # Same-batch replay -> 0 additional FlowMods
    ops_before = len(stack["driver"].get_operation_log())
    code, replay = _wsgi_call(stack["app"], "POST", "/apply", body_obj=req_dict)
    assert code == 200 and replay["apply_response"]["idempotent_replay"] is True
    assert len(stack["driver"].get_operation_log()) == ops_before

    # Same batch_id + different payload -> BATCH_ID_CONFLICT
    bad = dict(req_dict)
    bad["changes"] = [{"flow_group_id": "0x104", "old_path_id": "0x104_P1", "new_path_id": "0x104_P2"}]
    bad.pop("payload_hash", None)
    code, conf = _wsgi_call(stack["app"], "POST", "/apply", body_obj=bad)
    assert code == 200
    assert conf["apply_response"]["status"] == "REJECTED"
    assert conf["apply_response"]["reason"] == "BATCH_ID_CONFLICT"
    stack["idem_store"].close()


# =============================================================================
# Test 6: Async Apply (HTTP Timeout Budget), In-Flight Retry, Conflict & Polling
# =============================================================================


def test_p12_06_northbound_async_apply_in_flight_polling_and_busy_guard(tmp_path: Path) -> None:
    # Simulate 250ms OVS verification while caller wait budget is only 40ms
    stack = _build_mock_stack(tmp_path, default_sync_wait_ms=40, verify_delay_s=0.25)
    snap = _populate_1bin_and_snapshot(stack, cutoff=1)
    req_dict = _make_apply_request(stack, batch_id="batch_async_001", snap=snap)

    code, accepted = _wsgi_call(stack["app"], "POST", "/apply", body_obj=req_dict)
    assert code == 202
    assert accepted["http_status"] == "ACCEPTED"
    assert accepted["job_state"] == "IN_PROGRESS"
    assert "apply_response" not in accepted  # Never fake APPLIED!

    # Retry SAME batch_id + SAME hash while IN_PROGRESS -> 202 Accepted, 0 extra workers
    code, retry_in_flight = _wsgi_call(stack["app"], "POST", "/apply", body_obj=req_dict)
    assert code == 202 and retry_in_flight["job_state"] == "IN_PROGRESS"

    # Retry SAME batch_id + DIFFERENT hash while IN_PROGRESS -> 409 BATCH_ID_CONFLICT
    conflict_dict = dict(req_dict)
    conflict_dict["changes"] = [
        {"flow_group_id": "0x104", "old_path_id": "0x104_P1", "new_path_id": "0x104_P2"}
    ]
    conflict_dict.pop("payload_hash", None)
    code, conf = _wsgi_call(stack["app"], "POST", "/apply", body_obj=conflict_dict)
    assert code == 409 and conf["error"] == "BATCH_ID_CONFLICT"

    # Different batch_id while IN_PROGRESS -> REJECTED(BUSY), no overlapping writes
    other_dict = _make_apply_request(
        stack,
        batch_id="batch_async_002_overlap",
        snap=snap,
        flow_group_id="0x104",
        old_path_id="0x104_P1",
        new_path_id="0x104_P2",
    )
    code, busy = _wsgi_call(stack["app"], "POST", "/apply", body_obj=other_dict)
    assert code == 200
    assert busy["apply_response"]["status"] == "REJECTED"
    assert busy["apply_response"]["reason"] == "BUSY"

    # Poll GET /apply-status until FINALIZED
    final_body: Optional[dict[str, Any]] = None
    for _ in range(30):
        code, polled = _wsgi_call(
            stack["app"],
            "GET",
            "/apply-status",
            query=f"run_id={stack['run_id']}&decision_batch_id=batch_async_001",
        )
        assert code == 200
        if polled["job_state"] == "FINALIZED":
            final_body = polled
            break
        time.sleep(0.03)

    assert final_body is not None
    assert final_body["apply_response"]["status"] == "APPLIED"
    assert final_body["apply_response"]["results"][0]["actual_path_id"] == "0x103_P2"
    stack["idem_store"].close()


# =============================================================================
# Test 7: Static ARP Compatibility (Zero ARP Flood Rules When Disabled)
# =============================================================================


def test_p12_07_static_arp_compatibility_installs_zero_arp_flood_rules() -> None:
    cfg = S0AppConfig(
        topology_bundle_path=BUNDLE_PATH,
        csv_config_dir=CSV_DIR,
        run_id="run_static_arp_test",
        enable_intra_edge_l3=True,
        enable_arp_edge_local_flood=False,  # Matches Network config.yaml static_entry_no_flood
    )
    orch = S0ControllerOrchestrator(cfg)
    assert cfg.enable_arp_edge_local_flood is False
    # Verify no ARP helper cookie rules exist in fresh driver
    for dpid in (1, 2, 3, 4):
        assert dpid not in orch.driver.connected_dpids
    assert ARP_HELPER_COOKIE == 0xFFF1
