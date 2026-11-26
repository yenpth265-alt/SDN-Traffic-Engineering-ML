"""
SDN Phase 8 Runtime 48-Flow Audit, B=0/REJECTED/UNKNOWN & Durable Data Sync Gate Tests (Tasks C, D, E).

Tests the REAL runtime call chain:
    `SDNNorthboundHTTPApp._handle_apply()` -> `_publish_after_apply()` ->
    `DataEventPublisher.publish_routing_state()` + `publish_apply_response_v1_runtime()` ->
    `GET /health` & `GET /routing-state` (`data_sync_gate`).

Covers all 16 Task E scenarios plus B=0 no-op audit and KEPT `last_successful_change_at` preservation.
Classification: `PASS_CONTRACT_MOCK` (pure-Python, no live PostgreSQL).
"""

from __future__ import annotations

import io
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from sdn.adapters.data_api_v1_adapter import (
    DATA_V1_ENDPOINT_APPLY_RESULTS,
    DATA_V1_ENDPOINT_ROUTING,
    DataV1ContractBlockedError,
    DataV1MappingError,
    build_noop_48_flow_apply_result_data_v1,
)
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
from sdn.controller.decision_handler import DecisionHandler, DecisionHandlerConfig
from sdn.controller.state_manager import StateManager
from sdn.controller.topology_registry import TopologyRegistry
from sdn.flow_management.flow_matcher import FlowMatcher
from sdn.flow_management.route_compiler import RouteCompiler
from sdn.flow_management.rule_installer import RuleInstaller
from sdn.flow_management.switch_driver import InMemorySwitchDriver
from sdn.monitoring.snapshot_creator import SnapshotCreator
from sdn.route_verification.rule_verifier import RuleVerifier
from sdn.schemas.apply_models import ApplyFlowResult, ApplyResponse
from sdn.schemas.snapshot_models import DemandObservationRecord, OriginRefs
from sdn.schemas.state_models import (
    BatchStatus,
    FlowDecisionStatus,
    VerificationStatus,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TOPOLOGY_BUNDLE_PATH = REPO_ROOT / "configs" / "network" / "topology_bundle.json"
CATALOG_DIR = REPO_ROOT / "configs" / "network"


def _fmt_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _wsgi_call(
    app: SDNNorthboundHTTPApp,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    query: str = "",
) -> tuple[int, dict[str, Any]]:
    raw = json.dumps(body).encode("utf-8") if body is not None else b""
    env = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "QUERY_STRING": query,
        "CONTENT_LENGTH": str(len(raw)),
        "wsgi.input": io.BytesIO(raw),
        "HTTP_X_SDN_TOKEN": "tok-p8",
    }
    out: dict[str, Any] = {}

    def _start(status: str, headers: list[tuple[str, str]]) -> None:
        out["code"] = int(status.split(" ", 1)[0])

    chunks = app(env, _start)
    return out["code"], json.loads(b"".join(chunks).decode("utf-8"))


def _make_decision_rows(
    topo: TopologyRegistry, changes: dict[str, tuple[str, str]] | None = None
) -> list[dict[str, Any]]:
    changes = changes or {}
    return [
        {
            "flow_group_id": fg.flow_group_id,
            "old_path_id": changes.get(fg.flow_group_id, (f"{fg.flow_group_id}_P1", f"{fg.flow_group_id}_P1"))[0],
            "proposed_path_id": changes.get(fg.flow_group_id, (f"{fg.flow_group_id}_P1", f"{fg.flow_group_id}_P1"))[1],
        }
        for fg in topo.get_all_flow_groups()
    ]


def _setup_runtime(
    tmp_path: Path,
    run_id: str,
    *,
    sender_fn,
    decision_lookup_fn,
    outbox_subdir: str = "outbox",
):
    topo = TopologyRegistry.from_bundle_file(TOPOLOGY_BUNDLE_PATH, csv_config_dir=CATALOG_DIR)
    driver = InMemorySwitchDriver(topo)
    comp = RouteCompiler(topo, FlowMatcher(topo))
    installer = RuleInstaller(comp, driver, RuleVerifier(comp, driver))
    sm = StateManager(topo)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    boot_ts = _fmt_utc(now - timedelta(seconds=120))
    evs = installer.bootstrap_initial_paths(run_id, boot_ts)
    sm.initialize_run(run_id, boot_ts, verification_evidences=evs)

    dp = HTTPDemandReleaseProvider()
    sc = SnapshotCreator(topo, sm, dp)
    dh = DecisionHandler(topo, sm, installer, config=DecisionHandlerConfig.for_integration())

    # Register a valid fresh snapshot right before now (cutoff == now so never hits 2s stale boundary)
    meas_origin = now - timedelta(seconds=1)
    traffic_origin = meas_origin - timedelta(seconds=60)
    origins = OriginRefs(
        run_started_at_utc=_fmt_utc(traffic_origin),
        traffic_started_at_utc=_fmt_utc(traffic_origin),
        measurement_started_at_utc=_fmt_utc(meas_origin),
        warmup_s=60,
        delta_s=1,
    )
    sc.set_run_origin_refs(run_id, origins)
    for fg in topo.get_all_flow_groups():
        dp.ingest_release_payload(
            {
                "event_id": f"p8:{run_id}:{fg.flow_group_id}:0",
                "run_id": run_id,
                "flow_group_id": fg.flow_group_id,
                "bin_index": 0,
                "window_start_utc": _fmt_utc(meas_origin),
                "window_end_utc": _fmt_utc(meas_origin + timedelta(seconds=1)),
                "requested_mbps": 2.0,
                "released_at_utc": _fmt_utc(meas_origin + timedelta(seconds=1)),
            },
            server_receipt_utc=_fmt_utc(meas_origin + timedelta(seconds=1)),
        )
    snap = sc.create_snapshot(
        run_id,
        cutoff=1,
        lookback_L=1,
        snapshot_as_of_utc=_fmt_utc(meas_origin + timedelta(seconds=1)),
        origin_refs=origins,
    )
    dh.register_snapshot(snap)

    pub = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://data.local:8000",
            outbox_dir=tmp_path / outbox_subdir,
            api_mode="data_v1",
            api_key="p8-sdn-key",
            max_retries=1,
            auto_flush=False,
        ),
        sender_fn=sender_fn,
        decision_lookup_fn=decision_lookup_fn,
    )
    app = SDNNorthboundHTTPApp(
        state_manager=sm,
        decision_handler=dh,
        snapshot_creator=sc,
        demand_provider=dp,
        data_publisher=pub,
        config=NorthboundAPIConfig(auth_token="tok-p8", default_run_id=run_id, default_sync_wait_ms=2000),
    )
    return topo, sm, dh, pub, app, snap, now


def test_p8_runtime_b1_b4_causal_order_and_sync_gate_lifecycle(tmp_path: Path) -> None:
    """
    Scenarios 1, 2, 4, 12:
    - Real POST /apply (B=1 and B=4) -> _publish_after_apply() -> RoutingState ACK BEFORE 48-flow ApplyResult.
    - Unchanged KEPT flows preserve original `last_successful_change_at`.
    - `data_sync_gate` stays blocked while pending, turns eligible ONLY after both RoutingState and ApplyResult ACK.
    - Duplicate ApplyRequest with same decision_batch_id produces zero duplicate Data sends.
    """
    run_id = "run_p8_b1_b4"
    delivered: list[dict[str, Any]] = []
    decision_store: dict[str, list[dict[str, Any]]] = {}

    def _sender(url: str, payload: dict[str, Any], headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
        delivered.append({"url": url, "payload": payload})
        if url.endswith(DATA_V1_ENDPOINT_APPLY_RESULTS):
            assert any(d["url"].endswith(DATA_V1_ENDPOINT_ROUTING) for d in delivered), (
                "ApplyResult MUST NOT be sent before RoutingState ACK!"
            )
            return 200, '{"updated": 1}'
        return 200, '{"inserted": 1}'

    def _lookup(rid: str, bid: str) -> list[dict[str, Any]]:
        if bid not in decision_store:
            raise DataV1ContractBlockedError(f"404 Unknown decision_batch_id {bid}")
        return decision_store[bid]

    topo, sm, dh, pub, app, snap, now = _setup_runtime(
        tmp_path, run_id, sender_fn=_sender, decision_lookup_fn=_lookup
    )
    orig_rs = sm.get_routing_state(run_id)
    orig_104_last_change = next(e.last_successful_change_at for e in orig_rs.routing_state if e.flow_group_id == "0x104")

    # Scenario 1: B=1 changed flow (0x103: P1 -> P2)
    decision_store["batch_b1"] = _make_decision_rows(topo, {"0x103": ("0x103_P1", "0x103_P2")})
    req_b1 = {
        "run_id": run_id,
        "decision_batch_id": "batch_b1",
        "snapshot_id": snap.snapshot_id,
        "expected_state_version": orig_rs.state_version,
        "config_version": "1.0",
        "topology_version": topo.topology_version,
        "topology_hash": topo.topology_hash,
        "forecast_start": _fmt_utc(now),
        "forecast_end": _fmt_utc(now + timedelta(seconds=20)),
        "apply_deadline_utc": _fmt_utc(now + timedelta(seconds=15)),
        "changes": [{"flow_group_id": "0x103", "old_path_id": "0x103_P1", "new_path_id": "0x103_P2"}],
    }
    code, body = _wsgi_call(app, "POST", "/apply", body=req_b1)
    assert code == 200
    assert body["apply_response"]["status"] == "APPLIED"
    new_sv = body["apply_response"]["state_version"]
    assert new_sv == orig_rs.state_version + 1

    # Before Data flush: OVS is VERIFIED, but data_sync_gate MUST block new optimization ticks!
    _, h_before = _wsgi_call(app, "GET", "/health")
    gate_before = h_before["data_sync_gate"]
    assert gate_before["ovs_verified"] is True
    assert gate_before["data_sync_pending"] is True
    assert gate_before["optimization_tick_eligible"] is False
    assert gate_before["optimization_tick_block_reason"] == "DATA_ROUTING_SYNC_PENDING"

    # Flush -> MUST deliver RoutingState FIRST, then 48-flow ApplyResult!
    pub.flush_pending()
    paths = [d["url"].split(":8000", 1)[1] for d in delivered]
    assert paths == [DATA_V1_ENDPOINT_ROUTING, DATA_V1_ENDPOINT_APPLY_RESULTS]
    app_payload = delivered[1]["payload"]
    assert len(app_payload["rows"]) == 48
    assert sum(1 for r in app_payload["rows"] if r["status"] == "APPLIED") == 1
    assert sum(1 for r in app_payload["rows"] if r["status"] == "KEPT") == 47
    assert app_payload["response_state_version"] == new_sv

    # KEPT flow 0x104 must NOT have its last_successful_change_at overwritten
    rs_after = sm.get_routing_state(run_id)
    assert next(e.last_successful_change_at for e in rs_after.routing_state if e.flow_group_id == "0x104") == orig_104_last_change

    # After both ACKs: data_sync_gate turns eligible!
    _, h_after = _wsgi_call(app, "GET", "/health")
    gate_after = h_after["data_sync_gate"]
    assert gate_after["data_acknowledged_state_version"] == new_sv
    assert gate_after["data_sync_pending"] is False
    assert gate_after["optimization_tick_eligible"] is True
    assert gate_after["optimization_tick_block_reason"] is None

    # /routing-state also exposes data_sync_gate in data_v1 mode
    _, rs_http = _wsgi_call(app, "GET", "/routing-state")
    assert rs_http["data_sync_gate"]["optimization_tick_eligible"] is True

    # Scenario 12: Duplicate ApplyRequest -> idempotent replay, ZERO extra Data deliveries!
    c_dup, b_dup = _wsgi_call(app, "POST", "/apply", body=req_b1)
    assert c_dup == 200
    assert b_dup["apply_response"]["idempotent_replay"] is True
    pub.flush_pending()
    assert len(delivered) == 2


def test_p8_outage_timeout_after_commit_restart_and_conflict_reconciliation(tmp_path: Path) -> None:
    """
    Scenarios 5, 6, 7, 8, 9, 10, 11, 13:
    - Missing DecisionBatch (404) / Data 503 outage -> OVS stays VERIFIED, audit durably parked in
      `pending_apply_audits.jsonl`, NEVER fabricates 48 rows, survives controller restart!
    - Timeout after DB commit -> idempotent retry `200 {"inserted": 0}` unblocks ApplyResult.
    - Controller restart after RoutingState ACK but before ApplyResult ACK -> `apply_audit_pending=True`,
      `optimization_tick_eligible=False` (no false positive!).
    - Permanent 409 conflict on RoutingState -> `data_reconcile_required=True`, `reconcile_required=True`,
      dependent ApplyResult NEVER sent!
    """
    run_id = "run_p8_resilience"
    state = {"down": True, "timeout_once": True, "decision_ready": False, "route_409": False}
    decision_rows_store: dict[str, list[dict[str, Any]]] = {}
    delivered: list[str] = []

    def _sender(url: str, payload: dict[str, Any], headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
        if state["route_409"] and url.endswith(DATA_V1_ENDPOINT_ROUTING):
            return 409, '{"detail":"Routing version conflict"}'
        if state["down"]:
            return 503, '{"detail":"db down"}'
        if url.endswith(DATA_V1_ENDPOINT_ROUTING) and state["timeout_once"]:
            state["timeout_once"] = False
            raise TimeoutError("timed out after PG commit")
        delivered.append(url.split(":8000", 1)[1])
        return (200, '{"inserted": 0}') if url.endswith(DATA_V1_ENDPOINT_ROUTING) else (200, '{"updated": 1}')

    def _lookup(rid: str, bid: str) -> list[dict[str, Any]]:
        if state["down"] or not state["decision_ready"] or bid not in decision_rows_store:
            raise DataV1ContractBlockedError(f"DecisionBatch {bid} unavailable")
        return decision_rows_store[bid]

    topo, sm, dh, pub1, app1, snap, now = _setup_runtime(
        tmp_path, run_id, sender_fn=_sender, decision_lookup_fn=_lookup, outbox_subdir="ob_restart"
    )
    decision_rows_store["batch_r1"] = _make_decision_rows(topo, {"0x103": ("0x103_P1", "0x103_P2")})
    req = {
        "run_id": run_id,
        "decision_batch_id": "batch_r1",
        "snapshot_id": snap.snapshot_id,
        "expected_state_version": sm.get_state_version(run_id),
        "config_version": "1.0",
        "topology_version": topo.topology_version,
        "topology_hash": topo.topology_hash,
        "forecast_start": _fmt_utc(now),
        "forecast_end": _fmt_utc(now + timedelta(seconds=20)),
        "apply_deadline_utc": _fmt_utc(now + timedelta(seconds=15)),
        "changes": [{"flow_group_id": "0x103", "old_path_id": "0x103_P1", "new_path_id": "0x103_P2"}],
    }
    # Apply succeeds on OVS EVEN WHILE Data is 503 down and DecisionBatch lookup fails!
    code, body = _wsgi_call(app1, "POST", "/apply", body=req)
    assert code == 200
    assert body["apply_response"]["status"] == "APPLIED"
    sv_after = body["apply_response"]["state_version"]
    assert delivered == []

    # Simulate controller restart #1 BEFORE any Data ACK -> reload from disk!
    pub2 = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://data.local:8000",
            outbox_dir=tmp_path / "ob_restart",
            api_mode="data_v1",
            api_key="p8-sdn-key",
            max_retries=1,
            auto_flush=False,
        ),
        sender_fn=_sender,
        decision_lookup_fn=_lookup,
    )
    gate_after_restart1 = pub2.sync_gate_status(run_id, current_state_version=sv_after, ovs_verified=True)
    assert gate_after_restart1["optimization_tick_eligible"] is False
    assert gate_after_restart1["routing_sync_pending"] is True
    assert gate_after_restart1["apply_audit_pending"] is True

    # Bring Data up, but DecisionBatch is STILL missing (404) -> RoutingState times out once,
    # then retries with idempotent 200 {"inserted": 0} and is ACKed, while ApplyResult stays parked!
    state["down"] = False
    pub2.flush_pending()  # Attempt 1: RoutingState times out after DB commit
    assert delivered == []
    pub2.flush_pending()  # Attempt 2: RoutingState gets idempotent 200 {"inserted": 0}!
    assert delivered == [DATA_V1_ENDPOINT_ROUTING]

    # Simulate controller restart #2 AFTER RoutingState ACK, BEFORE ApplyResult ACK!
    pub3 = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://data.local:8000",
            outbox_dir=tmp_path / "ob_restart",
            api_mode="data_v1",
            api_key="p8-sdn-key",
            max_retries=1,
            auto_flush=False,
        ),
        sender_fn=_sender,
        decision_lookup_fn=_lookup,
    )
    gate_after_restart2 = pub3.sync_gate_status(run_id, current_state_version=sv_after, ovs_verified=True)
    assert gate_after_restart2["data_acknowledged_state_version"] == sv_after
    assert gate_after_restart2["routing_sync_pending"] is False
    assert gate_after_restart2["apply_audit_pending"] is True
    assert gate_after_restart2["optimization_tick_eligible"] is False
    assert gate_after_restart2["optimization_tick_block_reason"] == "DATA_APPLY_AUDIT_PENDING"

    # Now DecisionBatch becomes available -> flush resolves durable pending audit & delivers 48-flow ApplyResult!
    state["decision_ready"] = True
    pub3.flush_pending()
    assert delivered == [DATA_V1_ENDPOINT_ROUTING, DATA_V1_ENDPOINT_APPLY_RESULTS]
    gate_final = pub3.sync_gate_status(run_id, current_state_version=sv_after, ovs_verified=True)
    assert gate_final["optimization_tick_eligible"] is True

    # Scenario 8: Permanent 409 conflict on RoutingState -> data_reconcile_required=True!
    state["route_409"] = True
    pub_409 = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://data.local:8000",
            outbox_dir=tmp_path / "ob_409",
            api_mode="data_v1",
            api_key="p8-sdn-key",
            max_retries=1,
        ),
        sender_fn=_sender,
        decision_lookup_fn=_lookup,
    )
    pub_409.publish_routing_state(sm.get_routing_state(run_id), deliver_now=True)
    gate_409 = pub_409.sync_gate_status(run_id, current_state_version=sv_after, ovs_verified=True)
    assert gate_409["routing_conflict_detected"] is True
    assert gate_409["data_reconcile_required"] is True
    assert gate_409["reconcile_required"] is True
    assert gate_409["optimization_tick_eligible"] is False


def test_p8_b0_noop_audit_and_empty_apply_request_rejected(tmp_path: Path) -> None:
    """
    Scenario 3 (Task C B=0):
    - Empty ApplyRequest (`changes: []`) is still rejected by SDN `/apply` (core schema untouched).
    - `build_noop_48_flow_apply_result_data_v1()` produces 48 verified KEPT rows without fabricating.
    - Rejects if any decision row actually proposes a change (`proposed_path_id != old_path_id`).
    """
    run_id = "run_p8_b0"
    topo, sm, dh, pub, app, snap, now = _setup_runtime(
        tmp_path,
        run_id,
        sender_fn=lambda u, p, h, t: (200, '{"inserted":1}'),
        decision_lookup_fn=lambda r, b: [],
        outbox_subdir="ob_b0",
    )
    rs = sm.get_routing_state(run_id)

    # Empty ApplyRequest MUST still be rejected with 400 (core schema untouched)
    code_empty, _ = _wsgi_call(
        app,
        "POST",
        "/apply",
        body={
            "run_id": run_id,
            "decision_batch_id": "batch_empty",
            "snapshot_id": snap.snapshot_id,
            "expected_state_version": rs.state_version,
            "config_version": "1.0",
            "topology_version": topo.topology_version,
            "topology_hash": topo.topology_hash,
            "forecast_start": _fmt_utc(now),
            "forecast_end": _fmt_utc(now + timedelta(seconds=20)),
            "apply_deadline_utc": _fmt_utc(now + timedelta(seconds=15)),
            "changes": [],
        },
    )
    assert code_empty == 400

    # Grounded B=0 no-op audit helper -> 48 KEPT rows
    dec_b0 = _make_decision_rows(topo)
    audit_b0 = build_noop_48_flow_apply_result_data_v1(
        run_id=run_id,
        decision_batch_id="batch_noop_0",
        decision_rows=dec_b0,
        routing_state=rs,
    )
    assert audit_b0["response_status"] == "APPLIED"
    assert len(audit_b0["rows"]) == 48
    assert all(r["status"] == "KEPT" for r in audit_b0["rows"])

    # Rejects if DecisionBatch actually proposed a route change
    dec_changed = _make_decision_rows(topo, {"0x103": ("0x103_P1", "0x103_P2")})
    with pytest.raises(DataV1MappingError):
        build_noop_48_flow_apply_result_data_v1(
            run_id=run_id,
            decision_batch_id="batch_not_noop",
            decision_rows=dec_changed,
            routing_state=rs,
        )


def test_p9_corrupted_durable_jsonl_fail_closed_and_bounded_404_retry(tmp_path: Path) -> None:
    """
    Phase 9 Task 2G & 2I regression:
    - Corrupted/truncated durable JSONL line in `pending_apply_audits.jsonl` MUST fail closed:
      `durable_corruption_detected=True`, `reconcile_required=True`, `optimization_tick_eligible=False`.
    - Repeated `DecisionBatch 404` lookup stops retrying after `max_decision_lookup_404_retries`
      while preserving the durable pending audit record (never silently discarded!).
    """
    run_id = "run_p9_durability"
    # 1. Corrupted JSONL file -> fail closed on reload (zero false-positive tick eligibility!)
    corrupt_dir = tmp_path / "corrupt_ob"
    corrupt_dir.mkdir(parents=True, exist_ok=True)
    (corrupt_dir / "pending_apply_audits.jsonl").write_text('{"dedup_key": "trunc\n', encoding="utf-8")
    pub_corrupt = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://data.local:8000",
            outbox_dir=corrupt_dir,
            api_mode="data_v1",
            api_key="k",
        )
    )
    gate_corrupt = pub_corrupt.sync_gate_status(run_id, current_state_version=1, ovs_verified=True)
    assert gate_corrupt["durable_corruption_detected"] is True
    assert gate_corrupt["reconcile_required"] is True
    assert gate_corrupt["optimization_tick_eligible"] is False
    assert gate_corrupt["optimization_tick_block_reason"] == "DURABLE_OUTBOX_CORRUPTION_RECONCILIATION_REQUIRED"

    # 2. Bounded 404 retry policy for missing DecisionBatch (does not silently discard audit!)
    lookup_calls = {"n": 0}

    def _always_404(rid: str, bid: str) -> list[dict[str, Any]]:
        lookup_calls["n"] += 1
        raise DataV1ContractBlockedError(f"CONTRACT_BLOCKED: DecisionBatch lookup '{bid}' returned HTTP 404")

    topo, sm, _, _, _, _, now = _setup_runtime(
        tmp_path,
        run_id,
        sender_fn=lambda u, p, h, t: (200, '{"inserted": 1}'),
        decision_lookup_fn=_always_404,
        outbox_subdir="ob_404_unused",
    )
    rs = sm.get_routing_state(run_id)
    pub_404 = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://data.local:8000",
            outbox_dir=tmp_path / "ob_404",
            api_mode="data_v1",
            api_key="k",
            max_retries=1,
            max_decision_lookup_404_retries=2,
        ),
        sender_fn=lambda u, p, h, t: (200, '{"inserted": 1}'),
        decision_lookup_fn=_always_404,
    )
    pub_404.publish_routing_state(rs, deliver_now=True)
    resp = ApplyResponse(
        run_id=run_id,
        decision_batch_id="missing_batch_404",
        status=BatchStatus.APPLIED,
        state_version=rs.state_version,
        completed_at=_fmt_utc(now),
        results=(),
        rule_count=0,
    )
    pub_404.publish_apply_response_v1_runtime(resp, post_apply_routing_state=rs, deliver_now=False)
    assert lookup_calls["n"] == 1
    # Flush 4 more times -> MUST stop at 2 total 404 lookups (bounded!), never discard pending audit!
    for _ in range(4):
        pub_404.flush_pending()
    assert lookup_calls["n"] == 2
    gate_404 = pub_404.sync_gate_status(run_id, current_state_version=rs.state_version, ovs_verified=True)
    assert gate_404["apply_audit_pending"] is True
    assert "missing_batch_404" in gate_404["pending_apply_audit_batches"]
    assert gate_404["optimization_tick_eligible"] is False
    assert "DECISION_BATCH_404_RETRIES_EXHAUSTED" in (pub_404.status_summary()["last_error"] or "")

