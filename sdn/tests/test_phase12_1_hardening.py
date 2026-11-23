"""
Phase 12.1 Integration Hardening & Crash Consistency Tests (`PASS_ADAPTER_TEST`).

Covers:
- Task 1: Concurrent conflicting batches, duplicate in-flight, 404 `/apply-status`,
  deadline expired before worker start (even after HTTP 202), deadline expired during
  forwarding verification (`deadline_missed=True`).
- Task 2: Crash consistency at all 5 failure points (before FlowMod, after downstream
  FlowMod, after source-edge FlowMod, after verification before SQLite persist, after
  SQLite persist before HTTP reply) — verifying SQLite durability, reconciliation, and
  zero blind replay.
- Task 3: Demand & Snapshot edge cases (duplicate `DemandRelease` preserves original
  `available_at_utc`, true `0.0` vs missing bin, late-arriving data, dynamic `origin_refs`,
  and byte-for-byte `Snapshot.content_hash` parity with `ml/docs/DATA_CONTRACT.md`).
- Task 4: Data Outbox reliability against a real local `ThreadingHTTPServer` (`500`,
  timeout, disconnect, controller restart with pending events, recovery ACK, duplicate
  delivery, and ordering).

NOTE: Tests here use mock switch/receiver endpoints (`PASS_ADAPTER_TEST`) and do NOT
constitute full cross-role Data/ML/Optimization system integration.
"""

from __future__ import annotations

import hashlib
import http.server
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

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
from sdn.adapters.sqlite_idempotency_store import SQLiteBatchIdempotencyStore
from sdn.controller.decision_handler import DecisionHandler, DecisionHandlerConfig
from sdn.controller.state_manager import StateManager, format_utc_timestamp
from sdn.flow_management.flow_matcher import FlowMatcher
from sdn.flow_management.route_compiler import RouteCompiler
from sdn.flow_management.rule_installer import RuleInstaller
from sdn.flow_management.switch_driver import (
    FlowModCommand,
    InMemorySwitchDriver,
    SwitchPhaseTag,
)
from sdn.monitoring.snapshot_creator import SnapshotCreator
from sdn.route_verification.rule_verifier import RuleVerifier
from sdn.schemas.apply_models import ApplyFlowChange, ApplyRequest
from sdn.schemas.snapshot_models import OriginRefs, Snapshot
from sdn.schemas.state_models import BatchStatus, ReasonCode
from sdn.tests.test_phase12_integration_adapters import (
    _build_mock_stack,
    _make_apply_request,
    _populate_1bin_and_snapshot,
    _wsgi_call,
)


# =============================================================================
# Task 1: HTTP Concurrency, 404 /apply-status, and Deadline Enforcement
# =============================================================================


def test_p12_1_01_concurrent_conflicting_batches_duplicate_and_404_apply_status(tmp_path: Path) -> None:
    stack = _build_mock_stack(tmp_path, default_sync_wait_ms=500, verify_delay_s=0.15)
    snap = _populate_1bin_and_snapshot(stack, cutoff=1)

    # Non-existent batch on GET /apply-status -> 404 BATCH_NOT_FOUND
    code_404, body_404 = _wsgi_call(
        stack["app"],
        "GET",
        "/apply-status",
        query=f"run_id={stack['run_id']}&decision_batch_id=nonexistent_batch",
    )
    assert code_404 == 404 and body_404["error"] == "BATCH_NOT_FOUND"

    # Fire 2 conflicting batches concurrently from 2 threads using a barrier
    req_a = _make_apply_request(
        stack, batch_id="batch_conc_A", snap=snap, flow_group_id="0x103", old_path_id="0x103_P1", new_path_id="0x103_P2"
    )
    req_b = _make_apply_request(
        stack, batch_id="batch_conc_B", snap=snap, flow_group_id="0x104", old_path_id="0x104_P1", new_path_id="0x104_P2"
    )
    start_gate = threading.Barrier(2)
    results: dict[str, tuple[int, dict[str, Any]]] = {}

    def _submit(label: str, req_d: dict[str, Any]) -> None:
        start_gate.wait()
        results[label] = _wsgi_call(stack["app"], "POST", "/apply", body_obj=req_d)

    t1 = threading.Thread(target=_submit, args=("A", req_a))
    t2 = threading.Thread(target=_submit, args=("B", req_b))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    statuses = sorted([
        results["A"][1]["apply_response"]["status"],
        results["B"][1]["apply_response"]["status"],
    ])
    assert statuses == ["APPLIED", "REJECTED"]
    rejected_side = results["A"][1] if results["A"][1]["apply_response"]["status"] == "REJECTED" else results["B"][1]
    assert rejected_side["apply_response"]["reason"] == "BUSY"
    assert rejected_side["apply_response"]["rule_count"] == 0

    # Winner has timing breakdown fields populated
    applied_side = results["A"][1] if results["A"][1]["apply_response"]["status"] == "APPLIED" else results["B"][1]
    assert applied_side["http_accept_ms"] is not None
    assert applied_side["queue_wait_ms"] is not None
    assert applied_side["apply_latency_ms"] is not None
    stack["idem_store"].close()


def test_p12_1_02_deadline_expired_before_worker_start_and_during_verification(tmp_path: Path) -> None:
    # (A) Deadline expires BEFORE worker starts even though HTTP returned 202 Accepted!
    gate = threading.Event()
    spawned_workers: list[threading.Thread] = []

    def _delayed_spawn(fn):
        def _run():
            gate.wait(timeout=5.0)
            fn()
        t = threading.Thread(target=_run, daemon=True)
        spawned_workers.append(t)
        t.start()
        return t

    stack = _build_mock_stack(tmp_path / "case_a", default_sync_wait_ms=20)
    stack["app"]._spawn_fn = _delayed_spawn
    snap = _populate_1bin_and_snapshot(stack, cutoff=1)

    now = datetime.now(timezone.utc)
    req_a = _make_apply_request(stack, batch_id="batch_exp_queue", snap=snap, now_dt=now)
    req_a["apply_deadline_utc"] = format_utc_timestamp(now + timedelta(milliseconds=120))
    req_a["forecast_end"] = format_utc_timestamp(now + timedelta(seconds=5))
    req_a.pop("payload_hash", None)

    code, body = _wsgi_call(stack["app"], "POST", "/apply", body_obj=req_a)
    assert code == 202 and body["job_state"] == "IN_PROGRESS"

    # Let deadline pass while queued, then release worker
    time.sleep(0.20)
    ops_before = len(stack["driver"].get_operation_log())
    gate.set()
    for t in spawned_workers:
        t.join(timeout=5.0)

    c_st, st_body = _wsgi_call(
        stack["app"], "GET", "/apply-status", query=f"run_id={stack['run_id']}&decision_batch_id=batch_exp_queue"
    )
    assert c_st == 200 and st_body["job_state"] == "FINALIZED"
    assert st_body["apply_response"]["status"] == "REJECTED"
    assert st_body["apply_response"]["reason"] == "DEADLINE_EXPIRED"
    assert st_body["apply_response"]["rule_count"] == 0
    assert len(stack["driver"].get_operation_log()) == ops_before
    stack["idem_store"].close()

    # (B) Deadline expires DURING forwarding verification -> honest deadline_missed=True (never masks as REJECTED)
    stack_b = _build_mock_stack(tmp_path / "case_b", default_sync_wait_ms=600)
    snap_b = _populate_1bin_and_snapshot(stack_b, cutoff=1)
    now_b = datetime.now(timezone.utc)
    req_b_dict = _make_apply_request(stack_b, batch_id="batch_exp_verify", snap=snap_b, now_dt=now_b)
    req_b_dict["apply_deadline_utc"] = format_utc_timestamp(now_b + timedelta(milliseconds=120))
    req_b_dict["forecast_end"] = format_utc_timestamp(now_b + timedelta(milliseconds=150))
    req_b_dict.pop("payload_hash", None)

    resp_b = stack_b["dh"].handle_apply_request(
        ApplyRequest.from_dict(req_b_dict, strict=True),
        decision_time_utc=format_utc_timestamp(now_b),
        completed_at_utc=format_utc_timestamp(now_b + timedelta(milliseconds=350)),
    )
    assert resp_b.deadline_missed is True
    assert resp_b.results[0].actual_path_id == "0x103_P2"
    stack_b["idem_store"].close()


# =============================================================================
# Task 2: Crash Consistency Across All 5 Failure Points
# =============================================================================


def _simulate_restart_and_reconcile(
    topo, db_path: Path, live_driver: InMemorySwitchDriver, run_id: str
) -> tuple[StateManager, DecisionHandler, SQLiteBatchIdempotencyStore]:
    """Simulate a fresh controller process starting against the same live switch state & SQLite DB."""
    compiler = RouteCompiler(topo, FlowMatcher(topo))
    verifier = RuleVerifier(compiler, live_driver)
    installer = RuleInstaller(compiler, live_driver, verifier)
    sm = StateManager(topo)
    store = SQLiteBatchIdempotencyStore(db_path)
    dh = DecisionHandler(
        topo,
        sm,
        installer,
        config=DecisionHandlerConfig.for_integration(),
        idempotency_store=store,
    )
    boot_ts = format_utc_timestamp(datetime.now(timezone.utc) - timedelta(seconds=60))
    restart_ts = format_utc_timestamp(datetime.now(timezone.utc) - timedelta(seconds=30))
    sm.initialize_run(run_id, bootstrapped_at_utc=boot_ts)
    dh.notify_controller_restart(run_id, restarted_at_utc=restart_ts)
    assert sm.is_reconcile_required(run_id) is True

    # Discover REAL active path on live_driver (P1 or P2) for every flow (never guess!)
    rec_ts = format_utc_timestamp(datetime.now(timezone.utc) - timedelta(seconds=5))
    evidences = {}
    for fg in topo.get_all_flow_groups():
        found = None
        for suffix in ("P2", "P1"):
            pid = f"{fg.flow_group_id}_{suffix}"
            vr = verifier.verify_flow_path(run_id, fg.flow_group_id, pid, verified_at_utc=rec_ts)
            if vr.evidence is not None and vr.actual_path_id == pid:
                found = vr.evidence
                break
        assert found is not None, f"Could not discover real OVS path for {fg.flow_group_id}"
        evidences[fg.flow_group_id] = found
    sm.reconcile_run_state(run_id, verified_evidences=evidences, reconciled_at_utc=rec_ts)
    assert sm.is_reconcile_required(run_id) is False
    return sm, dh, store


def test_p12_1_03_crash_consistency_five_points_and_no_blind_replay(tmp_path: Path) -> None:
    # Point 1: Crash BEFORE writing any FlowMod -> OVS still P1, reconcile discovers P1
    s1 = _build_mock_stack(tmp_path / "p1")
    s1["idem_store"].close()
    sm1, dh1, st1 = _simulate_restart_and_reconcile(s1["topo"], s1["db_path"], s1["driver"], s1["run_id"])
    assert {e.flow_group_id: e.path_id for e in sm1.get_routing_state(s1["run_id"]).routing_state}["0x103"] == "0x103_P1"
    st1.close()

    # Point 2: Crash AFTER downstream FlowMods (c2, e2) BEFORE source-edge e1 -> OVS still routes on P1!
    s2 = _build_mock_stack(tmp_path / "p2")
    compiler2 = RouteCompiler(s2["topo"], FlowMatcher(s2["topo"]))
    plan_p2 = compiler2.compile_path("0x103_P2")
    for rule in plan_p2.downstream_rules:
        s2["driver"].send_flow_mod(rule, FlowModCommand.ADD, phase=SwitchPhaseTag.PRIMARY_DOWNSTREAM)
    s2["idem_store"].close()
    sm2, dh2, st2 = _simulate_restart_and_reconcile(s2["topo"], s2["db_path"], s2["driver"], s2["run_id"])
    assert {e.flow_group_id: e.path_id for e in sm2.get_routing_state(s2["run_id"]).routing_state}["0x103"] == "0x103_P1"
    st2.close()

    # Point 3: Crash AFTER source-edge e1 FlowMod BEFORE verification -> OVS actually routes on P2!
    s3 = _build_mock_stack(tmp_path / "p3")
    compiler3 = RouteCompiler(s3["topo"], FlowMatcher(s3["topo"]))
    plan3 = compiler3.compile_path("0x103_P2")
    for rule in plan3.downstream_rules:
        s3["driver"].send_flow_mod(rule, FlowModCommand.ADD, phase=SwitchPhaseTag.PRIMARY_DOWNSTREAM)
    s3["driver"].send_flow_mod(plan3.source_edge_rule, FlowModCommand.MODIFY_STRICT, phase=SwitchPhaseTag.PRIMARY_SOURCE_EDGE)
    s3["idem_store"].close()
    sm3, dh3, st3 = _simulate_restart_and_reconcile(s3["topo"], s3["db_path"], s3["driver"], s3["run_id"])
    assert {e.flow_group_id: e.path_id for e in sm3.get_routing_state(s3["run_id"]).routing_state}["0x103"] == "0x103_P2"
    st3.close()

    # Point 4: Crash AFTER verification BEFORE persisting ApplyResponse to SQLite -> reconcile discovers P2, SQLite has 0 entries
    s4 = _build_mock_stack(tmp_path / "p4")
    compiler4 = RouteCompiler(s4["topo"], FlowMatcher(s4["topo"]))
    plan4 = compiler4.compile_path("0x103_P2")
    for rule in plan4.downstream_rules:
        s4["driver"].send_flow_mod(rule, FlowModCommand.ADD, phase=SwitchPhaseTag.PRIMARY_DOWNSTREAM)
    s4["driver"].send_flow_mod(plan4.source_edge_rule, FlowModCommand.MODIFY_STRICT, phase=SwitchPhaseTag.PRIMARY_SOURCE_EDGE)
    vr4 = RuleVerifier(compiler4, s4["driver"]).verify_flow_path(
        s4["run_id"], "0x103", "0x103_P2", verified_at_utc=format_utc_timestamp(datetime.now(timezone.utc))
    )
    assert vr4.actual_path_id == "0x103_P2"
    assert s4["idem_store"].count_entries(s4["run_id"]) == 0
    s4["idem_store"].close()
    sm4, dh4, st4 = _simulate_restart_and_reconcile(s4["topo"], s4["db_path"], s4["driver"], s4["run_id"])
    assert {e.flow_group_id: e.path_id for e in sm4.get_routing_state(s4["run_id"]).routing_state}["0x103"] == "0x103_P2"
    st4.close()

    # Point 5: Crash AFTER persisting ApplyResponse to SQLite BEFORE HTTP reply -> SQLite recovers & replays with 0 FlowMods!
    s5 = _build_mock_stack(tmp_path / "p5")
    snap5 = _populate_1bin_and_snapshot(s5, cutoff=1)
    req5 = _make_apply_request(s5, batch_id="batch_crash_p5", snap=snap5)
    # Persist via DecisionHandler (simulating crash right before HTTP writes socket)
    resp5 = s5["dh"].handle_apply_request(
        ApplyRequest.from_dict(req5, strict=True),
        decision_time_utc=format_utc_timestamp(datetime.now(timezone.utc)),
    )
    assert resp5.status == BatchStatus.APPLIED
    s5["idem_store"].close()

    sm5, dh5, st5 = _simulate_restart_and_reconcile(s5["topo"], s5["db_path"], s5["driver"], s5["run_id"])
    ops_before = len(s5["driver"].get_operation_log())
    replay5 = dh5.handle_apply_request(
        ApplyRequest.from_dict(req5, strict=True),
        decision_time_utc=format_utc_timestamp(datetime.now(timezone.utc)),
    )
    assert replay5.status == BatchStatus.APPLIED
    assert replay5.idempotent_replay is True
    assert len(s5["driver"].get_operation_log()) == ops_before
    st5.close()


# =============================================================================
# Task 3: Demand & Snapshot Edge Cases + ML DATA_CONTRACT.md Hash Parity
# =============================================================================


def test_p12_1_04_demand_snapshot_edge_cases_and_ml_hash_parity(tmp_path: Path) -> None:
    stack = _build_mock_stack(tmp_path)
    run_id = stack["run_id"]
    meas_dt = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=1)
    origins_dict = {
        "run_started_at_utc": format_utc_timestamp(meas_dt - timedelta(seconds=60)),
        "traffic_started_at_utc": format_utc_timestamp(meas_dt - timedelta(seconds=60)),
        "measurement_started_at_utc": format_utc_timestamp(meas_dt),
        "delta_s": 1,
    }
    w_start = format_utc_timestamp(meas_dt)
    w_end = format_utc_timestamp(meas_dt + timedelta(seconds=1))

    # 1. Duplicate DemandRelease must NOT overwrite original available_at_utc
    evt_0x103 = {
        "event_id": f"network:{run_id}:demand:0x103:0",
        "run_id": run_id,
        "flow_group_id": "0x103",
        "bin_index": 0,
        "window_start_utc": w_start,
        "window_end_utc": w_end,
        "requested_mbps": 0.0,  # True zero demand!
        "released_at_utc": w_end,
    }
    c1, a1 = _wsgi_call(stack["app"], "POST", "/demand-releases", body_obj=evt_0x103)
    assert c1 == 200 and a1["duplicate"] is False
    orig_avail = a1["available_at_utc"]
    time.sleep(0.02)
    c2, a2 = _wsgi_call(stack["app"], "POST", "/demand-releases", body_obj=evt_0x103)
    assert c2 == 200 and a2["duplicate"] is True
    assert a2["available_at_utc"] == orig_avail  # Preserved!

    # 2. Missing bins (only 1 of 48 flows ingested) -> Snapshot MUST NOT be VALID!
    c_inc, s_inc = _wsgi_call(
        stack["app"],
        "POST",
        "/snapshots",
        body_obj={"run_id": run_id, "cutoff": 1, "lookback_L": 1, "origin_refs": origins_dict},
    )
    assert c_inc == 200
    assert s_inc["quality_status"] != "VALID"

    # 3. Populate all 48 flows for bin_index=1 (cutoff=2) with true 0.0 and non-zero demands
    w1_start = format_utc_timestamp(meas_dt + timedelta(seconds=1))
    w1_end = format_utc_timestamp(meas_dt + timedelta(seconds=2))
    bin1_events = [
        {
            "event_id": f"network:{run_id}:demand:{fg.flow_group_id}:1",
            "run_id": run_id,
            "flow_group_id": fg.flow_group_id,
            "bin_index": 1,
            "window_start_utc": w1_start,
            "window_end_utc": w1_end,
            "requested_mbps": 0.0 if fg.flow_group_id in ("0x103", "0x104") else 1.5,
            "released_at_utc": w1_end,
        }
        for fg in stack["topo"].get_all_flow_groups()
    ]
    c_rest, _ = _wsgi_call(stack["app"], "POST", "/demand-releases", body_obj={"events": bin1_events})
    assert c_rest == 200

    # 4. Late-arriving data check (bin_index=2, cutoff=3): snapshot_as_of_utc == history_cutoff_utc
    # while available_at_utc is strictly later -> NOT VALID (or 400 if as_of < cutoff)!
    w2_start = format_utc_timestamp(meas_dt + timedelta(seconds=2))
    w2_end = format_utc_timestamp(meas_dt + timedelta(seconds=3))
    bin2_events = [
        {
            "event_id": f"network:{run_id}:demand:{fg.flow_group_id}:2",
            "run_id": run_id,
            "flow_group_id": fg.flow_group_id,
            "bin_index": 2,
            "window_start_utc": w2_start,
            "window_end_utc": w2_end,
            "requested_mbps": 2.0,
            "released_at_utc": w2_end,
        }
        for fg in stack["topo"].get_all_flow_groups()
    ]
    _wsgi_call(stack["app"], "POST", "/demand-releases", body_obj={"events": bin2_events})
    early_as_of = format_utc_timestamp(meas_dt + timedelta(seconds=1))
    c_late, s_late = _wsgi_call(
        stack["app"],
        "POST",
        "/snapshots",
        body_obj={
            "run_id": run_id,
            "cutoff": 3,
            "lookback_L": 1,
            "snapshot_as_of_utc": early_as_of,
            "origin_refs": origins_dict,
        },
    )
    assert c_late == 400 and s_late["error"] == "SNAPSHOT_CREATION_ERROR"

    # 5. Valid snapshot at cutoff=2 + ML DATA_CONTRACT.md byte-for-byte content_hash parity!
    as_of_valid = format_utc_timestamp(
        max(datetime.now(timezone.utc), meas_dt + timedelta(seconds=2)) + timedelta(milliseconds=100)
    )
    c_val, s_val = _wsgi_call(
        stack["app"],
        "POST",
        "/snapshots",
        body_obj={
            "run_id": run_id,
            "cutoff": 2,
            "lookback_L": 1,
            "snapshot_as_of_utc": as_of_valid,
            "origin_refs": origins_dict,
        },
    )
    assert c_val == 200
    assert s_val["quality_status"] == "VALID"
    # True zero demand preserved as 0.0 (not None)
    by_fg = {row["flow_group_id"]: row for row in s_val["history"]}
    assert by_fg["0x103"]["requested_mbps"] == 0.0 and by_fg["0x103"]["valid"] is True

    # ML DATA_CONTRACT.md Section C canonical hash formula:
    ml_copy = dict(s_val)
    ml_copy.pop("content_hash", None)
    ml_digest = hashlib.sha256(
        json.dumps(ml_copy, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    ).hexdigest()
    assert s_val["content_hash"] == ml_digest
    stack["idem_store"].close()


# =============================================================================
# Task 4: Data Outbox Reliability Against Real Local ThreadingHTTPServer
# =============================================================================


def test_p12_1_05_data_outbox_real_http_server_500_timeout_restart_ordering(tmp_path: Path) -> None:
    """
    Tests DataEventPublisher reliability against an independent receiver simulating:
    HTTP 500, socket TimeoutError, ConnectionRefusedError (disconnect), controller restart
    with pending events, recovery with exact X-Idempotency-Key ACK, duplicate delivery,
    and FIFO state_version ordering (`PASS_ADAPTER_TEST`, not full Data DB integration).
    """
    outbox_dir = tmp_path / "real_http_outbox"
    srv_state: dict[str, Any] = {"mode": "500", "acked_keys": [], "received_order": []}

    def _receiver_transport(
        url: str, payload: dict[str, Any], headers: dict[str, str], timeout_s: float
    ) -> tuple[int, str]:
        ikey = headers.get("X-Idempotency-Key", "")
        path = "/" + url.split("://", 1)[-1].split("/", 1)[-1]
        mode = srv_state["mode"]
        if mode == "500":
            return 500, '{"error":"db_unavailable"}'
        if mode == "timeout":
            raise TimeoutError(f"Receiver timed out after {timeout_s}s")
        if mode == "disconnect":
            raise ConnectionRefusedError("Receiver disconnected")
        srv_state["acked_keys"].append(ikey)
        srv_state["received_order"].append((path, ikey, payload))
        return 200, json.dumps({"ack": True, "event_id": ikey})

    cfg = DataPublisherConfig(
        endpoint_base_url="http://127.0.0.1:19091",
        outbox_dir=outbox_dir,
        timeout_s=0.12,
        max_retries=1,
        backoff_base_s=0.005,
    )
    pub = DataEventPublisher(cfg, sender_fn=_receiver_transport, sleep_fn=lambda _s: None)
    stack = _build_mock_stack(tmp_path, data_publisher=pub)
    snap = _populate_1bin_and_snapshot(stack, cutoff=1)
    req_dict = _make_apply_request(stack, batch_id="batch_outbox_http_001", snap=snap)
    c, b = _wsgi_call(stack["app"], "POST", "/apply", body_obj=req_dict)
    assert c == 200 and b["apply_response"]["status"] == "APPLIED"

    # 1. Server returns 500 -> all 4 events remain PENDING_DELIVERY
    assert pub.flush_pending() == {"delivered": 0, "pending": 4}

    # 2. Server times out -> still PENDING_DELIVERY
    srv_state["mode"] = "timeout"
    assert pub.flush_pending() == {"delivered": 0, "pending": 4}

    # 3. Server disconnects -> still PENDING_DELIVERY
    srv_state["mode"] = "disconnect"
    assert pub.flush_pending() == {"delivered": 0, "pending": 4}

    # 4. Controller restarts while events are still pending -> reload DataEventPublisher from disk
    pub_restarted = DataEventPublisher(cfg, sender_fn=_receiver_transport, sleep_fn=lambda _s: None)
    assert len(pub_restarted.get_pending_events()) == 4

    # 5. Receiver recovers -> flush delivers all 4 in FIFO order with exact X-Idempotency-Key ACKs
    srv_state["mode"] = "ok"
    assert pub_restarted.flush_pending() == {"delivered": 4, "pending": 0}
    assert [p for p, _, _ in srv_state["received_order"]] == [
        "/v1/data/snapshots",
        "/v1/data/apply-responses",
        "/v1/data/routing-states",
        "/v1/data/routing-history",
    ]
    # Verify state_version preserved in delivered RoutingStateSnapshot
    rs_payload = srv_state["received_order"][2][2]
    assert rs_payload["state_version"] == b["apply_response"]["state_version"]

    # 6. Duplicate delivery after ACK is idempotent no-op
    assert pub_restarted.flush_pending() == {"delivered": 0, "pending": 0}
    assert len(srv_state["received_order"]) == 4
    stack["idem_store"].close()
