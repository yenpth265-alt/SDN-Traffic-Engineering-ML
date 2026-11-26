"""
Phase 12.2 Batch Verification Optimization & Async HTTP Performance Tests (`PASS_MOCK`).

Tests:
1. Datapath-grouped `FlowStats` batch verification (`B=4` flows): strict two-phase ordering,
   single `FlowStats` per unique switch per poll round, response reuse across flows, and
   stage timing breakdown (`openflow_install_ms`, `verification_ms`, `flow_stats_request_count`).
2. Shared batch deadline + partial silent-flow rollback (`B=2`: 1 active flow `APPLIED`,
   1 zero-traffic flow `ROLLED_BACK` -> `BatchStatus.PARTIAL`, never fakes `APPLIED`).
3. `POST /apply?async=1` (`wait_ms=0`) immediate `202 Accepted` (< 50 ms) with atomic
   job registration prior to ACK and idempotent retry.
"""

from __future__ import annotations

import time
from pathlib import Path

from sdn.apps.ryu_s0_app import S0AppConfig, S0ControllerOrchestrator
from sdn.flow_management.rule_installer import FlowChangeSpec
from sdn.flow_management.ryu_openflow_driver import RyuDriverConfig
from sdn.schemas.state_models import BatchStatus, FlowDecisionStatus, VerificationStatus
from sdn.tests.test_phase12_integration_adapters import (
    _build_mock_stack,
    _make_apply_request,
    _populate_1bin_and_snapshot,
    _wsgi_call,
)
from sdn.tests.test_ryu_openflow_driver_and_s0 import (
    CATALOG_DIR,
    TOPOLOGY_BUNDLE_PATH,
    _FakeDatapath,
)


def test_p12_2_01_batch_verification_groups_flow_stats_by_datapath_and_reuses_response() -> None:
    """
    Verify `B=4` flows (`0x103`, `0x104`, `0x105`, `0x106`) on `RyuOpenFlowDriver`:
    - Strict two-phase install: all downstream FlowMods + per-switch Barriers BEFORE source-edge.
    - Batched `verify_batch_forwarding_counter_or_probe` issues ONE FlowStats per unique DPID
      per poll round (`<= 4` switches: `e1, c2, e2, e3`), NOT `4 * 3 = 12` per round!
    - All 4 flows verify `APPLIED` on poll 2 when counters increment asynchronously.
    """
    sleeps = {"count": 0}
    pkt_val = {"p1": 10, "p2": 0}

    def _sleep(sec: float) -> None:
        sleeps["count"] += 1
        if sleeps["count"] >= 1:
            pkt_val["p2"] = 5  # Simulate OVS kernel revalidator updating counters on poll 2

    orch = S0ControllerOrchestrator(
        S0AppConfig(
            topology_bundle_path=TOPOLOGY_BUNDLE_PATH,
            csv_config_dir=CATALOG_DIR,
            run_id="run_p122_b4",
            driver_config=RyuDriverConfig(
                counter_verify_timeout_s=0.5,
                counter_verify_poll_interval_s=0.04,
            ),
        )
    )
    orch.driver._sleep_fn = _sleep

    for sw in orch.topology.get_all_switches():
        def _mk_prov(dpid: int):
            def _prov(_d: int, cookie: int) -> int:
                if dpid == 6:  # c2 (P2)
                    return pkt_val["p2"]
                return pkt_val["p1"] + pkt_val["p2"]
            return _prov

        dp = _FakeDatapath(sw.dpid, orch.driver, auto_reply=True, packet_count_provider=_mk_prov(sw.dpid))
        orch.on_switch_connected(dp)

    rep = orch.bootstrap_and_verify(verified_at_utc="2026-11-21T17:00:00Z")
    assert rep.verified_48_flows is True

    op_start = len(orch.driver.get_operation_log())
    changes = (
        FlowChangeSpec("0x103", "0x103_P1", "0x103_P2"),
        FlowChangeSpec("0x104", "0x104_P1", "0x104_P2"),
        FlowChangeSpec("0x105", "0x105_P1", "0x105_P2"),
        FlowChangeSpec("0x106", "0x106_P1", "0x106_P2"),
    )
    res = orch.installer.install_batch(
        run_id="run_p122_b4",
        decision_batch_id="batch_p122_b4",
        changes=changes,
        applied_at_utc="2026-11-21T17:00:15Z",
    )
    assert res.suggested_batch_status == BatchStatus.APPLIED
    assert len(res.outcomes) == 4
    assert all(o.status == FlowDecisionStatus.APPLIED for o in res.outcomes)
    assert all(o.verification_status == VerificationStatus.VERIFIED for o in res.outcomes)
    assert res.total_rule_count == 12
    assert res.openflow_install_ms is not None
    assert res.verification_ms is not None

    ops = orch.driver.get_operation_log()[op_start:]
    # Verify two-phase order: every PRIMARY_DOWNSTREAM FlowMod/Barrier precedes every PRIMARY_SOURCE_EDGE
    ds_indices = [i for i, o in enumerate(ops) if o.phase.value == "PRIMARY_DOWNSTREAM"]
    src_indices = [i for i, o in enumerate(ops) if o.phase.value == "PRIMARY_SOURCE_EDGE"]
    assert max(ds_indices) < min(src_indices)

    # Verify FlowStats grouping: 2 poll rounds * 4 unique switches (e1, e2, e3, c2) = 8 FlowStats calls
    # (compared to 4 flows * 3 hops * 2 rounds = 24 calls without datapath grouping!)
    fs_calls = [o for o in ops if o.op_type == "FLOW_STATS"]
    assert len(fs_calls) == 8
    assert res.flow_stats_request_count == 8


def test_p12_2_02_batch_shared_deadline_and_partial_silent_flow_rollback() -> None:
    """
    Verify shared batch deadline when `B=2`:
    - `0x103` has active traffic -> `APPLIED` (`0x103_P2`).
    - `0x104` has ZERO traffic (silent flow on `c2`) -> expires at shared deadline,
      safely rolls back to `0x104_P1` (`ROLLED_BACK`), batch status == `PARTIAL`.
    """
    sleeps = {"count": 0, "in_rollback": False}

    def _sleep(sec: float) -> None:
        sleeps["count"] += 1
        time.sleep(min(0.02, sec))

    orch = S0ControllerOrchestrator(
        S0AppConfig(
            topology_bundle_path=TOPOLOGY_BUNDLE_PATH,
            csv_config_dir=CATALOG_DIR,
            run_id="run_p122_partial",
            driver_config=RyuDriverConfig(
                counter_verify_timeout_s=0.08,
                counter_verify_poll_interval_s=0.02,
            ),
        )
    )
    orch.driver._sleep_fn = _sleep

    for sw in orch.topology.get_all_switches():
        def _mk_prov(dpid: int):
            def _prov(_d: int, cookie: int) -> int:
                if dpid == 6 and cookie == 0x104:
                    return 0  # Silent flow on P2!
                if cookie == 0x104 and dpid == 5 and sleeps["in_rollback"]:
                    return 25  # Active traffic on P1 after rollback
                return 15 + sleeps["count"]
            return _prov

        dp = _FakeDatapath(sw.dpid, orch.driver, auto_reply=True, packet_count_provider=_mk_prov(sw.dpid))
        orch.on_switch_connected(dp)

    rep = orch.bootstrap_and_verify(verified_at_utc="2026-11-21T17:00:00Z")
    assert rep.verified_48_flows is True

    orig_fm = orch.driver.send_flow_mod

    def _wrap_fm(rule, command, *, phase):
        if phase.value == "ROLLBACK_SOURCE_EDGE":
            sleeps["in_rollback"] = True
        return orig_fm(rule, command, phase=phase)

    orch.driver.send_flow_mod = _wrap_fm  # type: ignore[method-assign]

    res = orch.installer.install_batch(
        run_id="run_p122_partial",
        decision_batch_id="batch_p122_partial",
        changes=(
            FlowChangeSpec("0x103", "0x103_P1", "0x103_P2"),
            FlowChangeSpec("0x104", "0x104_P1", "0x104_P2"),
        ),
        applied_at_utc="2026-11-21T17:00:15Z",
    )
    by_fg = {o.flow_group_id: o for o in res.outcomes}
    assert by_fg["0x103"].status == FlowDecisionStatus.APPLIED
    assert by_fg["0x103"].actual_path_id == "0x103_P2"
    assert by_fg["0x104"].status == FlowDecisionStatus.ROLLED_BACK
    assert by_fg["0x104"].actual_path_id == "0x104_P1"
    assert res.suggested_batch_status == BatchStatus.PARTIAL


def test_p12_2_03_http_async_wait_ms_0_immediate_202_and_idempotent_retry(tmp_path: Path) -> None:
    stack = _build_mock_stack(tmp_path, default_sync_wait_ms=250, verify_delay_s=0.15)
    snap = _populate_1bin_and_snapshot(stack, cutoff=1)
    req_dict = _make_apply_request(stack, batch_id="batch_async_zero", snap=snap)

    t0 = time.monotonic()
    code, body = _wsgi_call(stack["app"], "POST", "/apply", query="async=1", body_obj=req_dict)
    elapsed_ms = (time.monotonic() - t0) * 1000.0
    assert code == 202
    assert body["job_state"] == "IN_PROGRESS"
    assert elapsed_ms < 50.0  # Far below 450ms caller timeout!

    # Job is already registered atomically before 202 was returned -> GET /apply-status never 404
    c_st, b_st = _wsgi_call(
        stack["app"],
        "GET",
        "/apply-status",
        query=f"run_id={stack['run_id']}&decision_batch_id=batch_async_zero",
    )
    assert c_st == 200 and b_st["job_state"] in ("IN_PROGRESS", "FINALIZED")

    # Idempotent retry while in-flight
    c_rep, b_rep = _wsgi_call(stack["app"], "POST", "/apply", query="async=1", body_obj=req_dict)
    assert c_rep in (200, 202)

    # Wait for finalization
    final = None
    for _ in range(30):
        _, bp = _wsgi_call(
            stack["app"],
            "GET",
            "/apply-status",
            query=f"run_id={stack['run_id']}&decision_batch_id=batch_async_zero",
        )
        if bp["job_state"] == "FINALIZED":
            final = bp
            break
        time.sleep(0.02)
    assert final is not None
    assert final["apply_response"]["status"] == "APPLIED"
    stack["idem_store"].close()
