"""
Phase 4 Unit, Contract & Integration Tests on Windows (PASS_MOCK):
- ApplyRequest / ApplyResponse / TransportEnvelope schemas & payload_hash
- DecisionHandler pre-write validation, idempotency, conflict detection, two-phase execution,
  rollback, UNKNOWN/INCONSISTENT, and post-write DEADLINE_MISSED
- SnapshotCreator causal windowing, timestamp gates, non-leakage of future demand,
  rejection of goodput/port counters, missing data vs. 0.0, and verified routing state checks
- Multi-run isolation across DecisionHandler, StateManager, and SnapshotCreator
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from pathlib import Path
import pytest

from sdn.controller.decision_handler import (
    DecisionHandler,
    DecisionHandlerConfig,
    InMemoryBatchIdempotencyStore,
)
from sdn.controller.state_manager import (
    StateManager,
    format_utc_timestamp,
    parse_utc_timestamp,
)
from sdn.controller.topology_registry import TopologyRegistry
from sdn.flow_management.flow_matcher import FlowMatcher, FlowMatcherConfig
from sdn.flow_management.route_compiler import RouteCompiler
from sdn.flow_management.rule_installer import RuleInstaller
from sdn.flow_management.switch_driver import (
    InMemorySwitchDriver,
    SwitchDriverError,
    SwitchPhaseTag,
)
from sdn.monitoring.snapshot_creator import (
    ForbiddenDemandSourceError,
    MockLinkTelemetryProvider,
    MockReleasedDemandProvider,
    SnapshotCreator,
    SnapshotCreatorConfig,
)
from sdn.route_verification.rule_verifier import RuleVerifier, RuleVerifierConfig
from sdn.schemas.apply_models import (
    ApplyFlowChange,
    ApplyRequest,
    TransportEnvelope,
    compute_apply_payload_hash,
)
from sdn.schemas.snapshot_models import (
    DemandObservationRecord,
    DemandSourceKind,
    LinkSampleRef,
    OriginRefs,
    Snapshot,
    SnapshotQualityStatus,
    compute_snapshot_content_hash,
)
from sdn.schemas.state_models import (
    BatchStatus,
    FlowDecisionStatus,
    ReasonCode,
    StateVersionPolicy,
    VerificationStatus,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TOPOLOGY_BUNDLE_PATH = REPO_ROOT / "configs" / "network" / "topology_bundle.json"
CATALOG_DIR = REPO_ROOT / "configs" / "network"

DEFAULT_ORIGINS = OriginRefs(
    run_started_at_utc="2026-11-12T08:59:00Z",
    traffic_started_at_utc="2026-11-12T08:59:00Z",
    measurement_started_at_utc="2026-11-12T09:00:00Z",
    warmup_s=60,
    delta_s=1,
)


@pytest.fixture(scope="module")
def topology() -> TopologyRegistry:
    return TopologyRegistry.from_bundle_file(
        TOPOLOGY_BUNDLE_PATH,
        csv_config_dir=CATALOG_DIR,
        verify_hash=True,
    )


def _count_flow_mods(driver: InMemorySwitchDriver) -> int:
    return sum(1 for e in driver.get_operation_log() if e.op_type == "FLOW_MOD")


def _build_controller_stack(
    topology: TopologyRegistry,
    *,
    run_id: str = "run_p4_01",
    bootstrapped_at_utc: str = "2026-11-12T09:00:00Z",
    verify_bootstrap: bool = True,
    handler_config: DecisionHandlerConfig | None = None,
    snapshot_config: SnapshotCreatorConfig | None = None,
) -> tuple[
    InMemorySwitchDriver,
    RuleVerifier,
    RuleInstaller,
    StateManager,
    DecisionHandler,
    MockReleasedDemandProvider,
    SnapshotCreator,
]:
    driver = InMemorySwitchDriver(topology)
    matcher = FlowMatcher(topology)
    compiler = RouteCompiler(topology, matcher)
    verifier = RuleVerifier(compiler, driver)
    installer = RuleInstaller(compiler, driver, verifier)
    state_mgr = StateManager(topology)

    if verify_bootstrap:
        evidences = installer.bootstrap_initial_paths(
            run_id=run_id,
            verified_at_utc=bootstrapped_at_utc,
            preinstall_both_downstream_branches=True,
        )
        state_mgr.initialize_run(
            run_id=run_id,
            bootstrapped_at_utc=bootstrapped_at_utc,
            verification_evidences=evidences,
        )
    else:
        state_mgr.initialize_run(
            run_id=run_id,
            bootstrapped_at_utc=bootstrapped_at_utc,
            verification_evidences=None,
        )

    demand_provider = MockReleasedDemandProvider()
    snap_creator = SnapshotCreator(
        topology_registry=topology,
        state_manager=state_mgr,
        demand_provider=demand_provider,
        config=snapshot_config,
    )
    snap_creator.set_run_origin_refs(run_id, DEFAULT_ORIGINS)

    handler = DecisionHandler(
        topology_registry=topology,
        state_manager=state_mgr,
        rule_installer=installer,
        config=handler_config or DecisionHandlerConfig.for_mock_unit_tests(),
        snapshot_lookup=snap_creator.get_closed_snapshot,
    )
    return (
        driver,
        verifier,
        installer,
        state_mgr,
        handler,
        demand_provider,
        snap_creator,
    )


def _make_valid_apply_request(
    topology: TopologyRegistry,
    *,
    run_id: str = "run_p4_01",
    decision_batch_id: str = "batch_001",
    snapshot_id: str = "snap:run_p4_01:t10:L5:v1",
    expected_state_version: int = 1,
    config_version: str = "1.0",
    forecast_start: str = "2026-11-12T09:00:10Z",
    forecast_end: str = "2026-11-12T09:00:20Z",
    apply_deadline_utc: str = "2026-11-12T09:00:12Z",
    changes: tuple[ApplyFlowChange, ...] = (
        ApplyFlowChange(
            flow_group_id="0x103",
            old_path_id="0x103_P1",
            new_path_id="0x103_P2",
        ),
    ),
    topology_hash: str | None = None,
    topology_version: str | None = None,
) -> ApplyRequest:
    return ApplyRequest(
        run_id=run_id,
        decision_batch_id=decision_batch_id,
        snapshot_id=snapshot_id,
        expected_state_version=expected_state_version,
        config_version=config_version,
        forecast_start=forecast_start,
        forecast_end=forecast_end,
        apply_deadline_utc=apply_deadline_utc,
        changes=changes,
        topology_version=topology_version or topology.topology_version,
        topology_hash=topology_hash if topology_hash is not None else topology.topology_hash,
    )


# =============================================================================
# 1. Valid ApplyRequest: 0x103_P1 -> 0x103_P2
# =============================================================================


def test_01_valid_apply_request_p1_to_p2(topology: TopologyRegistry) -> None:
    (
        driver,
        _verifier,
        _installer,
        state_mgr,
        handler,
        _demand_provider,
        _snap_creator,
    ) = _build_controller_stack(topology)

    driver.clear_operation_log()
    req = _make_valid_apply_request(topology)
    resp = handler.handle_apply_request(
        req,
        decision_time_utc="2026-11-12T09:00:10.200Z",
        applied_at_utc="2026-11-12T09:00:10.300Z",
        completed_at_utc="2026-11-12T09:00:10.350Z",
    )

    assert resp.status == BatchStatus.APPLIED
    assert resp.state_version == 2
    assert resp.rule_count == 3  # c2 ADD, e2 ADD, e1 MODIFY_STRICT
    assert resp.deadline_missed is False
    assert resp.idempotent_replay is False
    assert resp.is_network_atomic is False
    assert resp.reason is None
    assert len(resp.results) == 1

    flow_res = resp.results[0]
    assert flow_res.flow_group_id == "0x103"
    assert flow_res.actual_path_id == "0x103_P2"
    assert flow_res.status == FlowDecisionStatus.APPLIED
    assert flow_res.verification_status == VerificationStatus.VERIFIED
    assert flow_res.rule_count == 3

    # Verify StateManager state & hold-down timer
    assert state_mgr.get_state_version("run_p4_01") == 2
    live_0x103 = state_mgr.get_flow_state("run_p4_01", "0x103")
    assert live_0x103.path_id == "0x103_P2"
    assert live_0x103.verification_status == VerificationStatus.VERIFIED
    assert live_0x103.last_successful_change_at == format_utc_timestamp(
        "2026-11-12T09:00:10.300Z"
    )
    assert state_mgr.is_hold_down_active(
        "run_p4_01", "0x103", "2026-11-12T09:00:15Z"
    ) is True


# =============================================================================
# 2. Wrong topology_hash / topology_version -> REJECTED (TOPOLOGY_MISMATCH)
# =============================================================================


def test_02_reject_wrong_topology_hash(topology: TopologyRegistry) -> None:
    driver, _, _, state_mgr, handler, _, _ = _build_controller_stack(topology)
    driver.clear_operation_log()

    bad_req = _make_valid_apply_request(
        topology,
        decision_batch_id="batch_bad_topo",
        topology_hash="0" * 64,
    )
    resp = handler.handle_apply_request(
        bad_req, decision_time_utc="2026-11-12T09:00:10.200Z"
    )

    assert resp.status == BatchStatus.REJECTED
    assert resp.reason == ReasonCode.TOPOLOGY_MISMATCH.value
    assert resp.rule_count == 0
    assert resp.state_version == 1
    assert state_mgr.get_state_version("run_p4_01") == 1
    assert _count_flow_mods(driver) == 0


# =============================================================================
# 3. Wrong expected_state_version / config_version -> REJECTED (VERSION_MISMATCH)
# =============================================================================


def test_03_reject_wrong_expected_state_version_and_config_version(
    topology: TopologyRegistry,
) -> None:
    driver, _, _, state_mgr, handler, _, _ = _build_controller_stack(topology)
    driver.clear_operation_log()

    bad_ver_req = _make_valid_apply_request(
        topology,
        decision_batch_id="batch_bad_sv",
        expected_state_version=99,
    )
    resp = handler.handle_apply_request(
        bad_ver_req, decision_time_utc="2026-11-12T09:00:10.200Z"
    )

    assert resp.status == BatchStatus.REJECTED
    assert resp.reason == ReasonCode.VERSION_MISMATCH.value
    assert resp.rule_count == 0
    assert resp.state_version == 1
    assert _count_flow_mods(driver) == 0

    bad_cfg_req = _make_valid_apply_request(
        topology,
        decision_batch_id="batch_bad_cfg",
        config_version="999.0",
    )
    resp_cfg = handler.handle_apply_request(
        bad_cfg_req, decision_time_utc="2026-11-12T09:00:10.200Z"
    )
    assert resp_cfg.status == BatchStatus.REJECTED
    assert resp_cfg.reason == ReasonCode.CONFIG_MISMATCH.value
    assert resp_cfg.rule_count == 0
    assert state_mgr.get_state_version("run_p4_01") == 1
    assert _count_flow_mods(driver) == 0


# =============================================================================
# 4. Duplicate decision_batch_id with SAME payload -> Idempotent Replay (AC-S08)
# =============================================================================


def test_04_duplicate_batch_id_same_payload_idempotent_replay(
    topology: TopologyRegistry,
) -> None:
    driver, _, _, state_mgr, handler, _, _ = _build_controller_stack(topology)
    driver.clear_operation_log()

    req = _make_valid_apply_request(topology, decision_batch_id="batch_idem_01")
    first_resp = handler.handle_apply_request(
        req,
        decision_time_utc="2026-11-12T09:00:10.200Z",
        applied_at_utc="2026-11-12T09:00:10.300Z",
    )
    assert first_resp.status == BatchStatus.APPLIED
    assert first_resp.state_version == 2
    assert first_resp.idempotent_replay is False
    flow_mods_after_first = _count_flow_mods(driver)
    assert flow_mods_after_first == 3

    # Replay the exact same ApplyRequest -> 0 additional FlowMods, state_version stays 2
    replay_resp = handler.handle_apply_request(
        req,
        decision_time_utc="2026-11-12T09:00:11.000Z",
    )
    assert replay_resp.status == BatchStatus.APPLIED
    assert replay_resp.state_version == 2
    assert replay_resp.idempotent_replay is True
    assert replay_resp.payload_hash == first_resp.payload_hash
    assert replay_resp.to_core_dict() == first_resp.to_core_dict()
    assert _count_flow_mods(driver) == flow_mods_after_first
    assert state_mgr.get_state_version("run_p4_01") == 2


# =============================================================================
# 5. Duplicate decision_batch_id with DIFFERENT payload -> BATCH_ID_CONFLICT (AC-S08)
# =============================================================================


def test_05_duplicate_batch_id_different_payload_conflict(
    topology: TopologyRegistry,
) -> None:
    driver, _, _, state_mgr, handler, _, _ = _build_controller_stack(topology)
    driver.clear_operation_log()

    req_a = _make_valid_apply_request(topology, decision_batch_id="batch_conflict_01")
    resp_a = handler.handle_apply_request(
        req_a,
        decision_time_utc="2026-11-12T09:00:10.200Z",
        applied_at_utc="2026-11-12T09:00:10.300Z",
    )
    assert resp_a.status == BatchStatus.APPLIED
    assert state_mgr.get_state_version("run_p4_01") == 2
    mods_before = _count_flow_mods(driver)

    # Same decision_batch_id="batch_conflict_01" but different flow change payload (0x104)
    req_b = _make_valid_apply_request(
        topology,
        decision_batch_id="batch_conflict_01",
        expected_state_version=2,
        changes=(
            ApplyFlowChange(
                flow_group_id="0x104",
                old_path_id="0x104_P1",
                new_path_id="0x104_P2",
            ),
        ),
    )
    assert compute_apply_payload_hash(req_a) != compute_apply_payload_hash(req_b)

    resp_b = handler.handle_apply_request(
        req_b,
        decision_time_utc="2026-11-12T09:00:11.000Z",
    )
    assert resp_b.status == BatchStatus.REJECTED
    assert resp_b.reason == ReasonCode.BATCH_ID_CONFLICT.value
    assert resp_b.rule_count == 0
    assert resp_b.state_version == 2
    assert _count_flow_mods(driver) == mods_before


# =============================================================================
# 6. Controller BUSY or reconcile_required -> REJECTED
# =============================================================================


def test_06_reject_when_busy_or_reconcile_required(topology: TopologyRegistry) -> None:
    # 6a: Unverified bootstrap -> reconcile_required is True -> REJECTED (RECONCILIATION_REQUIRED)
    driver_unv, _, _, state_mgr_unv, handler_unv, _, _ = _build_controller_stack(
        topology,
        run_id="run_unv",
        verify_bootstrap=False,
    )
    req_unv = _make_valid_apply_request(topology, run_id="run_unv")
    resp_unv = handler_unv.handle_apply_request(
        req_unv, decision_time_utc="2026-11-12T09:00:10.200Z"
    )
    assert resp_unv.status == BatchStatus.REJECTED
    assert resp_unv.reason == ReasonCode.RECONCILIATION_REQUIRED.value
    assert resp_unv.rule_count == 0
    assert state_mgr_unv.get_state_version("run_unv") == 1
    assert _count_flow_mods(driver_unv) == 0

    # 6b: In-flight batch active -> REJECTED (BUSY)
    driver, _, _, state_mgr, handler, _, _ = _build_controller_stack(topology)
    driver.clear_operation_log()
    state_mgr.begin_batch_execution(
        "run_p4_01",
        "batch_in_flight_99",
        started_at_utc="2026-11-12T09:00:10.100Z",
        expected_state_version=1,
    )

    req_busy = _make_valid_apply_request(topology, decision_batch_id="batch_while_busy")
    resp_busy = handler.handle_apply_request(
        req_busy, decision_time_utc="2026-11-12T09:00:10.200Z"
    )
    assert resp_busy.status == BatchStatus.REJECTED
    assert resp_busy.reason == ReasonCode.BUSY.value
    assert resp_busy.rule_count == 0
    assert resp_busy.state_version == 1
    assert _count_flow_mods(driver) == 0


# =============================================================================
# 7. Deadline expired before write & Stale request -> REJECTED (AC-S02)
# =============================================================================


def test_07_deadline_expired_before_write_and_stale_snapshot(
    topology: TopologyRegistry,
) -> None:
    driver, _, _, state_mgr, handler, _, _ = _build_controller_stack(
        topology,
        handler_config=DecisionHandlerConfig.for_mock_unit_tests(
            stale_threshold_s=2.0,
            apply_guard_s=0.5,
        ),
    )
    driver.clear_operation_log()

    # 7a: decision_time_utc > apply_deadline_utc -> DEADLINE_EXPIRED
    req_late = _make_valid_apply_request(
        topology,
        decision_batch_id="batch_late_01",
        forecast_start="2026-11-12T09:00:10Z",
        forecast_end="2026-11-12T09:00:20Z",
        apply_deadline_utc="2026-11-12T09:00:12Z",
    )
    resp_late = handler.handle_apply_request(
        req_late, decision_time_utc="2026-11-12T09:00:12.001Z"
    )
    assert resp_late.status == BatchStatus.REJECTED
    assert resp_late.reason == ReasonCode.DEADLINE_EXPIRED.value
    assert resp_late.rule_count == 0
    assert state_mgr.get_state_version("run_p4_01") == 1
    assert _count_flow_mods(driver) == 0

    # 7b: decision_time_utc > forecast_end - apply_guard_s -> DEADLINE_EXPIRED
    req_guard = _make_valid_apply_request(
        topology,
        decision_batch_id="batch_guard_01",
        forecast_start="2026-11-12T09:00:10Z",
        forecast_end="2026-11-12T09:00:20Z",
        apply_deadline_utc="2026-11-12T09:00:25Z",
    )
    resp_guard = handler.handle_apply_request(
        req_guard, decision_time_utc="2026-11-12T09:00:19.600Z"
    )
    assert resp_guard.status == BatchStatus.REJECTED
    assert resp_guard.reason == ReasonCode.DEADLINE_EXPIRED.value
    assert resp_guard.rule_count == 0
    assert _count_flow_mods(driver) == 0

    # 7c: Stale history_cutoff_utc (> T_stale = 2.0s) -> STALE
    req_stale = replace(
        _make_valid_apply_request(topology, decision_batch_id="batch_stale_01"),
        history_cutoff_utc="2026-11-12T09:00:07Z",
    )
    resp_stale = handler.handle_apply_request(
        req_stale, decision_time_utc="2026-11-12T09:00:10.100Z"
    )
    assert resp_stale.status == BatchStatus.REJECTED
    assert resp_stale.reason == ReasonCode.STALE.value
    assert resp_stale.rule_count == 0
    assert _count_flow_mods(driver) == 0


# =============================================================================
# 8. Timeout or late verification AFTER switch writes started (AC-S09, REQ-13)
# =============================================================================


def test_08_late_completion_after_switch_write_records_deadline_missed(
    topology: TopologyRegistry,
) -> None:
    driver, _, _, state_mgr, handler, _, _ = _build_controller_stack(topology)
    driver.clear_operation_log()

    # Pre-write validation passes at 09:00:11.500Z (< apply_deadline_utc 09:00:12Z),
    # FlowMods execute, but verification finishes at 09:00:20.500Z (> forecast_end 09:00:20Z).
    # Per AC-S09, SDN MUST NOT mask the physical change as REJECTED!
    req = _make_valid_apply_request(
        topology,
        decision_batch_id="batch_late_finish",
        forecast_start="2026-11-12T09:00:10Z",
        forecast_end="2026-11-12T09:00:20Z",
        apply_deadline_utc="2026-11-12T09:00:12Z",
    )
    resp = handler.handle_apply_request(
        req,
        decision_time_utc="2026-11-12T09:00:11.500Z",
        applied_at_utc="2026-11-12T09:00:11.800Z",
        completed_at_utc="2026-11-12T09:00:20.500Z",
    )

    assert resp.status == BatchStatus.APPLIED
    assert resp.deadline_missed is True
    assert resp.reason == ReasonCode.DEADLINE_MISSED.value
    assert resp.state_version == 2
    assert resp.rule_count == 3
    assert resp.results[0].status == FlowDecisionStatus.APPLIED
    assert resp.results[0].actual_path_id == "0x103_P2"
    assert resp.results[0].reason == ReasonCode.DEADLINE_MISSED.value
    assert state_mgr.get_flow_state("run_p4_01", "0x103").path_id == "0x103_P2"


# =============================================================================
# 9. Successful Rollback -> BatchStatus.ROLLED_BACK (AC-S04)
# =============================================================================


def test_09_rollback_success_on_source_edge_failure(
    topology: TopologyRegistry,
) -> None:
    driver, _, _, state_mgr, handler, _, _ = _build_controller_stack(topology)
    driver.clear_operation_log()

    # Inject a FlowMod failure on source edge e1 (dpid=1) during PRIMARY_SOURCE_EDGE only;
    # ROLLBACK_SOURCE_EDGE is unaffected and succeeds!
    driver.inject_flowmod_failure(
        dpid=1,
        flow_group_id="0x103",
        phase=SwitchPhaseTag.PRIMARY_SOURCE_EDGE,
        reason="SIMULATED_E1_PRIMARY_FLOWMOD_ERROR",
        timed_out=False,
    )

    req = _make_valid_apply_request(topology, decision_batch_id="batch_rb_ok")
    resp = handler.handle_apply_request(
        req,
        decision_time_utc="2026-11-12T09:00:10.200Z",
        applied_at_utc="2026-11-12T09:00:10.300Z",
    )

    assert resp.status == BatchStatus.ROLLED_BACK
    assert resp.reason == ReasonCode.FLOWMOD_ERROR.value
    # 3 primary attempts (c2 ADD, e2 ADD, e1 MODIFY_STRICT failed) + 3 rollback attempts = 6
    assert resp.rule_count == 6
    assert len(resp.results) == 1
    res0 = resp.results[0]
    assert res0.status == FlowDecisionStatus.ROLLED_BACK
    assert res0.verification_status == VerificationStatus.VERIFIED
    assert res0.actual_path_id == "0x103_P1"
    assert res0.rule_count == 6

    # Route remains verified on 0x103_P1 and controller is NOT locked in reconcile_required
    live = state_mgr.get_flow_state("run_p4_01", "0x103")
    assert live.path_id == "0x103_P1"
    assert live.verification_status == VerificationStatus.VERIFIED
    assert state_mgr.is_reconcile_required("run_p4_01") is False


# =============================================================================
# 10. UNKNOWN & INCONSISTENT (and PARTIAL multi-flow batch) (AC-S04, REQ-16)
# =============================================================================


def test_10_unknown_inconsistent_and_partial_batches_lock_reconcile(
    topology: TopologyRegistry,
) -> None:
    # 10a: Primary FlowStats readback times out AND rollback FlowStats readback times out
    # -> Flow becomes UNKNOWN (actual_path_id=None) and batch becomes INCONSISTENT
    driver, _, _, state_mgr, handler, _, _ = _build_controller_stack(topology)
    driver.clear_operation_log()

    driver.inject_flow_stats_fault(
        dpid=1,
        flow_group_id="0x103",
        timed_out=True,
    )

    req = _make_valid_apply_request(topology, decision_batch_id="batch_unk_01")
    resp = handler.handle_apply_request(
        req,
        decision_time_utc="2026-11-12T09:00:10.200Z",
        applied_at_utc="2026-11-12T09:00:10.300Z",
    )

    assert resp.status == BatchStatus.INCONSISTENT
    assert resp.reason == ReasonCode.VERIFY_TIMEOUT.value
    assert resp.state_version == 2
    assert len(resp.results) == 1
    res0 = resp.results[0]
    assert res0.status == FlowDecisionStatus.UNKNOWN
    assert res0.verification_status == VerificationStatus.UNKNOWN
    assert res0.actual_path_id is None  # NEVER fake old_path_id or new_path_id!

    assert state_mgr.is_reconcile_required("run_p4_01") is True
    snap_state = state_mgr.get_routing_state("run_p4_01")
    assert snap_state.routing_state_complete is False
    assert "0x103" in snap_state.unknown_flow_group_ids

    # Next ApplyRequest must be rejected with RECONCILIATION_REQUIRED
    next_req = _make_valid_apply_request(
        topology,
        decision_batch_id="batch_after_inconsistent",
        expected_state_version=2,
        changes=(
            ApplyFlowChange(
                flow_group_id="0x104",
                old_path_id="0x104_P1",
                new_path_id="0x104_P2",
            ),
        ),
    )
    next_resp = handler.handle_apply_request(
        next_req, decision_time_utc="2026-11-12T09:00:11.000Z"
    )
    assert next_resp.status == BatchStatus.REJECTED
    assert next_resp.reason == ReasonCode.RECONCILIATION_REQUIRED.value
    assert next_resp.rule_count == 0

    # 10b: Multi-flow batch where 0x103 succeeds (APPLIED) and 0x104 rolls back (ROLLED_BACK)
    # -> BatchStatus.PARTIAL, is_network_atomic=False, and reconcile_required=True
    driver2, _, _, state_mgr2, handler2, _, _ = _build_controller_stack(
        topology, run_id="run_partial"
    )
    driver2.inject_flowmod_failure(
        dpid=1,
        flow_group_id="0x104",
        phase=SwitchPhaseTag.PRIMARY_SOURCE_EDGE,
        reason="SIMULATED_0X104_FAIL",
        timed_out=False,
    )
    partial_req = _make_valid_apply_request(
        topology,
        run_id="run_partial",
        decision_batch_id="batch_partial_01",
        changes=(
            ApplyFlowChange("0x103", "0x103_P1", "0x103_P2"),
            ApplyFlowChange("0x104", "0x104_P1", "0x104_P2"),
        ),
    )
    partial_resp = handler2.handle_apply_request(
        partial_req,
        decision_time_utc="2026-11-12T09:00:10.200Z",
        applied_at_utc="2026-11-12T09:00:10.300Z",
    )
    assert partial_resp.status == BatchStatus.PARTIAL
    assert partial_resp.is_network_atomic is False
    assert partial_resp.results[0].status == FlowDecisionStatus.APPLIED
    assert partial_resp.results[0].actual_path_id == "0x103_P2"
    assert partial_resp.results[1].status == FlowDecisionStatus.ROLLED_BACK
    assert partial_resp.results[1].actual_path_id == "0x104_P1"
    assert state_mgr2.is_reconcile_required("run_partial") is True


# =============================================================================
# 11. Causal Snapshot (VALID)
# =============================================================================


def test_11_causal_snapshot_valid(topology: TopologyRegistry) -> None:
    (
        _driver,
        _verifier,
        _installer,
        _state_mgr,
        handler,
        demand_provider,
        snap_creator,
    ) = _build_controller_stack(topology)

    flow_ids = [fg.flow_group_id for fg in topology.get_all_flow_groups()]
    demand_provider.populate_uniform_window(
        run_id="run_p4_01",
        flow_group_ids=flow_ids,
        origin_refs=DEFAULT_ORIGINS,
        start_bin=0,
        end_bin_exclusive=10,
        base_mbps=15.0,
    )

    snap = snap_creator.create_snapshot(
        run_id="run_p4_01",
        cutoff=10,
        lookback_L=5,
        snapshot_as_of_utc="2026-11-12T09:00:10.100Z",
    )

    assert snap.quality_status == SnapshotQualityStatus.VALID
    assert snap.is_valid is True
    assert snap.reason is None
    assert snap.expected_bin_count == 48 * 5
    assert snap.valid_bin_count == 48 * 5
    assert len(snap.history) == 240
    assert len(snap.routing_state) == 48
    assert snap.routing_state_complete is True
    assert len(snap.missing_groups) == 0
    assert len(snap.unknown_flow_group_ids) == 0
    assert snap.verify_content_hash() is True
    assert snap.provenance.demand_channel_status == "MUST_LOCK_MOCK_PROVIDER"

    # Every history item in [t - L*, t) = [5, 10) obeys all 3 causality inequalities
    cutoff_dt = parse_utc_timestamp(snap.history_cutoff_utc)
    as_of_dt = parse_utc_timestamp(snap.snapshot_as_of_utc)
    for item in snap.history:
        assert 5 <= item.bin_index < 10
        assert parse_utc_timestamp(item.window_end_utc) <= cutoff_dt
        assert item.released_at_utc is not None
        assert item.available_at_utc is not None
        assert parse_utc_timestamp(item.released_at_utc) <= as_of_dt
        assert parse_utc_timestamp(item.available_at_utc) <= as_of_dt
        assert item.valid is True
        assert item.requested_mbps is not None and item.requested_mbps > 0.0

    # Register snapshot in DecisionHandler and confirm ApplyRequest referencing it succeeds
    handler.register_snapshot(snap)
    req = _make_valid_apply_request(
        topology,
        snapshot_id=snap.snapshot_id,
    )
    resp = handler.handle_apply_request(
        req, decision_time_utc="2026-11-12T09:00:10.500Z"
    )
    assert resp.status == BatchStatus.APPLIED


# =============================================================================
# 12. Future Data Leakage Prevention & Snapshot Immutability (AC-D05, AC-S10)
# =============================================================================


def test_12_snapshot_future_data_leakage_prevention_and_immutability(
    topology: TopologyRegistry,
) -> None:
    _, _, _, _, _, demand_provider, snap_creator = _build_controller_stack(topology)
    flow_ids = [fg.flow_group_id for fg in topology.get_all_flow_groups()]

    # Populate causal bins [0, 10)
    demand_provider.populate_uniform_window(
        run_id="run_p4_01",
        flow_group_ids=flow_ids,
        origin_refs=DEFAULT_ORIGINS,
        start_bin=0,
        end_bin_exclusive=10,
        base_mbps=12.0,
    )
    snap_without_future = snap_creator.create_snapshot(
        run_id="run_p4_01",
        cutoff=10,
        lookback_L=5,
        snapshot_as_of_utc="2026-11-12T09:00:10.100Z",
        snapshot_id="snap_no_future",
    )

    # Now inject future demand bins [10, 20) (window_end_utc > history_cutoff_utc) with huge spike 999.0 Mbps
    demand_provider.populate_uniform_window(
        run_id="run_p4_01",
        flow_group_ids=flow_ids,
        origin_refs=DEFAULT_ORIGINS,
        start_bin=10,
        end_bin_exclusive=20,
        base_mbps=999.0,
        release_delay_s=-5.0,  # Even if marked released early, window_end_utc > cutoff_utc!
        available_delay_s=-5.0,
    )
    snap_with_future = snap_creator.create_snapshot(
        run_id="run_p4_01",
        cutoff=10,
        lookback_L=5,
        snapshot_as_of_utc="2026-11-12T09:00:10.100Z",
        snapshot_id="snap_with_future",
    )

    # History items must be 100% identical; future bins [10, 20) must be completely excluded
    assert snap_without_future.history == snap_with_future.history
    for item in snap_with_future.history:
        assert item.bin_index < 10
        assert item.requested_mbps is not None and item.requested_mbps < 100.0

    # Re-fetching the closed snapshot "snap_no_future" returns the exact frozen instance
    closed_again = snap_creator.create_snapshot(
        run_id="run_p4_01",
        cutoff=10,
        lookback_L=5,
        snapshot_as_of_utc="2026-11-12T09:00:10.100Z",
        snapshot_id="snap_no_future",
    )
    assert closed_again == snap_without_future
    assert closed_again.content_hash == snap_without_future.content_hash


# =============================================================================
# 13. Missing or Late released_at_utc / available_at_utc Timestamps
# =============================================================================


def test_13_missing_or_late_release_and_available_timestamps(
    topology: TopologyRegistry,
) -> None:
    _, _, _, _, _, demand_provider, snap_creator = _build_controller_stack(topology)
    flow_ids = [fg.flow_group_id for fg in topology.get_all_flow_groups()]

    # Populate bins [5, 9) normally, and bin 9 with late available_at_utc > snapshot_as_of_utc
    demand_provider.populate_uniform_window(
        run_id="run_p4_01",
        flow_group_ids=flow_ids,
        origin_refs=DEFAULT_ORIGINS,
        start_bin=5,
        end_bin_exclusive=9,
        base_mbps=20.0,
    )
    demand_provider.populate_uniform_window(
        run_id="run_p4_01",
        flow_group_ids=flow_ids,
        origin_refs=DEFAULT_ORIGINS,
        start_bin=9,
        end_bin_exclusive=10,
        base_mbps=20.0,
        release_delay_s=0.0,
        available_delay_s=0.5,  # available at 09:00:10.500Z > snapshot_as_of_utc (09:00:10.100Z)
    )

    snap_late_avail = snap_creator.create_snapshot(
        run_id="run_p4_01",
        cutoff=10,
        lookback_L=5,
        snapshot_as_of_utc="2026-11-12T09:00:10.100Z",
        snapshot_id="snap_late_avail",
    )
    assert snap_late_avail.quality_status == SnapshotQualityStatus.INCOMPLETE_HISTORY
    assert snap_late_avail.is_valid is False
    assert snap_late_avail.reason == ReasonCode.DATA_MISSING.value
    assert snap_late_avail.valid_bin_count == 48 * 4  # bin 9 rejected across all 48 flows

    bin9_items = [h for h in snap_late_avail.history if h.bin_index == 9]
    assert len(bin9_items) == 48
    for h in bin9_items:
        assert h.requested_mbps is None  # NEVER filled with 0.0!
        assert h.valid is False
        assert h.reason == ReasonCode.DATA_MISSING.value


# =============================================================================
# 14. Missing Demand History (Never 0.0) vs Genuine 0.0 + Forbidden Sources
# =============================================================================


def test_14_missing_demand_never_zero_filled_and_forbidden_sources_rejected(
    topology: TopologyRegistry,
) -> None:
    _, _, _, _, _, demand_provider, snap_creator = _build_controller_stack(topology)
    flow_ids = [fg.flow_group_id for fg in topology.get_all_flow_groups()]

    # 14a: Genuine 0.0 demand is valid!
    demand_provider.populate_uniform_window(
        run_id="run_p4_01",
        flow_group_ids=flow_ids,
        origin_refs=DEFAULT_ORIGINS,
        start_bin=5,
        end_bin_exclusive=10,
        base_mbps=0.0,
    )
    # Overwrite flow 0x103 at bin 5 with exact 0.0 Mbps
    demand_provider.add_observations(
        [
            DemandObservationRecord(
                run_id="run_p4_01",
                flow_group_id="0x103",
                bin_index=5,
                window_start_utc="2026-11-12T09:00:05Z",
                window_end_utc="2026-11-12T09:00:06Z",
                requested_mbps=0.0,
                released_at_utc="2026-11-12T09:00:06Z",
                available_at_utc="2026-11-12T09:00:06Z",
                source_kind=DemandSourceKind.RELEASED_DEMAND.value,
                valid=True,
            )
        ]
    )
    snap_zero = snap_creator.create_snapshot(
        run_id="run_p4_01",
        cutoff=10,
        lookback_L=5,
        snapshot_id="snap_genuine_zero",
    )
    assert snap_zero.is_valid is True
    item_0x103_b5 = next(
        h
        for h in snap_zero.history
        if h.flow_group_id == "0x103" and h.bin_index == 5
    )
    assert item_0x103_b5.requested_mbps == 0.0
    assert item_0x103_b5.valid is True

    # 14b: Missing bin -> requested_mbps is None (never 0.0), valid=False, reason="DATA_MISSING"
    demand_provider.clear_run("run_p4_01")
    demand_provider.populate_uniform_window(
        run_id="run_p4_01",
        flow_group_ids=[fid for fid in flow_ids if fid != "0x103"],
        origin_refs=DEFAULT_ORIGINS,
        start_bin=5,
        end_bin_exclusive=10,
        base_mbps=10.0,
    )
    snap_missing = snap_creator.create_snapshot(
        run_id="run_p4_01",
        cutoff=10,
        lookback_L=5,
        snapshot_id="snap_missing_0x103",
    )
    assert snap_missing.quality_status == SnapshotQualityStatus.INCOMPLETE_HISTORY
    assert snap_missing.is_valid is False
    assert snap_missing.missing_groups == ("0x103",)
    for h in snap_missing.history:
        if h.flow_group_id == "0x103":
            assert h.requested_mbps is None
            assert h.valid is False
            assert h.reason == ReasonCode.DATA_MISSING.value

    # 14c: Forbidden demand source (goodput_mbps / port_tx_mbps) -> rejected as DATA_INVALID
    demand_provider.populate_uniform_window(
        run_id="run_p4_01",
        flow_group_ids=["0x103"],
        origin_refs=DEFAULT_ORIGINS,
        start_bin=5,
        end_bin_exclusive=10,
        base_mbps=25.0,
        source_kind="goodput_mbps",
    )
    snap_forbidden = snap_creator.create_snapshot(
        run_id="run_p4_01",
        cutoff=10,
        lookback_L=5,
        snapshot_id="snap_forbidden_goodput",
    )
    assert snap_forbidden.quality_status == SnapshotQualityStatus.INVALID_DATA
    assert snap_forbidden.is_valid is False
    assert snap_forbidden.reason == ReasonCode.DATA_INVALID.value
    for h in snap_forbidden.history:
        if h.flow_group_id == "0x103":
            assert h.requested_mbps is None
            assert h.valid is False
            assert h.reason == ReasonCode.DATA_INVALID.value

    # Also verify strict_raise_on_forbidden_source=True raises ForbiddenDemandSourceError
    strict_creator = SnapshotCreator(
        topology_registry=topology,
        state_manager=snap_creator.state_manager,
        demand_provider=demand_provider,
        config=SnapshotCreatorConfig(strict_raise_on_forbidden_source=True),
    )
    with pytest.raises(ForbiddenDemandSourceError):
        strict_creator.create_snapshot(
            run_id="run_p4_01",
            cutoff=10,
            lookback_L=5,
            snapshot_id="snap_strict_raise",
        )


# =============================================================================
# 15. Route Not VERIFIED -> Snapshot INVALID_ROUTING_STATE (AC-S10)
# =============================================================================


def test_15_snapshot_invalid_when_routing_state_unverified_or_unknown(
    topology: TopologyRegistry,
) -> None:
    # 15a: Unverified bootstrap
    _, _, _, state_mgr_unv, _, demand_unv, snap_creator_unv = _build_controller_stack(
        topology,
        run_id="run_snap_unv",
        verify_bootstrap=False,
    )
    flow_ids = [fg.flow_group_id for fg in topology.get_all_flow_groups()]
    demand_unv.populate_uniform_window(
        run_id="run_snap_unv",
        flow_group_ids=flow_ids,
        origin_refs=DEFAULT_ORIGINS,
        start_bin=5,
        end_bin_exclusive=10,
        base_mbps=10.0,
    )
    snap_unv = snap_creator_unv.create_snapshot(
        run_id="run_snap_unv",
        cutoff=10,
        lookback_L=5,
    )
    assert snap_unv.quality_status == SnapshotQualityStatus.INVALID_ROUTING_STATE
    assert snap_unv.is_valid is False
    assert snap_unv.routing_state_complete is False
    assert len(snap_unv.unknown_flow_group_ids) == 48

    # 15b: Verified run transitions to UNKNOWN on controller restart
    _, _, _, state_mgr_ok, _, demand_ok, snap_creator_ok = _build_controller_stack(
        topology,
        run_id="run_snap_restart",
        verify_bootstrap=True,
    )
    demand_ok.populate_uniform_window(
        run_id="run_snap_restart",
        flow_group_ids=flow_ids,
        origin_refs=DEFAULT_ORIGINS,
        start_bin=5,
        end_bin_exclusive=10,
        base_mbps=10.0,
    )
    state_mgr_ok.mark_controller_restart(
        "run_snap_restart", restarted_at_utc="2026-11-12T09:00:08Z"
    )
    snap_after_restart = snap_creator_ok.create_snapshot(
        run_id="run_snap_restart",
        cutoff=10,
        lookback_L=5,
    )
    assert (
        snap_after_restart.quality_status
        == SnapshotQualityStatus.INVALID_ROUTING_STATE
    )
    assert snap_after_restart.is_valid is False
    assert snap_after_restart.routing_state_complete is False
    assert snap_after_restart.reason == ReasonCode.ROUTE_STATE_UNKNOWN.value
    assert state_mgr_unv.is_reconcile_required("run_snap_unv") is True


# =============================================================================
# 16. State Isolation Between Multiple run_id Instances
# =============================================================================


def test_16_multi_run_state_and_snapshot_isolation(
    topology: TopologyRegistry,
) -> None:
    driver = InMemorySwitchDriver(topology)
    matcher = FlowMatcher(topology)
    compiler = RouteCompiler(topology, matcher)
    verifier = RuleVerifier(compiler, driver)
    installer = RuleInstaller(compiler, driver, verifier)
    state_mgr = StateManager(topology)

    ev_a = installer.bootstrap_initial_paths("run_A", "2026-11-12T09:00:00Z")
    state_mgr.initialize_run(
        "run_A", "2026-11-12T09:00:00Z", verification_evidences=ev_a
    )
    ev_b = installer.bootstrap_initial_paths("run_B", "2026-11-12T09:00:00Z")
    state_mgr.initialize_run(
        "run_B", "2026-11-12T09:00:00Z", verification_evidences=ev_b
    )

    demand_provider = MockReleasedDemandProvider()
    link_provider = MockLinkTelemetryProvider()
    snap_creator = SnapshotCreator(
        topology,
        state_mgr,
        demand_provider,
        link_telemetry_provider=link_provider,
    )
    snap_creator.set_run_origin_refs("run_A", DEFAULT_ORIGINS)
    snap_creator.set_run_origin_refs("run_B", DEFAULT_ORIGINS)

    handler = DecisionHandler(
        topology,
        state_mgr,
        installer,
        config=DecisionHandlerConfig.for_mock_unit_tests(),
        snapshot_lookup=snap_creator.get_closed_snapshot,
    )

    # Apply route change 0x103_P1 -> 0x103_P2 in run_A with TransportEnvelope
    env_a = TransportEnvelope(
        run_id="run_A",
        topology_version=topology.topology_version,
        topology_hash=topology.topology_hash,
    )
    req_a = ApplyRequest.from_dict(
        {
            "run_id": "run_A",
            "decision_batch_id": "batch_shared_01",
            "snapshot_id": "snap_A_1",
            "expected_state_version": 1,
            "config_version": "1.0",
            "forecast_start": "2026-11-12T09:00:10Z",
            "forecast_end": "2026-11-12T09:00:20Z",
            "apply_deadline_utc": "2026-11-12T09:00:12Z",
            "changes": [
                {
                    "flow_group_id": "0x103",
                    "old_path_id": "0x103_P1",
                    "new_path_id": "0x103_P2",
                }
            ],
        },
        envelope=env_a,
    )
    resp_a = handler.handle_apply_request(
        req_a,
        decision_time_utc="2026-11-12T09:00:10.200Z",
        applied_at_utc="2026-11-12T09:00:10.300Z",
    )
    assert resp_a.status == BatchStatus.APPLIED
    assert resp_a.state_version == 2

    # Verify run_B is completely untouched (state_version == 1, 0x103 still on 0x103_P1)
    assert state_mgr.get_state_version("run_B") == 1
    assert state_mgr.get_flow_state("run_B", "0x103").path_id == "0x103_P1"

    # Mark controller restart on run_A; verify run_B can still create a VALID snapshot
    state_mgr.mark_controller_restart("run_A", "2026-11-12T09:00:11Z")
    assert state_mgr.is_reconcile_required("run_A") is True
    assert state_mgr.is_reconcile_required("run_B") is False

    flow_ids = [fg.flow_group_id for fg in topology.get_all_flow_groups()]
    demand_provider.populate_uniform_window(
        run_id="run_B",
        flow_group_ids=flow_ids,
        origin_refs=DEFAULT_ORIGINS,
        start_bin=5,
        end_bin_exclusive=10,
        base_mbps=18.0,
    )
    link_provider.add_samples(
        "run_B",
        [
            LinkSampleRef(
                sample_id="s1",
                link_id="e1>c1",
                sample_at_utc="2026-11-12T09:00:09Z",
                available_at_utc="2026-11-12T09:00:09.500Z",
            )
        ],
    )
    snap_b = snap_creator.create_snapshot(
        run_id="run_B",
        cutoff="2026-11-12T09:00:10Z",
        lookback_L=5,
    )
    assert snap_b.is_valid is True
    assert snap_b.state_version == 1
    assert len(snap_b.link_sample_refs) == 1
    assert snap_b.link_sample_refs[0].link_id == "e1>c1"


# =============================================================================
# 17. Hold-Down Enforcement & Canonical ID Validation in DecisionHandler
# =============================================================================


def test_17_hold_down_and_legacy_id_rejection_in_decision_handler(
    topology: TopologyRegistry,
) -> None:
    driver, _, _, state_mgr, handler, _, _ = _build_controller_stack(topology)

    # First switch 0x103_P1 -> 0x103_P2 at t = 10.300s
    req1 = _make_valid_apply_request(topology, decision_batch_id="batch_hd_1")
    resp1 = handler.handle_apply_request(
        req1,
        decision_time_utc="2026-11-12T09:00:10.200Z",
        applied_at_utc="2026-11-12T09:00:10.300Z",
    )
    assert resp1.status == BatchStatus.APPLIED
    driver.clear_operation_log()

    # Attempt to switch 0x103_P2 -> 0x103_P1 at t = 15.0s (elapsed 4.7s < T_hold=10s) -> HOLDDOWN
    req_hd = _make_valid_apply_request(
        topology,
        decision_batch_id="batch_hd_2",
        expected_state_version=2,
        forecast_start="2026-11-12T09:00:15Z",
        forecast_end="2026-11-12T09:00:25Z",
        apply_deadline_utc="2026-11-12T09:00:17Z",
        changes=(
            ApplyFlowChange(
                flow_group_id="0x103",
                old_path_id="0x103_P2",
                new_path_id="0x103_P1",
            ),
        ),
    )
    resp_hd = handler.handle_apply_request(
        req_hd, decision_time_utc="2026-11-12T09:00:15.000Z"
    )
    assert resp_hd.status == BatchStatus.REJECTED
    assert resp_hd.reason == ReasonCode.HOLDDOWN.value
    assert resp_hd.rule_count == 0
    assert _count_flow_mods(driver) == 0

    # Legacy ID ("h1_h3" or bare "P2") -> REJECTED (INVALID_REQUEST)
    req_legacy = _make_valid_apply_request(
        topology,
        decision_batch_id="batch_legacy_id",
        expected_state_version=2,
        changes=(
            ApplyFlowChange(
                flow_group_id="h1_h3",
                old_path_id="P1",
                new_path_id="P2",
            ),
        ),
    )
    resp_legacy = handler.handle_apply_request(
        req_legacy, decision_time_utc="2026-11-12T09:00:11.000Z"
    )
    assert resp_legacy.status == BatchStatus.REJECTED
    assert resp_legacy.reason == ReasonCode.INVALID_REQUEST.value
    assert resp_legacy.rule_count == 0
    assert state_mgr.get_state_version("run_p4_01") == 2


# =============================================================================
# 18. FINAL AUDIT — Crash Consistency (Pre-FlowMod, Mid-FlowMod, Post-Switch/Pre-Commit, Restart)
# =============================================================================


def test_18_crash_consistency_exception_before_and_after_flowmod_and_before_commit(
    topology: TopologyRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver, _, installer, state_mgr, handler, _, _ = _build_controller_stack(topology)
    driver.clear_operation_log()

    # 18a: Exception / invalid backward timestamp BEFORE sending any FlowMod ->
    # Rejected cleanly with rule_count=0 and state_version unchanged (still 1)
    req_backward = _make_valid_apply_request(
        topology,
        decision_batch_id="batch_crash_pre_write",
    )
    resp_pre = handler.handle_apply_request(
        req_backward,
        decision_time_utc="2026-11-12T08:59:59Z",  # Earlier than bootstrap 09:00:00Z!
    )
    assert resp_pre.status == BatchStatus.REJECTED
    assert resp_pre.reason == ReasonCode.INVALID_REQUEST.value
    assert resp_pre.rule_count == 0
    assert _count_flow_mods(driver) == 0
    assert state_mgr.get_state_version("run_p4_01") == 1
    assert state_mgr.is_reconcile_required("run_p4_01") is False

    # 18b: Driver raises SwitchDriverError during send_flow_mod on e_src (after downstream FlowMods) ->
    # RuleInstaller catches the exception, executes rollback to 0x103_P1, and verifies P1!
    orig_send_flow_mod = driver.send_flow_mod
    raised_once = {"done": False}

    def _raising_send_flow_mod(
        spec, command, *, phase=SwitchPhaseTag.PRIMARY_SOURCE_EDGE
    ):
        if (
            phase == SwitchPhaseTag.PRIMARY_SOURCE_EDGE
            and spec.flow_group_id == "0x103"
            and not raised_once["done"]
        ):
            raised_once["done"] = True
            raise SwitchDriverError("SIMULATED_SOCKET_EXCEPTION_ON_E_SRC")
        return orig_send_flow_mod(spec, command, phase=phase)

    monkeypatch.setattr(driver, "send_flow_mod", _raising_send_flow_mod)
    req_mid = _make_valid_apply_request(
        topology,
        decision_batch_id="batch_crash_mid_flowmod",
    )
    resp_mid = handler.handle_apply_request(
        req_mid,
        decision_time_utc="2026-11-12T09:00:10.200Z",
        applied_at_utc="2026-11-12T09:00:10.300Z",
    )
    monkeypatch.setattr(driver, "send_flow_mod", orig_send_flow_mod)

    assert resp_mid.status == BatchStatus.ROLLED_BACK
    assert resp_mid.results[0].status == FlowDecisionStatus.ROLLED_BACK
    assert resp_mid.results[0].actual_path_id == "0x103_P1"
    assert resp_mid.rule_count >= 5  # 2 downstream + 1 raising source + rollback rules
    assert state_mgr.get_flow_state("run_p4_01", "0x103").path_id == "0x103_P1"
    assert state_mgr.is_reconcile_required("run_p4_01") is False

    # 18c: Exception AFTER switch has already changed route to 0x103_P2, BEFORE StateManager commits!
    # Verify:
    # - Controller NEVER returns a fake REJECTED!
    # - Controller returns BatchStatus.INCONSISTENT with status=UNKNOWN, actual_path_id=None
    # - StateManager transitions 0x103 to UNKNOWN (reconcile_required=True)
    # - Subsequent recover_and_reconcile_run() reads the live switch tables and recovers 0x103_P2!
    orig_commit = state_mgr.commit_batch_outcome

    def _crashing_commit(*args, **kwargs):
        raise RuntimeError("SIMULATED_CRASH_AFTER_SWITCH_CHANGED_BEFORE_COMMIT")

    monkeypatch.setattr(state_mgr, "commit_batch_outcome", _crashing_commit)

    req_post_switch = _make_valid_apply_request(
        topology,
        decision_batch_id="batch_crash_before_commit",
        expected_state_version=state_mgr.get_state_version("run_p4_01"),
        forecast_start="2026-11-12T09:00:21Z",
        forecast_end="2026-11-12T09:00:31Z",
        apply_deadline_utc="2026-11-12T09:00:23Z",
    )
    resp_crash = handler.handle_apply_request(
        req_post_switch,
        decision_time_utc="2026-11-12T09:00:21.000Z",
        applied_at_utc="2026-11-12T09:00:21.100Z",
    )
    monkeypatch.setattr(state_mgr, "commit_batch_outcome", orig_commit)

    # Must NOT return fake REJECTED!
    assert resp_crash.status == BatchStatus.INCONSISTENT
    assert resp_crash.status != BatchStatus.REJECTED
    assert resp_crash.reason == ReasonCode.ROUTE_STATE_UNKNOWN.value
    assert resp_crash.results[0].status == FlowDecisionStatus.UNKNOWN
    assert resp_crash.results[0].verification_status == VerificationStatus.UNKNOWN
    assert resp_crash.results[0].actual_path_id is None
    assert state_mgr.is_reconcile_required("run_p4_01") is True
    assert state_mgr.get_flow_state("run_p4_01", "0x103").path_id is None

    # Physical switch already has 0x103_P2 installed; now simulate controller restart + reconciliation
    handler.notify_controller_restart("run_p4_01", "2026-11-12T09:00:22.000Z")
    reconciled_state = handler.recover_and_reconcile_run(
        "run_p4_01",
        "2026-11-12T09:00:22.500Z",
    )
    assert reconciled_state.reconcile_required is False
    assert reconciled_state.routing_state_complete is True
    assert state_mgr.get_flow_state("run_p4_01", "0x103").path_id == "0x103_P2"
    assert (
        state_mgr.get_flow_state("run_p4_01", "0x103").verification_status
        == VerificationStatus.VERIFIED
    )


# =============================================================================
# 19. FINAL AUDIT — Idempotency Across Controller Restart & Non-Durable Cache
# =============================================================================


def test_19_idempotency_across_controller_restart_and_volatile_store_isolation(
    topology: TopologyRegistry,
) -> None:
    driver, _, _, state_mgr, handler, _, _ = _build_controller_stack(topology)

    # Confirm explicit non-durable status of in-memory idempotency store
    assert isinstance(handler.idempotency_store, InMemoryBatchIdempotencyStore)
    assert handler.idempotency_store.is_durable is False
    assert (
        handler.idempotency_store.store_status
        == "VOLATILE_IN_MEMORY_MUST_LOCK_DURABLE_STORE"
    )

    req = _make_valid_apply_request(topology, decision_batch_id="batch_idem_restart")
    resp1 = handler.handle_apply_request(
        req,
        decision_time_utc="2026-11-12T09:00:10.200Z",
        applied_at_utc="2026-11-12T09:00:10.300Z",
    )
    assert resp1.status == BatchStatus.APPLIED
    assert resp1.state_version == 2
    assert resp1.idempotent_replay is False

    # Duplicate while controller is running -> idempotent replay with 0 new FlowMods
    driver.clear_operation_log()
    resp_replay = handler.handle_apply_request(
        req,
        decision_time_utc="2026-11-12T09:00:10.500Z",
    )
    assert resp_replay.status == BatchStatus.APPLIED
    assert resp_replay.idempotent_replay is True
    assert _count_flow_mods(driver) == 0

    # Controller restarts -> volatile in-memory idempotency cache is wiped and run is marked UNKNOWN
    handler.notify_controller_restart("run_p4_01", "2026-11-12T09:00:11.000Z")
    assert handler.idempotency_store.get("run_p4_01", "batch_idem_restart") is None

    # Duplicate request arriving after restart BEFORE reconciliation must be rejected with
    # RECONCILIATION_REQUIRED (0 FlowMods), NOT served from volatile memory!
    resp_unreconciled = handler.handle_apply_request(
        req,
        decision_time_utc="2026-11-12T09:00:11.200Z",
    )
    assert resp_unreconciled.status == BatchStatus.REJECTED
    assert resp_unreconciled.reason == ReasonCode.RECONCILIATION_REQUIRED.value
    assert resp_unreconciled.idempotent_replay is False
    assert resp_unreconciled.rule_count == 0
    assert _count_flow_mods(driver) == 0

    # Reconcile run from live switch state -> 0x103 is verified on 0x103_P2, state_version advances to 4
    handler.recover_and_reconcile_run("run_p4_01", "2026-11-12T09:00:11.500Z")
    assert state_mgr.get_state_version("run_p4_01") == 4
    assert state_mgr.get_flow_state("run_p4_01", "0x103").path_id == "0x103_P2"

    # Re-sending the old request (expected_state_version=1, old_path_id=0x103_P1) after reconciliation
    # is safely rejected pre-write with VERSION_MISMATCH (0 FlowMods)
    resp_after_reconcile = handler.handle_apply_request(
        req,
        decision_time_utc="2026-11-12T09:00:11.800Z",
    )
    assert resp_after_reconcile.status == BatchStatus.REJECTED
    assert resp_after_reconcile.reason == ReasonCode.VERSION_MISMATCH.value
    assert resp_after_reconcile.rule_count == 0
    assert _count_flow_mods(driver) == 0


# =============================================================================
# 20. FINAL AUDIT — Snapshot Causal Routing History & No Future Route Leakage
# =============================================================================


def test_20_snapshot_causal_routing_history_in_window_and_no_future_route_leakage(
    topology: TopologyRegistry,
) -> None:
    _, _, _, state_mgr, handler, demand_provider, snap_creator = (
        _build_controller_stack(topology)
    )
    flow_ids = [fg.flow_group_id for fg in topology.get_all_flow_groups()]
    demand_provider.populate_uniform_window(
        run_id="run_p4_01",
        flow_group_ids=flow_ids,
        origin_refs=DEFAULT_ORIGINS,
        start_bin=0,
        end_bin_exclusive=20,
        base_mbps=15.0,
    )

    # At t = 7s (09:00:07Z), inside observation window [5s, 10s), switch 0x103 from P1 -> P2
    req_t7 = _make_valid_apply_request(
        topology,
        decision_batch_id="batch_at_t7",
        forecast_start="2026-11-12T09:00:07Z",
        forecast_end="2026-11-12T09:00:17Z",
        apply_deadline_utc="2026-11-12T09:00:09Z",
    )
    resp_t7 = handler.handle_apply_request(
        req_t7,
        decision_time_utc="2026-11-12T09:00:06.800Z",
        applied_at_utc="2026-11-12T09:00:07.000Z",
        completed_at_utc="2026-11-12T09:00:07.000Z",
    )
    assert resp_t7.status == BatchStatus.APPLIED

    # Create snapshot for cutoff=10 (window [5, 10), snapshot_as_of_utc = 09:00:10.100Z)
    snap_before_t18 = snap_creator.create_snapshot(
        run_id="run_p4_01",
        cutoff=10,
        lookback_L=5,
        snapshot_as_of_utc="2026-11-12T09:00:10.100Z",
        snapshot_id="snap_cutoff10_before_t18",
    )
    assert snap_before_t18.is_valid is True

    # Verify routing_history for 0x103 in window [09:00:05Z, 09:00:10Z) captures BOTH P1 and P2 intervals!
    hist_0x103 = [
        iv for iv in snap_before_t18.routing_history if iv.flow_group_id == "0x103"
    ]
    assert len(hist_0x103) == 2
    assert hist_0x103[0].path_id == "0x103_P1"
    assert hist_0x103[0].valid_from == "2026-11-12T09:00:00Z"
    assert hist_0x103[0].valid_to == "2026-11-12T09:00:07Z"
    assert hist_0x103[1].path_id == "0x103_P2"
    assert hist_0x103[1].valid_from == "2026-11-12T09:00:07Z"
    assert hist_0x103[1].valid_to is None  # Still active as of 09:00:10.100Z!

    route_0x103_at_10 = next(
        r for r in snap_before_t18.routing_state if r.flow_group_id == "0x103"
    )
    assert route_0x103_at_10.path_id == "0x103_P2"

    # Now at t = 18s (09:00:18Z, after hold-down T_hold=10s from t=7s), switch 0x103 back P2 -> P1!
    req_t18 = _make_valid_apply_request(
        topology,
        decision_batch_id="batch_at_t18",
        expected_state_version=2,
        forecast_start="2026-11-12T09:00:18Z",
        forecast_end="2026-11-12T09:00:28Z",
        apply_deadline_utc="2026-11-12T09:00:20Z",
        changes=(
            ApplyFlowChange(
                flow_group_id="0x103",
                old_path_id="0x103_P2",
                new_path_id="0x103_P1",
            ),
        ),
    )
    resp_t18 = handler.handle_apply_request(
        req_t18,
        decision_time_utc="2026-11-12T09:00:17.800Z",
        applied_at_utc="2026-11-12T09:00:18.000Z",
        completed_at_utc="2026-11-12T09:00:18.000Z",
    )
    assert resp_t18.status == BatchStatus.APPLIED
    assert state_mgr.get_flow_state("run_p4_01", "0x103").path_id == "0x103_P1"

    # Reconstruct a causal snapshot for cutoff=10 (as_of=09:00:10.100Z) AFTER the t=18s change occurred:
    # Future route transition at t=18s MUST NOT leak into cutoff=10 snapshot!
    snap_after_t18 = snap_creator.create_snapshot(
        run_id="run_p4_01",
        cutoff=10,
        lookback_L=5,
        snapshot_as_of_utc="2026-11-12T09:00:10.100Z",
        snapshot_id="snap_cutoff10_after_t18",
    )
    assert snap_after_t18.state_version == snap_before_t18.state_version == 2
    assert snap_after_t18.routing_state == snap_before_t18.routing_state
    assert snap_after_t18.routing_history == snap_before_t18.routing_history


# =============================================================================
# 21. FINAL AUDIT — Snapshot Deep Immutability & Content Hash Tamper Detection
# =============================================================================


def test_21_snapshot_deep_immutability_and_hash_verification(
    topology: TopologyRegistry,
) -> None:
    _, _, _, _, _, demand_provider, snap_creator = _build_controller_stack(topology)
    flow_ids = [fg.flow_group_id for fg in topology.get_all_flow_groups()]
    demand_provider.populate_uniform_window(
        run_id="run_p4_01",
        flow_group_ids=flow_ids,
        origin_refs=DEFAULT_ORIGINS,
        start_bin=5,
        end_bin_exclusive=10,
        base_mbps=10.0,
    )

    snap = snap_creator.create_snapshot(
        run_id="run_p4_01",
        cutoff=10,
        lookback_L=5,
        snapshot_id="snap_immutable_01",
    )
    assert snap.verify_content_hash() is True

    # 1. Attempting to mutate top-level Snapshot or nested dataclass fields raises FrozenInstanceError
    with pytest.raises(FrozenInstanceError):
        snap.quality_status = SnapshotQualityStatus.INVALID_DATA  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        snap.history[0].requested_mbps = 999.0  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        snap.routing_state[0].path_id = "0x103_P2"  # type: ignore[misc]

    # 2. All container fields on Snapshot are immutable tuples
    assert isinstance(snap.history, tuple)
    assert isinstance(snap.routing_state, tuple)
    assert isinstance(snap.routing_history, tuple)
    assert isinstance(snap.missing_groups, tuple)
    assert isinstance(snap.unknown_flow_group_ids, tuple)
    assert isinstance(snap.link_sample_refs, tuple)

    # 3. Even if Snapshot is constructed directly with mutable lists, __post_init__ coerces them to tuples
    mutable_history_list = list(snap.history)
    snap_from_lists = Snapshot(
        snapshot_id="snap_list_test",
        run_id=snap.run_id,
        snapshot_as_of_utc=snap.snapshot_as_of_utc,
        history_cutoff_utc=snap.history_cutoff_utc,
        cutoff_offset_s=snap.cutoff_offset_s,
        lookback_L=snap.lookback_L,
        delta_s=snap.delta_s,
        state_version=snap.state_version,
        config_version=snap.config_version,
        config_hash=snap.config_hash,
        topology_version=snap.topology_version,
        topology_hash=snap.topology_hash,
        content_hash=snap.content_hash,
        origin_refs=snap.origin_refs,
        flow_group_ids=list(snap.flow_group_ids),  # type: ignore[arg-type]
        history=mutable_history_list,  # type: ignore[arg-type]
        routing_state=list(snap.routing_state),  # type: ignore[arg-type]
        link_sample_refs=list(snap.link_sample_refs),  # type: ignore[arg-type]
        quality_status=snap.quality_status,
        missing_groups=list(snap.missing_groups),  # type: ignore[arg-type]
        routing_state_complete=snap.routing_state_complete,
        unknown_flow_group_ids=list(snap.unknown_flow_group_ids),  # type: ignore[arg-type]
        provenance=snap.provenance,
        reason=snap.reason,
        expected_bin_count=snap.expected_bin_count,
        valid_bin_count=snap.valid_bin_count,
        routing_history=list(snap.routing_history),  # type: ignore[arg-type]
    )
    mutable_history_list.clear()
    assert isinstance(snap_from_lists.history, tuple)
    assert len(snap_from_lists.history) == 240

    # 4. Tampering with serialized payload changes content_hash and fails verification
    payload = snap.to_dict()
    assert compute_snapshot_content_hash(payload) == snap.content_hash
    payload["history"][0]["requested_mbps"] = 999.0
    assert compute_snapshot_content_hash(payload) != snap.content_hash


# =============================================================================
# 22. FINAL AUDIT — Explicit MUST-LOCK Tags & Role Boundary Verification
# =============================================================================


def test_22_must_lock_parameter_tags_and_role_boundaries(
    topology: TopologyRegistry,
) -> None:
    _, _, _, state_mgr, handler, demand_provider, _ = _build_controller_stack(topology)

    # 1. Timing budgets & pilot control parameters tagged as MUST_LOCK_TIMING_BUDGET / configurable
    assert handler.config.timing_budget_policy_status == "MUST_LOCK_TIMING_BUDGET"
    assert (
        handler.config.idempotency_store_status
        == "VOLATILE_IN_MEMORY_MUST_LOCK_DURABLE_STORE"
    )
    assert state_mgr.config.hold_down_s == 10.0
    assert state_mgr.config.max_changed_flows_B == 4
    assert handler.config.stale_threshold_s == 2.0

    # 2. StateVersionPolicy tagged as MUST_LOCK_PROVISIONAL_DEFAULT
    policy = StateVersionPolicy()
    assert policy.policy_status == "MUST_LOCK_PROVISIONAL_DEFAULT"

    # 3. Released demand delivery channel tagged as MUST_LOCK_MOCK_PROVIDER
    assert demand_provider.demand_channel_status == "MUST_LOCK_MOCK_PROVIDER"

    # 4. TransportEnvelope tagged as MUST_LOCK_TRANSPORT_ADAPTER
    env = TransportEnvelope(
        run_id="run_p4_01",
        topology_version=topology.topology_version,
        topology_hash=topology.topology_hash,
    )
    assert env.transport_status == "MUST_LOCK_TRANSPORT_ADAPTER"

    # 5. L3/L4 FlowMatchPolicy and ProbeVerificationPolicy explicitly tagged as MUST-LOCK configurable
    fm_cfg = FlowMatcherConfig()
    assert fm_cfg.policy_status == "MUST_LOCK_PROVISIONAL_DEFAULT"
    rv_cfg = RuleVerifierConfig()
    assert rv_cfg.policy_status == "MUST_LOCK_PROVISIONAL_DEFAULT"


def test_23_fail_closed_snapshot_validation_vs_test_only_policy(
    topology: TopologyRegistry,
) -> None:
    """
    Verify fail-closed snapshot validation in integration/production mode vs explicit
    test-only policy (`DecisionHandlerConfig.for_mock_unit_tests()`):
    1. Default `DecisionHandlerConfig()` (`FAIL_CLOSED_INTEGRATION`, `require_registered_snapshot=True`)
       rejects any unregistered `snapshot_id`, even if timestamps are supplied.
    2. `DecisionHandlerConfig(require_registered_snapshot=False)` (still `FAIL_CLOSED_INTEGRATION`)
       rejects an unregistered `snapshot_id` when BOTH `history_cutoff_utc` and
       `snapshot_as_of_utc` are missing (no freshness bypass!).
    3. `DecisionHandlerConfig(require_registered_snapshot=False)` with a valid fresh
       `history_cutoff_utc` checks staleness (`> stale_threshold_s` -> `STALE`, `<=` -> `APPLIED`).
    4. Only explicit `DecisionHandlerConfig.for_mock_unit_tests()` (`TEST_ONLY_ALLOW_UNREGISTERED`)
       allows synthetic unit-test requests without registered snapshot or timestamps.
    """
    from dataclasses import replace

    # 1. Default fail-closed integration config rejects unregistered snapshot_id even with timestamps
    driver_int, _, _, state_int, handler_int, _, _ = _build_controller_stack(
        topology,
        handler_config=DecisionHandlerConfig(),
    )
    driver_int.clear_operation_log()
    req_unreg_with_ts = replace(
        _make_valid_apply_request(
            topology,
            decision_batch_id="batch_fail_closed_default",
            snapshot_id="snap:non_existent_id",
        ),
        history_cutoff_utc="2026-11-12T09:00:10Z",
    )
    resp_int = handler_int.handle_apply_request(
        req_unreg_with_ts,
        decision_time_utc="2026-11-12T09:00:10.200Z",
    )
    assert resp_int.status == BatchStatus.REJECTED
    assert resp_int.reason == ReasonCode.INVALID_REQUEST.value
    assert resp_int.rule_count == 0
    assert state_int.get_state_version("run_p4_01") == 1
    assert _count_flow_mods(driver_int) == 0

    # 2. require_registered_snapshot=False under FAIL_CLOSED_INTEGRATION MUST STILL REJECT
    #    when both history_cutoff_utc and snapshot_as_of_utc are missing!
    driver_fc, _, _, state_fc, handler_fc, _, _ = _build_controller_stack(
        topology,
        handler_config=DecisionHandlerConfig(require_registered_snapshot=False),
    )
    driver_fc.clear_operation_log()
    req_no_ts = _make_valid_apply_request(
        topology,
        decision_batch_id="batch_fail_closed_no_ts",
        snapshot_id="snap:non_existent_id",
    )
    assert req_no_ts.history_cutoff_utc is None
    assert req_no_ts.snapshot_as_of_utc is None

    resp_no_ts = handler_fc.handle_apply_request(
        req_no_ts,
        decision_time_utc="2026-11-12T09:00:10.200Z",
    )
    assert resp_no_ts.status == BatchStatus.REJECTED
    assert resp_no_ts.reason == ReasonCode.INVALID_REQUEST.value
    assert resp_no_ts.rule_count == 0
    assert state_fc.get_state_version("run_p4_01") == 1
    assert _count_flow_mods(driver_fc) == 0

    # 3. require_registered_snapshot=False with explicit timestamps still enforces T_stale_s
    req_stale_ts = replace(
        _make_valid_apply_request(
            topology,
            decision_batch_id="batch_unreg_stale_ts",
            snapshot_id="snap:non_existent_id",
        ),
        history_cutoff_utc="2026-11-12T09:00:05Z",
    )
    resp_stale_ts = handler_fc.handle_apply_request(
        req_stale_ts,
        decision_time_utc="2026-11-12T09:00:10.200Z",
    )
    assert resp_stale_ts.status == BatchStatus.REJECTED
    assert resp_stale_ts.reason == ReasonCode.STALE.value
    assert resp_stale_ts.rule_count == 0

    req_fresh_ts = replace(
        _make_valid_apply_request(
            topology,
            decision_batch_id="batch_unreg_fresh_ts",
            snapshot_id="snap:non_existent_id",
        ),
        history_cutoff_utc="2026-11-12T09:00:10Z",
    )
    resp_fresh_ts = handler_fc.handle_apply_request(
        req_fresh_ts,
        decision_time_utc="2026-11-12T09:00:10.200Z",
    )
    assert resp_fresh_ts.status == BatchStatus.APPLIED
    assert resp_fresh_ts.state_version == 2

    # 4. Explicit test-only policy allows synthetic fixtures without timestamps
    _, _, _, _, handler_test_only, _, _ = _build_controller_stack(
        topology,
        handler_config=DecisionHandlerConfig.for_mock_unit_tests(),
    )
    resp_test_only = handler_test_only.handle_apply_request(
        req_no_ts,
        decision_time_utc="2026-11-12T09:00:10.200Z",
    )
    assert resp_test_only.status == BatchStatus.APPLIED
    assert resp_test_only.state_version == 2


