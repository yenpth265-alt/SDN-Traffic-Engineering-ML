"""
Unit, contract, and edge-case audit tests for Phase 2: StateManager
(Windows Pure-Python / Simulated Verification Evidence — PASS_MOCK).

Covers:
1. Bootstrap verification (S1, AC-S03, AC-S10):
   - Configuring initial P1 paths without VerificationEvidence leaves all 48 flows UNVERIFIED
     (never auto-marked VERIFIED), routing_state_complete=False, reconcile_required=True,
     and routing_history empty.
   - Marking VERIFIED requires valid VerificationEvidence for all 48 canonical flows.
2. VerificationEvidence validation (AC-S03):
   - Barrier-only is rejected; requires both rule_readback_ok and counter_or_probe_ok.
   - Wrong run_id, flow_group_id, or verified_path_id is rejected.
3. Proposal-only and pre-write REJECTED events do NOT increment state_version or alter
   verified routing_state / last_successful_change_at / routing_history (AC-S02, AC-S07, REQ-16).
4. Edge Case 1: Two competing batches (concurrent in-flight BUSY & stale expected_state_version VERSION_MISMATCH).
5. Edge Case 2: Duplicate decision_batch_id with identical payload (idempotent replay) vs.
   different payload/outcomes (BatchIdConflictError / BATCH_ID_CONFLICT, AC-S08).
6. Edge Case 3: Backward or invalid UTC timestamps rejected across bootstrap, batch execution,
   commit (including FAILED/KEPT), restart, and reconciliation.
7. Edge Case 4: Reconciliation with fewer than 48 flows rejected; requires all 48 canonical flows.
8. Edge Case 5: VerificationEvidence with mismatched run_id, flow_group_id, or verified_path_id rejected.
9. Edge Case 6: Multi-flow batch where one flow is UNKNOWN -> BatchStatus.INCONSISTENT,
   UncertaintyGapRecord created, reconcile_required=True.
10. Edge Case 7: Atomicity on exception between routing_state and routing_history updates
    (zero partial mutation on failure).
11. Edge Case 8: Verified rollback to same path under both StateVersionPolicy configurations
    and explicit MUST-LOCK policy enforcement.
12. Edge Case 9: Global state_version synchronization invariant across all 48 flows after every transition.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional
import pytest

from sdn.controller.state_manager import (
    BatchIdConflictError,
    ReconciliationRequiredError,
    StateManager,
    StateValidationError,
)
from sdn.controller.topology_registry import TopologyLookupError, TopologyRegistry
from sdn.schemas.state_models import (
    BatchStatus,
    EvidenceMode,
    FlowBatchOutcome,
    FlowDecisionStatus,
    HistoryIntervalStatus,
    ReasonCode,
    SDNStateConfig,
    StateVersionPolicy,
    VerificationEvidence,
    VerificationStatus,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
TOPOLOGY_BUNDLE_PATH = REPO_ROOT / "configs" / "network" / "topology_bundle.json"
CATALOG_DIR = REPO_ROOT / "configs" / "network"


@pytest.fixture(scope="module")
def topology_registry() -> TopologyRegistry:
    return TopologyRegistry.from_bundle_file(
        TOPOLOGY_BUNDLE_PATH,
        csv_config_dir=CATALOG_DIR,
        verify_hash=True,
    )


@pytest.fixture()
def state_manager(topology_registry: TopologyRegistry) -> StateManager:
    return StateManager(topology_registry=topology_registry)


def _make_mock_evidence(
    flow_group_id: str,
    verified_path_id: str,
    verified_at_utc: str,
    *,
    run_id: Optional[str] = None,
    rule_readback_ok: bool = True,
    counter_or_probe_ok: bool = True,
    barrier_confirmed: bool = True,
) -> VerificationEvidence:
    return VerificationEvidence(
        run_id=run_id,
        flow_group_id=flow_group_id,
        verified_path_id=verified_path_id,
        verified_at_utc=verified_at_utc,
        evidence_mode=EvidenceMode.MOCK_SIMULATED,
        rule_readback_ok=rule_readback_ok,
        counter_or_probe_ok=counter_or_probe_ok,
        barrier_confirmed=barrier_confirmed,
        xid_refs=(101, 102),
        probe_id=f"mock_probe_{flow_group_id}",
    )


def _make_all_48_p1_evidences(
    topology_registry: TopologyRegistry,
    verified_at_utc: str = "2026-04-06T00:00:00Z",
    *,
    run_id: Optional[str] = None,
    overrides: Optional[dict[str, str]] = None,
) -> dict[str, VerificationEvidence]:
    overrides = overrides or {}
    return {
        fg.flow_group_id: _make_mock_evidence(
            fg.flow_group_id,
            overrides.get(fg.flow_group_id, f"{fg.flow_group_id}_P1"),
            verified_at_utc,
            run_id=run_id,
        )
        for fg in topology_registry.get_all_flow_groups()
    }


def _init_verified_run(
    state_manager: StateManager,
    topology_registry: TopologyRegistry,
    run_id: str,
    bootstrapped_at_utc: str = "2026-04-06T00:00:00Z",
) -> None:
    evidences = _make_all_48_p1_evidences(
        topology_registry, bootstrapped_at_utc, run_id=run_id
    )
    state_manager.initialize_run(
        run_id=run_id,
        bootstrapped_at_utc=bootstrapped_at_utc,
        verification_evidences=evidences,
    )


# -----------------------------------------------------------------------------
# 1. Bootstrap Verification: P1 Initialization != Auto-VERIFIED
# -----------------------------------------------------------------------------


def test_initialize_run_without_evidence_stays_unverified(
    state_manager: StateManager, topology_registry: TopologyRegistry
) -> None:
    """
    Audit Requirement 2: Initializing a run on P1 without VerificationEvidence MUST NOT
    auto-mark flows as VERIFIED. All 48 flows remain UNVERIFIED until 48-flow evidence is supplied.
    """
    snap_unverified = state_manager.initialize_run(
        run_id="run_unverified_boot",
        bootstrapped_at_utc="2026-04-06T00:00:00Z",
    )

    assert snap_unverified.run_id == "run_unverified_boot"
    assert snap_unverified.state_version == 1
    assert snap_unverified.routing_state_complete is False
    assert snap_unverified.reconcile_required is True
    assert len(snap_unverified.unknown_flow_group_ids) == 48
    assert snap_unverified.evidence_mode is None

    for entry in snap_unverified.routing_state:
        assert entry.path_id == f"{entry.flow_group_id}_P1"
        assert entry.verification_status == VerificationStatus.UNVERIFIED
        assert entry.confirmed_at is None
        assert entry.last_successful_change_at is None
        assert entry.evidence_mode is None

    # No verified routing_history interval may exist before verification
    assert state_manager.get_routing_history("run_unverified_boot") == ()

    can_apply, reason = state_manager.can_accept_apply("run_unverified_boot")
    assert can_apply is False
    assert reason == ReasonCode.RECONCILIATION_REQUIRED

    with pytest.raises(ReconciliationRequiredError):
        state_manager.begin_batch_execution(
            "run_unverified_boot", "batch_early", "2026-04-06T00:00:01Z"
        )

    # Now supply full 48-flow verification evidence via verify_bootstrap()
    evidences = _make_all_48_p1_evidences(
        topology_registry, "2026-04-06T00:00:01Z", run_id="run_unverified_boot"
    )
    snap_verified = state_manager.verify_bootstrap(
        "run_unverified_boot", evidences, verified_at_utc="2026-04-06T00:00:01Z"
    )

    assert snap_verified.state_version == 1
    assert snap_verified.routing_state_complete is True
    assert snap_verified.reconcile_required is False
    assert snap_verified.unknown_flow_group_ids == ()
    assert snap_verified.evidence_mode == EvidenceMode.MOCK_SIMULATED
    assert len(state_manager.get_routing_history("run_unverified_boot")) == 48
    for entry in snap_verified.routing_state:
        assert entry.verification_status == VerificationStatus.VERIFIED
        assert entry.confirmed_at == "2026-04-06T00:00:01Z"


def test_initialize_run_with_48_valid_evidences_bootstraps_verified(
    state_manager: StateManager, topology_registry: TopologyRegistry
) -> None:
    evidences = _make_all_48_p1_evidences(
        topology_registry, "2026-04-06T00:00:00Z", run_id="run_boot_001"
    )
    snap = state_manager.initialize_run(
        run_id="run_boot_001",
        bootstrapped_at_utc="2026-04-06T00:00:00Z",
        verification_evidences=evidences,
    )

    assert snap.run_id == "run_boot_001"
    assert snap.state_version == 1
    assert snap.routing_state_complete is True
    assert snap.reconcile_required is False
    assert snap.unknown_flow_group_ids == ()
    assert snap.evidence_mode == EvidenceMode.MOCK_SIMULATED
    assert len(snap.routing_state) == 48
    assert snap.routing_state[0].flow_group_id == "0x103"
    assert snap.routing_state[-1].flow_group_id == "0x806"

    history = state_manager.get_routing_history("run_boot_001")
    assert len(history) == 48
    for interval in history:
        assert interval.path_id == f"{interval.flow_group_id}_P1"
        assert interval.valid_from == "2026-04-06T00:00:00Z"
        assert interval.valid_to is None
        assert interval.status == HistoryIntervalStatus.INITIAL


def test_bootstrap_rejects_incomplete_or_invalid_48_flow_evidence(
    state_manager: StateManager, topology_registry: TopologyRegistry
) -> None:
    # 1) Missing 1 flow (47/48)
    partial_evidences = _make_all_48_p1_evidences(
        topology_registry, "2026-04-06T00:00:00Z"
    )
    partial_evidences.pop("0x806")
    with pytest.raises(StateValidationError, match="Full 48-flow verification evidence required"):
        state_manager.initialize_run(
            "run_incomplete_ev",
            "2026-04-06T00:00:00Z",
            verification_evidences=partial_evidences,
        )

    # 2) One of the 48 flows has Barrier-only evidence (counter_or_probe_ok=False)
    bad_evidences = _make_all_48_p1_evidences(
        topology_registry, "2026-04-06T00:00:00Z"
    )
    bad_evidences["0x204"] = _make_mock_evidence(
        "0x204",
        "0x204_P1",
        "2026-04-06T00:00:00Z",
        rule_readback_ok=True,
        counter_or_probe_ok=False,
    )
    with pytest.raises(StateValidationError, match="Barrier alone is insufficient"):
        state_manager.initialize_run(
            "run_bad_boot_ev",
            "2026-04-06T00:00:00Z",
            verification_evidences=bad_evidences,
        )

    # 3) One of the 48 flows claims P2 instead of initial P1 during bootstrap
    wrong_path_evidences = _make_all_48_p1_evidences(
        topology_registry, "2026-04-06T00:00:00Z", overrides={"0x103": "0x103_P2"}
    )
    with pytest.raises(StateValidationError, match="does not match claimed path '0x103_P1'"):
        state_manager.initialize_run(
            "run_wrong_boot_path",
            "2026-04-06T00:00:00Z",
            verification_evidences=wrong_path_evidences,
        )


# -----------------------------------------------------------------------------
# 2. Barrier-Only Rejection & Proposals/Pre-Write Rejections
# -----------------------------------------------------------------------------


def test_barrier_only_evidence_rejected_for_verified(
    state_manager: StateManager, topology_registry: TopologyRegistry
) -> None:
    _init_verified_run(state_manager, topology_registry, "run_ev_001")

    bad_ev_readback = _make_mock_evidence(
        "0x103",
        "0x103_P2",
        "2026-04-06T00:01:00Z",
        rule_readback_ok=False,
        counter_or_probe_ok=True,
    )
    with pytest.raises(StateValidationError, match="Barrier alone is insufficient"):
        state_manager.commit_batch_outcome(
            run_id="run_ev_001",
            decision_batch_id="batch_bad_ev_1",
            outcomes=[
                FlowBatchOutcome(
                    flow_group_id="0x103",
                    old_path_id="0x103_P1",
                    proposed_path_id="0x103_P2",
                    actual_path_id="0x103_P2",
                    status=FlowDecisionStatus.APPLIED,
                    verification_status=VerificationStatus.VERIFIED,
                    rule_count=3,
                    evidence=bad_ev_readback,
                )
            ],
            completed_at_utc="2026-04-06T00:01:00Z",
        )

    assert state_manager.get_state_version("run_ev_001") == 1
    assert state_manager.get_flow_state("run_ev_001", "0x103").path_id == "0x103_P1"


def test_proposal_and_prewrite_rejected_do_not_increment_state_version(
    state_manager: StateManager, topology_registry: TopologyRegistry
) -> None:
    _init_verified_run(state_manager, topology_registry, "run_no_bump")

    v_after_prop = state_manager.record_proposal_only(
        run_id="run_no_bump",
        decision_batch_id="batch_prop_01",
        proposed_changes={"0x103": "0x103_P2", "0x104": "0x104_P2"},
        recorded_at_utc="2026-04-06T00:01:00Z",
    )
    assert v_after_prop == 1

    v_after_rej = state_manager.record_rejected_request(
        run_id="run_no_bump",
        decision_batch_id="batch_rej_01",
        reason=ReasonCode.VERSION_MISMATCH,
        rejected_at_utc="2026-04-06T00:01:01Z",
    )
    assert v_after_rej == 1
    assert state_manager.get_flow_state("run_no_bump", "0x103").path_id == "0x103_P1"


# -----------------------------------------------------------------------------
# 3. Hold-Down Timer Semantics (< T_hold, == T_hold, > T_hold, Bootstrap None)
# -----------------------------------------------------------------------------


def test_hold_down_before_at_and_after_threshold(
    state_manager: StateManager, topology_registry: TopologyRegistry
) -> None:
    _init_verified_run(state_manager, topology_registry, "run_hold")

    assert (
        state_manager.is_hold_down_active(
            "run_hold", "0x103", "2026-04-06T00:00:01Z"
        )
        is False
    )

    state_manager.commit_batch_outcome(
        run_id="run_hold",
        decision_batch_id="batch_hold_1",
        outcomes=[
            FlowBatchOutcome(
                flow_group_id="0x103",
                old_path_id="0x103_P1",
                proposed_path_id="0x103_P2",
                actual_path_id="0x103_P2",
                status=FlowDecisionStatus.APPLIED,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=3,
                evidence=_make_mock_evidence("0x103", "0x103_P2", "2026-04-06T00:01:00Z"),
            )
        ],
        completed_at_utc="2026-04-06T00:01:00Z",
    )

    assert (
        state_manager.is_hold_down_active(
            "run_hold", "0x103", "2026-04-06T00:01:09.999Z"
        )
        is True
    )
    assert (
        state_manager.is_hold_down_active(
            "run_hold", "0x103", "2026-04-06T00:01:10.000Z"
        )
        is False
    )
    assert (
        state_manager.is_hold_down_active(
            "run_hold", "0x103", "2026-04-06T00:01:10.001Z"
        )
        is False
    )


# -----------------------------------------------------------------------------
# 4. Edge Case 1: Two Competing Batches Updating State (BUSY & VERSION_MISMATCH)
# -----------------------------------------------------------------------------


def test_edge_case_1_two_competing_batches_busy_and_version_mismatch(
    state_manager: StateManager, topology_registry: TopologyRegistry
) -> None:
    _init_verified_run(state_manager, topology_registry, "run_compete")

    # Both batch_A and batch_B observe state_version = 1.
    # batch_A acquires the execution lock first.
    state_manager.begin_batch_execution(
        "run_compete",
        "batch_A",
        "2026-04-06T00:01:00Z",
        expected_state_version=1,
        payload_hash="hash_A",
    )

    # While batch_A is in flight, batch_B cannot begin execution or commit (BUSY)
    can_apply, reason = state_manager.can_accept_apply("run_compete")
    assert can_apply is False
    assert reason == ReasonCode.BUSY

    with pytest.raises(StateValidationError, match="BUSY"):
        state_manager.begin_batch_execution(
            "run_compete",
            "batch_B",
            "2026-04-06T00:01:00.050Z",
            expected_state_version=1,
        )

    with pytest.raises(StateValidationError, match="BUSY"):
        state_manager.commit_batch_outcome(
            run_id="run_compete",
            decision_batch_id="batch_B",
            outcomes=[
                FlowBatchOutcome(
                    flow_group_id="0x104",
                    old_path_id="0x104_P1",
                    proposed_path_id="0x104_P2",
                    actual_path_id="0x104_P2",
                    status=FlowDecisionStatus.APPLIED,
                    verification_status=VerificationStatus.VERIFIED,
                    rule_count=3,
                    evidence=_make_mock_evidence("0x104", "0x104_P2", "2026-04-06T00:01:00.100Z"),
                )
            ],
            completed_at_utc="2026-04-06T00:01:00.100Z",
            expected_state_version=1,
        )

    # batch_A finishes and increments state_version 1 -> 2
    status_a, ver_a = state_manager.commit_batch_outcome(
        run_id="run_compete",
        decision_batch_id="batch_A",
        outcomes=[
            FlowBatchOutcome(
                flow_group_id="0x103",
                old_path_id="0x103_P1",
                proposed_path_id="0x103_P2",
                actual_path_id="0x103_P2",
                status=FlowDecisionStatus.APPLIED,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=3,
                evidence=_make_mock_evidence("0x103", "0x103_P2", "2026-04-06T00:01:00.200Z"),
            )
        ],
        completed_at_utc="2026-04-06T00:01:00.200Z",
        expected_state_version=1,
        payload_hash="hash_A",
    )
    assert status_a == BatchStatus.APPLIED
    assert ver_a == 2

    # Now batch_B (even though targeting disjoint flow 0x104) has stale expected_state_version=1
    with pytest.raises(StateValidationError, match="VERSION_MISMATCH"):
        state_manager.begin_batch_execution(
            "run_compete",
            "batch_B",
            "2026-04-06T00:01:01Z",
            expected_state_version=1,
        )

    with pytest.raises(StateValidationError, match="VERSION_MISMATCH"):
        state_manager.commit_batch_outcome(
            run_id="run_compete",
            decision_batch_id="batch_B",
            outcomes=[
                FlowBatchOutcome(
                    flow_group_id="0x104",
                    old_path_id="0x104_P1",
                    proposed_path_id="0x104_P2",
                    actual_path_id="0x104_P2",
                    status=FlowDecisionStatus.APPLIED,
                    verification_status=VerificationStatus.VERIFIED,
                    rule_count=3,
                    evidence=_make_mock_evidence("0x104", "0x104_P2", "2026-04-06T00:01:01Z"),
                )
            ],
            completed_at_utc="2026-04-06T00:01:01Z",
            expected_state_version=1,
        )


# -----------------------------------------------------------------------------
# 5. Edge Case 2: Duplicate decision_batch_id With Different Payload (BATCH_ID_CONFLICT)
# -----------------------------------------------------------------------------


def test_edge_case_2_duplicate_batch_id_with_different_payload_raises_conflict(
    state_manager: StateManager, topology_registry: TopologyRegistry
) -> None:
    _init_verified_run(state_manager, topology_registry, "run_dedup")

    outcomes_orig = [
        FlowBatchOutcome(
            flow_group_id="0x103",
            old_path_id="0x103_P1",
            proposed_path_id="0x103_P2",
            actual_path_id="0x103_P2",
            status=FlowDecisionStatus.APPLIED,
            verification_status=VerificationStatus.VERIFIED,
            rule_count=3,
            evidence=_make_mock_evidence("0x103", "0x103_P2", "2026-04-06T00:01:00Z"),
        )
    ]

    status_1, ver_1 = state_manager.commit_batch_outcome(
        run_id="run_dedup",
        decision_batch_id="batch_001",
        outcomes=outcomes_orig,
        completed_at_utc="2026-04-06T00:01:00Z",
        payload_hash="sha256:aaa111",
    )
    assert status_1 == BatchStatus.APPLIED
    assert ver_1 == 2

    # 1) Exact replay (same batch_id, same outcomes, same payload_hash) -> idempotent
    status_replay, ver_replay = state_manager.commit_batch_outcome(
        run_id="run_dedup",
        decision_batch_id="batch_001",
        outcomes=outcomes_orig,
        completed_at_utc="2026-04-06T00:01:00Z",
        payload_hash="sha256:aaa111",
    )
    assert status_replay == BatchStatus.APPLIED
    assert ver_replay == 2

    # 2) Same decision_batch_id but different payload_hash -> BatchIdConflictError
    with pytest.raises(BatchIdConflictError, match="BATCH_ID_CONFLICT"):
        state_manager.commit_batch_outcome(
            run_id="run_dedup",
            decision_batch_id="batch_001",
            outcomes=outcomes_orig,
            completed_at_utc="2026-04-06T00:01:00Z",
            payload_hash="sha256:bbb222",
        )

    # 3) Same decision_batch_id but different outcomes (e.g. different flow 0x104) -> BatchIdConflictError
    outcomes_diff = [
        FlowBatchOutcome(
            flow_group_id="0x104",
            old_path_id="0x104_P1",
            proposed_path_id="0x104_P2",
            actual_path_id="0x104_P2",
            status=FlowDecisionStatus.APPLIED,
            verification_status=VerificationStatus.VERIFIED,
            rule_count=3,
            evidence=_make_mock_evidence("0x104", "0x104_P2", "2026-04-06T00:01:05Z"),
        )
    ]
    with pytest.raises(BatchIdConflictError, match="BATCH_ID_CONFLICT"):
        state_manager.commit_batch_outcome(
            run_id="run_dedup",
            decision_batch_id="batch_001",
            outcomes=outcomes_diff,
            completed_at_utc="2026-04-06T00:01:05Z",
        )

    # State remains at version 2 with 0x104 untouched
    assert state_manager.get_state_version("run_dedup") == 2
    assert state_manager.get_flow_state("run_dedup", "0x104").path_id == "0x104_P1"


# -----------------------------------------------------------------------------
# 6. Edge Case 3: Backward or Invalid Timestamps
# -----------------------------------------------------------------------------


def test_edge_case_3_backward_and_invalid_timestamps_rejected(
    state_manager: StateManager, topology_registry: TopologyRegistry
) -> None:
    _init_verified_run(
        state_manager, topology_registry, "run_time", "2026-04-06T00:05:00Z"
    )

    # 1) Invalid / naive timestamp without timezone
    with pytest.raises(StateValidationError, match="explicit UTC timezone"):
        state_manager.record_proposal_only(
            "run_time", "b_naive", {"0x103": "0x103_P2"}, "2026-04-06T00:05:10"
        )

    with pytest.raises(StateValidationError, match="Invalid RFC3339 timestamp"):
        state_manager.record_proposal_only(
            "run_time", "b_malformed", {"0x103": "0x103_P2"}, "not-a-timestamp"
        )

    # 2) Backward timestamp on APPLIED outcome (< confirmed_at 00:05:00Z)
    with pytest.raises(StateValidationError, match="Backward timestamp"):
        state_manager.commit_batch_outcome(
            run_id="run_time",
            decision_batch_id="b_back_applied",
            outcomes=[
                FlowBatchOutcome(
                    flow_group_id="0x103",
                    old_path_id="0x103_P1",
                    proposed_path_id="0x103_P2",
                    actual_path_id="0x103_P2",
                    status=FlowDecisionStatus.APPLIED,
                    verification_status=VerificationStatus.VERIFIED,
                    rule_count=3,
                    evidence=_make_mock_evidence("0x103", "0x103_P2", "2026-04-06T00:04:59Z"),
                )
            ],
            completed_at_utc="2026-04-06T00:05:01Z",
        )

    # 3) Backward timestamp on FAILED outcome (< confirmed_at 00:05:00Z)
    with pytest.raises(StateValidationError, match="Backward timestamp"):
        state_manager.commit_batch_outcome(
            run_id="run_time",
            decision_batch_id="b_back_failed",
            outcomes=[
                FlowBatchOutcome(
                    flow_group_id="0x103",
                    old_path_id="0x103_P1",
                    proposed_path_id="0x103_P2",
                    actual_path_id="0x103_P1",
                    status=FlowDecisionStatus.FAILED,
                    verification_status=VerificationStatus.VERIFIED,
                    rule_count=1,
                    evidence=_make_mock_evidence("0x103", "0x103_P1", "2026-04-06T00:04:50Z"),
                )
            ],
            completed_at_utc="2026-04-06T00:05:01Z",
        )

    # 4) completed_at_utc precedes verified_at_utc
    with pytest.raises(StateValidationError, match="precedes flow '0x103' verified_at_utc"):
        state_manager.commit_batch_outcome(
            run_id="run_time",
            decision_batch_id="b_back_completed",
            outcomes=[
                FlowBatchOutcome(
                    flow_group_id="0x103",
                    old_path_id="0x103_P1",
                    proposed_path_id="0x103_P2",
                    actual_path_id="0x103_P2",
                    status=FlowDecisionStatus.APPLIED,
                    verification_status=VerificationStatus.VERIFIED,
                    rule_count=3,
                    evidence=_make_mock_evidence("0x103", "0x103_P2", "2026-04-06T00:05:10Z"),
                )
            ],
            completed_at_utc="2026-04-06T00:05:09Z",
        )

    # 5) Backward timestamp on mark_controller_restart (< last_transition_at_utc)
    with pytest.raises(StateValidationError, match="Backward timestamp"):
        state_manager.mark_controller_restart("run_time", "2026-04-06T00:04:59Z")


# -----------------------------------------------------------------------------
# 7. Edge Case 4: Reconcile With Fewer Than 48 Flows Rejected
# -----------------------------------------------------------------------------


def test_edge_case_4_reconcile_requires_all_48_flows(
    state_manager: StateManager, topology_registry: TopologyRegistry
) -> None:
    _init_verified_run(state_manager, topology_registry, "run_rec_48")

    # Put 1 flow (0x103) into UNKNOWN
    state_manager.commit_batch_outcome(
        run_id="run_rec_48",
        decision_batch_id="batch_unk_103",
        outcomes=[
            FlowBatchOutcome(
                flow_group_id="0x103",
                old_path_id="0x103_P1",
                proposed_path_id="0x103_P2",
                actual_path_id=None,
                status=FlowDecisionStatus.UNKNOWN,
                verification_status=VerificationStatus.UNKNOWN,
                rule_count=4,
                applied_at_utc="2026-04-06T00:01:00Z",
            )
        ],
        completed_at_utc="2026-04-06T00:01:00Z",
    )
    assert state_manager.is_reconcile_required("run_rec_48") is True

    # Attempting to reconcile with only the 1 UNKNOWN flow (1/48) MUST fail
    with pytest.raises(StateValidationError, match="Full 48-flow verification evidence required"):
        state_manager.reconcile_run_state(
            run_id="run_rec_48",
            verified_evidences={
                "0x103": _make_mock_evidence("0x103", "0x103_P2", "2026-04-06T00:01:05Z")
            },
            reconciled_at_utc="2026-04-06T00:01:05Z",
        )

    # Attempting to reconcile with 47/48 flows MUST also fail
    ev_47 = _make_all_48_p1_evidences(
        topology_registry,
        "2026-04-06T00:01:05Z",
        overrides={"0x103": "0x103_P2"},
    )
    ev_47.pop("0x806")
    with pytest.raises(StateValidationError, match="Full 48-flow verification evidence required"):
        state_manager.reconcile_run_state(
            run_id="run_rec_48",
            verified_evidences=ev_47,
            reconciled_at_utc="2026-04-06T00:01:05Z",
        )

    # State remains locked until all 48 flows are supplied
    assert state_manager.is_reconcile_required("run_rec_48") is True
    assert state_manager.get_flow_state("run_rec_48", "0x103").path_id is None

    # Supplying all 48 flows succeeds and clears reconcile_required
    ev_48 = _make_all_48_p1_evidences(
        topology_registry,
        "2026-04-06T00:01:05Z",
        run_id="run_rec_48",
        overrides={"0x103": "0x103_P2"},
    )
    snap_reconciled = state_manager.reconcile_run_state(
        run_id="run_rec_48",
        verified_evidences=ev_48,
        reconciled_at_utc="2026-04-06T00:01:05Z",
    )
    assert snap_reconciled.reconcile_required is False
    assert snap_reconciled.routing_state_complete is True
    assert snap_reconciled.state_version == 3
    assert state_manager.get_flow_state("run_rec_48", "0x103").path_id == "0x103_P2"


# -----------------------------------------------------------------------------
# 8. Edge Case 5: VerificationEvidence With Wrong run_id / flow_id / path_id
# -----------------------------------------------------------------------------


def test_edge_case_5_verification_evidence_wrong_run_flow_or_path_id(
    state_manager: StateManager, topology_registry: TopologyRegistry
) -> None:
    _init_verified_run(state_manager, topology_registry, "run_ev_match")

    # 1) Wrong run_id in VerificationEvidence
    with pytest.raises(StateValidationError, match="does not match target run 'run_ev_match'"):
        state_manager.commit_batch_outcome(
            run_id="run_ev_match",
            decision_batch_id="b_wrong_run",
            outcomes=[
                FlowBatchOutcome(
                    flow_group_id="0x103",
                    old_path_id="0x103_P1",
                    proposed_path_id="0x103_P2",
                    actual_path_id="0x103_P2",
                    status=FlowDecisionStatus.APPLIED,
                    verification_status=VerificationStatus.VERIFIED,
                    rule_count=3,
                    evidence=_make_mock_evidence(
                        "0x103", "0x103_P2", "2026-04-06T00:01:00Z", run_id="other_run"
                    ),
                )
            ],
            completed_at_utc="2026-04-06T00:01:00Z",
        )

    # 2) Wrong flow_group_id in VerificationEvidence
    with pytest.raises(StateValidationError, match="does not match target flow '0x103'"):
        state_manager.commit_batch_outcome(
            run_id="run_ev_match",
            decision_batch_id="b_wrong_flow",
            outcomes=[
                FlowBatchOutcome(
                    flow_group_id="0x103",
                    old_path_id="0x103_P1",
                    proposed_path_id="0x103_P2",
                    actual_path_id="0x103_P2",
                    status=FlowDecisionStatus.APPLIED,
                    verification_status=VerificationStatus.VERIFIED,
                    rule_count=3,
                    evidence=_make_mock_evidence(
                        "0x104", "0x103_P2", "2026-04-06T00:01:00Z", run_id="run_ev_match"
                    ),
                )
            ],
            completed_at_utc="2026-04-06T00:01:00Z",
        )

    # 3) Wrong verified_path_id in VerificationEvidence (0x103_P1 instead of 0x103_P2)
    with pytest.raises(StateValidationError, match="does not match claimed path '0x103_P2'"):
        state_manager.commit_batch_outcome(
            run_id="run_ev_match",
            decision_batch_id="b_wrong_path",
            outcomes=[
                FlowBatchOutcome(
                    flow_group_id="0x103",
                    old_path_id="0x103_P1",
                    proposed_path_id="0x103_P2",
                    actual_path_id="0x103_P2",
                    status=FlowDecisionStatus.APPLIED,
                    verification_status=VerificationStatus.VERIFIED,
                    rule_count=3,
                    evidence=_make_mock_evidence(
                        "0x103", "0x103_P1", "2026-04-06T00:01:00Z", run_id="run_ev_match"
                    ),
                )
            ],
            completed_at_utc="2026-04-06T00:01:00Z",
        )


# -----------------------------------------------------------------------------
# 9. Edge Case 6: Multi-Flow Batch With One Flow UNKNOWN -> INCONSISTENT
# -----------------------------------------------------------------------------


def test_edge_case_6_multi_flow_batch_with_one_unknown_is_inconsistent(
    state_manager: StateManager, topology_registry: TopologyRegistry
) -> None:
    _init_verified_run(state_manager, topology_registry, "run_multi_unk")

    batch_status, ver = state_manager.commit_batch_outcome(
        run_id="run_multi_unk",
        decision_batch_id="batch_mix_unk",
        outcomes=[
            FlowBatchOutcome(
                flow_group_id="0x103",
                old_path_id="0x103_P1",
                proposed_path_id="0x103_P2",
                actual_path_id="0x103_P2",
                status=FlowDecisionStatus.APPLIED,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=3,
                evidence=_make_mock_evidence("0x103", "0x103_P2", "2026-04-06T00:01:00Z"),
            ),
            FlowBatchOutcome(
                flow_group_id="0x104",
                old_path_id="0x104_P1",
                proposed_path_id="0x104_P2",
                actual_path_id="0x104_P1",
                status=FlowDecisionStatus.ROLLED_BACK,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=4,
                evidence=_make_mock_evidence("0x104", "0x104_P1", "2026-04-06T00:01:00Z"),
                had_intermediate_physical_change=True,
            ),
            FlowBatchOutcome(
                flow_group_id="0x203",
                old_path_id="0x203_P1",
                proposed_path_id="0x203_P2",
                actual_path_id=None,
                status=FlowDecisionStatus.UNKNOWN,
                verification_status=VerificationStatus.UNKNOWN,
                rule_count=5,
                reason=ReasonCode.ROLLBACK_FAILED.value,
                applied_at_utc="2026-04-06T00:01:00Z",
            ),
        ],
        completed_at_utc="2026-04-06T00:01:00Z",
    )

    assert batch_status == BatchStatus.INCONSISTENT
    assert ver == 2
    assert state_manager.is_reconcile_required("run_multi_unk") is True

    snap = state_manager.get_routing_state("run_multi_unk")
    assert snap.routing_state_complete is False
    assert snap.unknown_flow_group_ids == ("0x203",)

    # 0x103 is VERIFIED on P2, 0x104 is VERIFIED on P1, 0x203 is UNKNOWN with path_id=None
    assert state_manager.get_flow_state("run_multi_unk", "0x103").path_id == "0x103_P2"
    assert state_manager.get_flow_state("run_multi_unk", "0x104").path_id == "0x104_P1"
    f203 = state_manager.get_flow_state("run_multi_unk", "0x203")
    assert f203.path_id is None
    assert f203.verification_status == VerificationStatus.UNKNOWN

    # Open uncertainty gap for 0x203 and closed initial history interval
    gaps_203 = state_manager.get_uncertainty_gaps("run_multi_unk", "0x203")
    assert len(gaps_203) == 1
    assert gaps_203[0].last_known_path_id == "0x203_P1"
    assert gaps_203[0].resolved_at_utc is None

    hist_203 = state_manager.get_routing_history("run_multi_unk", "0x203")
    assert len(hist_203) == 1
    assert hist_203[0].valid_to == "2026-04-06T00:01:00Z"


# -----------------------------------------------------------------------------
# 10. Edge Case 7: Atomicity When Exception Occurs Mid-Transition
# -----------------------------------------------------------------------------


def test_edge_case_7_atomicity_on_exception_between_state_and_history_update(
    state_manager: StateManager,
    topology_registry: TopologyRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Audit Requirement 4.7 & 5: If an exception occurs mid-transition (e.g., after flow 0x103
    has updated its staged state/history, when flow 0x104 is updating its history),
    the entire batch transition must roll back cleanly with ZERO partial mutation.
    """
    _init_verified_run(state_manager, topology_registry, "run_atomic")

    snap_before = state_manager.get_routing_state("run_atomic")
    hist_before = state_manager.get_routing_history("run_atomic")
    events_before = state_manager.get_events("run_atomic")

    orig_open_history = state_manager._open_new_history_interval
    call_count = 0

    def _failing_on_second_flow_history(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("Injected fault between flow 1 commit and flow 2 history update")
        return orig_open_history(*args, **kwargs)

    monkeypatch.setattr(
        state_manager, "_open_new_history_interval", _failing_on_second_flow_history
    )

    with pytest.raises(RuntimeError, match="Injected fault between flow 1 commit"):
        state_manager.commit_batch_outcome(
            run_id="run_atomic",
            decision_batch_id="batch_atomic_fail",
            outcomes=[
                FlowBatchOutcome(
                    flow_group_id="0x103",
                    old_path_id="0x103_P1",
                    proposed_path_id="0x103_P2",
                    actual_path_id="0x103_P2",
                    status=FlowDecisionStatus.APPLIED,
                    verification_status=VerificationStatus.VERIFIED,
                    rule_count=3,
                    evidence=_make_mock_evidence("0x103", "0x103_P2", "2026-04-06T00:01:00Z"),
                ),
                FlowBatchOutcome(
                    flow_group_id="0x104",
                    old_path_id="0x104_P1",
                    proposed_path_id="0x104_P2",
                    actual_path_id="0x104_P2",
                    status=FlowDecisionStatus.APPLIED,
                    verification_status=VerificationStatus.VERIFIED,
                    rule_count=3,
                    evidence=_make_mock_evidence("0x104", "0x104_P2", "2026-04-06T00:01:00Z"),
                ),
            ],
            completed_at_utc="2026-04-06T00:01:00Z",
        )

    # Restore original method and verify 0x103 was NOT partially mutated!
    monkeypatch.undo()
    snap_after = state_manager.get_routing_state("run_atomic")
    hist_after = state_manager.get_routing_history("run_atomic")
    events_after = state_manager.get_events("run_atomic")

    assert snap_after == snap_before
    assert hist_after == hist_before
    assert events_after == events_before
    assert state_manager.get_flow_state("run_atomic", "0x103").path_id == "0x103_P1"
    assert state_manager.get_state_version("run_atomic") == 1
    state_manager.assert_run_invariants("run_atomic")


# -----------------------------------------------------------------------------
# 11. Edge Case 8: Same-Path Rollback With Both StateVersionPolicies & MUST-LOCK Guard
# -----------------------------------------------------------------------------


def test_edge_case_8_same_path_rollback_with_both_state_version_policies(
    topology_registry: TopologyRegistry,
) -> None:
    # 1) When require_explicit_version_policy=True, provisional default policy is rejected
    with pytest.raises(StateValidationError, match="MUST_LOCK_PROVISIONAL_DEFAULT"):
        StateManager(
            topology_registry=topology_registry,
            config=SDNStateConfig(require_explicit_version_policy=True),
        )

    # 2) Policy A: increment_on_same_path_verified_rollback=True, update_hold_down=True
    cfg_true = SDNStateConfig(
        version_policy=StateVersionPolicy(
            increment_on_same_path_verified_rollback=True,
            update_hold_down_on_same_path_rollback=True,
            policy_status="EXPLICIT_TEST_POLICY_A",
        ),
        require_explicit_version_policy=True,
    )
    sm_true = StateManager(topology_registry=topology_registry, config=cfg_true)
    _init_verified_run(sm_true, topology_registry, "run_pol_true")

    status_t, ver_t = sm_true.commit_batch_outcome(
        run_id="run_pol_true",
        decision_batch_id="b_rb_true",
        outcomes=[
            FlowBatchOutcome(
                flow_group_id="0x103",
                old_path_id="0x103_P1",
                proposed_path_id="0x103_P2",
                actual_path_id="0x103_P1",
                status=FlowDecisionStatus.ROLLED_BACK,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=4,
                evidence=_make_mock_evidence("0x103", "0x103_P1", "2026-04-06T00:01:00Z"),
                had_intermediate_physical_change=True,
            )
        ],
        completed_at_utc="2026-04-06T00:01:00Z",
    )
    assert status_t == BatchStatus.ROLLED_BACK
    assert ver_t == 2
    assert (
        sm_true.get_flow_state("run_pol_true", "0x103").last_successful_change_at
        == "2026-04-06T00:01:00Z"
    )

    # 3) Policy B: increment_on_same_path_verified_rollback=False, update_hold_down=False
    cfg_false = SDNStateConfig(
        version_policy=StateVersionPolicy(
            increment_on_same_path_verified_rollback=False,
            update_hold_down_on_same_path_rollback=False,
            policy_status="EXPLICIT_TEST_POLICY_B",
        ),
        require_explicit_version_policy=True,
    )
    sm_false = StateManager(topology_registry=topology_registry, config=cfg_false)
    _init_verified_run(sm_false, topology_registry, "run_pol_false")

    status_f, ver_f = sm_false.commit_batch_outcome(
        run_id="run_pol_false",
        decision_batch_id="b_rb_false",
        outcomes=[
            FlowBatchOutcome(
                flow_group_id="0x103",
                old_path_id="0x103_P1",
                proposed_path_id="0x103_P2",
                actual_path_id="0x103_P1",
                status=FlowDecisionStatus.ROLLED_BACK,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=4,
                evidence=_make_mock_evidence("0x103", "0x103_P1", "2026-04-06T00:01:00Z"),
                had_intermediate_physical_change=True,
            )
        ],
        completed_at_utc="2026-04-06T00:01:00Z",
    )
    assert status_f == BatchStatus.ROLLED_BACK
    assert ver_f == 1
    assert (
        sm_false.get_flow_state("run_pol_false", "0x103").last_successful_change_at
        is None
    )


# -----------------------------------------------------------------------------
# 12. Edge Case 9: Uniform state_version Invariant Across All 48 Flows After Every Transition
# -----------------------------------------------------------------------------


def test_edge_case_9_uniform_state_version_invariant_across_48_flows(
    state_manager: StateManager, topology_registry: TopologyRegistry
) -> None:
    def _assert_48_uniform_version(run_id: str, expected_version: int) -> None:
        state_manager.assert_run_invariants(run_id)
        snap = state_manager.get_routing_state(run_id)
        assert snap.state_version == expected_version
        assert len(snap.routing_state) == 48
        versions = {entry.state_version for entry in snap.routing_state}
        assert versions == {expected_version}

    # Transition 1: Unverified bootstrap -> state_version = 1 across all 48 flows
    state_manager.initialize_run("run_inv", "2026-04-06T00:00:00Z")
    _assert_48_uniform_version("run_inv", 1)

    # Transition 2: Verify bootstrap -> state_version = 1 across all 48 flows
    state_manager.verify_bootstrap(
        "run_inv",
        _make_all_48_p1_evidences(topology_registry, "2026-04-06T00:00:01Z", run_id="run_inv"),
        verified_at_utc="2026-04-06T00:00:01Z",
    )
    _assert_48_uniform_version("run_inv", 1)

    # Transition 3: Proposal only -> state_version = 1 across all 48 flows
    state_manager.record_proposal_only(
        "run_inv", "b_prop", {"0x103": "0x103_P2"}, "2026-04-06T00:00:05Z"
    )
    _assert_48_uniform_version("run_inv", 1)

    # Transition 4: Rejected pre-write -> state_version = 1 across all 48 flows
    state_manager.record_rejected_request(
        "run_inv", "b_rej", ReasonCode.STALE, "2026-04-06T00:00:06Z"
    )
    _assert_48_uniform_version("run_inv", 1)

    # Transition 5: Commit APPLIED on 2 flows -> state_version = 2 across all 48 flows
    state_manager.commit_batch_outcome(
        run_id="run_inv",
        decision_batch_id="b_app_1",
        outcomes=[
            FlowBatchOutcome(
                flow_group_id="0x103",
                old_path_id="0x103_P1",
                proposed_path_id="0x103_P2",
                actual_path_id="0x103_P2",
                status=FlowDecisionStatus.APPLIED,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=3,
                evidence=_make_mock_evidence("0x103", "0x103_P2", "2026-04-06T00:01:00Z"),
            ),
            FlowBatchOutcome(
                flow_group_id="0x104",
                old_path_id="0x104_P1",
                proposed_path_id="0x104_P2",
                actual_path_id="0x104_P2",
                status=FlowDecisionStatus.APPLIED,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=3,
                evidence=_make_mock_evidence("0x104", "0x104_P2", "2026-04-06T00:01:00Z"),
            ),
        ],
        completed_at_utc="2026-04-06T00:01:00Z",
    )
    _assert_48_uniform_version("run_inv", 2)

    # Transition 6: Commit PARTIAL -> state_version = 3 across all 48 flows
    state_manager.commit_batch_outcome(
        run_id="run_inv",
        decision_batch_id="b_part_1",
        outcomes=[
            FlowBatchOutcome(
                flow_group_id="0x203",
                old_path_id="0x203_P1",
                proposed_path_id="0x203_P2",
                actual_path_id="0x203_P2",
                status=FlowDecisionStatus.APPLIED,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=3,
                evidence=_make_mock_evidence("0x203", "0x203_P2", "2026-04-06T00:01:20Z"),
            ),
            FlowBatchOutcome(
                flow_group_id="0x204",
                old_path_id="0x204_P1",
                proposed_path_id="0x204_P2",
                actual_path_id="0x204_P1",
                status=FlowDecisionStatus.FAILED,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=1,
                evidence=_make_mock_evidence("0x204", "0x204_P1", "2026-04-06T00:01:20Z"),
            ),
        ],
        completed_at_utc="2026-04-06T00:01:20Z",
    )
    _assert_48_uniform_version("run_inv", 3)

    # Transition 7: Full 48-flow reconciliation after PARTIAL -> state_version = 4 across all 48 flows
    state_manager.reconcile_run_state(
        run_id="run_inv",
        verified_evidences=_make_all_48_p1_evidences(
            topology_registry,
            "2026-04-06T00:01:25Z",
            run_id="run_inv",
            overrides={
                "0x103": "0x103_P2",
                "0x104": "0x104_P2",
                "0x203": "0x203_P2",
            },
        ),
        reconciled_at_utc="2026-04-06T00:01:25Z",
    )
    _assert_48_uniform_version("run_inv", 4)

    # Transition 8: Controller restart -> state_version = 5 across all 48 flows
    state_manager.mark_controller_restart("run_inv", "2026-04-06T00:02:00Z")
    _assert_48_uniform_version("run_inv", 5)

    # Transition 9: Full 48-flow reconciliation after restart -> state_version = 6 across all 48 flows
    state_manager.reconcile_run_state(
        run_id="run_inv",
        verified_evidences=_make_all_48_p1_evidences(
            topology_registry,
            "2026-04-06T00:02:05Z",
            run_id="run_inv",
            overrides={
                "0x103": "0x103_P2",
                "0x104": "0x104_P2",
                "0x203": "0x203_P2",
            },
        ),
        reconciled_at_utc="2026-04-06T00:02:05Z",
    )
    _assert_48_uniform_version("run_inv", 6)


# -----------------------------------------------------------------------------
# 13. Defensive Immutability, Multi-Run Isolation & Legacy ID Rejection
# -----------------------------------------------------------------------------


def test_defensive_event_copy_and_multi_run_isolation(
    state_manager: StateManager, topology_registry: TopologyRegistry
) -> None:
    _init_verified_run(state_manager, topology_registry, "run_A")
    _init_verified_run(state_manager, topology_registry, "run_B")

    # Mutating returned event detail dictionary must NOT alter internal StateManager state
    evts = state_manager.get_events("run_A")
    evts[0].detail["injected_key"] = "corrupted"
    assert "injected_key" not in state_manager.get_events("run_A")[0].detail

    state_manager.commit_batch_outcome(
        run_id="run_A",
        decision_batch_id="batch_A_1",
        outcomes=[
            FlowBatchOutcome(
                flow_group_id="0x103",
                old_path_id="0x103_P1",
                proposed_path_id="0x103_P2",
                actual_path_id="0x103_P2",
                status=FlowDecisionStatus.APPLIED,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=3,
                evidence=_make_mock_evidence("0x103", "0x103_P2", "2026-04-06T00:01:00Z"),
            )
        ],
        completed_at_utc="2026-04-06T00:01:00Z",
    )

    assert state_manager.get_state_version("run_A") == 2
    assert state_manager.get_flow_state("run_A", "0x103").path_id == "0x103_P2"
    assert state_manager.get_state_version("run_B") == 1
    assert state_manager.get_flow_state("run_B", "0x103").path_id == "0x103_P1"

    with pytest.raises(TopologyLookupError):
        state_manager.get_flow_state("run_A", "h1_h3")
