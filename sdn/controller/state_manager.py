"""
StateManager for the SDN Controller (Integration Guide v1.4 & Testing v2 compliant).

Responsibilities:
- Authoritative live routing_state and global state_version per run_id.
- Explicit bootstrap verification: configuring initial P1 paths does NOT auto-mark VERIFIED
  without valid VerificationEvidence for all 48 canonical flows.
- Non-overlapping routing_history intervals (strictly verified paths only) and
  separate UncertaintyGapRecord tracking for UNKNOWN intervals.
- Hold-down tracking from verified actual path changes (bootstrap None = unlocked;
  elapsed < T_hold locked; elapsed >= T_hold unlocked).
- Strict verification evidence checking (Barrier-only is rejected; run_id/flow_id/path_id
  checked; MOCK_SIMULATED vs. LIVE_OVS is explicitly recorded).
- Transactional atomicity across routing_state, state_version, routing_history,
  uncertainty_gaps, and reconcile_required.
- Locking of apply/complete-snapshot during unverified bootstrap, in-flight batches,
  after PARTIAL or INCONSISTENT outcomes, and after controller restart until full
  48-flow reconciliation completes.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Mapping, Optional, Sequence

from sdn.controller.topology_registry import TopologyLookupError, TopologyRegistry
from sdn.schemas.state_models import (
    BatchStatus,
    EvidenceMode,
    FlowBatchOutcome,
    FlowDecisionStatus,
    HistoryIntervalStatus,
    ReasonCode,
    RoutingHistoryInterval,
    RoutingStateEntry,
    RoutingStateSnapshot,
    SDNStateConfig,
    StateTransitionEvent,
    UncertaintyGapRecord,
    VerificationEvidence,
    VerificationStatus,
    normalize_iso8601_fractional_seconds,
)


class StateManagerError(Exception):
    """Base exception for StateManager errors."""


class StateValidationError(StateManagerError, ValueError):
    """Raised when state updates, timestamps, or verification evidence violate v1.4 rules."""


class BatchIdConflictError(StateValidationError):
    """Raised when a duplicate decision_batch_id is submitted with a conflicting payload/outcome (AC-S08)."""

    reason_code: ReasonCode = ReasonCode.BATCH_ID_CONFLICT


class ReconciliationRequiredError(StateManagerError):
    """Raised when an operation requires a clean verified state but reconciliation is pending."""

    reason_code: ReasonCode = ReasonCode.RECONCILIATION_REQUIRED


def parse_utc_timestamp(ts: str | datetime) -> datetime:
    """Parse an RFC3339 / ISO-8601 UTC timestamp string (or datetime) into a timezone-aware UTC datetime."""
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            raise StateValidationError(f"Datetime '{ts}' must be timezone-aware UTC.")
        return ts.astimezone(timezone.utc)
    if not isinstance(ts, str) or not ts.strip():
        raise StateValidationError("Timestamp must be a non-empty RFC3339 UTC string.")
    raw = ts.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(normalize_iso8601_fractional_seconds(raw))
    except ValueError as exc:
        raise StateValidationError(f"Invalid RFC3339 timestamp '{ts}': {exc}") from exc
    if dt.tzinfo is None:
        raise StateValidationError(
            f"Timestamp '{ts}' must include explicit UTC timezone ('Z' or '+00:00')."
        )
    return dt.astimezone(timezone.utc)


def format_utc_timestamp(ts: str | datetime) -> str:
    """Normalize a UTC timestamp to canonical RFC3339 'Z' notation."""
    dt = parse_utc_timestamp(ts)
    if dt.microsecond:
        ms_str = f"{dt.microsecond:06d}".rstrip("0")
        return dt.strftime(f"%Y-%m-%dT%H:%M:%S.{ms_str}Z")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _compute_outcomes_fingerprint(
    outcomes: Sequence[FlowBatchOutcome],
) -> tuple[tuple[str, str, str, Optional[str], str, str, int], ...]:
    """Compute a deterministic fingerprint of per-flow batch outcomes for idempotency / conflict detection."""
    return tuple(
        (
            item.flow_group_id,
            item.old_path_id,
            item.proposed_path_id,
            item.actual_path_id,
            item.status.value if isinstance(item.status, FlowDecisionStatus) else str(item.status),
            (
                item.verification_status.value
                if isinstance(item.verification_status, VerificationStatus)
                else str(item.verification_status)
            ),
            int(item.rule_count),
        )
        for item in outcomes
    )


@dataclass
class _FinalizedBatchRecord:
    """Stored record of a finalized decision batch for AC-S08 idempotency vs. conflict checking."""

    batch_status: BatchStatus
    result_state_version: int
    outcomes_fingerprint: tuple[tuple[str, str, str, Optional[str], str, str, int], ...]
    payload_hash: Optional[str] = None


@dataclass
class _MutableFlowRecord:
    """Internal per-flow mutable state within a single run."""

    flow_group_id: str
    path_id: Optional[str]
    confirmed_at: Optional[str]
    last_successful_change_at: Optional[str]
    last_optimization_change_at: Optional[str]
    verification_status: VerificationStatus
    evidence_mode: Optional[EvidenceMode]


@dataclass
class _RunStateContainer:
    """Isolated state container for a single run_id."""

    run_id: str
    bootstrapped_at_utc: str
    last_transition_at_utc: str
    config_version: str
    state_version: int
    reconcile_required: bool
    reconcile_reason: Optional[str]
    in_flight_batch_id: Optional[str]
    in_flight_started_at_utc: Optional[str] = None
    in_flight_payload_hash: Optional[str] = None
    flows: dict[str, _MutableFlowRecord] = field(default_factory=dict)
    history: dict[str, list[RoutingHistoryInterval]] = field(default_factory=dict)
    uncertainty_gaps: dict[str, list[UncertaintyGapRecord]] = field(default_factory=dict)
    events: list[StateTransitionEvent] = field(default_factory=list)
    finalized_batches: dict[str, _FinalizedBatchRecord] = field(default_factory=dict)


class StateManager:
    """
    Manages authoritative live routing_state, global state_version, routing_history,
    hold-down timers, and reconciliation locks across one or more runs.
    """

    def __init__(
        self,
        topology_registry: TopologyRegistry,
        config: Optional[SDNStateConfig] = None,
    ) -> None:
        self._topology = topology_registry
        self._config = config or SDNStateConfig()
        if (
            self._config.require_explicit_version_policy
            and self._config.version_policy.policy_status == "MUST_LOCK_PROVISIONAL_DEFAULT"
        ):
            raise StateValidationError(
                "SDNStateConfig.require_explicit_version_policy is True, but StateVersionPolicy "
                "is still set to 'MUST_LOCK_PROVISIONAL_DEFAULT'. Provide an explicitly locked policy."
            )
        self._runs: dict[str, _RunStateContainer] = {}

    @property
    def topology(self) -> TopologyRegistry:
        return self._topology

    @property
    def config(self) -> SDNStateConfig:
        return self._config

    def _get_run(self, run_id: str) -> _RunStateContainer:
        if not isinstance(run_id, str) or not run_id.strip():
            raise StateValidationError("run_id must be a non-empty string.")
        container = self._runs.get(run_id)
        if container is None:
            raise StateValidationError(
                f"Run '{run_id}' has not been initialized in StateManager."
            )
        return container

    def _validate_evidence_for_path(
        self,
        run_id: str,
        flow_group_id: str,
        expected_path_id: str,
        evidence: Optional[VerificationEvidence],
    ) -> VerificationEvidence:
        """
        Verify that a flow claiming VERIFIED status has valid verification evidence
        matching run_id, canonical flow_group_id, and candidate path_id (AC-S03).
        """
        if evidence is None:
            raise StateValidationError(
                f"Flow '{flow_group_id}' cannot be marked VERIFIED without VerificationEvidence."
            )
        if evidence.run_id is not None and evidence.run_id != run_id:
            raise StateValidationError(
                f"VerificationEvidence run_id '{evidence.run_id}' does not match target run '{run_id}'."
            )
        if not isinstance(evidence.evidence_mode, EvidenceMode):
            raise StateValidationError(
                f"Flow '{flow_group_id}' evidence must specify EvidenceMode (MOCK_SIMULATED or LIVE_OVS)."
            )
        if evidence.flow_group_id != flow_group_id:
            raise StateValidationError(
                f"VerificationEvidence flow_group_id '{evidence.flow_group_id}' "
                f"does not match target flow '{flow_group_id}'."
            )
        if evidence.verified_path_id != expected_path_id:
            raise StateValidationError(
                f"VerificationEvidence path '{evidence.verified_path_id}' "
                f"does not match claimed path '{expected_path_id}' for flow '{flow_group_id}'."
            )
        if not evidence.is_valid_for_verified:
            raise StateValidationError(
                f"Flow '{flow_group_id}' rejected as VERIFIED: Barrier alone is insufficient; "
                f"both rule_readback_ok ({evidence.rule_readback_ok}) and "
                f"counter_or_probe_ok ({evidence.counter_or_probe_ok}) must be True (AC-S03)."
            )
        parse_utc_timestamp(evidence.verified_at_utc)
        return evidence

    def _validate_flow_timestamp_not_backward(
        self,
        run_state: _RunStateContainer,
        flow_group_id: str,
        candidate_ts_utc: str,
        *,
        context: str,
    ) -> datetime:
        """
        Ensure candidate_ts_utc for flow_group_id does not move backward relative to
        bootstrapped_at_utc, confirmed_at, routing_history, or uncertainty_gaps.
        """
        cand_dt = parse_utc_timestamp(candidate_ts_utc)
        boot_dt = parse_utc_timestamp(run_state.bootstrapped_at_utc)
        if cand_dt < boot_dt:
            raise StateValidationError(
                f"Backward timestamp in {context} for flow '{flow_group_id}': "
                f"'{candidate_ts_utc}' precedes bootstrapped_at_utc '{run_state.bootstrapped_at_utc}'."
            )

        rec = run_state.flows[flow_group_id]
        if rec.confirmed_at is not None:
            conf_dt = parse_utc_timestamp(rec.confirmed_at)
            if cand_dt < conf_dt:
                raise StateValidationError(
                    f"Backward timestamp in {context} for flow '{flow_group_id}': "
                    f"'{candidate_ts_utc}' precedes confirmed_at '{rec.confirmed_at}'."
                )

        intervals = run_state.history[flow_group_id]
        if intervals:
            last_iv = intervals[-1]
            iv_from_dt = parse_utc_timestamp(last_iv.valid_from)
            if cand_dt < iv_from_dt:
                raise StateValidationError(
                    f"Backward timestamp in {context} for flow '{flow_group_id}': "
                    f"'{candidate_ts_utc}' precedes routing_history valid_from '{last_iv.valid_from}'."
                )
            if last_iv.valid_to is not None:
                iv_to_dt = parse_utc_timestamp(last_iv.valid_to)
                if cand_dt < iv_to_dt:
                    raise StateValidationError(
                        f"Backward timestamp in {context} for flow '{flow_group_id}': "
                        f"'{candidate_ts_utc}' precedes routing_history valid_to '{last_iv.valid_to}'."
                    )

        gaps = run_state.uncertainty_gaps[flow_group_id]
        if gaps:
            last_gap = gaps[-1]
            lost_dt = parse_utc_timestamp(last_gap.lost_confirmation_at_utc)
            if cand_dt < lost_dt:
                raise StateValidationError(
                    f"Backward timestamp in {context} for flow '{flow_group_id}': "
                    f"'{candidate_ts_utc}' precedes lost_confirmation_at_utc '{last_gap.lost_confirmation_at_utc}'."
                )
            if last_gap.resolved_at_utc is not None:
                res_dt = parse_utc_timestamp(last_gap.resolved_at_utc)
                if cand_dt < res_dt:
                    raise StateValidationError(
                        f"Backward timestamp in {context} for flow '{flow_group_id}': "
                        f"'{candidate_ts_utc}' precedes resolved_at_utc '{last_gap.resolved_at_utc}'."
                    )
        return cand_dt

    def _append_event(
        self,
        run_state: _RunStateContainer,
        *,
        event_at_utc: str,
        event_type: str,
        batch_id: Optional[str] = None,
        flow_group_id: Optional[str] = None,
        detail: Optional[dict] = None,
    ) -> StateTransitionEvent:
        seq = len(run_state.events) + 1
        evt = StateTransitionEvent(
            event_id=f"sdn:{run_state.run_id}:state_evt:{seq:06d}",
            run_id=run_state.run_id,
            event_at_utc=format_utc_timestamp(event_at_utc),
            event_type=event_type,
            state_version=run_state.state_version,
            batch_id=batch_id,
            flow_group_id=flow_group_id,
            detail=copy.deepcopy(dict(detail or {})),
        )
        run_state.events.append(evt)
        return evt

    def _close_open_history_interval(
        self,
        run_state: _RunStateContainer,
        flow_group_id: str,
        closed_at_utc: str,
    ) -> None:
        """Close the currently open routing_history interval for flow_group_id, enforcing non-overlap."""
        intervals = run_state.history[flow_group_id]
        if not intervals:
            return
        last_interval = intervals[-1]
        if last_interval.valid_to is not None:
            return

        close_dt = parse_utc_timestamp(closed_at_utc)
        start_dt = parse_utc_timestamp(last_interval.valid_from)
        if close_dt < start_dt:
            raise StateValidationError(
                f"Cannot close routing_history interval for flow '{flow_group_id}' at '{closed_at_utc}' "
                f"before its valid_from '{last_interval.valid_from}'."
            )
        intervals[-1] = replace(
            last_interval, valid_to=format_utc_timestamp(closed_at_utc)
        )

    def _open_new_history_interval(
        self,
        run_state: _RunStateContainer,
        *,
        flow_group_id: str,
        path_id: str,
        valid_from_utc: str,
        status: HistoryIntervalStatus,
        batch_id: Optional[str],
        evidence_mode: EvidenceMode,
    ) -> None:
        """Open a new verified routing_history interval, ensuring no overlap with previous intervals."""
        if not path_id:
            raise StateValidationError(
                f"routing_history cannot contain null/empty path_id for flow '{flow_group_id}'."
            )
        intervals = run_state.history[flow_group_id]
        new_start_dt = parse_utc_timestamp(valid_from_utc)

        if intervals:
            prev = intervals[-1]
            if prev.valid_to is None:
                raise StateValidationError(
                    f"Cannot open new routing_history interval for flow '{flow_group_id}' while previous "
                    f"interval from '{prev.valid_from}' is still open."
                )
            prev_end_dt = parse_utc_timestamp(prev.valid_to)
            if new_start_dt < prev_end_dt:
                raise StateValidationError(
                    f"Overlapping routing_history interval for flow '{flow_group_id}': "
                    f"new valid_from '{valid_from_utc}' < previous valid_to '{prev.valid_to}'."
                )

        intervals.append(
            RoutingHistoryInterval(
                run_id=run_state.run_id,
                flow_group_id=flow_group_id,
                path_id=path_id,
                valid_from=format_utc_timestamp(valid_from_utc),
                valid_to=None,
                status=status,
                batch_id=batch_id,
                evidence_mode=evidence_mode,
            )
        )

    def _open_uncertainty_gap(
        self,
        run_state: _RunStateContainer,
        *,
        flow_group_id: str,
        last_known_path_id: Optional[str],
        lost_at_utc: str,
        batch_id: Optional[str],
        reason: Optional[str],
    ) -> None:
        gaps = run_state.uncertainty_gaps[flow_group_id]
        if gaps and gaps[-1].resolved_at_utc is None:
            return
        gaps.append(
            UncertaintyGapRecord(
                run_id=run_state.run_id,
                flow_group_id=flow_group_id,
                last_known_path_id=last_known_path_id,
                lost_confirmation_at_utc=format_utc_timestamp(lost_at_utc),
                resolved_at_utc=None,
                resolved_path_id=None,
                batch_id=batch_id,
                reason=reason,
            )
        )

    def _resolve_open_uncertainty_gap(
        self,
        run_state: _RunStateContainer,
        *,
        flow_group_id: str,
        resolved_at_utc: str,
        resolved_path_id: str,
    ) -> None:
        gaps = run_state.uncertainty_gaps[flow_group_id]
        if not gaps or gaps[-1].resolved_at_utc is not None:
            return
        last_gap = gaps[-1]
        res_dt = parse_utc_timestamp(resolved_at_utc)
        lost_dt = parse_utc_timestamp(last_gap.lost_confirmation_at_utc)
        if res_dt < lost_dt:
            raise StateValidationError(
                f"Cannot resolve uncertainty gap for flow '{flow_group_id}' at '{resolved_at_utc}' "
                f"before lost_confirmation_at_utc '{last_gap.lost_confirmation_at_utc}'."
            )
        gaps[-1] = replace(
            last_gap,
            resolved_at_utc=format_utc_timestamp(resolved_at_utc),
            resolved_path_id=resolved_path_id,
        )

    def _validate_full_48_evidences_for_paths(
        self,
        run_id: str,
        evidences: Mapping[str, VerificationEvidence],
        expected_paths_by_flow: Optional[Mapping[str, str]] = None,
    ) -> dict[str, VerificationEvidence]:
        """
        Validate that `evidences` covers all 48 canonical flows with valid VerificationEvidence.
        Rejects missing flows, unknown/legacy flow keys, wrong run_id/flow_id/path_id, or Barrier-only evidence.
        """
        if not isinstance(evidences, Mapping):
            raise StateValidationError("verification_evidences must be a mapping of flow_group_id -> VerificationEvidence.")

        all_flows = self._topology.get_all_flow_groups()
        expected_ids = {fg.flow_group_id for fg in all_flows}
        provided_ids = set(evidences.keys())

        extra_ids = provided_ids - expected_ids
        if extra_ids:
            raise StateValidationError(
                f"Invalid or non-canonical flow_group_id keys in verification_evidences: {sorted(extra_ids)}"
            )

        missing_ids = expected_ids - provided_ids
        if missing_ids:
            raise StateValidationError(
                f"Full 48-flow verification evidence required for run '{run_id}'; "
                f"got {len(provided_ids)}/48, missing flows: {sorted(missing_ids)}"
            )

        validated: dict[str, VerificationEvidence] = {}
        for fg in all_flows:
            fid = fg.flow_group_id
            ev = evidences[fid]
            if ev is None:
                raise StateValidationError(
                    f"Flow '{fid}' cannot be marked VERIFIED without VerificationEvidence."
                )
            target_path_id = (
                expected_paths_by_flow[fid]
                if expected_paths_by_flow is not None
                else ev.verified_path_id
            )
            try:
                cp = self._topology.get_candidate_path(target_path_id)
            except TopologyLookupError as exc:
                raise StateValidationError(str(exc)) from exc
            if cp.flow_group_id != fid:
                raise StateValidationError(
                    f"Verified path '{target_path_id}' does not belong to flow '{fid}'."
                )
            validated[fid] = self._validate_evidence_for_path(
                run_id, fid, target_path_id, ev
            )
        return validated

    # -------------------------------------------------------------------------
    # 1. Bootstrap & Explicit Initial Route Verification
    # -------------------------------------------------------------------------

    def initialize_run(
        self,
        run_id: str,
        bootstrapped_at_utc: str,
        *,
        verification_evidences: Optional[Mapping[str, VerificationEvidence]] = None,
        config_version: Optional[str] = None,
    ) -> RoutingStateSnapshot:
        """
        Initialize all 48 canonical flows for a run_id from TopologyRegistry's initial_paths (P1).

        CRITICAL CONTRACT RULE (S1, AC-S03, AC-S10):
        - Configuring initial P1 paths does NOT automatically mark routes as VERIFIED.
        - If `verification_evidences` is None:
            * All 48 flows are initialized on their P1 path with `verification_status = UNVERIFIED`,
              `confirmed_at = None`, `last_successful_change_at = None`, and `evidence_mode = None`.
            * `reconcile_required = True` and `routing_state_complete = False`.
            * `routing_history` remains empty until `verify_bootstrap()` or `reconcile_run_state()`
              provides valid VerificationEvidence for all 48 flows.
        - If `verification_evidences` is provided:
            * Must contain valid VerificationEvidence (`rule_readback_ok=True`, `counter_or_probe_ok=True`,
              `barrier_confirmed=True`) for all 48 flows on their initial P1 paths.
            * Marks all 48 flows `VERIFIED`, sets `state_version = 1`, and opens 48 `INITIAL`
              intervals in `routing_history`.
        """
        if not isinstance(run_id, str) or not run_id.strip():
            raise StateValidationError("run_id must be a non-empty string.")
        if run_id in self._runs:
            raise StateValidationError(f"Run '{run_id}' is already initialized.")

        norm_boot_ts = format_utc_timestamp(bootstrapped_at_utc)
        boot_dt = parse_utc_timestamp(norm_boot_ts)
        cfg_ver = (config_version or self._config.config_version).strip()
        all_flows = self._topology.get_all_flow_groups()
        init_paths_by_flow = {
            fg.flow_group_id: self._topology.get_initial_candidate_path(fg.flow_group_id).path_id
            for fg in all_flows
        }

        validated_evidences: Optional[dict[str, VerificationEvidence]] = None
        if verification_evidences is not None:
            validated_evidences = self._validate_full_48_evidences_for_paths(
                run_id,
                verification_evidences,
                expected_paths_by_flow=init_paths_by_flow,
            )
            for fid, ev in validated_evidences.items():
                if parse_utc_timestamp(ev.verified_at_utc) < boot_dt:
                    raise StateValidationError(
                        f"Bootstrap VerificationEvidence for flow '{fid}' at '{ev.verified_at_utc}' "
                        f"precedes bootstrapped_at_utc '{norm_boot_ts}'."
                    )

        is_verified_bootstrap = validated_evidences is not None
        run_state = _RunStateContainer(
            run_id=run_id,
            bootstrapped_at_utc=norm_boot_ts,
            last_transition_at_utc=norm_boot_ts,
            config_version=cfg_ver,
            state_version=1,
            reconcile_required=not is_verified_bootstrap,
            reconcile_reason=None if is_verified_bootstrap else "BOOTSTRAP_UNVERIFIED",
            in_flight_batch_id=None,
        )

        latest_ts_dt = boot_dt
        for fg in all_flows:
            fid = fg.flow_group_id
            init_path_id = init_paths_by_flow[fid]
            run_state.history[fid] = []
            run_state.uncertainty_gaps[fid] = []

            if validated_evidences is not None:
                ev = validated_evidences[fid]
                confirmed_ts = format_utc_timestamp(ev.verified_at_utc)
                ev_dt = parse_utc_timestamp(confirmed_ts)
                if ev_dt > latest_ts_dt:
                    latest_ts_dt = ev_dt
                run_state.flows[fid] = _MutableFlowRecord(
                    flow_group_id=fid,
                    path_id=init_path_id,
                    confirmed_at=confirmed_ts,
                    last_successful_change_at=None,
                    last_optimization_change_at=None,
                    verification_status=VerificationStatus.VERIFIED,
                    evidence_mode=ev.evidence_mode,
                )
                self._open_new_history_interval(
                    run_state,
                    flow_group_id=fid,
                    path_id=init_path_id,
                    valid_from_utc=confirmed_ts,
                    status=HistoryIntervalStatus.INITIAL,
                    batch_id=None,
                    evidence_mode=ev.evidence_mode,
                )
            else:
                run_state.flows[fid] = _MutableFlowRecord(
                    flow_group_id=fid,
                    path_id=init_path_id,
                    confirmed_at=None,
                    last_successful_change_at=None,
                    last_optimization_change_at=None,
                    verification_status=VerificationStatus.UNVERIFIED,
                    evidence_mode=None,
                )

        run_state.last_transition_at_utc = format_utc_timestamp(latest_ts_dt)
        self._append_event(
            run_state,
            event_at_utc=run_state.last_transition_at_utc,
            event_type="RUN_INITIALIZED" if is_verified_bootstrap else "RUN_INITIALIZED_UNVERIFIED",
            detail={
                "flow_count": len(all_flows),
                "verified": is_verified_bootstrap,
                "initial_state_version": 1,
            },
        )
        self._runs[run_id] = run_state
        self.assert_run_invariants(run_id)
        return self.get_routing_state(run_id)

    def verify_bootstrap(
        self,
        run_id: str,
        verification_evidences: Mapping[str, VerificationEvidence],
        verified_at_utc: Optional[str] = None,
    ) -> RoutingStateSnapshot:
        """
        Complete bootstrap verification for a run that was initialized without verification_evidences.
        Requires valid VerificationEvidence for all 48 canonical flows on their initial P1 paths.
        """
        run_state = self._get_run(run_id)
        if any(
            rec.verification_status != VerificationStatus.UNVERIFIED
            for rec in run_state.flows.values()
        ):
            raise StateValidationError(
                f"Run '{run_id}' is not in unverified bootstrap state; use reconcile_run_state() instead."
            )

        all_flows = self._topology.get_all_flow_groups()
        init_paths_by_flow = {
            fg.flow_group_id: self._topology.get_initial_candidate_path(fg.flow_group_id).path_id
            for fg in all_flows
        }
        validated = self._validate_full_48_evidences_for_paths(
            run_id,
            verification_evidences,
            expected_paths_by_flow=init_paths_by_flow,
        )

        staged = copy.deepcopy(run_state)
        latest_dt = parse_utc_timestamp(staged.bootstrapped_at_utc)

        for fg in all_flows:
            fid = fg.flow_group_id
            ev = validated[fid]
            ev_dt = self._validate_flow_timestamp_not_backward(
                staged, fid, ev.verified_at_utc, context="verify_bootstrap"
            )
            if ev_dt > latest_dt:
                latest_dt = ev_dt
            confirmed_ts = format_utc_timestamp(ev.verified_at_utc)
            rec = staged.flows[fid]
            rec.path_id = init_paths_by_flow[fid]
            rec.confirmed_at = confirmed_ts
            rec.verification_status = VerificationStatus.VERIFIED
            rec.evidence_mode = ev.evidence_mode
            self._open_new_history_interval(
                staged,
                flow_group_id=fid,
                path_id=init_paths_by_flow[fid],
                valid_from_utc=confirmed_ts,
                status=HistoryIntervalStatus.INITIAL,
                batch_id=None,
                evidence_mode=ev.evidence_mode,
            )

        event_ts = format_utc_timestamp(verified_at_utc) if verified_at_utc is not None else format_utc_timestamp(latest_dt)
        if parse_utc_timestamp(event_ts) < latest_dt:
            raise StateValidationError(
                f"verified_at_utc '{verified_at_utc}' precedes flow verification timestamp '{format_utc_timestamp(latest_dt)}'."
            )

        staged.reconcile_required = False
        staged.reconcile_reason = None
        staged.last_transition_at_utc = event_ts
        self._append_event(
            staged,
            event_at_utc=event_ts,
            event_type="BOOTSTRAP_VERIFIED",
            detail={"flow_count": len(all_flows), "state_version": staged.state_version},
        )
        self._runs[run_id] = staged
        self.assert_run_invariants(run_id)
        return self.get_routing_state(run_id)

    # -------------------------------------------------------------------------
    # 2. Querying Live State, History, Hold-Down, Locks & Invariants
    # -------------------------------------------------------------------------

    def get_state_version(self, run_id: str) -> int:
        """Return the current global state_version for run_id."""
        return self._get_run(run_id).state_version

    def get_last_transition_at_utc(self, run_id: str) -> str:
        """Return the latest state transition timestamp for run_id."""
        return self._get_run(run_id).last_transition_at_utc

    def is_reconcile_required(self, run_id: str) -> bool:
        """Return True if run_id is blocked awaiting switch flow-table verification/reconciliation."""
        run_state = self._get_run(run_id)
        if run_state.reconcile_required:
            return True
        return any(
            rec.verification_status != VerificationStatus.VERIFIED or rec.path_id is None
            for rec in run_state.flows.values()
        )

    def can_accept_apply(self, run_id: str) -> tuple[bool, Optional[ReasonCode]]:
        """
        Check if the controller can accept a new ApplyRequest for run_id (AC-S02, AC-S04, AC-S08).
        Returns (True, None) when ready, or (False, ReasonCode.BUSY / RECONCILIATION_REQUIRED).
        """
        run_state = self._get_run(run_id)
        if run_state.in_flight_batch_id is not None:
            return (False, ReasonCode.BUSY)
        if self.is_reconcile_required(run_id):
            return (False, ReasonCode.RECONCILIATION_REQUIRED)
        return (True, None)

    def get_flow_state(self, run_id: str, flow_group_id: str) -> RoutingStateEntry:
        """Return the current RoutingStateEntry for a specific canonical flow_group_id."""
        run_state = self._get_run(run_id)
        self._topology.get_flow_group(flow_group_id)
        rec = run_state.flows[flow_group_id]
        return RoutingStateEntry(
            run_id=run_id,
            flow_group_id=rec.flow_group_id,
            path_id=rec.path_id,
            state_version=run_state.state_version,
            confirmed_at=rec.confirmed_at,
            last_successful_change_at=rec.last_successful_change_at,
            verification_status=rec.verification_status,
            config_version=run_state.config_version,
            evidence_mode=rec.evidence_mode,
            last_optimization_change_at=rec.last_optimization_change_at,
        )

    def get_routing_state(self, run_id: str) -> RoutingStateSnapshot:
        """
        Return an immutable RoutingStateSnapshot for run_id across all 48 flows
        ordered by ascending numeric hex flow_group_id.
        """
        run_state = self._get_run(run_id)
        ordered_flows = self._topology.get_all_flow_groups()
        entries: list[RoutingStateEntry] = []
        unknown_ids: list[str] = []
        modes: set[EvidenceMode] = set()

        for fg in ordered_flows:
            rec = run_state.flows[fg.flow_group_id]
            if rec.verification_status != VerificationStatus.VERIFIED or rec.path_id is None:
                unknown_ids.append(fg.flow_group_id)
            if rec.evidence_mode is not None:
                modes.add(rec.evidence_mode)
            entries.append(
                RoutingStateEntry(
                    run_id=run_id,
                    flow_group_id=rec.flow_group_id,
                    path_id=rec.path_id,
                    state_version=run_state.state_version,
                    confirmed_at=rec.confirmed_at,
                    last_successful_change_at=rec.last_successful_change_at,
                    verification_status=rec.verification_status,
                    config_version=run_state.config_version,
                    evidence_mode=rec.evidence_mode,
                    last_optimization_change_at=rec.last_optimization_change_at,
                )
            )

        reconcile_req = run_state.reconcile_required or bool(unknown_ids)
        complete = (
            not reconcile_req
            and run_state.in_flight_batch_id is None
            and len(unknown_ids) == 0
        )
        summary_mode = next(iter(modes)) if len(modes) == 1 else None

        return RoutingStateSnapshot(
            run_id=run_id,
            state_version=run_state.state_version,
            config_version=run_state.config_version,
            topology_version=self._topology.topology_version,
            routing_state_complete=complete,
            reconcile_required=reconcile_req,
            in_flight_batch_id=run_state.in_flight_batch_id,
            unknown_flow_group_ids=tuple(unknown_ids),
            routing_state=tuple(entries),
            evidence_mode=summary_mode,
        )

    def get_routing_history(
        self, run_id: str, flow_group_id: Optional[str] = None
    ) -> tuple[RoutingHistoryInterval, ...]:
        """Return verified routing_history intervals for a flow or the entire run."""
        run_state = self._get_run(run_id)
        if flow_group_id is not None:
            self._topology.get_flow_group(flow_group_id)
            return tuple(run_state.history[flow_group_id])
        all_intervals: list[RoutingHistoryInterval] = []
        for fg in self._topology.get_all_flow_groups():
            all_intervals.extend(run_state.history[fg.flow_group_id])
        return tuple(all_intervals)

    def get_routing_state_as_of(
        self, run_id: str, as_of_utc: str
    ) -> RoutingStateSnapshot:
        """
        Reconstruct the causal RoutingStateSnapshot of `run_id` as of `as_of_utc` (`IF05`, `AC-S10`).
        Prevents future route transitions (`confirmed_at > as_of_utc`) from leaking into a
        historical/cutoff snapshot.
        """
        run_state = self._get_run(run_id)
        as_of_dt = parse_utc_timestamp(as_of_utc)
        last_dt = parse_utc_timestamp(run_state.last_transition_at_utc)
        if as_of_dt >= last_dt:
            return self.get_routing_state(run_id)

        # Reconstruct state_version and reconcile/in-flight status from causal events <= as_of_dt
        causal_sv = 1
        reconcile_req = False
        in_flight_id: Optional[str] = None
        for evt in run_state.events:
            if parse_utc_timestamp(evt.event_at_utc) <= as_of_dt:
                causal_sv = evt.state_version
                if evt.event_type == "RUN_INITIALIZED_UNVERIFIED":
                    reconcile_req = True
                elif evt.event_type in ("RUN_INITIALIZED", "BOOTSTRAP_VERIFIED", "RUN_RECONCILED"):
                    reconcile_req = False
                    in_flight_id = None
                elif evt.event_type == "BATCH_EXECUTION_STARTED":
                    in_flight_id = evt.batch_id
                elif evt.event_type in ("BATCH_COMMITTED", "BATCH_ABORTED_UNKNOWN"):
                    in_flight_id = None
                    if evt.detail.get("reconcile_required"):
                        reconcile_req = True
                elif evt.event_type == "CONTROLLER_RESTART_RECONCILE_REQUIRED":
                    in_flight_id = None
                    reconcile_req = True

        ordered_flows = self._topology.get_all_flow_groups()
        entries: list[RoutingStateEntry] = []
        unknown_ids: list[str] = []
        modes: set[EvidenceMode] = set()

        for fg in ordered_flows:
            fid = fg.flow_group_id
            # Check if an uncertainty gap was active at as_of_dt
            active_gap = next(
                (
                    g
                    for g in reversed(run_state.uncertainty_gaps[fid])
                    if parse_utc_timestamp(g.lost_confirmation_at_utc) <= as_of_dt
                    and (
                        g.resolved_at_utc is None
                        or parse_utc_timestamp(g.resolved_at_utc) > as_of_dt
                    )
                ),
                None,
            )

            # Find causal history intervals that started on or before as_of_dt
            causal_intervals = [
                iv
                for iv in run_state.history[fid]
                if parse_utc_timestamp(iv.valid_from) <= as_of_dt
            ]

            last_change_ts: Optional[str] = None
            last_opt_ts: Optional[str] = None
            prev_pid: Optional[str] = None
            for iv in causal_intervals:
                if iv.status == HistoryIntervalStatus.APPLIED:
                    last_change_ts = iv.valid_from
                    last_opt_ts = iv.valid_from
                elif (
                    iv.status == HistoryIntervalStatus.RECONCILED
                    and prev_pid is not None
                    and prev_pid != iv.path_id
                ):
                    last_change_ts = iv.valid_from
                prev_pid = iv.path_id

            if active_gap is not None:
                unknown_ids.append(fid)
                entries.append(
                    RoutingStateEntry(
                        run_id=run_id,
                        flow_group_id=fid,
                        path_id=None,
                        state_version=causal_sv,
                        confirmed_at=None,
                        last_successful_change_at=last_change_ts,
                        verification_status=VerificationStatus.UNKNOWN,
                        config_version=run_state.config_version,
                        evidence_mode=None,
                        last_optimization_change_at=last_opt_ts,
                    )
                )
                continue

            active_iv = next(
                (
                    iv
                    for iv in reversed(causal_intervals)
                    if iv.valid_to is None
                    or parse_utc_timestamp(iv.valid_to) > as_of_dt
                ),
                None,
            )

            if active_iv is not None:
                modes.add(active_iv.evidence_mode)
                entries.append(
                    RoutingStateEntry(
                        run_id=run_id,
                        flow_group_id=fid,
                        path_id=active_iv.path_id,
                        state_version=causal_sv,
                        confirmed_at=active_iv.valid_from,
                        last_successful_change_at=last_change_ts,
                        verification_status=VerificationStatus.VERIFIED,
                        config_version=run_state.config_version,
                        evidence_mode=active_iv.evidence_mode,
                        last_optimization_change_at=last_opt_ts,
                    )
                )
            else:
                init_pid = self._topology.get_initial_candidate_path(fid).path_id
                unknown_ids.append(fid)
                entries.append(
                    RoutingStateEntry(
                        run_id=run_id,
                        flow_group_id=fid,
                        path_id=init_pid,
                        state_version=causal_sv,
                        confirmed_at=None,
                        last_successful_change_at=None,
                        verification_status=VerificationStatus.UNVERIFIED,
                        config_version=run_state.config_version,
                        evidence_mode=None,
                        last_optimization_change_at=None,
                    )
                )

        eff_reconcile = reconcile_req or bool(unknown_ids)
        complete = (
            not eff_reconcile and in_flight_id is None and len(unknown_ids) == 0
        )
        summary_mode = next(iter(modes)) if len(modes) == 1 else None

        return RoutingStateSnapshot(
            run_id=run_id,
            state_version=causal_sv,
            config_version=run_state.config_version,
            topology_version=self._topology.topology_version,
            routing_state_complete=complete,
            reconcile_required=eff_reconcile,
            in_flight_batch_id=in_flight_id,
            unknown_flow_group_ids=tuple(unknown_ids),
            routing_state=tuple(entries),
            evidence_mode=summary_mode,
        )

    def get_routing_history_in_window(
        self,
        run_id: str,
        window_start_utc: str,
        window_end_utc: str,
        *,
        as_of_utc: Optional[str] = None,
    ) -> tuple[RoutingHistoryInterval, ...]:
        """
        Return verified `RoutingHistoryInterval` entries overlapping `[window_start_utc, window_end_utc)`
        causally clipped at `as_of_utc` (defaults to `window_end_utc`) so future route closures
        after `as_of_utc` never leak into a historical snapshot (`IF05`, `AC-S10`).
        """
        run_state = self._get_run(run_id)
        w_start_dt = parse_utc_timestamp(window_start_utc)
        w_end_dt = parse_utc_timestamp(window_end_utc)
        as_of_dt = parse_utc_timestamp(as_of_utc or window_end_utc)

        clipped: list[RoutingHistoryInterval] = []
        for fg in self._topology.get_all_flow_groups():
            fid = fg.flow_group_id
            for iv in run_state.history[fid]:
                from_dt = parse_utc_timestamp(iv.valid_from)
                if from_dt >= w_end_dt or from_dt > as_of_dt:
                    continue
                if iv.valid_to is not None:
                    to_dt = parse_utc_timestamp(iv.valid_to)
                    if to_dt <= w_start_dt:
                        continue
                    # If the interval was closed AFTER as_of_dt, it was still open at as_of_dt!
                    causal_valid_to = None if to_dt > as_of_dt else iv.valid_to
                else:
                    causal_valid_to = None
                clipped.append(replace(iv, valid_to=causal_valid_to))
        return tuple(clipped)

    def get_uncertainty_gaps(
        self, run_id: str, flow_group_id: Optional[str] = None
    ) -> tuple[UncertaintyGapRecord, ...]:
        """Return UNKNOWN uncertainty gap records for a flow or the entire run."""
        run_state = self._get_run(run_id)
        if flow_group_id is not None:
            self._topology.get_flow_group(flow_group_id)
            return tuple(run_state.uncertainty_gaps[flow_group_id])
        all_gaps: list[UncertaintyGapRecord] = []
        for fg in self._topology.get_all_flow_groups():
            all_gaps.extend(run_state.uncertainty_gaps[fg.flow_group_id])
        return tuple(all_gaps)

    def get_events(self, run_id: str) -> tuple[StateTransitionEvent, ...]:
        """Return a defensive copy of all state transition events recorded for run_id."""
        run_state = self._get_run(run_id)
        return tuple(
            replace(evt, detail=copy.deepcopy(evt.detail)) for evt in run_state.events
        )

    def assert_run_invariants(self, run_id: str) -> None:
        """
        Verify internal consistency invariants across state_version, routing_state,
        routing_history, uncertainty_gaps, and reconcile_required for all 48 flows (interfaces_v2.md §11).
        Raises StateValidationError if any invariant is violated.
        """
        snap = self.get_routing_state(run_id)
        expected_flows = self._topology.get_all_flow_groups()
        if len(snap.routing_state) != len(expected_flows):
            raise StateValidationError(
                f"Invariant violation: expected {len(expected_flows)} flows, got {len(snap.routing_state)}."
            )

        for fg, entry in zip(expected_flows, snap.routing_state):
            fid = fg.flow_group_id
            if entry.flow_group_id != fid:
                raise StateValidationError(
                    f"Invariant violation: flow order mismatch '{entry.flow_group_id}' != '{fid}'."
                )
            if entry.state_version != snap.state_version:
                raise StateValidationError(
                    f"Invariant violation: flow '{fid}' state_version ({entry.state_version}) "
                    f"!= run state_version ({snap.state_version})."
                )

            hist = self.get_routing_history(run_id, fid)
            gaps = self.get_uncertainty_gaps(run_id, fid)

            # Verify non-overlapping history intervals and non-null path_id
            for idx, iv in enumerate(hist):
                if not iv.path_id:
                    raise StateValidationError(
                        f"Invariant violation: null/empty path_id in routing_history for '{fid}'."
                    )
                from_dt = parse_utc_timestamp(iv.valid_from)
                if iv.valid_to is not None:
                    to_dt = parse_utc_timestamp(iv.valid_to)
                    if to_dt < from_dt:
                        raise StateValidationError(
                            f"Invariant violation: valid_to < valid_from in routing_history for '{fid}'."
                        )
                elif idx != len(hist) - 1:
                    raise StateValidationError(
                        f"Invariant violation: non-terminal open routing_history interval for '{fid}'."
                    )
                if idx > 0:
                    prev_iv = hist[idx - 1]
                    if prev_iv.valid_to is None or parse_utc_timestamp(prev_iv.valid_to) > from_dt:
                        raise StateValidationError(
                            f"Invariant violation: overlapping routing_history intervals for '{fid}'."
                        )

            if entry.verification_status == VerificationStatus.VERIFIED:
                if entry.path_id is None or entry.confirmed_at is None or entry.evidence_mode is None:
                    raise StateValidationError(
                        f"Invariant violation: VERIFIED flow '{fid}' has null path_id/confirmed_at/evidence_mode."
                    )
                if not hist or hist[-1].valid_to is not None or hist[-1].path_id != entry.path_id:
                    raise StateValidationError(
                        f"Invariant violation: VERIFIED flow '{fid}' does not match open routing_history interval."
                    )
                if gaps and gaps[-1].resolved_at_utc is None:
                    raise StateValidationError(
                        f"Invariant violation: VERIFIED flow '{fid}' still has an unresolved UncertaintyGapRecord."
                    )
            elif entry.verification_status == VerificationStatus.UNKNOWN:
                if entry.path_id is not None or entry.confirmed_at is not None:
                    raise StateValidationError(
                        f"Invariant violation: UNKNOWN flow '{fid}' must have path_id=None and confirmed_at=None."
                    )
                if hist and hist[-1].valid_to is None:
                    raise StateValidationError(
                        f"Invariant violation: UNKNOWN flow '{fid}' still has an open routing_history interval."
                    )
                if not gaps or gaps[-1].resolved_at_utc is not None:
                    raise StateValidationError(
                        f"Invariant violation: UNKNOWN flow '{fid}' is missing an open UncertaintyGapRecord."
                    )
            elif entry.verification_status == VerificationStatus.UNVERIFIED:
                if entry.confirmed_at is not None or hist:
                    raise StateValidationError(
                        f"Invariant violation: UNVERIFIED flow '{fid}' must not have confirmed_at or routing_history."
                    )

    def is_hold_down_active(
        self,
        run_id: str,
        flow_group_id: str,
        reference_time_utc: str | datetime,
        *,
        hold_down_s: Optional[float] = None,
    ) -> bool:
        """
        Evaluate hold-down lock for flow_group_id at reference_time_utc (AC-O04, AC-S02):
        - Bootstrap (last_successful_change_at is None) -> False (unlocked).
        - elapsed < T_hold -> True (locked).
        - elapsed == T_hold -> False (unlocked).
        - elapsed > T_hold -> False (unlocked).
        """
        run_state = self._get_run(run_id)
        self._topology.get_flow_group(flow_group_id)
        rec = run_state.flows[flow_group_id]

        if rec.last_successful_change_at is None:
            return False

        ref_dt = parse_utc_timestamp(reference_time_utc)
        last_dt = parse_utc_timestamp(rec.last_successful_change_at)
        elapsed_s = (ref_dt - last_dt).total_seconds()
        if elapsed_s < 0:
            raise StateValidationError(
                f"reference_time_utc '{reference_time_utc}' precedes last_successful_change_at "
                f"'{rec.last_successful_change_at}' for flow '{flow_group_id}'."
            )

        threshold_s = (
            self._config.hold_down_s if hold_down_s is None else float(hold_down_s)
        )
        return elapsed_s < threshold_s

    # -------------------------------------------------------------------------
    # 3. Proposal-Only, Pre-Write Rejection, and In-Flight Batch Guards
    # -------------------------------------------------------------------------

    def record_proposal_only(
        self,
        run_id: str,
        decision_batch_id: str,
        proposed_changes: Mapping[str, str],
        recorded_at_utc: str,
    ) -> int:
        """
        Record that an optimization proposal was received without executing FlowMods.
        Per REQ-05, REQ-16, and AC-S07: receiving a proposal NEVER alters verified
        routing_state, last_successful_change_at, or global state_version.
        """
        run_state = self._get_run(run_id)
        norm_ts = format_utc_timestamp(recorded_at_utc)
        for fid, new_pid in proposed_changes.items():
            try:
                self._topology.get_flow_group(fid)
                cp = self._topology.get_candidate_path(new_pid)
            except TopologyLookupError as exc:
                raise StateValidationError(str(exc)) from exc
            if cp.flow_group_id != fid:
                raise StateValidationError(
                    f"Proposed path '{new_pid}' does not belong to flow '{fid}'."
                )

        staged = copy.deepcopy(run_state)
        self._append_event(
            staged,
            event_at_utc=norm_ts,
            event_type="PROPOSAL_RECEIVED_NO_STATE_CHANGE",
            batch_id=decision_batch_id,
            detail={"proposed_changes": dict(proposed_changes)},
        )
        self._runs[run_id] = staged
        return staged.state_version

    def record_rejected_request(
        self,
        run_id: str,
        decision_batch_id: str,
        reason: ReasonCode | str,
        rejected_at_utc: str,
    ) -> int:
        """
        Record a pre-write ApplyRequest rejection (BatchStatus.REJECTED).
        Per S4, AC-S02, and REQ-16: rejecting before FlowMod leaves state_version
        and verified routing_state completely unchanged.
        """
        run_state = self._get_run(run_id)
        norm_ts = format_utc_timestamp(rejected_at_utc)
        reason_str = reason.value if isinstance(reason, ReasonCode) else str(reason)

        staged = copy.deepcopy(run_state)
        self._append_event(
            staged,
            event_at_utc=norm_ts,
            event_type="BATCH_REJECTED_PRE_WRITE",
            batch_id=decision_batch_id,
            detail={"status": BatchStatus.REJECTED.value, "reason": reason_str},
        )
        self._runs[run_id] = staged
        return staged.state_version

    def begin_batch_execution(
        self,
        run_id: str,
        decision_batch_id: str,
        started_at_utc: str,
        *,
        expected_state_version: Optional[int] = None,
        payload_hash: Optional[str] = None,
    ) -> None:
        """
        Acquire the single in-flight batch execution lock for run_id.
        Fails if expected_state_version mismatches, decision_batch_id conflicts,
        another batch is in flight (BUSY), or reconciliation is required.
        """
        run_state = self._get_run(run_id)
        if not isinstance(decision_batch_id, str) or not decision_batch_id.strip():
            raise StateValidationError("decision_batch_id must be a non-empty string.")
        norm_start_ts = format_utc_timestamp(started_at_utc)
        start_dt = parse_utc_timestamp(norm_start_ts)
        if start_dt < parse_utc_timestamp(run_state.last_transition_at_utc):
            raise StateValidationError(
                f"Backward timestamp in begin_batch_execution: started_at_utc '{norm_start_ts}' "
                f"precedes last_transition_at_utc '{run_state.last_transition_at_utc}'."
            )

        if decision_batch_id in run_state.finalized_batches:
            prev_rec = run_state.finalized_batches[decision_batch_id]
            if (
                payload_hash is not None
                and prev_rec.payload_hash is not None
                and payload_hash != prev_rec.payload_hash
            ):
                raise BatchIdConflictError(
                    f"Duplicate decision_batch_id '{decision_batch_id}' with conflicting payload_hash "
                    f"({ReasonCode.BATCH_ID_CONFLICT.value})."
                )
            raise StateValidationError(
                f"Batch '{decision_batch_id}' has already been finalized for run '{run_id}'."
            )

        if (
            run_state.in_flight_batch_id == decision_batch_id
            and payload_hash is not None
            and run_state.in_flight_payload_hash is not None
            and payload_hash != run_state.in_flight_payload_hash
        ):
            raise BatchIdConflictError(
                f"In-flight decision_batch_id '{decision_batch_id}' re-submitted with conflicting "
                f"payload_hash ({ReasonCode.BATCH_ID_CONFLICT.value})."
            )

        can_apply, block_reason = self.can_accept_apply(run_id)
        if not can_apply:
            if block_reason == ReasonCode.RECONCILIATION_REQUIRED:
                raise ReconciliationRequiredError(
                    f"Run '{run_id}' cannot begin batch '{decision_batch_id}': reconciliation required."
                )
            raise StateValidationError(
                f"Run '{run_id}' cannot begin batch '{decision_batch_id}': "
                f"another batch '{run_state.in_flight_batch_id}' is in flight ({ReasonCode.BUSY.value})."
            )

        if (
            expected_state_version is not None
            and expected_state_version != run_state.state_version
        ):
            raise StateValidationError(
                f"Cannot begin batch '{decision_batch_id}': expected_state_version={expected_state_version} "
                f"!= current state_version={run_state.state_version} ({ReasonCode.VERSION_MISMATCH.value})."
            )

        staged = copy.deepcopy(run_state)
        staged.in_flight_batch_id = decision_batch_id
        staged.in_flight_started_at_utc = norm_start_ts
        staged.in_flight_payload_hash = payload_hash
        staged.last_transition_at_utc = norm_start_ts
        self._append_event(
            staged,
            event_at_utc=norm_start_ts,
            event_type="BATCH_EXECUTION_STARTED",
            batch_id=decision_batch_id,
            detail={"payload_hash": payload_hash} if payload_hash else None,
        )
        self._runs[run_id] = staged

    # -------------------------------------------------------------------------
    # 4. Commit Batch Outcome (APPLIED / ROLLED_BACK / FAILED / PARTIAL / INCONSISTENT)
    # -------------------------------------------------------------------------

    def commit_batch_outcome(
        self,
        run_id: str,
        decision_batch_id: str,
        outcomes: Sequence[FlowBatchOutcome],
        completed_at_utc: str,
        *,
        expected_state_version: Optional[int] = None,
        payload_hash: Optional[str] = None,
    ) -> tuple[BatchStatus, int]:
        """
        Atomically commit the verified per-flow outcomes of a decision batch at finalization
        (S6, S7, AC-S04, AC-S07, AC-S08, REQ-16):

        - Idempotent if the same decision_batch_id + identical outcomes/payload_hash was already finalized.
        - Raises BatchIdConflictError (BATCH_ID_CONFLICT) if decision_batch_id was already finalized
          (or started in-flight) with different outcomes or payload_hash.
        - Validates expected_state_version (VERSION_MISMATCH), in-flight lock (BUSY), canonical IDs,
          monotonic timestamps, and VerificationEvidence before mutating any state.
        - Stages all updates on a transactional copy so any error midway leaves run_state untouched.
        """
        run_state = self._get_run(run_id)
        if not isinstance(decision_batch_id, str) or not decision_batch_id.strip():
            raise StateValidationError("decision_batch_id must be a non-empty string.")
        if not outcomes:
            raise StateValidationError("outcomes cannot be empty when committing a batch.")

        incoming_fp = _compute_outcomes_fingerprint(outcomes)

        # AC-S08: Idempotent replay vs. BATCH_ID_CONFLICT
        if decision_batch_id in run_state.finalized_batches:
            prev_rec = run_state.finalized_batches[decision_batch_id]
            hash_conflict = (
                payload_hash is not None
                and prev_rec.payload_hash is not None
                and payload_hash != prev_rec.payload_hash
            )
            if hash_conflict or incoming_fp != prev_rec.outcomes_fingerprint:
                raise BatchIdConflictError(
                    f"Batch '{decision_batch_id}' in run '{run_id}' conflicts with previously "
                    f"finalized batch payload/outcomes ({ReasonCode.BATCH_ID_CONFLICT.value})."
                )
            return (prev_rec.batch_status, prev_rec.result_state_version)

        if (
            run_state.in_flight_batch_id is not None
            and run_state.in_flight_batch_id != decision_batch_id
        ):
            raise StateValidationError(
                f"Cannot commit batch '{decision_batch_id}' while '{run_state.in_flight_batch_id}' "
                f"is in flight ({ReasonCode.BUSY.value})."
            )
        if (
            run_state.in_flight_batch_id == decision_batch_id
            and payload_hash is not None
            and run_state.in_flight_payload_hash is not None
            and payload_hash != run_state.in_flight_payload_hash
        ):
            raise BatchIdConflictError(
                f"Batch '{decision_batch_id}' payload_hash conflicts with in-flight batch "
                f"({ReasonCode.BATCH_ID_CONFLICT.value})."
            )
        if run_state.in_flight_batch_id is None and self.is_reconcile_required(run_id):
            raise ReconciliationRequiredError(
                f"Run '{run_id}' requires reconciliation before committing new batch '{decision_batch_id}'."
            )
        if (
            expected_state_version is not None
            and expected_state_version != run_state.state_version
        ):
            raise StateValidationError(
                f"Cannot commit batch '{decision_batch_id}': expected_state_version={expected_state_version} "
                f"!= current state_version={run_state.state_version} ({ReasonCode.VERSION_MISMATCH.value})."
            )
        if len(outcomes) > self._config.max_changed_flows_B:
            raise StateValidationError(
                f"Batch '{decision_batch_id}' has {len(outcomes)} flow outcomes, exceeding "
                f"max_changed_flows_B={self._config.max_changed_flows_B}."
            )

        norm_completed_at = format_utc_timestamp(completed_at_utc)
        completed_dt = parse_utc_timestamp(norm_completed_at)
        if run_state.in_flight_started_at_utc is not None:
            if completed_dt < parse_utc_timestamp(run_state.in_flight_started_at_utc):
                raise StateValidationError(
                    f"Backward timestamp: completed_at_utc '{norm_completed_at}' precedes "
                    f"in_flight_started_at_utc '{run_state.in_flight_started_at_utc}'."
                )

        # Stage all mutations on an isolated transactional copy for strict atomicity
        staged = copy.deepcopy(run_state)

        # Phase A: Validate all FlowBatchOutcome items & timestamps before mutating staged state
        seen_flows: set[str] = set()
        validated_evidence_by_flow: dict[str, Optional[VerificationEvidence]] = {}

        for item in outcomes:
            fid = item.flow_group_id
            try:
                self._topology.get_flow_group(fid)
                old_cp = self._topology.get_candidate_path(item.old_path_id)
                prop_cp = self._topology.get_candidate_path(item.proposed_path_id)
            except TopologyLookupError as exc:
                raise StateValidationError(str(exc)) from exc

            if old_cp.flow_group_id != fid or prop_cp.flow_group_id != fid:
                raise StateValidationError(
                    f"Outcome paths ({item.old_path_id}, {item.proposed_path_id}) do not belong to flow '{fid}'."
                )
            if fid in seen_flows:
                raise StateValidationError(
                    f"Duplicate flow_group_id '{fid}' in batch '{decision_batch_id}' outcomes."
                )
            seen_flows.add(fid)

            current_rec = staged.flows[fid]
            if (
                current_rec.verification_status != VerificationStatus.VERIFIED
                or current_rec.path_id != item.old_path_id
            ):
                raise StateValidationError(
                    f"Flow '{fid}' old_path_id '{item.old_path_id}' does not match current VERIFIED state "
                    f"({current_rec.path_id}, {current_rec.verification_status.value})."
                )

            if item.status == FlowDecisionStatus.UNKNOWN or (
                item.verification_status == VerificationStatus.UNKNOWN
            ):
                if item.actual_path_id is not None:
                    raise StateValidationError(
                        f"Flow '{fid}' is UNKNOWN; actual_path_id MUST be None (cannot fake old/new path)."
                    )
                if (
                    item.status != FlowDecisionStatus.UNKNOWN
                    or item.verification_status != VerificationStatus.UNKNOWN
                ):
                    raise StateValidationError(
                        f"Flow '{fid}' with UNKNOWN status must set both status=UNKNOWN and verification_status=UNKNOWN."
                    )
                eff_ts = format_utc_timestamp(item.applied_at_utc or norm_completed_at)
                eff_dt = self._validate_flow_timestamp_not_backward(
                    staged, fid, eff_ts, context="commit_batch_outcome(UNKNOWN)"
                )
                if completed_dt < eff_dt:
                    raise StateValidationError(
                        f"completed_at_utc '{norm_completed_at}' precedes flow '{fid}' applied_at_utc '{eff_ts}'."
                    )
                validated_evidence_by_flow[fid] = None
            else:
                if item.actual_path_id is None:
                    raise StateValidationError(
                        f"Flow '{fid}' with status '{item.status.value}' must specify a verified actual_path_id."
                    )
                try:
                    actual_cp = self._topology.get_candidate_path(item.actual_path_id)
                except TopologyLookupError as exc:
                    raise StateValidationError(str(exc)) from exc
                if actual_cp.flow_group_id != fid:
                    raise StateValidationError(
                        f"actual_path_id '{item.actual_path_id}' does not belong to flow '{fid}'."
                    )
                if item.verification_status != VerificationStatus.VERIFIED:
                    raise StateValidationError(
                        f"Flow '{fid}' with known actual_path_id '{item.actual_path_id}' must have "
                        f"verification_status=VERIFIED (got '{item.verification_status.value}')."
                    )
                if item.status == FlowDecisionStatus.APPLIED and (
                    item.actual_path_id != item.proposed_path_id
                ):
                    raise StateValidationError(
                        f"Flow '{fid}' marked APPLIED must have actual_path_id == proposed_path_id "
                        f"('{item.proposed_path_id}'), got '{item.actual_path_id}'."
                    )
                if item.status in (
                    FlowDecisionStatus.ROLLED_BACK,
                    FlowDecisionStatus.FAILED,
                    FlowDecisionStatus.KEPT,
                ) and (item.actual_path_id != item.old_path_id):
                    raise StateValidationError(
                        f"Flow '{fid}' marked {item.status.value} must have actual_path_id == old_path_id "
                        f"('{item.old_path_id}'), got '{item.actual_path_id}'."
                    )

                ev = self._validate_evidence_for_path(
                    run_id, fid, item.actual_path_id, item.evidence
                )
                ev_dt = self._validate_flow_timestamp_not_backward(
                    staged, fid, ev.verified_at_utc, context=f"commit_batch_outcome({item.status.value})"
                )
                if completed_dt < ev_dt:
                    raise StateValidationError(
                        f"completed_at_utc '{norm_completed_at}' precedes flow '{fid}' verified_at_utc '{ev.verified_at_utc}'."
                    )
                validated_evidence_by_flow[fid] = ev

        # Phase B: Apply state transitions on staged copy & determine if global state_version increments
        should_increment_version = False
        statuses: list[FlowDecisionStatus] = []

        for item in outcomes:
            fid = item.flow_group_id
            rec = staged.flows[fid]
            prev_path = rec.path_id
            statuses.append(item.status)
            applied_ts = format_utc_timestamp(item.applied_at_utc or norm_completed_at)

            if item.status == FlowDecisionStatus.UNKNOWN:
                self._close_open_history_interval(staged, fid, applied_ts)
                self._open_uncertainty_gap(
                    staged,
                    flow_group_id=fid,
                    last_known_path_id=prev_path,
                    lost_at_utc=applied_ts,
                    batch_id=decision_batch_id,
                    reason=item.reason or ReasonCode.VERIFY_TIMEOUT.value,
                )
                rec.path_id = None
                rec.confirmed_at = None
                rec.verification_status = VerificationStatus.UNKNOWN
                should_increment_version = True
            else:
                ev = validated_evidence_by_flow[fid]
                assert ev is not None
                verified_ts = format_utc_timestamp(ev.verified_at_utc)
                rec.evidence_mode = ev.evidence_mode

                if item.status == FlowDecisionStatus.APPLIED:
                    self._close_open_history_interval(staged, fid, verified_ts)
                    self._open_new_history_interval(
                        staged,
                        flow_group_id=fid,
                        path_id=ev.verified_path_id,
                        valid_from_utc=verified_ts,
                        status=HistoryIntervalStatus.APPLIED,
                        batch_id=decision_batch_id,
                        evidence_mode=ev.evidence_mode,
                    )
                    rec.path_id = ev.verified_path_id
                    rec.confirmed_at = verified_ts
                    rec.last_successful_change_at = verified_ts
                    rec.last_optimization_change_at = verified_ts
                    rec.verification_status = VerificationStatus.VERIFIED
                    should_increment_version = True

                elif item.status == FlowDecisionStatus.ROLLED_BACK:
                    self._close_open_history_interval(staged, fid, verified_ts)
                    self._open_new_history_interval(
                        staged,
                        flow_group_id=fid,
                        path_id=ev.verified_path_id,
                        valid_from_utc=verified_ts,
                        status=HistoryIntervalStatus.ROLLED_BACK,
                        batch_id=decision_batch_id,
                        evidence_mode=ev.evidence_mode,
                    )
                    rec.path_id = ev.verified_path_id
                    rec.confirmed_at = verified_ts
                    rec.verification_status = VerificationStatus.VERIFIED
                    if (
                        item.had_intermediate_physical_change
                        and self._config.version_policy.update_hold_down_on_same_path_rollback
                    ):
                        rec.last_successful_change_at = verified_ts
                    if self._config.version_policy.increment_on_same_path_verified_rollback:
                        should_increment_version = True

                elif item.status in (FlowDecisionStatus.FAILED, FlowDecisionStatus.KEPT):
                    rec.confirmed_at = verified_ts
                    rec.verification_status = VerificationStatus.VERIFIED

        # Phase C: Determine BatchStatus (interfaces_v2.md §10)
        status_set = set(statuses)
        if FlowDecisionStatus.UNKNOWN in status_set:
            batch_status = BatchStatus.INCONSISTENT
        elif status_set == {FlowDecisionStatus.APPLIED}:
            batch_status = BatchStatus.APPLIED
        elif status_set == {FlowDecisionStatus.ROLLED_BACK}:
            batch_status = BatchStatus.ROLLED_BACK
        elif status_set <= {FlowDecisionStatus.FAILED, FlowDecisionStatus.KEPT}:
            batch_status = BatchStatus.FAILED
        else:
            batch_status = BatchStatus.PARTIAL

        if should_increment_version:
            staged.state_version += 1

        if batch_status in (BatchStatus.PARTIAL, BatchStatus.INCONSISTENT):
            staged.reconcile_required = True
            staged.reconcile_reason = batch_status.value

        eff_payload_hash = payload_hash or staged.in_flight_payload_hash
        staged.in_flight_batch_id = None
        staged.in_flight_started_at_utc = None
        staged.in_flight_payload_hash = None
        staged.last_transition_at_utc = norm_completed_at
        staged.finalized_batches[decision_batch_id] = _FinalizedBatchRecord(
            batch_status=batch_status,
            result_state_version=staged.state_version,
            outcomes_fingerprint=incoming_fp,
            payload_hash=eff_payload_hash,
        )

        self._append_event(
            staged,
            event_at_utc=norm_completed_at,
            event_type="BATCH_COMMITTED",
            batch_id=decision_batch_id,
            detail={
                "batch_status": batch_status.value,
                "state_version": staged.state_version,
                "reconcile_required": staged.reconcile_required,
                "per_flow_status": {o.flow_group_id: o.status.value for o in outcomes},
            },
        )

        # Atomic commit of staged run state
        self._runs[run_id] = staged
        self.assert_run_invariants(run_id)
        return (batch_status, staged.state_version)

    def abort_in_flight_batch_as_unknown(
        self,
        run_id: str,
        decision_batch_id: str,
        flow_group_ids: Sequence[str],
        aborted_at_utc: str,
        *,
        reason: str = ReasonCode.ROUTE_STATE_UNKNOWN.value,
    ) -> int:
        """
        Emergency crash-consistency transition if an unexpected exception occurs after
        `begin_batch_execution` (when switch writes may have started or completed, or
        `commit_batch_outcome` failed) (`AC-S04`, `REQ-13`, `REQ-16`).

        Marks all affected flows in `flow_group_ids` as `UNKNOWN` (`path_id=None`),
        closes their open `routing_history` intervals, opens `UncertaintyGapRecord`s,
        increments `state_version` by `+1`, clears `in_flight_batch_id`, and sets
        `reconcile_required = True`.
        """
        run_state = self._get_run(run_id)
        norm_ts = format_utc_timestamp(aborted_at_utc)
        staged = copy.deepcopy(run_state)

        for fid in flow_group_ids:
            if fid not in staged.flows:
                continue
            rec = staged.flows[fid]
            prev_path = rec.path_id
            self._close_open_history_interval(staged, fid, norm_ts)
            self._open_uncertainty_gap(
                staged,
                flow_group_id=fid,
                last_known_path_id=prev_path,
                lost_at_utc=norm_ts,
                batch_id=decision_batch_id,
                reason=reason,
            )
            rec.path_id = None
            rec.confirmed_at = None
            rec.verification_status = VerificationStatus.UNKNOWN

        staged.in_flight_batch_id = None
        staged.in_flight_started_at_utc = None
        staged.in_flight_payload_hash = None
        staged.reconcile_required = True
        staged.reconcile_reason = "BATCH_ABORTED_UNKNOWN"
        staged.state_version += 1
        staged.last_transition_at_utc = norm_ts

        self._append_event(
            staged,
            event_at_utc=norm_ts,
            event_type="BATCH_ABORTED_UNKNOWN",
            batch_id=decision_batch_id,
            detail={
                "affected_flows": list(flow_group_ids),
                "reason": reason,
                "state_version": staged.state_version,
            },
        )
        self._runs[run_id] = staged
        self.assert_run_invariants(run_id)
        return staged.state_version

    # -------------------------------------------------------------------------
    # 5. Controller Restart & Full 48-Flow Switch Reconciliation (S7, AC-S04, AC-S10)
    # -------------------------------------------------------------------------

    def mark_controller_restart(self, run_id: str, restarted_at_utc: str) -> int:
        """
        Mark that the SDN Controller restarted during run_id (S7, AC-S10, REQ-13, REQ-16).
        Atomically validates timestamps, closes open routing_history intervals, transitions
        all 48 flows to UNKNOWN, and sets reconcile_required = True.
        """
        run_state = self._get_run(run_id)
        norm_ts = format_utc_timestamp(restarted_at_utc)
        restart_dt = parse_utc_timestamp(norm_ts)
        if restart_dt < parse_utc_timestamp(run_state.last_transition_at_utc):
            raise StateValidationError(
                f"Backward timestamp in mark_controller_restart: '{norm_ts}' precedes "
                f"last_transition_at_utc '{run_state.last_transition_at_utc}'."
            )

        staged = copy.deepcopy(run_state)
        for fid in staged.flows:
            self._validate_flow_timestamp_not_backward(
                staged, fid, norm_ts, context="mark_controller_restart"
            )

        for fid, rec in staged.flows.items():
            last_known = rec.path_id
            self._close_open_history_interval(staged, fid, norm_ts)
            self._open_uncertainty_gap(
                staged,
                flow_group_id=fid,
                last_known_path_id=last_known,
                lost_at_utc=norm_ts,
                batch_id=staged.in_flight_batch_id,
                reason="CONTROLLER_RESTART",
            )
            rec.path_id = None
            rec.confirmed_at = None
            rec.verification_status = VerificationStatus.UNKNOWN

        staged.in_flight_batch_id = None
        staged.in_flight_started_at_utc = None
        staged.in_flight_payload_hash = None
        staged.reconcile_required = True
        staged.reconcile_reason = "CONTROLLER_RESTART"
        staged.state_version += 1
        staged.last_transition_at_utc = norm_ts

        self._append_event(
            staged,
            event_at_utc=norm_ts,
            event_type="CONTROLLER_RESTART_RECONCILE_REQUIRED",
            detail={"state_version": staged.state_version},
        )
        self._runs[run_id] = staged
        self.assert_run_invariants(run_id)
        return staged.state_version

    def reconcile_run_state(
        self,
        run_id: str,
        verified_evidences: Mapping[str, VerificationEvidence],
        reconciled_at_utc: str,
        *,
        reconciliation_id: str = "reconcile",
    ) -> RoutingStateSnapshot:
        """
        Atomically reconcile routing_state from verified switch flow-table evidence across
        ALL 48 canonical flows (S5, S7, AC-S04, AC-S10).

        - Requires valid VerificationEvidence for all 48 canonical flows (0x103..0x806).
        - Validates monotonic timestamps before mutating any state.
        - Updates last_successful_change_at for any flow whose reconciled actual path differs
          from its last known verified path before the uncertainty gap.
        - Increments global state_version by +1 if any state/certainty/reconcile status changed,
          and clears reconcile_required = False once all 48 flows are VERIFIED.
        """
        run_state = self._get_run(run_id)
        norm_ts = format_utc_timestamp(reconciled_at_utc)
        reconcile_dt = parse_utc_timestamp(norm_ts)
        if reconcile_dt < parse_utc_timestamp(run_state.last_transition_at_utc):
            raise StateValidationError(
                f"Backward timestamp in reconcile_run_state: '{norm_ts}' precedes "
                f"last_transition_at_utc '{run_state.last_transition_at_utc}'."
            )

        validated = self._validate_full_48_evidences_for_paths(
            run_id, verified_evidences, expected_paths_by_flow=None
        )

        staged = copy.deepcopy(run_state)
        for fid, ev in validated.items():
            ev_dt = self._validate_flow_timestamp_not_backward(
                staged, fid, ev.verified_at_utc, context="reconcile_run_state"
            )
            if reconcile_dt < ev_dt:
                raise StateValidationError(
                    f"reconciled_at_utc '{norm_ts}' precedes flow '{fid}' verified_at_utc '{ev.verified_at_utc}'."
                )

        state_changed = bool(staged.reconcile_required)

        for fid, ev in validated.items():
            rec = staged.flows[fid]
            ev_ts = format_utc_timestamp(ev.verified_at_utc)
            new_path = ev.verified_path_id

            was_unverified_or_unknown = (
                rec.verification_status != VerificationStatus.VERIFIED or rec.path_id is None
            )
            last_known_before_gap: Optional[str] = rec.path_id
            if was_unverified_or_unknown and staged.uncertainty_gaps[fid]:
                last_known_before_gap = staged.uncertainty_gaps[fid][-1].last_known_path_id

            if was_unverified_or_unknown or rec.path_id != new_path:
                self._close_open_history_interval(staged, fid, ev_ts)
                if was_unverified_or_unknown:
                    self._resolve_open_uncertainty_gap(
                        staged,
                        flow_group_id=fid,
                        resolved_at_utc=ev_ts,
                        resolved_path_id=new_path,
                    )
                self._open_new_history_interval(
                    staged,
                    flow_group_id=fid,
                    path_id=new_path,
                    valid_from_utc=ev_ts,
                    status=HistoryIntervalStatus.RECONCILED,
                    batch_id=reconciliation_id,
                    evidence_mode=ev.evidence_mode,
                )
                if last_known_before_gap is not None and last_known_before_gap != new_path:
                    rec.last_successful_change_at = ev_ts

                rec.path_id = new_path
                rec.confirmed_at = ev_ts
                rec.verification_status = VerificationStatus.VERIFIED
                rec.evidence_mode = ev.evidence_mode
                state_changed = True
            else:
                rec.confirmed_at = ev_ts
                rec.evidence_mode = ev.evidence_mode

        staged.reconcile_required = False
        staged.reconcile_reason = None
        staged.in_flight_batch_id = None
        staged.in_flight_started_at_utc = None
        staged.in_flight_payload_hash = None
        staged.last_transition_at_utc = norm_ts

        if state_changed:
            staged.state_version += 1

        self._append_event(
            staged,
            event_at_utc=norm_ts,
            event_type="RUN_RECONCILED",
            batch_id=reconciliation_id,
            detail={
                "reconciled_flow_count": len(validated),
                "state_version": staged.state_version,
            },
        )
        self._runs[run_id] = staged
        self.assert_run_invariants(run_id)
        return self.get_routing_state(run_id)
