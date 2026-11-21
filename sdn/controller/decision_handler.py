"""
DecisionHandler for SDN Controller Phase 4 (Integration Guide v1.4 & Testing v2 compliant).

Responsibilities (IF08, REQ-04, REQ-13, REQ-16, S2–S7, AC-S02..AC-S09):
1. Receive and validate `ApplyRequest` before any OpenFlow `FlowMod` is issued (`AC-S02`).
2. Enforce idempotency and conflict detection by `(run_id, decision_batch_id)` and
   `payload_hash` (`AC-S08`):
   - Same `(run_id, decision_batch_id)` + same `payload_hash` -> return cached `ApplyResponse`
     with `idempotent_replay=True` and ZERO additional `FlowMod`s.
   - Same `(run_id, decision_batch_id)` + different `payload_hash` -> reject with
     `BatchStatus.REJECTED` and `ReasonCode.BATCH_ID_CONFLICT`.
   - Explicitly isolate volatile in-memory idempotency caching behind `BatchIdempotencyStoreProtocol`
     (`store_status="VOLATILE_IN_MEMORY_MUST_LOCK_DURABLE_STORE"`) and invalidate volatile cache
     upon controller restart so in-memory state is never mistaken for durable storage.
3. Reject requests when another batch is in flight (`BUSY`) or when switch flow-table
   reconciliation is pending (`RECONCILIATION_REQUIRED`).
4. Validate `topology_version`, `topology_hash`, `config_version`, `expected_state_version`,
   and monotonic timestamps before sending any `FlowMod`.
5. Validate `apply_deadline_utc`, `forecast_start`, `forecast_end`, `apply_guard_s`, and
   snapshot freshness (`T_stale_s`) before sending any `FlowMod` (`DEADLINE_EXPIRED`, `STALE`).
6. Validate canonical `flow_group_id` (`0x103`) and `path_id` (`0x103_P1`, `0x103_P2`),
   batch size `1..B`, current `VERIFIED` path equality (`old_path_id`), and per-flow
   hold-down timer (`T_hold_s`).
7. Execute two-phase downstream-first rule installation and true verification via
   `RuleInstaller` and `RuleVerifier`.
8. Guarantee crash consistency: if an unexpected exception occurs after `begin_batch_execution`
   (e.g., after switch rules changed but before `StateManager` commits), never return a fake
   `REJECTED`; transition the affected flows to `UNKNOWN` (`actual_path_id=None`), lock
   `reconcile_required=True`, and return `BatchStatus.INCONSISTENT`.
9. Handle post-write late verification (`AC-S09`, `REQ-13`, `REQ-16`): if verification
   completes after `forecast_end` or `apply_deadline_utc` *after* `FlowMod` writes have
   started, commit the actual verified route and mark `deadline_missed=True` with
   `ReasonCode.DEADLINE_MISSED` rather than masking the physical change as `REJECTED`.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta
from enum import Enum
import time
from typing import Callable, Mapping, Optional, Protocol, runtime_checkable

from sdn.controller.state_manager import (
    BatchIdConflictError,
    StateManager,
    StateValidationError,
    format_utc_timestamp,
    parse_utc_timestamp,
)
from sdn.controller.topology_registry import TopologyLookupError, TopologyRegistry
from sdn.flow_management.rule_installer import FlowChangeSpec, RuleInstaller
from sdn.schemas.apply_models import (
    ApplyFlowResult,
    ApplyRequest,
    ApplyResponse,
)
from sdn.schemas.snapshot_models import Snapshot
from sdn.schemas.state_models import (
    BatchStatus,
    FlowDecisionStatus,
    ReasonCode,
    RoutingStateSnapshot,
    VerificationEvidence,
    VerificationStatus,
)


@dataclass(frozen=True)
class CachedBatchEntry:
    """Cached record of a processed `(run_id, decision_batch_id)` for AC-S08 idempotency."""

    payload_hash: str
    response: ApplyResponse


@runtime_checkable
class BatchIdempotencyStoreProtocol(Protocol):
    """
    Abstract store interface for `(run_id, decision_batch_id)` idempotency records (`AC-S08`).
    Explicitly distinguishes volatile in-memory caching (`is_durable=False`) from an injected
    durable store (`is_durable=True`).
    """

    @property
    def is_durable(self) -> bool:
        ...

    @property
    def store_status(self) -> str:
        ...

    def get(self, run_id: str, decision_batch_id: str) -> Optional[CachedBatchEntry]:
        ...

    def put(
        self, run_id: str, decision_batch_id: str, entry: CachedBatchEntry
    ) -> None:
        ...

    def invalidate_run_on_restart(self, run_id: str) -> None:
        ...


class InMemoryBatchIdempotencyStore:
    """
    Default volatile in-memory idempotency store for a running SDN Controller instance.
    Explicitly marked `is_durable = False` and `store_status = 'VOLATILE_IN_MEMORY_MUST_LOCK_DURABLE_STORE'`
    so it is never mistaken for durable persistence across controller restarts.
    """

    def __init__(self) -> None:
        self._entries: dict[tuple[str, str], CachedBatchEntry] = {}

    @property
    def is_durable(self) -> bool:
        return False

    @property
    def store_status(self) -> str:
        return "VOLATILE_IN_MEMORY_MUST_LOCK_DURABLE_STORE"

    def get(self, run_id: str, decision_batch_id: str) -> Optional[CachedBatchEntry]:
        return self._entries.get((run_id, decision_batch_id))

    def put(
        self, run_id: str, decision_batch_id: str, entry: CachedBatchEntry
    ) -> None:
        self._entries[(run_id, decision_batch_id)] = entry

    def invalidate_run_on_restart(self, run_id: str) -> None:
        """
        Clear volatile in-memory cached responses for `run_id` when the controller restarts,
        because an in-memory cache does not survive process restarts and must not return
        stale pre-restart `APPLIED` responses while the controller requires reconciliation.
        """
        keys_to_remove = [k for k in self._entries if k[0] == run_id]
        for k in keys_to_remove:
            self._entries.pop(k, None)


class SnapshotValidationPolicy(str, Enum):
    """
    Controls how `DecisionHandler` validates `ApplyRequest.snapshot_id` and freshness:
    - `FAIL_CLOSED_INTEGRATION`: Production / integration default. Never allows an
      unregistered `snapshot_id` without verified freshness timestamps to proceed.
    - `TEST_ONLY_ALLOW_UNREGISTERED`: Isolated unit-test policy only; allows synthetic
      `ApplyRequest` fixtures that omit `SnapshotCreator` registration and timestamps.
    """

    FAIL_CLOSED_INTEGRATION = "FAIL_CLOSED_INTEGRATION"
    TEST_ONLY_ALLOW_UNREGISTERED = "TEST_ONLY_ALLOW_UNREGISTERED"


@dataclass(frozen=True)
class DecisionHandlerConfig:
    """
    Configuration for `DecisionHandler` pre-write validation and timing guards.

    All pilot parameters (`T_stale_s`, `max_changed_flows_B`, `apply_guard_s`) are
    parameterized in config rather than hard-coded in logic (`[PROPOSED]` / `[MUST-LOCK]`).
    """

    stale_threshold_s: float = 2.0
    apply_guard_s: float = 0.0
    require_topology_hash: bool = True
    require_registered_snapshot: bool = True
    snapshot_validation_policy: SnapshotValidationPolicy = (
        SnapshotValidationPolicy.FAIL_CLOSED_INTEGRATION
    )
    expected_config_hash: Optional[str] = None
    cleanup_old_downstream: bool = False
    timing_budget_policy_status: str = "MUST_LOCK_TIMING_BUDGET"
    idempotency_store_status: str = "VOLATILE_IN_MEMORY_MUST_LOCK_DURABLE_STORE"

    @classmethod
    def for_integration(
        cls,
        *,
        stale_threshold_s: float = 2.0,
        apply_guard_s: float = 0.0,
        require_topology_hash: bool = True,
        expected_config_hash: Optional[str] = None,
        cleanup_old_downstream: bool = False,
    ) -> "DecisionHandlerConfig":
        """Fail-closed configuration for integration and production runs."""
        return cls(
            stale_threshold_s=stale_threshold_s,
            apply_guard_s=apply_guard_s,
            require_topology_hash=require_topology_hash,
            require_registered_snapshot=True,
            snapshot_validation_policy=SnapshotValidationPolicy.FAIL_CLOSED_INTEGRATION,
            expected_config_hash=expected_config_hash,
            cleanup_old_downstream=cleanup_old_downstream,
        )

    @classmethod
    def for_mock_unit_tests(
        cls,
        *,
        stale_threshold_s: float = 2.0,
        apply_guard_s: float = 0.0,
        require_topology_hash: bool = True,
        expected_config_hash: Optional[str] = None,
        cleanup_old_downstream: bool = False,
    ) -> "DecisionHandlerConfig":
        """
        Explicit test-only configuration for isolated unit tests that construct synthetic
        `ApplyRequest` objects without registering a `Snapshot` in `SnapshotCreator`.
        """
        return cls(
            stale_threshold_s=stale_threshold_s,
            apply_guard_s=apply_guard_s,
            require_topology_hash=require_topology_hash,
            require_registered_snapshot=False,
            snapshot_validation_policy=SnapshotValidationPolicy.TEST_ONLY_ALLOW_UNREGISTERED,
            expected_config_hash=expected_config_hash,
            cleanup_old_downstream=cleanup_old_downstream,
        )


class DecisionHandler:
    """
    Coordinates pre-write contract validation, two-phase rule installation (`RuleInstaller`),
    route verification (`RuleVerifier`), crash recovery, and atomic state commits (`StateManager`).
    """

    def __init__(
        self,
        topology_registry: TopologyRegistry,
        state_manager: StateManager,
        rule_installer: RuleInstaller,
        config: Optional[DecisionHandlerConfig] = None,
        *,
        snapshot_lookup: Optional[Callable[[str, str], Optional[Snapshot]]] = None,
        idempotency_store: Optional[BatchIdempotencyStoreProtocol] = None,
    ) -> None:
        self._topology = topology_registry
        self._state_manager = state_manager
        self._rule_installer = rule_installer
        self._config = config or DecisionHandlerConfig()
        self._external_snapshot_lookup = snapshot_lookup
        self._registered_snapshots: dict[tuple[str, str], Snapshot] = {}
        self._idempotency_store: BatchIdempotencyStoreProtocol = (
            idempotency_store or InMemoryBatchIdempotencyStore()
        )

    @property
    def topology(self) -> TopologyRegistry:
        return self._topology

    @property
    def state_manager(self) -> StateManager:
        return self._state_manager

    @property
    def rule_installer(self) -> RuleInstaller:
        return self._rule_installer

    @property
    def config(self) -> DecisionHandlerConfig:
        return self._config

    @property
    def idempotency_store(self) -> BatchIdempotencyStoreProtocol:
        return self._idempotency_store

    def register_snapshot(self, snapshot: Snapshot) -> None:
        """Register an SDN-created `Snapshot` so `ApplyRequest.snapshot_id` can be verified."""
        self._registered_snapshots[(snapshot.run_id, snapshot.snapshot_id)] = snapshot

    def get_registered_snapshot(
        self, run_id: str, snapshot_id: str
    ) -> Optional[Snapshot]:
        """Look up a registered `Snapshot` by `(run_id, snapshot_id)`."""
        snap = self._registered_snapshots.get((run_id, snapshot_id))
        if snap is not None:
            return snap
        if self._external_snapshot_lookup is not None:
            return self._external_snapshot_lookup(run_id, snapshot_id)
        return None

    def notify_controller_restart(self, run_id: str, restarted_at_utc: str) -> int:
        """
        Handle an SDN Controller restart for `run_id` (`REQ-13`, `REQ-16`, `AC-S04`, `AC-S10`):
        1. Invalidates volatile in-memory idempotency cache entries for `run_id` (since volatile
           RAM does not survive process restarts and must not return pre-restart responses while
           reconciliation is pending).
        2. Marks controller restart in `StateManager` (transitioning all 48 flows to `UNKNOWN`
           and locking `reconcile_required = True`).
        """
        if not self._idempotency_store.is_durable:
            self._idempotency_store.invalidate_run_on_restart(run_id)
        return self._state_manager.mark_controller_restart(
            run_id, restarted_at_utc=restarted_at_utc
        )

    def recover_and_reconcile_run(
        self,
        run_id: str,
        reconciled_at_utc: str,
        *,
        reinstall_missing_from_paths: Optional[Mapping[str, str]] = None,
        reconciliation_id: str = "reconcile",
    ) -> RoutingStateSnapshot:
        """
        Recover and reconcile actual routing state from switches after a controller or switch
        restart or an `INCONSISTENT`/`PARTIAL` batch (`REQ-13`, `REQ-16`, `AC-S04`, `AC-S10`):
        1. Reads back `FlowStats` and forwarding counter/probe evidence across all 48 flows
           via `RuleVerifier.discover_all_active_paths()`.
        2. If switch tables were wiped and `reinstall_missing_from_paths` is supplied,
           re-installs missing paths via `RuleInstaller.reinstall_and_verify_paths()`.
        3. Commits the 48 verified evidences atomically via `StateManager.reconcile_run_state()`.
        """
        norm_ts = format_utc_timestamp(reconciled_at_utc)
        discovered = self._rule_installer.rule_verifier.discover_all_active_paths(
            run_id, norm_ts
        )
        evidences: dict[str, VerificationEvidence] = {}
        missing_flows: dict[str, str] = {}

        for fid, res in discovered.items():
            if res.is_verified and res.evidence is not None:
                evidences[fid] = res.evidence
            elif reinstall_missing_from_paths is not None and fid in reinstall_missing_from_paths:
                missing_flows[fid] = reinstall_missing_from_paths[fid]

        if missing_flows:
            reinstalled = self._rule_installer.reinstall_and_verify_paths(
                run_id=run_id,
                target_paths_by_flow=missing_flows,
                verified_at_utc=norm_ts,
            )
            evidences.update(reinstalled)

        return self._state_manager.reconcile_run_state(
            run_id=run_id,
            verified_evidences=evidences,
            reconciled_at_utc=norm_ts,
            reconciliation_id=reconciliation_id,
        )

    def _build_rejected_results(
        self,
        request: ApplyRequest,
        run_id: str,
        reason: ReasonCode,
        *,
        run_initialized: bool,
    ) -> tuple[ApplyFlowResult, ...]:
        """Build per-flow `ApplyFlowResult` entries for a pre-write `REJECTED` request."""
        results: list[ApplyFlowResult] = []
        for chg in request.changes:
            actual_pid: Optional[str] = None
            ver_status = VerificationStatus.UNVERIFIED
            if run_initialized:
                try:
                    entry = self._state_manager.get_flow_state(run_id, chg.flow_group_id)
                    actual_pid = (
                        entry.path_id
                        if entry.verification_status == VerificationStatus.VERIFIED
                        else None
                    )
                    ver_status = entry.verification_status
                except (StateValidationError, TopologyLookupError):
                    actual_pid = None
                    ver_status = VerificationStatus.UNVERIFIED
            results.append(
                ApplyFlowResult(
                    flow_group_id=chg.flow_group_id,
                    actual_path_id=actual_pid,
                    status=FlowDecisionStatus.REJECTED,
                    verification_status=ver_status,
                    reason=reason.value,
                    rule_count=0,
                    applied_at=None,
                    old_path_id=chg.old_path_id,
                    proposed_path_id=chg.new_path_id,
                )
            )
        return tuple(results)

    def _reject_pre_write(
        self,
        request: ApplyRequest,
        *,
        reason: ReasonCode,
        rejected_at_utc: str,
        run_initialized: bool,
        cache_for_idempotency: bool = True,
    ) -> ApplyResponse:
        """
        Reject `ApplyRequest` before any `FlowMod` is sent (`AC-S02`, `S4`).
        Leaves `state_version` unchanged and sets `rule_count = 0`.
        """
        norm_ts = format_utc_timestamp(rejected_at_utc)
        req_hash = request.effective_payload_hash
        if run_initialized:
            current_sv = self._state_manager.record_rejected_request(
                run_id=request.run_id,
                decision_batch_id=request.decision_batch_id or "MISSING_BATCH_ID",
                reason=reason,
                rejected_at_utc=norm_ts,
            )
        else:
            current_sv = 0

        resp = ApplyResponse(
            run_id=request.run_id,
            decision_batch_id=request.decision_batch_id,
            status=BatchStatus.REJECTED,
            state_version=current_sv,
            completed_at=norm_ts,
            results=self._build_rejected_results(
                request,
                request.run_id,
                reason,
                run_initialized=run_initialized,
            ),
            reason=reason.value,
            rule_count=0,
            deadline_missed=False,
            idempotent_replay=False,
            payload_hash=req_hash,
            is_network_atomic=False,
        )
        if (
            cache_for_idempotency
            and run_initialized
            and isinstance(request.decision_batch_id, str)
            and request.decision_batch_id.strip()
        ):
            existing = self._idempotency_store.get(
                request.run_id, request.decision_batch_id
            )
            if existing is None:
                self._idempotency_store.put(
                    request.run_id,
                    request.decision_batch_id,
                    CachedBatchEntry(payload_hash=req_hash, response=resp),
                )
        return resp

    def handle_apply_request(
        self,
        request: ApplyRequest,
        *,
        decision_time_utc: str,
        applied_at_utc: Optional[str] = None,
        completed_at_utc: Optional[str] = None,
    ) -> ApplyResponse:
        """
        Validate and execute an `ApplyRequest` (`IF08`, `REQ-04`, `AC-S02`..`AC-S09`).

        Parameters:
        - `request`: The incoming `ApplyRequest`.
        - `decision_time_utc`: UTC timestamp when the controller evaluates pre-write validation.
        - `applied_at_utc`: Optional UTC timestamp passed to `RuleInstaller` when switch writes
          execute (defaults to `decision_time_utc`).
        - `completed_at_utc`: Optional UTC timestamp when batch verification finishes
          (defaults to `applied_at_utc` or `decision_time_utc`). Used to detect post-write
          `DEADLINE_MISSED` (`AC-S09`).
        """
        norm_decision_ts = format_utc_timestamp(decision_time_utc)
        decision_dt = parse_utc_timestamp(norm_decision_ts)

        # 1. Validate run_id and decision_batch_id presence
        if not isinstance(request.run_id, str) or not request.run_id.strip():
            return self._reject_pre_write(
                request,
                reason=ReasonCode.INVALID_REQUEST,
                rejected_at_utc=norm_decision_ts,
                run_initialized=False,
                cache_for_idempotency=False,
            )

        try:
            current_state_snap = self._state_manager.get_routing_state(request.run_id)
        except StateValidationError:
            return self._reject_pre_write(
                request,
                reason=ReasonCode.INVALID_REQUEST,
                rejected_at_utc=norm_decision_ts,
                run_initialized=False,
                cache_for_idempotency=False,
            )

        if (
            not isinstance(request.decision_batch_id, str)
            or not request.decision_batch_id.strip()
        ):
            return self._reject_pre_write(
                request,
                reason=ReasonCode.INVALID_REQUEST,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
                cache_for_idempotency=False,
            )

        if (
            request.envelope is not None
            and request.envelope.run_id != request.run_id
        ):
            return self._reject_pre_write(
                request,
                reason=ReasonCode.INVALID_REQUEST,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
                cache_for_idempotency=False,
            )

        req_payload_hash = request.effective_payload_hash

        # 2. Idempotency & BATCH_ID_CONFLICT check (AC-S08)
        cached_entry = self._idempotency_store.get(
            request.run_id, request.decision_batch_id
        )
        if cached_entry is not None:
            if cached_entry.payload_hash == req_payload_hash:
                # If reconciliation is currently required (e.g. after a restart or inconsistent batch),
                # never mask the active reconciliation lock with a stale cached response!
                if self._state_manager.is_reconcile_required(request.run_id) and (
                    cached_entry.response.status != BatchStatus.INCONSISTENT
                ):
                    return self._reject_pre_write(
                        request,
                        reason=ReasonCode.RECONCILIATION_REQUIRED,
                        rejected_at_utc=norm_decision_ts,
                        run_initialized=True,
                        cache_for_idempotency=False,
                    )
                # Exact duplicate replay while running -> return cached ApplyResponse with 0 new FlowMods
                return replace(cached_entry.response, idempotent_replay=True)
            # Duplicate decision_batch_id with different payload -> reject with BATCH_ID_CONFLICT
            return self._reject_pre_write(
                request,
                reason=ReasonCode.BATCH_ID_CONFLICT,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
                cache_for_idempotency=False,
            )

        # 3. Check in-flight batch (BUSY / BATCH_ID_CONFLICT) & reconciliation lock (AC-S02, AC-S04)
        can_apply, block_reason = self._state_manager.can_accept_apply(request.run_id)
        if not can_apply:
            assert block_reason is not None
            return self._reject_pre_write(
                request,
                reason=block_reason,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
                cache_for_idempotency=False,
            )

        # 4. Pre-write monotonic timestamp validation (prevents post-write timestamp failure)
        try:
            norm_applied_ts = format_utc_timestamp(applied_at_utc or norm_decision_ts)
            norm_completed_ts = format_utc_timestamp(completed_at_utc or norm_applied_ts)
            applied_dt = parse_utc_timestamp(norm_applied_ts)
            completed_dt = parse_utc_timestamp(norm_completed_ts)
            last_trans_dt = parse_utc_timestamp(
                self._state_manager.get_last_transition_at_utc(request.run_id)
            )
        except StateValidationError:
            return self._reject_pre_write(
                request,
                reason=ReasonCode.INVALID_REQUEST,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
                cache_for_idempotency=False,
            )

        if (
            decision_dt < last_trans_dt
            or applied_dt < decision_dt
            or completed_dt < applied_dt
        ):
            return self._reject_pre_write(
                request,
                reason=ReasonCode.INVALID_REQUEST,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
                cache_for_idempotency=False,
            )

        # 5. Check topology_version and topology_hash (REQ-04, AC-S02)
        eff_topo_ver = request.effective_topology_version
        if (
            eff_topo_ver is not None
            and eff_topo_ver != self._topology.topology_version
        ):
            return self._reject_pre_write(
                request,
                reason=ReasonCode.TOPOLOGY_MISMATCH,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
            )

        eff_topo_hash = request.effective_topology_hash
        if self._config.require_topology_hash and not eff_topo_hash:
            return self._reject_pre_write(
                request,
                reason=ReasonCode.TOPOLOGY_MISMATCH,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
            )
        if (
            eff_topo_hash is not None
            and eff_topo_hash != self._topology.topology_hash
        ):
            return self._reject_pre_write(
                request,
                reason=ReasonCode.TOPOLOGY_MISMATCH,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
            )

        # 6. Check config_version and optional config_hash (AC-S02)
        if request.config_version != current_state_snap.config_version:
            return self._reject_pre_write(
                request,
                reason=ReasonCode.CONFIG_MISMATCH,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
            )
        eff_cfg_hash = request.effective_config_hash
        if self._config.expected_config_hash is not None:
            if not eff_cfg_hash or eff_cfg_hash != self._config.expected_config_hash:
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.CONFIG_MISMATCH,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )

        # 7. Check expected_state_version (AC-S02, S4)
        if request.expected_state_version != current_state_snap.state_version:
            return self._reject_pre_write(
                request,
                reason=ReasonCode.VERSION_MISMATCH,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
            )

        # 8. Check snapshot_id validity, future timestamps, consistency & staleness (T_stale_s) (AC-S02, S2, Phase 7.4)
        if not isinstance(request.snapshot_id, str) or not request.snapshot_id.strip():
            return self._reject_pre_write(
                request,
                reason=ReasonCode.INVALID_REQUEST,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
            )

        req_cutoff_dt = None
        req_as_of_dt = None
        if request.history_cutoff_utc is not None:
            try:
                req_cutoff_dt = parse_utc_timestamp(request.history_cutoff_utc)
            except StateValidationError:
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.INVALID_REQUEST,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )
            if req_cutoff_dt > decision_dt:
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.INVALID_REQUEST,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )

        if request.snapshot_as_of_utc is not None:
            try:
                req_as_of_dt = parse_utc_timestamp(request.snapshot_as_of_utc)
            except StateValidationError:
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.INVALID_REQUEST,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )
            if req_as_of_dt > decision_dt:
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.INVALID_REQUEST,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )

        if (
            req_cutoff_dt is not None
            and req_as_of_dt is not None
            and req_as_of_dt < req_cutoff_dt
        ):
            return self._reject_pre_write(
                request,
                reason=ReasonCode.INVALID_REQUEST,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
            )

        reg_snapshot = self.get_registered_snapshot(
            request.run_id, request.snapshot_id
        )
        # Under FAIL_CLOSED_INTEGRATION, require_registered_snapshot=False MUST NOT
        # bypass authoritative registered snapshot verification (Phase 7.4 Req 1-4).
        must_have_registered_snapshot = (
            self._config.snapshot_validation_policy
            == SnapshotValidationPolicy.FAIL_CLOSED_INTEGRATION
        ) or self._config.require_registered_snapshot

        if must_have_registered_snapshot and reg_snapshot is None:
            return self._reject_pre_write(
                request,
                reason=ReasonCode.INVALID_REQUEST,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
            )

        if reg_snapshot is not None:
            if reg_snapshot.run_id != request.run_id or not reg_snapshot.is_valid:
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.STALE,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )
            if (
                reg_snapshot.topology_version != self._topology.topology_version
                or reg_snapshot.topology_hash != self._topology.topology_hash
            ):
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.TOPOLOGY_MISMATCH,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )
            if reg_snapshot.config_version != current_state_snap.config_version:
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.CONFIG_MISMATCH,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )
            if (
                self._config.expected_config_hash is not None
                and reg_snapshot.config_hash != self._config.expected_config_hash
            ):
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.CONFIG_MISMATCH,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )
            if reg_snapshot.state_version != request.expected_state_version:
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.VERSION_MISMATCH,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )
            snap_ref_dt = parse_utc_timestamp(reg_snapshot.history_cutoff_utc)
            snap_as_of_dt = parse_utc_timestamp(reg_snapshot.snapshot_as_of_utc)
            if (
                snap_ref_dt > decision_dt
                or snap_as_of_dt > decision_dt
                or snap_as_of_dt < snap_ref_dt
            ):
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.INVALID_REQUEST,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )
            if (
                req_cutoff_dt is not None
                and format_utc_timestamp(req_cutoff_dt) != reg_snapshot.history_cutoff_utc
            ):
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.INVALID_REQUEST,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )
            if (
                req_as_of_dt is not None
                and format_utc_timestamp(req_as_of_dt) != reg_snapshot.snapshot_as_of_utc
            ):
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.INVALID_REQUEST,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )
            if (decision_dt - snap_ref_dt).total_seconds() > self._config.stale_threshold_s:
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.STALE,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )
        else:
            # Reachable ONLY when snapshot_validation_policy == TEST_ONLY_ALLOW_UNREGISTERED
            ref_dt = req_cutoff_dt or req_as_of_dt
            if ref_dt is not None:
                if (
                    decision_dt - ref_dt
                ).total_seconds() > self._config.stale_threshold_s:
                    return self._reject_pre_write(
                        request,
                        reason=ReasonCode.STALE,
                        rejected_at_utc=norm_decision_ts,
                        run_initialized=True,
                    )

        # 9. Check forecast_start, forecast_end, apply_deadline_utc & apply_guard_s (AC-S02)
        try:
            forecast_start_dt = parse_utc_timestamp(request.forecast_start)
            forecast_end_dt = parse_utc_timestamp(request.forecast_end)
            apply_deadline_dt = parse_utc_timestamp(request.apply_deadline_utc)
        except StateValidationError:
            return self._reject_pre_write(
                request,
                reason=ReasonCode.INVALID_REQUEST,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
            )

        if forecast_end_dt <= forecast_start_dt:
            return self._reject_pre_write(
                request,
                reason=ReasonCode.INVALID_REQUEST,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
            )

        guard_deadline_dt = forecast_end_dt - timedelta(
            seconds=self._config.apply_guard_s
        )
        if (
            decision_dt > apply_deadline_dt
            or decision_dt > guard_deadline_dt
            or decision_dt >= forecast_end_dt
        ):
            return self._reject_pre_write(
                request,
                reason=ReasonCode.DEADLINE_EXPIRED,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
            )

        # 10. Validate changes array (1..B, canonical IDs, live VERIFIED path, hold-down)
        max_b = self._state_manager.config.max_changed_flows_B
        if len(request.changes) == 0 or len(request.changes) > max_b:
            return self._reject_pre_write(
                request,
                reason=ReasonCode.INVALID_REQUEST,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
            )

        seen_flows: set[str] = set()
        change_specs: list[FlowChangeSpec] = []

        for chg in request.changes:
            fid = chg.flow_group_id
            if fid in seen_flows:
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.INVALID_REQUEST,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )
            seen_flows.add(fid)

            try:
                self._topology.get_flow_group(fid)
                old_cp = self._topology.get_candidate_path(chg.old_path_id)
                new_cp = self._topology.get_candidate_path(chg.new_path_id)
            except TopologyLookupError:
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.INVALID_REQUEST,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )

            if (
                old_cp.flow_group_id != fid
                or new_cp.flow_group_id != fid
                or chg.old_path_id == chg.new_path_id
            ):
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.INVALID_REQUEST,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )

            live_entry = self._state_manager.get_flow_state(request.run_id, fid)
            if (
                live_entry.verification_status != VerificationStatus.VERIFIED
                or live_entry.path_id != chg.old_path_id
            ):
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.INVALID_REQUEST,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )

            try:
                in_hold_down = self._state_manager.is_hold_down_active(
                    request.run_id, fid, norm_decision_ts
                )
            except StateValidationError:
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.INVALID_REQUEST,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )

            if in_hold_down:
                return self._reject_pre_write(
                    request,
                    reason=ReasonCode.HOLDDOWN,
                    rejected_at_utc=norm_decision_ts,
                    run_initialized=True,
                )

            change_specs.append(
                FlowChangeSpec(
                    flow_group_id=fid,
                    old_path_id=chg.old_path_id,
                    new_path_id=chg.new_path_id,
                )
            )

        # 11. Acquire in-flight batch lock in StateManager
        try:
            self._state_manager.begin_batch_execution(
                request.run_id,
                request.decision_batch_id,
                started_at_utc=norm_decision_ts,
                expected_state_version=request.expected_state_version,
                payload_hash=req_payload_hash,
            )
        except BatchIdConflictError:
            return self._reject_pre_write(
                request,
                reason=ReasonCode.BATCH_ID_CONFLICT,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
                cache_for_idempotency=False,
            )
        except StateValidationError:
            return self._reject_pre_write(
                request,
                reason=ReasonCode.INVALID_REQUEST,
                rejected_at_utc=norm_decision_ts,
                run_initialized=True,
                cache_for_idempotency=False,
            )

        # 12. Execute two-phase installation & verification + atomic commit with Crash Consistency guard:
        # Once begin_batch_execution has succeeded, any unexpected exception during switch writes
        # or state commit MUST NEVER return a fake REJECTED! Instead, transition affected flows
        # to UNKNOWN (actual_path_id=None) and return BatchStatus.INCONSISTENT.
        try:
            t_exec_start = time.monotonic()
            # Constrain driver's bounded forwarding verification polling to the remaining
            # ApplyRequest deadline budget (`Phase 10.1`) so polling never exceeds budget.
            budget_s = max(
                0.0,
                min(
                    (apply_deadline_dt - decision_dt).total_seconds(),
                    (forecast_end_dt - decision_dt).total_seconds(),
                ),
            )
            set_budget_fn = getattr(
                self._rule_installer.switch_driver,
                "set_verification_budget_deadline",
                None,
            )
            if callable(set_budget_fn):
                set_budget_fn(budget_s)
            try:
                install_result = self._rule_installer.install_batch(
                    run_id=request.run_id,
                    decision_batch_id=request.decision_batch_id,
                    changes=change_specs,
                    applied_at_utc=norm_applied_ts,
                    cleanup_old_downstream=self._config.cleanup_old_downstream,
                )
                self._last_batch_timing = {
                    "decision_batch_id": request.decision_batch_id,
                    "openflow_install_ms": install_result.openflow_install_ms,
                    "verification_ms": install_result.verification_ms,
                    "flow_stats_request_count": install_result.flow_stats_request_count,
                }
            finally:
                if callable(set_budget_fn):
                    set_budget_fn(None)
            elapsed_exec_s = max(0.0, time.monotonic() - t_exec_start)
            if (
                completed_at_utc is None
                and self._rule_installer.switch_driver.evidence_mode.value == "LIVE_OVS"
                and elapsed_exec_s >= 0.001
            ):
                completed_dt = applied_dt + timedelta(seconds=elapsed_exec_s)
                norm_completed_ts = format_utc_timestamp(completed_dt)

            batch_status, result_state_version = self._state_manager.commit_batch_outcome(
                run_id=request.run_id,
                decision_batch_id=request.decision_batch_id,
                outcomes=install_result.outcomes,
                completed_at_utc=norm_completed_ts,
                expected_state_version=request.expected_state_version,
                payload_hash=req_payload_hash,
            )
        except Exception:
            affected_fids = [c.flow_group_id for c in change_specs]
            aborted_sv = self._state_manager.abort_in_flight_batch_as_unknown(
                run_id=request.run_id,
                decision_batch_id=request.decision_batch_id,
                flow_group_ids=affected_fids,
                aborted_at_utc=norm_completed_ts,
                reason=ReasonCode.ROUTE_STATE_UNKNOWN.value,
            )
            unk_results: list[ApplyFlowResult] = []
            total_attempted = 0
            for c in change_specs:
                fb = self._rule_installer.get_last_flow_count_breakdown(c.flow_group_id)
                f_rules = fb.attempted_flow_mod_count if fb is not None else 0
                total_attempted += f_rules
                unk_results.append(
                    ApplyFlowResult(
                        flow_group_id=c.flow_group_id,
                        actual_path_id=None,
                        status=FlowDecisionStatus.UNKNOWN,
                        verification_status=VerificationStatus.UNKNOWN,
                        reason=ReasonCode.ROUTE_STATE_UNKNOWN.value,
                        rule_count=f_rules,
                        applied_at=norm_applied_ts,
                        old_path_id=c.old_path_id,
                        proposed_path_id=c.new_path_id,
                    )
                )
            crash_resp = ApplyResponse(
                run_id=request.run_id,
                decision_batch_id=request.decision_batch_id,
                status=BatchStatus.INCONSISTENT,
                state_version=aborted_sv,
                completed_at=norm_completed_ts,
                results=tuple(unk_results),
                reason=ReasonCode.ROUTE_STATE_UNKNOWN.value,
                rule_count=total_attempted,
                deadline_missed=(
                    completed_dt > forecast_end_dt or completed_dt > apply_deadline_dt
                ),
                idempotent_replay=False,
                payload_hash=req_payload_hash,
                is_network_atomic=False,
            )
            self._idempotency_store.put(
                request.run_id,
                request.decision_batch_id,
                CachedBatchEntry(payload_hash=req_payload_hash, response=crash_resp),
            )
            return crash_resp

        # 13. Check post-write deadline miss (AC-S09, REQ-13, REQ-16):
        # Never mask an already-started physical change as REJECTED! Record actual verified
        # path and flag DEADLINE_MISSED if completion exceeded forecast_end or apply_deadline_utc.
        deadline_missed = (
            completed_dt > forecast_end_dt or completed_dt > apply_deadline_dt
        )

        flow_results: list[ApplyFlowResult] = []
        first_flow_reason: Optional[str] = None
        for outcome in install_result.outcomes:
            flow_reason = outcome.reason
            if flow_reason is None and deadline_missed:
                flow_reason = ReasonCode.DEADLINE_MISSED.value
            if flow_reason is not None and first_flow_reason is None:
                first_flow_reason = flow_reason

            flow_results.append(
                ApplyFlowResult(
                    flow_group_id=outcome.flow_group_id,
                    actual_path_id=outcome.actual_path_id,
                    status=outcome.status,
                    verification_status=outcome.verification_status,
                    reason=flow_reason,
                    rule_count=outcome.rule_count,
                    applied_at=outcome.applied_at_utc,
                    old_path_id=outcome.old_path_id,
                    proposed_path_id=outcome.proposed_path_id,
                )
            )

        batch_reason: Optional[str] = first_flow_reason
        if deadline_missed and batch_reason is None:
            batch_reason = ReasonCode.DEADLINE_MISSED.value

        response = ApplyResponse(
            run_id=request.run_id,
            decision_batch_id=request.decision_batch_id,
            status=batch_status,
            state_version=result_state_version,
            completed_at=norm_completed_ts,
            results=tuple(flow_results),
            reason=batch_reason,
            rule_count=install_result.total_rule_count,
            deadline_missed=deadline_missed,
            idempotent_replay=False,
            payload_hash=req_payload_hash,
            is_network_atomic=install_result.is_network_atomic,
        )

        self._idempotency_store.put(
            request.run_id,
            request.decision_batch_id,
            CachedBatchEntry(
                payload_hash=req_payload_hash,
                response=response,
            ),
        )
        return response

    # Alias for ergonomic invocation
    apply = handle_apply_request
