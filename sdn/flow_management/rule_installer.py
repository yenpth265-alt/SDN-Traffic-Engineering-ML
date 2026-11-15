"""
RuleInstaller for SDN Controller Phase 3 (Integration Guide v1.4 & Testing v2 compliant).

Responsibilities:
- Execute safe two-phase OpenFlow 1.3 route installation (S3, AC-S01, AC-S03, AC-S04, AC-S06):
  1. Phase 1 (Downstream-first): Install/update rules on `c_core` and `e_dst` first,
     then send Barrier to both downstream switches and await confirmation.
     If any downstream FlowMod or Barrier fails, STOP IMMEDIATELY without touching `e_src`.
  2. Phase 2 (Source-edge last): Only after downstream switches are ready and barrier-confirmed,
     update the uplink selection rule (`port 3 <-> port 4`) on `e_src` + Barrier.
  3. Phase 3 (True Verification): Verify the new path via `RuleVerifier` (`FlowStats` read-back
     + forwarding counter/probe). Barrier confirmation alone NEVER produces `APPLIED`.
  4. Phase 4 (Rollback & UNKNOWN handling): If any FlowMod, Barrier, or verification step fails,
     roll back to `old_path_id` and verify `old_path_id` via `RuleVerifier`.
     - If `old_path_id` is verified -> return `FlowDecisionStatus.ROLLED_BACK` with valid `VerificationEvidence`.
     - If `old_path_id` cannot be verified -> return `FlowDecisionStatus.UNKNOWN` with `actual_path_id=None`
       (producing `BatchStatus.INCONSISTENT`).
- Count EVERY attempted FlowMod (primary, failed, rollback, cleanup) in `rule_count` (AC-S06).
- Never modify rules of flow groups outside the requested changes (AC-S06).
- Never mutate `StateManager` or `state_version` directly; emit `FlowBatchOutcome` / `BatchInstallResult`
  for `StateManager` consumption.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Optional, Sequence

from sdn.controller.state_manager import format_utc_timestamp
from sdn.flow_management.route_compiler import CompiledPathPlan, RouteCompiler
from sdn.flow_management.switch_driver import (
    FlowModCommand,
    SwitchDriverError,
    SwitchDriverProtocol,
    SwitchPhaseTag,
    SwitchTimeoutError,
)
from sdn.route_verification.rule_verifier import RuleVerifier
from sdn.schemas.state_models import (
    BatchStatus,
    FlowBatchOutcome,
    FlowDecisionStatus,
    ReasonCode,
    VerificationEvidence,
    VerificationStatus,
)


@dataclass(frozen=True)
class FlowChangeSpec:
    """Input specification for a single flow route change within a batch."""

    flow_group_id: str
    old_path_id: str
    new_path_id: str


@dataclass(frozen=True)
class FlowModCountBreakdown:
    """
    Granular breakdown of OpenFlow FlowMod operations and active switch rule count.

    Clearly distinguishes (`Audit Phase 3` §4, `interfaces_v2.md` §10, `AC-S06`):
    - `attempted_flow_mod_count`: Total FlowMods attempted (primary + rollback + cleanup,
      including failed/rejected/timed-out FlowMods). Equals `FlowBatchOutcome.rule_count`.
    - `succeeded_flow_mod_count`: Number of attempted FlowMods accepted by switches (`accepted=True`).
    - `failed_flow_mod_count`: Number of attempted FlowMods rejected or timed out (`accepted=False`).
    - `primary_flow_mod_count`: Number of FlowMods attempted during primary install (downstream + source-edge).
    - `rollback_flow_mod_count`: Number of FlowMods attempted during rollback (`ROLLBACK_DOWNSTREAM` + `ROLLBACK_SOURCE_EDGE`).
    - `cleanup_flow_mod_count`: Number of FlowMods attempted during post-verification cleanup (`DELETE_STRICT`).
    - `active_switch_rule_count`: Number of rules currently installed in switch flow tables
      (for the specific flow on per-flow breakdown, or across all switches on batch breakdown).
    """

    attempted_flow_mod_count: int
    succeeded_flow_mod_count: int
    failed_flow_mod_count: int
    primary_flow_mod_count: int
    rollback_flow_mod_count: int
    cleanup_flow_mod_count: int
    active_switch_rule_count: int


@dataclass(frozen=True)
class BatchInstallResult:
    """
    Aggregated result of executing a multi-flow decision batch on switches.

    Note (`interfaces_v2.md` §10, `acceptance_criteria_v2.md` §6):
    A multi-switch / multi-flow OpenFlow batch is NOT a network-wide atomic transaction
    (`is_network_atomic = False`). Individual flows may succeed while others roll back
    (`BatchStatus.PARTIAL`) or become unverified (`BatchStatus.INCONSISTENT`).
    """

    run_id: str
    decision_batch_id: str
    completed_at_utc: str
    outcomes: tuple[FlowBatchOutcome, ...]
    suggested_batch_status: BatchStatus
    total_rule_count: int
    is_network_atomic: bool = False
    count_breakdown: Optional[FlowModCountBreakdown] = None
    per_flow_count_breakdowns: dict[str, FlowModCountBreakdown] = field(default_factory=dict)


class RuleInstaller:
    """
    Orchestrates two-phase OpenFlow 1.3 rule installation, rollback, and verification
    via RouteCompiler, SwitchDriverProtocol, and RuleVerifier.
    """

    def __init__(
        self,
        route_compiler: RouteCompiler,
        switch_driver: SwitchDriverProtocol,
        rule_verifier: RuleVerifier,
    ) -> None:
        self._compiler = route_compiler
        self._driver = switch_driver
        self._verifier = rule_verifier
        self._last_flow_count_breakdowns: dict[str, FlowModCountBreakdown] = {}

    @property
    def route_compiler(self) -> RouteCompiler:
        return self._compiler

    @property
    def switch_driver(self) -> SwitchDriverProtocol:
        return self._driver

    @property
    def rule_verifier(self) -> RuleVerifier:
        return self._verifier

    def get_last_flow_count_breakdown(
        self, flow_group_id: str
    ) -> Optional[FlowModCountBreakdown]:
        """Return the granular FlowModCountBreakdown recorded for the most recent operation on `flow_group_id`."""
        return self._last_flow_count_breakdowns.get(flow_group_id)

    def get_active_switch_rule_count(
        self,
        *,
        dpid: Optional[int] = None,
        flow_group_id: Optional[str] = None,
    ) -> int:
        """
        Return the number of active flow entries currently installed in switch tables,
        queried via the driver (or `read_flow_stats` fallback if the driver does not expose
        `get_installed_rule_count`).
        """
        if hasattr(self._driver, "get_installed_rule_count"):
            return int(
                self._driver.get_installed_rule_count(  # type: ignore[attr-defined]
                    dpid=dpid, flow_group_id=flow_group_id
                )
            )

        topology = self._compiler.topology
        dpids = (
            [dpid]
            if dpid is not None
            else [sw.dpid for sw in topology.get_all_switches()]
        )
        total = 0
        for sw_dpid in dpids:
            try:
                entries = self._driver.read_flow_stats(
                    sw_dpid, flow_group_id=flow_group_id
                )
                total += len(entries)
            except SwitchTimeoutError:
                continue
        return total

    def bootstrap_initial_paths(
        self,
        run_id: str,
        verified_at_utc: str,
        *,
        preinstall_both_downstream_branches: bool = True,
    ) -> dict[str, VerificationEvidence]:
        """
        Bootstrap all 48 canonical flows on their initial P1 paths (`AC-S01`, `S1`):
        1. Pre-install downstream rules (`c1`/`c2` and `e_dst`) for P1 (and P2 when
           `preinstall_both_downstream_branches=True`) across all 48 flows + Barriers.
        2. Install source-edge (`e_src`) uplink rules for initial P1 paths + Barriers.
        3. Verify all 48 initial P1 routes via `RuleVerifier` (`FlowStats` + counter/probe).
        4. Return the 48 `VerificationEvidence` objects for `StateManager.initialize_run`
           or `StateManager.verify_bootstrap`.
        """
        norm_ts = format_utc_timestamp(verified_at_utc)
        topology = self._compiler.topology
        all_flows = topology.get_all_flow_groups()
        xids_by_flow: dict[str, list[int]] = {fg.flow_group_id: [] for fg in all_flows}
        touched_downstream_dpids: set[int] = set()
        touched_source_dpids: set[int] = set()

        # Phase 1: Pre-install downstream rules for all 48 flows
        for fg in all_flows:
            fid = fg.flow_group_id
            p1_plan, p2_plan = self._compiler.compile_flow_candidates(fid)
            plans_to_preinstall = (
                (p1_plan, p2_plan) if preinstall_both_downstream_branches else (p1_plan,)
            )
            for plan in plans_to_preinstall:
                for ds_rule in plan.downstream_rules:
                    rec = self._driver.send_flow_mod(
                        ds_rule,
                        FlowModCommand.ADD,
                        phase=SwitchPhaseTag.BOOTSTRAP,
                    )
                    xids_by_flow[fid].append(rec.xid)
                    touched_downstream_dpids.add(ds_rule.dpid)
                    if not rec.accepted:
                        raise RuntimeError(
                            f"Bootstrap downstream FlowMod failed on switch '{ds_rule.switch_id}' "
                            f"for flow '{fid}': {rec.error_reason}"
                        )

        for dpid in sorted(touched_downstream_dpids):
            b_rec = self._driver.send_barrier(dpid, phase=SwitchPhaseTag.BOOTSTRAP)
            if not b_rec.confirmed:
                raise RuntimeError(
                    f"Bootstrap downstream Barrier failed on switch '{b_rec.switch_id}' "
                    f"(dpid={dpid}): {b_rec.error_reason}"
                )

        # Phase 2: Install source-edge initial P1 rules for all 48 flows
        for fg in all_flows:
            fid = fg.flow_group_id
            init_cp = topology.get_initial_candidate_path(fid)
            init_plan = self._compiler.compile_path(init_cp.path_id)
            src_rule = init_plan.source_edge_rule
            rec = self._driver.send_flow_mod(
                src_rule,
                FlowModCommand.ADD,
                phase=SwitchPhaseTag.BOOTSTRAP,
            )
            xids_by_flow[fid].append(rec.xid)
            touched_source_dpids.add(src_rule.dpid)
            if not rec.accepted:
                raise RuntimeError(
                    f"Bootstrap source-edge FlowMod failed on switch '{src_rule.switch_id}' "
                    f"for flow '{fid}': {rec.error_reason}"
                )

        for dpid in sorted(touched_source_dpids):
            b_rec = self._driver.send_barrier(dpid, phase=SwitchPhaseTag.BOOTSTRAP)
            if not b_rec.confirmed:
                raise RuntimeError(
                    f"Bootstrap source-edge Barrier failed on switch '{b_rec.switch_id}' "
                    f"(dpid={dpid}): {b_rec.error_reason}"
                )

        # Phase 3: Verify all 48 initial P1 paths via RuleVerifier
        evidences: dict[str, VerificationEvidence] = {}
        for fg in all_flows:
            fid = fg.flow_group_id
            init_path_id = topology.get_initial_candidate_path(fid).path_id
            v_res = self._verifier.verify_flow_path(
                run_id=run_id,
                flow_group_id=fid,
                expected_path_id=init_path_id,
                verified_at_utc=norm_ts,
                barrier_confirmed=True,
                xid_refs=xids_by_flow[fid],
            )
            if not v_res.is_verified or v_res.evidence is None:
                raise RuntimeError(
                    f"Bootstrap verification failed for flow '{fid}' on '{init_path_id}': "
                    f"{v_res.detail}"
                )
            evidences[fid] = v_res.evidence

        return evidences

    def reinstall_and_verify_paths(
        self,
        run_id: str,
        target_paths_by_flow: Mapping[str, str],
        verified_at_utc: str,
        *,
        preinstall_both_downstream_branches: bool = False,
    ) -> dict[str, VerificationEvidence]:
        """
        Re-install and verify a specified mapping `{flow_group_id: path_id}` across switches
        using two-phase `OFPFC_ADD` (downstream + Barrier first, source-edge + Barrier second)
        followed by `RuleVerifier` verification (`REQ-13`, `REQ-16`).

        Used to recover from a switch restart where switch flow tables were wiped.
        """
        norm_ts = format_utc_timestamp(verified_at_utc)
        xids_by_flow: dict[str, list[int]] = {fid: [] for fid in target_paths_by_flow}
        touched_downstream_dpids: set[int] = set()
        touched_source_dpids: set[int] = set()

        # Phase 1: Install downstream rules via OFPFC_ADD + Barrier
        for fid, target_pid in target_paths_by_flow.items():
            target_plan = self._compiler.compile_path(target_pid)
            if target_plan.flow_group_id != fid:
                raise ValueError(
                    f"Target path '{target_pid}' does not belong to flow '{fid}'."
                )
            plans = (
                self._compiler.compile_flow_candidates(fid)
                if preinstall_both_downstream_branches
                else (target_plan,)
            )
            for plan in plans:
                for ds_rule in plan.downstream_rules:
                    rec = self._driver.send_flow_mod(
                        ds_rule,
                        FlowModCommand.ADD,
                        phase=SwitchPhaseTag.BOOTSTRAP,
                    )
                    xids_by_flow[fid].append(rec.xid)
                    touched_downstream_dpids.add(ds_rule.dpid)
                    if not rec.accepted:
                        raise RuntimeError(
                            f"Recovery downstream FlowMod failed on switch '{ds_rule.switch_id}' "
                            f"for flow '{fid}': {rec.error_reason}"
                        )

        for dpid in sorted(touched_downstream_dpids):
            b_rec = self._driver.send_barrier(dpid, phase=SwitchPhaseTag.BOOTSTRAP)
            if not b_rec.confirmed:
                raise RuntimeError(
                    f"Recovery downstream Barrier failed on switch '{b_rec.switch_id}' "
                    f"(dpid={dpid}): {b_rec.error_reason}"
                )

        # Phase 2: Install source-edge rules via OFPFC_ADD + Barrier
        for fid, target_pid in target_paths_by_flow.items():
            target_plan = self._compiler.compile_path(target_pid)
            src_rule = target_plan.source_edge_rule
            rec = self._driver.send_flow_mod(
                src_rule,
                FlowModCommand.ADD,
                phase=SwitchPhaseTag.BOOTSTRAP,
            )
            xids_by_flow[fid].append(rec.xid)
            touched_source_dpids.add(src_rule.dpid)
            if not rec.accepted:
                raise RuntimeError(
                    f"Recovery source-edge FlowMod failed on switch '{src_rule.switch_id}' "
                    f"for flow '{fid}': {rec.error_reason}"
                )

        for dpid in sorted(touched_source_dpids):
            b_rec = self._driver.send_barrier(dpid, phase=SwitchPhaseTag.BOOTSTRAP)
            if not b_rec.confirmed:
                raise RuntimeError(
                    f"Recovery source-edge Barrier failed on switch '{b_rec.switch_id}' "
                    f"(dpid={dpid}): {b_rec.error_reason}"
                )

        # Phase 3: Verify all target paths via RuleVerifier
        evidences: dict[str, VerificationEvidence] = {}
        for fid, target_pid in target_paths_by_flow.items():
            v_res = self._verifier.verify_flow_path(
                run_id=run_id,
                flow_group_id=fid,
                expected_path_id=target_pid,
                verified_at_utc=norm_ts,
                barrier_confirmed=True,
                xid_refs=xids_by_flow[fid],
            )
            if not v_res.is_verified or v_res.evidence is None:
                raise RuntimeError(
                    f"Recovery verification failed for flow '{fid}' on '{target_pid}': {v_res.detail}"
                )
            evidences[fid] = v_res.evidence

        return evidences

    def _record_flow_breakdown(
        self,
        flow_group_id: str,
        *,
        attempted: int,
        succeeded: int,
        primary: int,
        rollback: int,
        cleanup: int,
    ) -> FlowModCountBreakdown:
        breakdown = FlowModCountBreakdown(
            attempted_flow_mod_count=attempted,
            succeeded_flow_mod_count=succeeded,
            failed_flow_mod_count=attempted - succeeded,
            primary_flow_mod_count=primary,
            rollback_flow_mod_count=rollback,
            cleanup_flow_mod_count=cleanup,
            active_switch_rule_count=self.get_active_switch_rule_count(
                flow_group_id=flow_group_id
            ),
        )
        self._last_flow_count_breakdowns[flow_group_id] = breakdown
        return breakdown

    def _rollback_and_verify_old_path(
        self,
        *,
        run_id: str,
        flow_group_id: str,
        old_plan: CompiledPathPlan,
        proposed_path_id: str,
        applied_at_utc: str,
        primary_failure_reason: ReasonCode,
        rule_count_so_far: int,
        succeeded_so_far: int,
        primary_so_far: int,
        xids_so_far: list[int],
        had_intermediate_physical_change: bool,
    ) -> FlowBatchOutcome:
        """
        Execute two-phase rollback to `old_plan`:
        1. Ensure `old_plan` downstream rules via `OFPFC_ADD` + Barrier.
        2. Restore `old_plan` source-edge rule via `OFPFC_MODIFY_STRICT` + Barrier.
        3. Verify `old_plan.path_id` via `RuleVerifier`.
        Never fakes `actual_path_id = old_plan.path_id` if rollback verification fails!
        """
        rule_count = rule_count_so_far
        succeeded_count = succeeded_so_far
        rollback_mods = 0
        xids = list(xids_so_far)
        rollback_ok = True
        rollback_timed_out = False

        # Rollback Phase 1: Ensure old_plan downstream rules (OFPFC_ADD) + Barrier
        ds_dpids: list[int] = []
        for ds_rule in old_plan.downstream_rules:
            rule_count += 1
            rollback_mods += 1
            try:
                rec = self._driver.send_flow_mod(
                    ds_rule,
                    FlowModCommand.ADD,
                    phase=SwitchPhaseTag.ROLLBACK_DOWNSTREAM,
                )
            except SwitchTimeoutError:
                rollback_ok = False
                rollback_timed_out = True
                break
            except (SwitchDriverError, RuntimeError):
                rollback_ok = False
                break
            xids.append(rec.xid)
            ds_dpids.append(ds_rule.dpid)
            if rec.accepted:
                succeeded_count += 1
            else:
                rollback_ok = False
                if rec.timed_out:
                    rollback_timed_out = True

        if rollback_ok:
            for dpid in ds_dpids:
                try:
                    b_rec = self._driver.send_barrier(
                        dpid, phase=SwitchPhaseTag.ROLLBACK_DOWNSTREAM
                    )
                except SwitchTimeoutError:
                    rollback_ok = False
                    rollback_timed_out = True
                    break
                except (SwitchDriverError, RuntimeError):
                    rollback_ok = False
                    break
                xids.append(b_rec.xid)
                if not b_rec.confirmed:
                    rollback_ok = False
                    if b_rec.timed_out:
                        rollback_timed_out = True

        # Rollback Phase 2: Restore old_plan source_edge rule (OFPFC_MODIFY_STRICT) + Barrier
        if rollback_ok:
            src_rule = old_plan.source_edge_rule
            rule_count += 1
            rollback_mods += 1
            try:
                src_rec = self._driver.send_flow_mod(
                    src_rule,
                    FlowModCommand.MODIFY_STRICT,
                    phase=SwitchPhaseTag.ROLLBACK_SOURCE_EDGE,
                )
            except SwitchTimeoutError:
                rollback_ok = False
                rollback_timed_out = True
                src_rec = None
            except (SwitchDriverError, RuntimeError):
                rollback_ok = False
                src_rec = None

            if src_rec is not None:
                xids.append(src_rec.xid)
                if not src_rec.accepted:
                    rollback_ok = False
                    if src_rec.timed_out:
                        rollback_timed_out = True
                else:
                    succeeded_count += 1
                    try:
                        src_bar = self._driver.send_barrier(
                            src_rule.dpid, phase=SwitchPhaseTag.ROLLBACK_SOURCE_EDGE
                        )
                        xids.append(src_bar.xid)
                        if not src_bar.confirmed:
                            rollback_ok = False
                            if src_bar.timed_out:
                                rollback_timed_out = True
                    except SwitchTimeoutError:
                        rollback_ok = False
                        rollback_timed_out = True
                    except (SwitchDriverError, RuntimeError):
                        rollback_ok = False

        self._record_flow_breakdown(
            flow_group_id,
            attempted=rule_count,
            succeeded=succeeded_count,
            primary=primary_so_far,
            rollback=rollback_mods,
            cleanup=0,
        )

        # Rollback Phase 3: Verify old_plan via RuleVerifier
        if rollback_ok:
            try:
                rb_verify = self._verifier.verify_flow_path(
                    run_id=run_id,
                    flow_group_id=flow_group_id,
                    expected_path_id=old_plan.path_id,
                    verified_at_utc=applied_at_utc,
                    barrier_confirmed=True,
                    xid_refs=xids,
                )
            except SwitchTimeoutError:
                rb_verify = None
                rollback_timed_out = True
            except (SwitchDriverError, RuntimeError):
                rb_verify = None

            if rb_verify is not None and rb_verify.is_verified and rb_verify.evidence is not None:
                return FlowBatchOutcome(
                    flow_group_id=flow_group_id,
                    old_path_id=old_plan.path_id,
                    proposed_path_id=proposed_path_id,
                    actual_path_id=old_plan.path_id,
                    status=FlowDecisionStatus.ROLLED_BACK,
                    verification_status=VerificationStatus.VERIFIED,
                    rule_count=rule_count,
                    reason=primary_failure_reason.value,
                    applied_at_utc=applied_at_utc,
                    evidence=rb_verify.evidence,
                    had_intermediate_physical_change=had_intermediate_physical_change,
                )
            if rb_verify is not None and rb_verify.verification_status == VerificationStatus.VERIFY_TIMEOUT:
                rollback_timed_out = True

        # Rollback could not be verified -> MUST return UNKNOWN with actual_path_id=None (AC-S04)
        unk_reason = (
            ReasonCode.VERIFY_TIMEOUT.value
            if rollback_timed_out
            else ReasonCode.ROLLBACK_FAILED.value
        )
        return FlowBatchOutcome(
            flow_group_id=flow_group_id,
            old_path_id=old_plan.path_id,
            proposed_path_id=proposed_path_id,
            actual_path_id=None,
            status=FlowDecisionStatus.UNKNOWN,
            verification_status=VerificationStatus.UNKNOWN,
            rule_count=rule_count,
            reason=unk_reason,
            applied_at_utc=applied_at_utc,
            evidence=None,
            had_intermediate_physical_change=had_intermediate_physical_change,
        )

    def install_flow_change(
        self,
        run_id: str,
        flow_group_id: str,
        old_path_id: str,
        new_path_id: str,
        applied_at_utc: str,
        *,
        cleanup_old_downstream: bool = False,
    ) -> FlowBatchOutcome:
        """
        Execute a safe two-phase route change for `flow_group_id` from `old_path_id` to `new_path_id` (S3):
        1. Install/ensure downstream switches (`c_core`, `e_dst`) first via `OFPFC_ADD`
           (creates rule if downstream branch was not pre-installed, or idempotently replaces it).
        2. Send Barrier to downstream switches and confirm reply.
           * If any downstream FlowMod or Barrier fails, abort BEFORE touching `e_src`,
             rollback/verify `old_path_id`, and return `ROLLED_BACK` (or `UNKNOWN` if unverified).
        3. Update `e_src` uplink selection rule (`port 3 <-> port 4`) last via `OFPFC_MODIFY_STRICT`
           (strictly modifying the existing `(table_id, priority, match)` entry on `e_src`) + Barrier.
        4. Verify `new_path_id` via `RuleVerifier` (`FlowStats` + counter/probe).
        5. Optionally delete obsolete downstream rules (`OFPFC_DELETE_STRICT`) ONLY after `new_path_id` is verified.
        """
        norm_ts = format_utc_timestamp(applied_at_utc)
        old_plan = self._compiler.compile_path(old_path_id)
        new_plan = self._compiler.compile_path(new_path_id)

        if old_plan.flow_group_id != flow_group_id or new_plan.flow_group_id != flow_group_id:
            raise ValueError(
                f"Paths ({old_path_id}, {new_path_id}) do not match flow_group_id '{flow_group_id}'."
            )

        if old_path_id == new_path_id:
            self._record_flow_breakdown(
                flow_group_id,
                attempted=0,
                succeeded=0,
                primary=0,
                rollback=0,
                cleanup=0,
            )
            v_same = self._verifier.verify_flow_path(
                run_id=run_id,
                flow_group_id=flow_group_id,
                expected_path_id=old_path_id,
                verified_at_utc=norm_ts,
                barrier_confirmed=True,
            )
            if v_same.is_verified and v_same.evidence is not None:
                return FlowBatchOutcome(
                    flow_group_id=flow_group_id,
                    old_path_id=old_path_id,
                    proposed_path_id=new_path_id,
                    actual_path_id=old_path_id,
                    status=FlowDecisionStatus.KEPT,
                    verification_status=VerificationStatus.VERIFIED,
                    rule_count=0,
                    reason=None,
                    applied_at_utc=norm_ts,
                    evidence=v_same.evidence,
                    had_intermediate_physical_change=False,
                )
            return FlowBatchOutcome(
                flow_group_id=flow_group_id,
                old_path_id=old_path_id,
                proposed_path_id=new_path_id,
                actual_path_id=None,
                status=FlowDecisionStatus.UNKNOWN,
                verification_status=VerificationStatus.UNKNOWN,
                rule_count=0,
                reason=ReasonCode.ROUTE_STATE_UNKNOWN.value,
                applied_at_utc=norm_ts,
                evidence=None,
                had_intermediate_physical_change=False,
            )

        rule_count = 0
        succeeded_count = 0
        primary_mods = 0
        cleanup_mods = 0
        xids: list[int] = []

        # ---------------------------------------------------------------------
        # Phase 1: Downstream Switches First (`c_core` and `e_dst`) via OFPFC_ADD + Barrier
        # ---------------------------------------------------------------------
        downstream_dpids: list[int] = []
        for ds_rule in new_plan.downstream_rules:
            rule_count += 1
            primary_mods += 1
            try:
                ds_rec = self._driver.send_flow_mod(
                    ds_rule,
                    FlowModCommand.ADD,
                    phase=SwitchPhaseTag.PRIMARY_DOWNSTREAM,
                )
            except (SwitchTimeoutError, SwitchDriverError, RuntimeError) as exc:
                fail_reason = (
                    ReasonCode.VERIFY_TIMEOUT
                    if isinstance(exc, SwitchTimeoutError)
                    else ReasonCode.FLOWMOD_ERROR
                )
                return self._rollback_and_verify_old_path(
                    run_id=run_id,
                    flow_group_id=flow_group_id,
                    old_plan=old_plan,
                    proposed_path_id=new_path_id,
                    applied_at_utc=norm_ts,
                    primary_failure_reason=fail_reason,
                    rule_count_so_far=rule_count,
                    succeeded_so_far=succeeded_count,
                    primary_so_far=primary_mods,
                    xids_so_far=xids,
                    had_intermediate_physical_change=False,
                )
            xids.append(ds_rec.xid)
            downstream_dpids.append(ds_rule.dpid)

            if not ds_rec.accepted:
                # Abort BEFORE touching source edge!
                fail_reason = (
                    ReasonCode.VERIFY_TIMEOUT
                    if ds_rec.timed_out
                    else ReasonCode.FLOWMOD_ERROR
                )
                return self._rollback_and_verify_old_path(
                    run_id=run_id,
                    flow_group_id=flow_group_id,
                    old_plan=old_plan,
                    proposed_path_id=new_path_id,
                    applied_at_utc=norm_ts,
                    primary_failure_reason=fail_reason,
                    rule_count_so_far=rule_count,
                    succeeded_so_far=succeeded_count,
                    primary_so_far=primary_mods,
                    xids_so_far=xids,
                    had_intermediate_physical_change=False,
                )
            succeeded_count += 1

        for dpid in downstream_dpids:
            try:
                ds_bar = self._driver.send_barrier(
                    dpid, phase=SwitchPhaseTag.PRIMARY_DOWNSTREAM
                )
            except (SwitchTimeoutError, SwitchDriverError, RuntimeError) as exc:
                fail_reason = (
                    ReasonCode.VERIFY_TIMEOUT
                    if isinstance(exc, SwitchTimeoutError)
                    else ReasonCode.VERIFY_FAILED
                )
                return self._rollback_and_verify_old_path(
                    run_id=run_id,
                    flow_group_id=flow_group_id,
                    old_plan=old_plan,
                    proposed_path_id=new_path_id,
                    applied_at_utc=norm_ts,
                    primary_failure_reason=fail_reason,
                    rule_count_so_far=rule_count,
                    succeeded_so_far=succeeded_count,
                    primary_so_far=primary_mods,
                    xids_so_far=xids,
                    had_intermediate_physical_change=False,
                )
            xids.append(ds_bar.xid)
            if not ds_bar.confirmed:
                # Downstream Barrier failed -> DO NOT touch source edge!
                fail_reason = (
                    ReasonCode.VERIFY_TIMEOUT
                    if ds_bar.timed_out
                    else ReasonCode.VERIFY_FAILED
                )
                return self._rollback_and_verify_old_path(
                    run_id=run_id,
                    flow_group_id=flow_group_id,
                    old_plan=old_plan,
                    proposed_path_id=new_path_id,
                    applied_at_utc=norm_ts,
                    primary_failure_reason=fail_reason,
                    rule_count_so_far=rule_count,
                    succeeded_so_far=succeeded_count,
                    primary_so_far=primary_mods,
                    xids_so_far=xids,
                    had_intermediate_physical_change=False,
                )

        # ---------------------------------------------------------------------
        # Phase 2: Source-Edge Switch (`e_src` uplink 3 <-> 4) Last via OFPFC_MODIFY_STRICT + Barrier
        # ---------------------------------------------------------------------
        src_rule = new_plan.source_edge_rule
        rule_count += 1
        primary_mods += 1
        try:
            src_rec = self._driver.send_flow_mod(
                src_rule,
                FlowModCommand.MODIFY_STRICT,
                phase=SwitchPhaseTag.PRIMARY_SOURCE_EDGE,
            )
        except (SwitchTimeoutError, SwitchDriverError, RuntimeError) as exc:
            fail_reason = (
                ReasonCode.VERIFY_TIMEOUT
                if isinstance(exc, SwitchTimeoutError)
                else ReasonCode.FLOWMOD_ERROR
            )
            return self._rollback_and_verify_old_path(
                run_id=run_id,
                flow_group_id=flow_group_id,
                old_plan=old_plan,
                proposed_path_id=new_path_id,
                applied_at_utc=norm_ts,
                primary_failure_reason=fail_reason,
                rule_count_so_far=rule_count,
                succeeded_so_far=succeeded_count,
                primary_so_far=primary_mods,
                xids_so_far=xids,
                had_intermediate_physical_change=True,
            )
        xids.append(src_rec.xid)

        if not src_rec.accepted:
            fail_reason = (
                ReasonCode.VERIFY_TIMEOUT
                if src_rec.timed_out
                else ReasonCode.FLOWMOD_ERROR
            )
            return self._rollback_and_verify_old_path(
                run_id=run_id,
                flow_group_id=flow_group_id,
                old_plan=old_plan,
                proposed_path_id=new_path_id,
                applied_at_utc=norm_ts,
                primary_failure_reason=fail_reason,
                rule_count_so_far=rule_count,
                succeeded_so_far=succeeded_count,
                primary_so_far=primary_mods,
                xids_so_far=xids,
                had_intermediate_physical_change=False,
            )
        succeeded_count += 1

        # Once e_src FlowMod was accepted, physical forwarding at source edge has been touched
        try:
            src_bar = self._driver.send_barrier(
                src_rule.dpid, phase=SwitchPhaseTag.PRIMARY_SOURCE_EDGE
            )
        except (SwitchTimeoutError, SwitchDriverError, RuntimeError) as exc:
            fail_reason = (
                ReasonCode.VERIFY_TIMEOUT
                if isinstance(exc, SwitchTimeoutError)
                else ReasonCode.VERIFY_FAILED
            )
            return self._rollback_and_verify_old_path(
                run_id=run_id,
                flow_group_id=flow_group_id,
                old_plan=old_plan,
                proposed_path_id=new_path_id,
                applied_at_utc=norm_ts,
                primary_failure_reason=fail_reason,
                rule_count_so_far=rule_count,
                succeeded_so_far=succeeded_count,
                primary_so_far=primary_mods,
                xids_so_far=xids,
                had_intermediate_physical_change=True,
            )
        xids.append(src_bar.xid)
        if not src_bar.confirmed:
            fail_reason = (
                ReasonCode.VERIFY_TIMEOUT
                if src_bar.timed_out
                else ReasonCode.VERIFY_FAILED
            )
            return self._rollback_and_verify_old_path(
                run_id=run_id,
                flow_group_id=flow_group_id,
                old_plan=old_plan,
                proposed_path_id=new_path_id,
                applied_at_utc=norm_ts,
                primary_failure_reason=fail_reason,
                rule_count_so_far=rule_count,
                succeeded_so_far=succeeded_count,
                primary_so_far=primary_mods,
                xids_so_far=xids,
                had_intermediate_physical_change=True,
            )

        # ---------------------------------------------------------------------
        # Phase 3: True Verification via RuleVerifier (FlowStats + Counter/Probe)
        # ---------------------------------------------------------------------
        verify_res = self._verifier.verify_flow_path(
            run_id=run_id,
            flow_group_id=flow_group_id,
            expected_path_id=new_path_id,
            verified_at_utc=norm_ts,
            barrier_confirmed=True,
            xid_refs=xids,
        )

        if not verify_res.is_verified or verify_res.evidence is None:
            fail_reason = verify_res.reason or ReasonCode.VERIFY_FAILED
            return self._rollback_and_verify_old_path(
                run_id=run_id,
                flow_group_id=flow_group_id,
                old_plan=old_plan,
                proposed_path_id=new_path_id,
                applied_at_utc=norm_ts,
                primary_failure_reason=fail_reason,
                rule_count_so_far=rule_count,
                succeeded_so_far=succeeded_count,
                primary_so_far=primary_mods,
                xids_so_far=xids,
                had_intermediate_physical_change=True,
            )

        # ---------------------------------------------------------------------
        # Phase 4 (Optional): Clean Up Old Downstream Rules Only After Verify
        # ---------------------------------------------------------------------
        if cleanup_old_downstream:
            for old_ds_rule in old_plan.downstream_rules:
                del_rec = self._driver.send_flow_mod(
                    old_ds_rule,
                    FlowModCommand.DELETE_STRICT,
                    phase=SwitchPhaseTag.CLEANUP,
                )
                rule_count += 1
                cleanup_mods += 1
                if del_rec.accepted:
                    succeeded_count += 1
                xids.append(del_rec.xid)
                self._driver.send_barrier(
                    old_ds_rule.dpid, phase=SwitchPhaseTag.CLEANUP
                )

        self._record_flow_breakdown(
            flow_group_id,
            attempted=rule_count,
            succeeded=succeeded_count,
            primary=primary_mods,
            rollback=0,
            cleanup=cleanup_mods,
        )

        return FlowBatchOutcome(
            flow_group_id=flow_group_id,
            old_path_id=old_path_id,
            proposed_path_id=new_path_id,
            actual_path_id=new_path_id,
            status=FlowDecisionStatus.APPLIED,
            verification_status=VerificationStatus.VERIFIED,
            rule_count=rule_count,
            reason=None,
            applied_at_utc=norm_ts,
            evidence=verify_res.evidence,
            had_intermediate_physical_change=True,
        )

    def install_batch(
        self,
        run_id: str,
        decision_batch_id: str,
        changes: Sequence[FlowChangeSpec],
        applied_at_utc: str,
        *,
        cleanup_old_downstream: bool = False,
    ) -> BatchInstallResult:
        """
        Execute a sequence of flow route changes (`changes`) and return a `BatchInstallResult`
        containing the per-flow `FlowBatchOutcome` tuple for `StateManager.commit_batch_outcome()`.

        Does NOT mutate `StateManager` or increment `state_version` directly.
        """
        if not changes:
            raise ValueError("changes cannot be empty in install_batch.")

        norm_ts = format_utc_timestamp(applied_at_utc)
        outcomes: list[FlowBatchOutcome] = []
        total_rules = 0
        per_flow_breakdowns: dict[str, FlowModCountBreakdown] = {}

        for chg in changes:
            outcome = self.install_flow_change(
                run_id=run_id,
                flow_group_id=chg.flow_group_id,
                old_path_id=chg.old_path_id,
                new_path_id=chg.new_path_id,
                applied_at_utc=norm_ts,
                cleanup_old_downstream=cleanup_old_downstream,
            )
            outcomes.append(outcome)
            total_rules += outcome.rule_count
            fb = self.get_last_flow_count_breakdown(chg.flow_group_id)
            if fb is not None:
                per_flow_breakdowns[chg.flow_group_id] = fb

        batch_breakdown = FlowModCountBreakdown(
            attempted_flow_mod_count=sum(
                b.attempted_flow_mod_count for b in per_flow_breakdowns.values()
            ),
            succeeded_flow_mod_count=sum(
                b.succeeded_flow_mod_count for b in per_flow_breakdowns.values()
            ),
            failed_flow_mod_count=sum(
                b.failed_flow_mod_count for b in per_flow_breakdowns.values()
            ),
            primary_flow_mod_count=sum(
                b.primary_flow_mod_count for b in per_flow_breakdowns.values()
            ),
            rollback_flow_mod_count=sum(
                b.rollback_flow_mod_count for b in per_flow_breakdowns.values()
            ),
            cleanup_flow_mod_count=sum(
                b.cleanup_flow_mod_count for b in per_flow_breakdowns.values()
            ),
            active_switch_rule_count=self.get_active_switch_rule_count(),
        )

        statuses = {o.status for o in outcomes}
        if FlowDecisionStatus.UNKNOWN in statuses:
            suggested_status = BatchStatus.INCONSISTENT
        elif statuses == {FlowDecisionStatus.APPLIED}:
            suggested_status = BatchStatus.APPLIED
        elif statuses == {FlowDecisionStatus.ROLLED_BACK}:
            suggested_status = BatchStatus.ROLLED_BACK
        elif statuses <= {FlowDecisionStatus.FAILED, FlowDecisionStatus.KEPT}:
            suggested_status = BatchStatus.FAILED
        else:
            suggested_status = BatchStatus.PARTIAL

        return BatchInstallResult(
            run_id=run_id,
            decision_batch_id=decision_batch_id,
            completed_at_utc=norm_ts,
            outcomes=tuple(outcomes),
            suggested_batch_status=suggested_status,
            total_rule_count=total_rules,
            is_network_atomic=False,
            count_breakdown=batch_breakdown,
            per_flow_count_breakdowns=per_flow_breakdowns,
        )


