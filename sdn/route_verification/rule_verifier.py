"""
RuleVerifier for SDN Controller Phase 3 (Integration Guide v1.4 & Testing v2 compliant).

Responsibilities:
- Verify that a canonical path_id (e.g. '0x103_P1' or '0x103_P2') is genuinely active
  across all 3 traversed switches (e_src, c_core, e_dst) before producing VerificationEvidence.
- Enforce AC-S03:
  * Barrier confirmation alone is NEVER sufficient for VERIFIED.
  * Both OpenFlow FlowStats rule read-back (`rule_readback_ok=True`) AND data-plane forwarding
    evidence via counter or probe (`counter_or_probe_ok=True`) are mandatory.
- Isolate the [OPEN / MUST-LOCK] active-probe vs. counter policy behind a configurable
  RuleVerifierConfig (`policy_status="MUST_LOCK_PROVISIONAL_DEFAULT"`).
- Preserve EvidenceMode (EvidenceMode.MOCK_SIMULATED on Windows vs. EvidenceMode.LIVE_OVS on Ubuntu).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Sequence

from sdn.controller.state_manager import format_utc_timestamp
from sdn.flow_management.route_compiler import CompiledPathPlan, RouteCompiler
from sdn.flow_management.switch_driver import (
    SwitchDriverError,
    SwitchDriverProtocol,
    SwitchTimeoutError,
)
from sdn.schemas.state_models import (
    ReasonCode,
    VerificationEvidence,
    VerificationStatus,
)


class ProbeVerificationPolicy(str, Enum):
    """
    Configurable data-plane forwarding verification policy ([MUST-LOCK] item).

    - COUNTER_OR_PROBE: Use port/flow counter delta when flow has active traffic,
      and active probe packet when demand is zero (AC-S03).
    - ACTIVE_PROBE_ALWAYS: Always inject an active probe packet matching the flow's IP pair.
    - COUNTER_ONLY: Verify via OpenFlow flow/port counter delta only.
    """

    COUNTER_OR_PROBE = "COUNTER_OR_PROBE"
    ACTIVE_PROBE_ALWAYS = "ACTIVE_PROBE_ALWAYS"
    COUNTER_ONLY = "COUNTER_ONLY"


@dataclass(frozen=True)
class RuleVerifierConfig:
    """
    Configuration for RuleVerifier.
    Marked MUST_LOCK_PROVISIONAL_DEFAULT until probe interval/timeout parameters are frozen.
    """

    probe_policy: ProbeVerificationPolicy = ProbeVerificationPolicy.COUNTER_OR_PROBE
    verify_timeout_ms: int = 250
    policy_status: str = "MUST_LOCK_PROVISIONAL_DEFAULT"


@dataclass(frozen=True)
class RouteVerificationResult:
    """
    Structured outcome of RuleVerifier.verify_flow_path().
    Only when `is_verified` is True is `evidence` populated with a valid VerificationEvidence.
    """

    run_id: str
    flow_group_id: str
    expected_path_id: str
    actual_path_id: Optional[str]
    is_verified: bool
    verification_status: VerificationStatus
    barrier_confirmed: bool
    rule_readback_ok: bool
    counter_or_probe_ok: bool
    evidence: Optional[VerificationEvidence]
    reason: Optional[ReasonCode] = None
    detail: Optional[str] = None


class RuleVerifier:
    """
    Verifies actual OpenFlow 1.3 route installation and forwarding across switch hops.
    Does NOT mutate StateManager directly; returns RouteVerificationResult containing
    VerificationEvidence for StateManager consumption.
    """

    def __init__(
        self,
        route_compiler: RouteCompiler,
        switch_driver: SwitchDriverProtocol,
        config: Optional[RuleVerifierConfig] = None,
    ) -> None:
        self._compiler = route_compiler
        self._driver = switch_driver
        self._config = config or RuleVerifierConfig()

    @property
    def route_compiler(self) -> RouteCompiler:
        return self._compiler

    @property
    def switch_driver(self) -> SwitchDriverProtocol:
        return self._driver

    @property
    def config(self) -> RuleVerifierConfig:
        return self._config

    def verify_flow_path(
        self,
        run_id: str,
        flow_group_id: str,
        expected_path_id: str,
        verified_at_utc: str,
        *,
        barrier_confirmed: bool = True,
        xid_refs: Sequence[int] = (),
        probe_id: Optional[str] = None,
    ) -> RouteVerificationResult:
        """
        Verify that `expected_path_id` for `flow_group_id` is installed and forwarding packets.

        Checks:
        1. `barrier_confirmed` is True.
        2. FlowStats read-back on all 3 switches (`e_src`, `c_core`, `e_dst`) matches the
           compiled `OpenFlowRuleSpec` (table_id, priority, match fields, cookie, and out_port).
        3. Forwarding counter/probe check (`verify_forwarding_counter_or_probe`) confirms
           end-to-end forwarding on `expected_path_id`.
        """
        norm_ts = format_utc_timestamp(verified_at_utc)
        plan: CompiledPathPlan = self._compiler.compile_path(expected_path_id)
        if plan.flow_group_id != flow_group_id:
            raise ValueError(
                f"Path '{expected_path_id}' belongs to flow '{plan.flow_group_id}', "
                f"not '{flow_group_id}'."
            )

        # Step 1: Barrier check (AC-S03)
        if not barrier_confirmed:
            return RouteVerificationResult(
                run_id=run_id,
                flow_group_id=flow_group_id,
                expected_path_id=expected_path_id,
                actual_path_id=None,
                is_verified=False,
                verification_status=VerificationStatus.VERIFY_FAILED,
                barrier_confirmed=False,
                rule_readback_ok=False,
                counter_or_probe_ok=False,
                evidence=None,
                reason=ReasonCode.VERIFY_FAILED,
                detail="Barrier reply was not confirmed across updated switches.",
            )

        # Step 2: FlowStats rule read-back across all 3 hops (e_src, c_core, e_dst)
        for hop_rule in plan.ordered_hop_rules:
            try:
                stats_entries = self._driver.read_flow_stats(
                    hop_rule.dpid,
                    flow_group_id=flow_group_id,
                    cookie_int=hop_rule.cookie_int,
                )
            except SwitchTimeoutError as exc:
                return RouteVerificationResult(
                    run_id=run_id,
                    flow_group_id=flow_group_id,
                    expected_path_id=expected_path_id,
                    actual_path_id=None,
                    is_verified=False,
                    verification_status=VerificationStatus.VERIFY_TIMEOUT,
                    barrier_confirmed=True,
                    rule_readback_ok=False,
                    counter_or_probe_ok=False,
                    evidence=None,
                    reason=ReasonCode.VERIFY_TIMEOUT,
                    detail=str(exc),
                )
            except (SwitchDriverError, RuntimeError) as exc:
                return RouteVerificationResult(
                    run_id=run_id,
                    flow_group_id=flow_group_id,
                    expected_path_id=expected_path_id,
                    actual_path_id=None,
                    is_verified=False,
                    verification_status=VerificationStatus.VERIFY_FAILED,
                    barrier_confirmed=True,
                    rule_readback_ok=False,
                    counter_or_probe_ok=False,
                    evidence=None,
                    reason=ReasonCode.VERIFY_FAILED,
                    detail=str(exc),
                )

            matching_entry = next(
                (
                    e
                    for e in stats_entries
                    if (e.table_id, e.priority, e.match.match_tuple_key)
                    == hop_rule.table_entry_key
                    and e.cookie_int == hop_rule.cookie_int
                ),
                None,
            )
            if matching_entry is None:
                return RouteVerificationResult(
                    run_id=run_id,
                    flow_group_id=flow_group_id,
                    expected_path_id=expected_path_id,
                    actual_path_id=None,
                    is_verified=False,
                    verification_status=VerificationStatus.VERIFY_FAILED,
                    barrier_confirmed=True,
                    rule_readback_ok=False,
                    counter_or_probe_ok=False,
                    evidence=None,
                    reason=ReasonCode.VERIFY_FAILED,
                    detail=(
                        f"Missing rule in FlowStats on switch '{hop_rule.switch_id}' "
                        f"(dpid={hop_rule.dpid}) for path '{expected_path_id}'."
                    ),
                )
            if matching_entry.out_port != hop_rule.out_port:
                return RouteVerificationResult(
                    run_id=run_id,
                    flow_group_id=flow_group_id,
                    expected_path_id=expected_path_id,
                    actual_path_id=None,
                    is_verified=False,
                    verification_status=VerificationStatus.VERIFY_FAILED,
                    barrier_confirmed=True,
                    rule_readback_ok=False,
                    counter_or_probe_ok=False,
                    evidence=None,
                    reason=ReasonCode.VERIFY_FAILED,
                    detail=(
                        f"FlowStats out_port mismatch on switch '{hop_rule.switch_id}' "
                        f"(dpid={hop_rule.dpid}): expected out_port={hop_rule.out_port}, "
                        f"got out_port={matching_entry.out_port} (F12)."
                    ),
                )

        # Step 3: Forwarding counter or active probe verification (AC-S03)
        try:
            fwd_res = self._driver.verify_forwarding_counter_or_probe(
                plan, probe_id=probe_id
            )
        except SwitchTimeoutError as exc:
            return RouteVerificationResult(
                run_id=run_id,
                flow_group_id=flow_group_id,
                expected_path_id=expected_path_id,
                actual_path_id=None,
                is_verified=False,
                verification_status=VerificationStatus.VERIFY_TIMEOUT,
                barrier_confirmed=True,
                rule_readback_ok=True,
                counter_or_probe_ok=False,
                evidence=None,
                reason=ReasonCode.VERIFY_TIMEOUT,
                detail=str(exc),
            )
        except (SwitchDriverError, RuntimeError) as exc:
            return RouteVerificationResult(
                run_id=run_id,
                flow_group_id=flow_group_id,
                expected_path_id=expected_path_id,
                actual_path_id=None,
                is_verified=False,
                verification_status=VerificationStatus.VERIFY_FAILED,
                barrier_confirmed=True,
                rule_readback_ok=True,
                counter_or_probe_ok=False,
                evidence=None,
                reason=ReasonCode.VERIFY_FAILED,
                detail=str(exc),
            )

        if not fwd_res.counter_or_probe_ok:
            return RouteVerificationResult(
                run_id=run_id,
                flow_group_id=flow_group_id,
                expected_path_id=expected_path_id,
                actual_path_id=None,
                is_verified=False,
                verification_status=VerificationStatus.VERIFY_FAILED,
                barrier_confirmed=True,
                rule_readback_ok=True,
                counter_or_probe_ok=False,
                evidence=None,
                reason=ReasonCode.VERIFY_FAILED,
                detail=fwd_res.error_reason
                or f"Counter/probe failed to confirm forwarding on '{expected_path_id}'.",
            )

        # Step 4: Construct valid VerificationEvidence
        evidence = VerificationEvidence(
            run_id=run_id,
            flow_group_id=flow_group_id,
            verified_path_id=expected_path_id,
            verified_at_utc=norm_ts,
            evidence_mode=self._driver.evidence_mode,
            rule_readback_ok=True,
            counter_or_probe_ok=True,
            barrier_confirmed=True,
            xid_refs=tuple(int(x) for x in xid_refs),
            probe_id=fwd_res.probe_id,
            detail=f"Verified via {self._config.probe_policy.value} ({self._driver.evidence_mode.value})",
        )
        return RouteVerificationResult(
            run_id=run_id,
            flow_group_id=flow_group_id,
            expected_path_id=expected_path_id,
            actual_path_id=expected_path_id,
            is_verified=True,
            verification_status=VerificationStatus.VERIFIED,
            barrier_confirmed=True,
            rule_readback_ok=True,
            counter_or_probe_ok=True,
            evidence=evidence,
            reason=None,
            detail=evidence.detail,
        )

    def discover_active_flow_path(
        self,
        run_id: str,
        flow_group_id: str,
        verified_at_utc: str,
    ) -> RouteVerificationResult:
        """
        Inspect switch flow tables for `flow_group_id` across both candidate paths (P1, P2)
        and return the verified RouteVerificationResult if either candidate path is active
        and verified, or an UNKNOWN result if neither can be verified.
        """
        p1_plan, p2_plan = self._compiler.compile_flow_candidates(flow_group_id)
        res_p1 = self.verify_flow_path(
            run_id, flow_group_id, p1_plan.path_id, verified_at_utc
        )
        if res_p1.is_verified:
            return res_p1

        res_p2 = self.verify_flow_path(
            run_id, flow_group_id, p2_plan.path_id, verified_at_utc
        )
        if res_p2.is_verified:
            return res_p2

        return RouteVerificationResult(
            run_id=run_id,
            flow_group_id=flow_group_id,
            expected_path_id=p1_plan.path_id,
            actual_path_id=None,
            is_verified=False,
            verification_status=VerificationStatus.UNKNOWN,
            barrier_confirmed=bool(res_p1.barrier_confirmed or res_p2.barrier_confirmed),
            rule_readback_ok=bool(res_p1.rule_readback_ok or res_p2.rule_readback_ok),
            counter_or_probe_ok=False,
            evidence=None,
            reason=ReasonCode.ROUTE_STATE_UNKNOWN,
            detail=f"Neither {p1_plan.path_id} nor {p2_plan.path_id} could be verified on switches.",
        )

    def verify_all_initial_paths(
        self,
        run_id: str,
        verified_at_utc: str,
    ) -> dict[str, RouteVerificationResult]:
        """
        Verify existing switch flow tables for all 48 initial P1 paths WITHOUT installing rules.
        Used to confirm that topology catalog data alone never marks bootstrap VERIFIED when
        switch flow tables are empty or incomplete.
        """
        norm_ts = format_utc_timestamp(verified_at_utc)
        topology = self._compiler.topology
        results: dict[str, RouteVerificationResult] = {}
        for fg in topology.get_all_flow_groups():
            fid = fg.flow_group_id
            init_path_id = topology.get_initial_candidate_path(fid).path_id
            results[fid] = self.verify_flow_path(
                run_id=run_id,
                flow_group_id=fid,
                expected_path_id=init_path_id,
                verified_at_utc=norm_ts,
                barrier_confirmed=True,
            )
        return results

    def discover_all_active_paths(
        self,
        run_id: str,
        verified_at_utc: str,
    ) -> dict[str, RouteVerificationResult]:
        """
        Read back FlowStats and forwarding evidence across all 48 canonical flows (`P1` and `P2`)
        to discover the currently active path for each flow after a controller/switch restart (`REQ-13`, `REQ-16`).
        """
        norm_ts = format_utc_timestamp(verified_at_utc)
        topology = self._compiler.topology
        results: dict[str, RouteVerificationResult] = {}
        for fg in topology.get_all_flow_groups():
            fid = fg.flow_group_id
            results[fid] = self.discover_active_flow_path(
                run_id=run_id,
                flow_group_id=fid,
                verified_at_utc=norm_ts,
            )
        return results


