"""
RouteCompiler for SDN Controller Phase 3 (Integration Guide v1.4 & Testing v2 compliant).

Responsibilities:
- Compile a canonical path_id (e.g. '0x103_P1' or '0x103_P2') into abstract OpenFlow 1.3
  per-switch rules (OpenFlowRuleSpec) and a two-phase installation plan (CompiledPathPlan).
- Zero hard-coded DPIDs or ports: every switch hop, DPID, in_port, out_port, and directed
  link ID is resolved dynamically from TopologyRegistry.
- Partition hop rules into:
  1) `downstream_rules`: (core_switch c1/c2, dest_edge e_dst) — MUST be installed and
     barrier-confirmed FIRST (S3 Phase 1).
  2) `source_edge_rule`: (src_edge e_src uplink selection port 3 <-> port 4) — MUST be
     installed LAST after downstream barrier confirmation (S3 Phase 2).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from sdn.controller.topology_registry import CandidatePath, SwitchHop, TopologyRegistry
from sdn.flow_management.flow_matcher import FlowMatchPolicy, FlowMatchSpec, FlowMatcher


DEFAULT_FLOW_TABLE_ID = 0
DEFAULT_FLOW_PRIORITY = 100


class HopStage(str, Enum):
    """Logical role of a switch hop along a 3-switch Fat-Tree inter-edge path."""

    SOURCE_EDGE = "SOURCE_EDGE"
    CORE = "CORE"
    DEST_EDGE = "DEST_EDGE"


@dataclass(frozen=True)
class OpenFlowRuleSpec:
    """
    Abstract OpenFlow 1.3 flow rule specification for a single switch hop.
    """

    flow_group_id: str
    path_id: str
    path_label: str
    hop_index: int
    hop_stage: HopStage
    switch_id: str
    dpid: int
    table_id: int
    priority: int
    cookie_hex: str
    cookie_int: int
    match: FlowMatchSpec
    out_port: int
    ingress_link_id: str
    egress_link_id: str

    @property
    def is_downstream(self) -> bool:
        """True for CORE (c1/c2) and DEST_EDGE (e_dst) hops; False for SOURCE_EDGE (e_src)."""
        return self.hop_stage in (HopStage.CORE, HopStage.DEST_EDGE)

    @property
    def table_entry_key(
        self,
    ) -> tuple[int, int, tuple[int, int, str, str, Optional[int], Optional[int]]]:
        """
        Unique OpenFlow flow-table key on a switch: (table_id, priority, match_tuple_key).
        Note: On e_src, P1 and P2 share the exact same table_entry_key (same host in_port
        and IPv4 src/dst) and differ only in `out_port` (port 3 vs. port 4).
        On c1/c2 and e_dst, P1 and P2 have distinct (dpid, in_port) keys so both downstream
        branches can coexist simultaneously without conflict (AC-S01).
        """
        return (self.table_id, self.priority, self.match.match_tuple_key)


@dataclass(frozen=True)
class CompiledPathPlan:
    """
    Complete two-phase OpenFlow 1.3 compilation result for a canonical candidate path.
    """

    flow_group_id: str
    path_id: str
    path_label: str
    core_switch: str
    ordered_link_ids: tuple[str, ...]
    fabric_link_ids: tuple[str, ...]
    downstream_rules: tuple[OpenFlowRuleSpec, ...]
    source_edge_rule: OpenFlowRuleSpec
    ordered_hop_rules: tuple[OpenFlowRuleSpec, ...]


class RouteCompiler:
    """
    Compiles canonical candidate paths ('0x103_P1', '0x103_P2') into two-phase
    OpenFlow 1.3 rule plans using TopologyRegistry and FlowMatcher.
    """

    def __init__(
        self,
        topology_registry: TopologyRegistry,
        flow_matcher: Optional[FlowMatcher] = None,
        *,
        table_id: int = DEFAULT_FLOW_TABLE_ID,
        priority: int = DEFAULT_FLOW_PRIORITY,
    ) -> None:
        self._topology = topology_registry
        self._matcher = flow_matcher or FlowMatcher(topology_registry)
        self._table_id = int(table_id)
        self._priority = int(priority)

    @property
    def topology(self) -> TopologyRegistry:
        return self._topology

    @property
    def flow_matcher(self) -> FlowMatcher:
        return self._matcher

    def compile_path(
        self,
        path_id: str,
        *,
        policy_override: Optional[FlowMatchPolicy] = None,
    ) -> CompiledPathPlan:
        """
        Compile a canonical path_id (e.g. '0x103_P1' or '0x103_P2') into a CompiledPathPlan.
        Rejects bare labels ('P1', 'P2') or legacy path IDs via TopologyRegistry.
        """
        cp: CandidatePath = self._topology.get_candidate_path(path_id)
        fg = self._topology.get_flow_group(cp.flow_group_id)
        hops: tuple[SwitchHop, ...] = self._topology.get_switch_hops(path_id)

        if len(hops) != 3:
            raise ValueError(
                f"Expected 3 switch hops (src_edge, core, dst_edge) for path '{path_id}', got {len(hops)}."
            )

        stage_by_index = {
            0: HopStage.SOURCE_EDGE,
            1: HopStage.CORE,
            2: HopStage.DEST_EDGE,
        }

        compiled_hops: list[OpenFlowRuleSpec] = []
        for hop in hops:
            stage = stage_by_index[hop.hop_index]
            match_spec = self._matcher.build_match_for_hop(
                fg.flow_group_id,
                hop.in_port,
                policy_override=policy_override,
            )
            compiled_hops.append(
                OpenFlowRuleSpec(
                    flow_group_id=fg.flow_group_id,
                    path_id=cp.path_id,
                    path_label=cp.path_label,
                    hop_index=hop.hop_index,
                    hop_stage=stage,
                    switch_id=hop.switch_id,
                    dpid=hop.dpid,
                    table_id=self._table_id,
                    priority=self._priority,
                    cookie_hex=fg.flow_cookie,
                    cookie_int=fg.cookie_int,
                    match=match_spec,
                    out_port=hop.out_port,
                    ingress_link_id=hop.ingress_link_id,
                    egress_link_id=hop.egress_link_id,
                )
            )

        src_rule = compiled_hops[0]
        downstream = (compiled_hops[1], compiled_hops[2])

        return CompiledPathPlan(
            flow_group_id=fg.flow_group_id,
            path_id=cp.path_id,
            path_label=cp.path_label,
            core_switch=cp.core_switch,
            ordered_link_ids=cp.ordered_link_ids,
            fabric_link_ids=cp.fabric_link_ids,
            downstream_rules=downstream,
            source_edge_rule=src_rule,
            ordered_hop_rules=tuple(compiled_hops),
        )

    def compile_flow_candidates(
        self,
        flow_group_id: str,
        *,
        policy_override: Optional[FlowMatchPolicy] = None,
    ) -> tuple[CompiledPathPlan, CompiledPathPlan]:
        """Return (P1_plan, P2_plan) for the given canonical flow_group_id."""
        cp_p1, cp_p2 = self._topology.get_candidate_paths_for_flow(flow_group_id)
        return (
            self.compile_path(cp_p1.path_id, policy_override=policy_override),
            self.compile_path(cp_p2.path_id, policy_override=policy_override),
        )

