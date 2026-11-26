"""
SwitchDriverProtocol and InMemorySwitchDriver for SDN Controller Phase 3
(Integration Guide v1.4 & Testing v2 compliant).

Responsibilities:
- Define the pure-Python SwitchDriverProtocol isolating OpenFlow 1.3 switch interactions
  (FlowMod, Barrier, FlowStats read-back, and forwarding counter/probe checks) from Ryu.
- Implement InMemorySwitchDriver (EvidenceMode.MOCK_SIMULATED) for deterministic Windows
  testing without Ryu, Mininet, or Open vSwitch.
- Support deterministic fault injection hooks (F09–F12):
  * FlowMod rejection or drop (F10, F11)
  * Barrier failure or timeout (F09)
  * FlowStats read-back mismatch / wrong out_port / missing rule (F12)
  * Counter / active probe verification failure (AC-S03, F09)
  * Unresponsive switch / timeout during primary install or rollback (F09)
- Record a complete chronological command log (FLOW_MOD, BARRIER, FLOW_STATS, FORWARDING_CHECK)
  so tests can verify that downstream switches are always updated and barrier-confirmed before
  the source edge switch (S3).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Optional, Protocol, runtime_checkable

from sdn.controller.topology_registry import TopologyRegistry
from sdn.flow_management.flow_matcher import FlowMatchSpec
from sdn.flow_management.route_compiler import CompiledPathPlan, HopStage, OpenFlowRuleSpec
from sdn.schemas.state_models import EvidenceMode


class SwitchDriverError(Exception):
    """Base exception for switch driver operations."""


class SwitchTimeoutError(SwitchDriverError):
    """Raised when a switch does not respond within the OpenFlow/probe timeout budget (F09)."""


class FlowModCommand(str, Enum):
    """OpenFlow 1.3 FlowMod command types."""

    ADD = "OFPFC_ADD"
    MODIFY_STRICT = "OFPFC_MODIFY_STRICT"
    DELETE_STRICT = "OFPFC_DELETE_STRICT"


class SwitchPhaseTag(str, Enum):
    """Execution phase tag recorded on switch operations for audit and fault injection."""

    BOOTSTRAP = "BOOTSTRAP"
    PRIMARY_DOWNSTREAM = "PRIMARY_DOWNSTREAM"
    PRIMARY_SOURCE_EDGE = "PRIMARY_SOURCE_EDGE"
    ROLLBACK_DOWNSTREAM = "ROLLBACK_DOWNSTREAM"
    ROLLBACK_SOURCE_EDGE = "ROLLBACK_SOURCE_EDGE"
    CLEANUP = "CLEANUP"
    VERIFICATION = "VERIFICATION"


@dataclass(frozen=True)
class FlowModReceipt:
    """Result of sending a single OpenFlow 1.3 FlowMod message to a switch."""

    xid: int
    dpid: int
    switch_id: str
    command: FlowModCommand
    flow_group_id: str
    path_id: str
    hop_stage: HopStage
    out_port: int
    phase: SwitchPhaseTag
    accepted: bool
    timed_out: bool = False
    error_reason: Optional[str] = None


@dataclass(frozen=True)
class BarrierReceipt:
    """Result of sending an OpenFlow 1.3 BarrierRequest to a switch and awaiting BarrierReply."""

    xid: int
    dpid: int
    switch_id: str
    phase: SwitchPhaseTag
    confirmed: bool
    timed_out: bool = False
    error_reason: Optional[str] = None


@dataclass(frozen=True)
class SwitchFlowStatsEntry:
    """Simulated or live OpenFlow 1.3 FlowStats entry read back from a switch flow table."""

    dpid: int
    switch_id: str
    table_id: int
    priority: int
    cookie_int: int
    cookie_hex: str
    flow_group_id: str
    path_id: str
    match: FlowMatchSpec
    out_port: int
    packet_count: int = 0
    byte_count: int = 0


@dataclass(frozen=True)
class SwitchPortDescEntry:
    """OpenFlow 1.3 Port Description (`OFPPortDescStatsReply` / `OFPPort`) read from a switch."""

    dpid: int
    switch_id: str
    port_no: int
    name: str
    hw_addr: str = "00:00:00:00:00:00"
    config: int = 0
    state: int = 0
    is_live: bool = True


@dataclass(frozen=True)
class ForwardingCheckResult:
    """
    Result of verifying actual data-plane forwarding (counter increment or active probe)
    along a candidate path (AC-S03).
    """

    flow_group_id: str
    checked_path_id: str
    actual_forwarded_path_id: Optional[str]
    counter_or_probe_ok: bool
    probe_id: Optional[str]
    packet_delta: int = 0
    timed_out: bool = False
    error_reason: Optional[str] = None


@dataclass(frozen=True)
class SwitchOperationLogEntry:
    """Chronological audit log entry for every switch interaction."""

    seq: int
    op_type: str
    xid: int
    dpid: int
    switch_id: str
    phase: SwitchPhaseTag
    flow_group_id: Optional[str]
    path_id: Optional[str]
    hop_stage: Optional[HopStage]
    out_port: Optional[int]
    success: bool
    timed_out: bool = False
    error_reason: Optional[str] = None
    command: Optional[FlowModCommand] = None


@runtime_checkable
class SwitchDriverProtocol(Protocol):
    """
    Abstract driver interface for OpenFlow 1.3 switch control and verification.
    Implemented by InMemorySwitchDriver on Windows (MOCK_SIMULATED) and RyuOpenFlowDriver
    on Ubuntu Linux (LIVE_OVS).
    """

    @property
    def evidence_mode(self) -> EvidenceMode:
        ...

    def send_flow_mod(
        self,
        rule: OpenFlowRuleSpec,
        command: FlowModCommand,
        *,
        phase: SwitchPhaseTag,
    ) -> FlowModReceipt:
        ...

    def send_barrier(
        self,
        dpid: int,
        *,
        phase: SwitchPhaseTag,
    ) -> BarrierReceipt:
        ...

    def read_flow_stats(
        self,
        dpid: int,
        *,
        flow_group_id: Optional[str] = None,
        cookie_int: Optional[int] = None,
    ) -> tuple[SwitchFlowStatsEntry, ...]:
        ...

    def verify_forwarding_counter_or_probe(
        self,
        compiled_plan: CompiledPathPlan,
        *,
        probe_id: Optional[str] = None,
    ) -> ForwardingCheckResult:
        ...

    def get_operation_log(self) -> tuple[SwitchOperationLogEntry, ...]:
        ...


@dataclass(frozen=True)
class _FlowStatsFaultSpec:
    wrong_out_port: Optional[int] = None
    omit_rule: bool = False
    timed_out: bool = False


class InMemorySwitchDriver(SwitchDriverProtocol):
    """
    Pure-Python simulated OpenFlow 1.3 switch fabric for Windows unit and contract testing.

    - Maintains per-switch flow tables keyed by `(table_id, priority, match_tuple_key)`.
    - Distinguishes staged (pending-barrier) writes from barrier-committed writes.
    - Evaluates actual end-to-end forwarding across `e_src -> c_core -> e_dst` tables when
      `verify_forwarding_counter_or_probe` is invoked.
    - Provides deterministic fault injection hooks (`F09`–`F12`).
    """

    def __init__(self, topology_registry: TopologyRegistry) -> None:
        self._topology = topology_registry
        self._xid_counter: int = 0
        self._op_log: list[SwitchOperationLogEntry] = []

        # Per-switch active flow table: dpid -> {table_entry_key: SwitchFlowStatsEntry}
        self._flow_tables: dict[int, dict[tuple, SwitchFlowStatsEntry]] = {
            sw.dpid: {} for sw in self._topology.get_all_switches()
        }
        # Track whether a switch has uncommitted FlowMods awaiting a Barrier
        self._pending_barrier_dpids: set[int] = set()

        # Fault injection registries (F09–F12)
        self._unresponsive_dpids: set[int] = set()
        self._flowmod_faults: dict[
            tuple[int, Optional[str], Optional[SwitchPhaseTag]], tuple[str, bool]
        ] = {}
        self._barrier_faults: dict[
            tuple[int, Optional[SwitchPhaseTag]], tuple[str, bool]
        ] = {}
        self._flowstats_faults: dict[
            tuple[int, Optional[str]], _FlowStatsFaultSpec
        ] = {}
        self._probe_faults: dict[
            tuple[str, Optional[str]], tuple[str, bool]
        ] = {}

    @property
    def evidence_mode(self) -> EvidenceMode:
        return EvidenceMode.MOCK_SIMULATED

    @property
    def topology(self) -> TopologyRegistry:
        return self._topology

    def _next_xid(self) -> int:
        self._xid_counter += 1
        return self._xid_counter

    def _record_op(
        self,
        *,
        op_type: str,
        xid: int,
        dpid: int,
        switch_id: str,
        phase: SwitchPhaseTag,
        flow_group_id: Optional[str] = None,
        path_id: Optional[str] = None,
        hop_stage: Optional[HopStage] = None,
        out_port: Optional[int] = None,
        success: bool,
        timed_out: bool = False,
        error_reason: Optional[str] = None,
        command: Optional[FlowModCommand] = None,
    ) -> None:
        seq = len(self._op_log) + 1
        self._op_log.append(
            SwitchOperationLogEntry(
                seq=seq,
                op_type=op_type,
                xid=xid,
                dpid=dpid,
                switch_id=switch_id,
                phase=phase,
                flow_group_id=flow_group_id,
                path_id=path_id,
                hop_stage=hop_stage,
                out_port=out_port,
                success=success,
                timed_out=timed_out,
                error_reason=error_reason,
                command=command,
            )
        )

    # -------------------------------------------------------------------------
    # Switch Flow Table Inspection & Restart Simulation Helpers
    # -------------------------------------------------------------------------

    def clear_switch_flow_table(self, dpid: Optional[int] = None) -> None:
        """
        Clear installed flow entries on a specific switch (`dpid`) or across all 6 switches
        (`dpid=None`) to simulate an empty flow table at cold start or a switch restart
        that wipes OpenFlow tables (`REQ-13`, `REQ-16`).
        """
        if dpid is not None:
            self._topology.get_switch_by_dpid(dpid)
            self._flow_tables[dpid].clear()
            self._pending_barrier_dpids.discard(dpid)
            return

        for sw_dpid in self._flow_tables:
            self._flow_tables[sw_dpid].clear()
        self._pending_barrier_dpids.clear()

    def get_installed_rule_count(
        self,
        dpid: Optional[int] = None,
        *,
        flow_group_id: Optional[str] = None,
    ) -> int:
        """
        Return the number of active flow rules currently present in switch flow tables,
        optionally filtered by `dpid` and/or `flow_group_id`.
        """
        if dpid is not None:
            self._topology.get_switch_by_dpid(dpid)
            target_tables = [self._flow_tables[dpid]]
        else:
            target_tables = list(self._flow_tables.values())

        count = 0
        for tbl in target_tables:
            for entry in tbl.values():
                if flow_group_id is not None and entry.flow_group_id != flow_group_id:
                    continue
                count += 1
        return count

    def has_pending_barrier(self, dpid: int) -> bool:
        """Return True if `dpid` has accepted FlowMods that have not yet been barrier-confirmed."""
        self._topology.get_switch_by_dpid(dpid)
        return dpid in self._pending_barrier_dpids

    # -------------------------------------------------------------------------
    # Fault Injection Hooks (F09–F12)
    # -------------------------------------------------------------------------

    def clear_faults(self) -> None:
        """Reset all injected fault hooks while preserving current switch flow tables."""
        self._unresponsive_dpids.clear()
        self._flowmod_faults.clear()
        self._barrier_faults.clear()
        self._flowstats_faults.clear()
        self._probe_faults.clear()

    def clear_operation_log(self) -> None:
        """Clear the recorded switch operation log."""
        self._op_log.clear()

    def set_switch_unresponsive(self, dpid: int, unresponsive: bool = True) -> None:
        """Simulate a switch becoming completely unresponsive / timing out (F09)."""
        self._topology.get_switch_by_dpid(dpid)
        if unresponsive:
            self._unresponsive_dpids.add(dpid)
        else:
            self._unresponsive_dpids.discard(dpid)

    def inject_flowmod_failure(
        self,
        dpid: int,
        *,
        flow_group_id: Optional[str] = None,
        phase: Optional[SwitchPhaseTag] = None,
        reason: str = "FLOWMOD_ERROR",
        timed_out: bool = False,
    ) -> None:
        """Inject a deterministic FlowMod rejection or timeout on `dpid` (F10, F11)."""
        self._topology.get_switch_by_dpid(dpid)
        self._flowmod_faults[(dpid, flow_group_id, phase)] = (reason, timed_out)

    def inject_barrier_failure(
        self,
        dpid: int,
        *,
        phase: Optional[SwitchPhaseTag] = None,
        reason: str = "BARRIER_FAILED",
        timed_out: bool = False,
    ) -> None:
        """Inject a deterministic Barrier failure or timeout on `dpid` (F09)."""
        self._topology.get_switch_by_dpid(dpid)
        self._barrier_faults[(dpid, phase)] = (reason, timed_out)

    def inject_flow_stats_fault(
        self,
        dpid: int,
        *,
        flow_group_id: Optional[str] = None,
        wrong_out_port: Optional[int] = None,
        omit_rule: bool = False,
        timed_out: bool = False,
    ) -> None:
        """
        Inject a FlowStats read-back mismatch (e.g. wrong out_port F12, missing rule, or timeout F09).
        """
        self._topology.get_switch_by_dpid(dpid)
        self._flowstats_faults[(dpid, flow_group_id)] = _FlowStatsFaultSpec(
            wrong_out_port=wrong_out_port,
            omit_rule=omit_rule,
            timed_out=timed_out,
        )

    def inject_forwarding_probe_failure(
        self,
        flow_group_id: str,
        *,
        path_id: Optional[str] = None,
        reason: str = "COUNTER_OR_PROBE_UNCONFIRMED",
        timed_out: bool = False,
    ) -> None:
        """
        Inject a forwarding counter / active probe verification failure (AC-S03, F09).
        """
        self._topology.get_flow_group(flow_group_id)
        self._probe_faults[(flow_group_id, path_id)] = (reason, timed_out)

    def _lookup_flowmod_fault(
        self, dpid: int, flow_group_id: str, phase: SwitchPhaseTag
    ) -> Optional[tuple[str, bool]]:
        for key in (
            (dpid, flow_group_id, phase),
            (dpid, flow_group_id, None),
            (dpid, None, phase),
            (dpid, None, None),
        ):
            if key in self._flowmod_faults:
                return self._flowmod_faults[key]
        return None

    def _lookup_barrier_fault(
        self, dpid: int, phase: SwitchPhaseTag
    ) -> Optional[tuple[str, bool]]:
        for key in ((dpid, phase), (dpid, None)):
            if key in self._barrier_faults:
                return self._barrier_faults[key]
        return None

    def _lookup_flowstats_fault(
        self, dpid: int, flow_group_id: Optional[str]
    ) -> Optional[_FlowStatsFaultSpec]:
        if flow_group_id is not None and (dpid, flow_group_id) in self._flowstats_faults:
            return self._flowstats_faults[(dpid, flow_group_id)]
        if (dpid, None) in self._flowstats_faults:
            return self._flowstats_faults[(dpid, None)]
        return None

    def _lookup_probe_fault(
        self, flow_group_id: str, path_id: str
    ) -> Optional[tuple[str, bool]]:
        for key in ((flow_group_id, path_id), (flow_group_id, None)):
            if key in self._probe_faults:
                return self._probe_faults[key]
        return None

    # -------------------------------------------------------------------------
    # SwitchDriverProtocol Implementation
    # -------------------------------------------------------------------------

    def send_flow_mod(
        self,
        rule: OpenFlowRuleSpec,
        command: FlowModCommand,
        *,
        phase: SwitchPhaseTag,
    ) -> FlowModReceipt:
        """
        Apply an OpenFlow 1.3 FlowMod (ADD, MODIFY_STRICT, DELETE_STRICT) to a switch's
        simulated flow table. Every attempted FlowMod is logged with a unique xid.
        """
        sw = self._topology.get_switch_by_dpid(rule.dpid)
        xid = self._next_xid()

        if rule.dpid in self._unresponsive_dpids:
            self._record_op(
                op_type="FLOW_MOD",
                xid=xid,
                dpid=rule.dpid,
                switch_id=sw.switch_id,
                phase=phase,
                flow_group_id=rule.flow_group_id,
                path_id=rule.path_id,
                hop_stage=rule.hop_stage,
                out_port=rule.out_port,
                success=False,
                timed_out=True,
                error_reason="SWITCH_UNRESPONSIVE_TIMEOUT",
                command=command,
            )
            return FlowModReceipt(
                xid=xid,
                dpid=rule.dpid,
                switch_id=sw.switch_id,
                command=command,
                flow_group_id=rule.flow_group_id,
                path_id=rule.path_id,
                hop_stage=rule.hop_stage,
                out_port=rule.out_port,
                phase=phase,
                accepted=False,
                timed_out=True,
                error_reason="SWITCH_UNRESPONSIVE_TIMEOUT",
            )

        fault = self._lookup_flowmod_fault(rule.dpid, rule.flow_group_id, phase)
        if fault is not None:
            reason, timed_out = fault
            self._record_op(
                op_type="FLOW_MOD",
                xid=xid,
                dpid=rule.dpid,
                switch_id=sw.switch_id,
                phase=phase,
                flow_group_id=rule.flow_group_id,
                path_id=rule.path_id,
                hop_stage=rule.hop_stage,
                out_port=rule.out_port,
                success=False,
                timed_out=timed_out,
                error_reason=reason,
                command=command,
            )
            return FlowModReceipt(
                xid=xid,
                dpid=rule.dpid,
                switch_id=sw.switch_id,
                command=command,
                flow_group_id=rule.flow_group_id,
                path_id=rule.path_id,
                hop_stage=rule.hop_stage,
                out_port=rule.out_port,
                phase=phase,
                accepted=False,
                timed_out=timed_out,
                error_reason=reason,
            )

        table = self._flow_tables[rule.dpid]
        entry_key = rule.table_entry_key

        if command == FlowModCommand.ADD:
            # OpenFlow 1.3 OFPFC_ADD (without OFPFF_CHECK_OVERLAP):
            # Creates a new flow entry, or replaces an existing entry with identical
            # (table_id, priority, match) and resets its counters.
            table[entry_key] = SwitchFlowStatsEntry(
                dpid=rule.dpid,
                switch_id=sw.switch_id,
                table_id=rule.table_id,
                priority=rule.priority,
                cookie_int=rule.cookie_int,
                cookie_hex=rule.cookie_hex,
                flow_group_id=rule.flow_group_id,
                path_id=rule.path_id,
                match=rule.match,
                out_port=rule.out_port,
                packet_count=0,
                byte_count=0,
            )
        elif command == FlowModCommand.MODIFY_STRICT:
            # OpenFlow 1.3 OFPFC_MODIFY_STRICT:
            # Strictly matches (table_id, priority, match). MUST NOT create a new rule
            # if no matching entry exists in the switch flow table!
            prev = table.get(entry_key)
            if prev is None:
                self._record_op(
                    op_type="FLOW_MOD",
                    xid=xid,
                    dpid=rule.dpid,
                    switch_id=sw.switch_id,
                    phase=phase,
                    flow_group_id=rule.flow_group_id,
                    path_id=rule.path_id,
                    hop_stage=rule.hop_stage,
                    out_port=rule.out_port,
                    success=False,
                    timed_out=False,
                    error_reason="OFPFMFC_UNKNOWN_STRICT_ENTRY_NOT_FOUND",
                    command=command,
                )
                return FlowModReceipt(
                    xid=xid,
                    dpid=rule.dpid,
                    switch_id=sw.switch_id,
                    command=command,
                    flow_group_id=rule.flow_group_id,
                    path_id=rule.path_id,
                    hop_stage=rule.hop_stage,
                    out_port=rule.out_port,
                    phase=phase,
                    accepted=False,
                    timed_out=False,
                    error_reason="OFPFMFC_UNKNOWN_STRICT_ENTRY_NOT_FOUND",
                )
            table[entry_key] = SwitchFlowStatsEntry(
                dpid=rule.dpid,
                switch_id=sw.switch_id,
                table_id=rule.table_id,
                priority=rule.priority,
                cookie_int=prev.cookie_int,
                cookie_hex=prev.cookie_hex,
                flow_group_id=rule.flow_group_id,
                path_id=rule.path_id,
                match=rule.match,
                out_port=rule.out_port,
                packet_count=prev.packet_count,
                byte_count=prev.byte_count,
            )
        elif command == FlowModCommand.DELETE_STRICT:
            existing = table.get(entry_key)
            if existing is not None and existing.out_port == rule.out_port:
                table.pop(entry_key, None)

        self._pending_barrier_dpids.add(rule.dpid)
        self._record_op(
            op_type="FLOW_MOD",
            xid=xid,
            dpid=rule.dpid,
            switch_id=sw.switch_id,
            phase=phase,
            flow_group_id=rule.flow_group_id,
            path_id=rule.path_id,
            hop_stage=rule.hop_stage,
            out_port=rule.out_port,
            success=True,
            command=command,
        )
        return FlowModReceipt(
            xid=xid,
            dpid=rule.dpid,
            switch_id=sw.switch_id,
            command=command,
            flow_group_id=rule.flow_group_id,
            path_id=rule.path_id,
            hop_stage=rule.hop_stage,
            out_port=rule.out_port,
            phase=phase,
            accepted=True,
        )

    def send_barrier(
        self,
        dpid: int,
        *,
        phase: SwitchPhaseTag,
    ) -> BarrierReceipt:
        """
        Send an OpenFlow 1.3 BarrierRequest to `dpid` and return a BarrierReceipt.
        Note (S3, AC-S03): BarrierReply confirms message processing order on `dpid`,
        but NEVER suffices alone to mark a route VERIFIED.
        """
        sw = self._topology.get_switch_by_dpid(dpid)
        xid = self._next_xid()

        if dpid in self._unresponsive_dpids:
            self._record_op(
                op_type="BARRIER",
                xid=xid,
                dpid=dpid,
                switch_id=sw.switch_id,
                phase=phase,
                success=False,
                timed_out=True,
                error_reason="SWITCH_UNRESPONSIVE_TIMEOUT",
            )
            return BarrierReceipt(
                xid=xid,
                dpid=dpid,
                switch_id=sw.switch_id,
                phase=phase,
                confirmed=False,
                timed_out=True,
                error_reason="SWITCH_UNRESPONSIVE_TIMEOUT",
            )

        fault = self._lookup_barrier_fault(dpid, phase)
        if fault is not None:
            reason, timed_out = fault
            self._record_op(
                op_type="BARRIER",
                xid=xid,
                dpid=dpid,
                switch_id=sw.switch_id,
                phase=phase,
                success=False,
                timed_out=timed_out,
                error_reason=reason,
            )
            return BarrierReceipt(
                xid=xid,
                dpid=dpid,
                switch_id=sw.switch_id,
                phase=phase,
                confirmed=False,
                timed_out=timed_out,
                error_reason=reason,
            )

        self._pending_barrier_dpids.discard(dpid)
        self._record_op(
            op_type="BARRIER",
            xid=xid,
            dpid=dpid,
            switch_id=sw.switch_id,
            phase=phase,
            success=True,
        )
        return BarrierReceipt(
            xid=xid,
            dpid=dpid,
            switch_id=sw.switch_id,
            phase=phase,
            confirmed=True,
        )

    def read_flow_stats(
        self,
        dpid: int,
        *,
        flow_group_id: Optional[str] = None,
        cookie_int: Optional[int] = None,
    ) -> tuple[SwitchFlowStatsEntry, ...]:
        """
        Read FlowStats entries from switch `dpid` (simulating OFPMP_FLOW).
        Raises SwitchTimeoutError if `dpid` is unresponsive or has a timeout fault injected.
        """
        sw = self._topology.get_switch_by_dpid(dpid)
        xid = self._next_xid()

        if dpid in self._unresponsive_dpids:
            self._record_op(
                op_type="FLOW_STATS",
                xid=xid,
                dpid=dpid,
                switch_id=sw.switch_id,
                phase=SwitchPhaseTag.VERIFICATION,
                flow_group_id=flow_group_id,
                success=False,
                timed_out=True,
                error_reason="SWITCH_UNRESPONSIVE_TIMEOUT",
            )
            raise SwitchTimeoutError(
                f"Switch '{sw.switch_id}' (dpid={dpid}) timed out during FlowStats request."
            )

        fault = self._lookup_flowstats_fault(dpid, flow_group_id)
        if fault is not None and fault.timed_out:
            self._record_op(
                op_type="FLOW_STATS",
                xid=xid,
                dpid=dpid,
                switch_id=sw.switch_id,
                phase=SwitchPhaseTag.VERIFICATION,
                flow_group_id=flow_group_id,
                success=False,
                timed_out=True,
                error_reason="FLOWSTATS_TIMEOUT",
            )
            raise SwitchTimeoutError(
                f"Switch '{sw.switch_id}' (dpid={dpid}) timed out during FlowStats read-back."
            )

        entries: list[SwitchFlowStatsEntry] = []
        for entry in self._flow_tables[dpid].values():
            if flow_group_id is not None and entry.flow_group_id != flow_group_id:
                continue
            if cookie_int is not None and entry.cookie_int != cookie_int:
                continue

            item_fault = self._lookup_flowstats_fault(dpid, entry.flow_group_id)
            if item_fault is not None:
                if item_fault.omit_rule:
                    continue
                if item_fault.wrong_out_port is not None:
                    entry = replace(entry, out_port=item_fault.wrong_out_port)
            entries.append(entry)

        self._record_op(
            op_type="FLOW_STATS",
            xid=xid,
            dpid=dpid,
            switch_id=sw.switch_id,
            phase=SwitchPhaseTag.VERIFICATION,
            flow_group_id=flow_group_id,
            success=True,
        )
        return tuple(entries)

    def verify_forwarding_counter_or_probe(
        self,
        compiled_plan: CompiledPathPlan,
        *,
        probe_id: Optional[str] = None,
    ) -> ForwardingCheckResult:
        """
        Simulate end-to-end forwarding verification (counter increment or active probe)
        along `compiled_plan` (e_src -> c_core -> e_dst).

        Raises SwitchTimeoutError if any traversed switch is unresponsive or if a probe
        timeout fault is injected.
        """
        fid = compiled_plan.flow_group_id
        pid = compiled_plan.path_id
        src_dpid = compiled_plan.source_edge_rule.dpid
        src_sw = compiled_plan.source_edge_rule.switch_id
        xid = self._next_xid()

        for hop_rule in compiled_plan.ordered_hop_rules:
            if hop_rule.dpid in self._unresponsive_dpids:
                self._record_op(
                    op_type="FORWARDING_CHECK",
                    xid=xid,
                    dpid=hop_rule.dpid,
                    switch_id=hop_rule.switch_id,
                    phase=SwitchPhaseTag.VERIFICATION,
                    flow_group_id=fid,
                    path_id=pid,
                    success=False,
                    timed_out=True,
                    error_reason="SWITCH_UNRESPONSIVE_TIMEOUT",
                )
                raise SwitchTimeoutError(
                    f"Switch '{hop_rule.switch_id}' (dpid={hop_rule.dpid}) unresponsive during forwarding check."
                )

        probe_fault = self._lookup_probe_fault(fid, pid)
        if probe_fault is not None:
            reason, timed_out = probe_fault
            self._record_op(
                op_type="FORWARDING_CHECK",
                xid=xid,
                dpid=src_dpid,
                switch_id=src_sw,
                phase=SwitchPhaseTag.VERIFICATION,
                flow_group_id=fid,
                path_id=pid,
                success=False,
                timed_out=timed_out,
                error_reason=reason,
            )
            if timed_out:
                raise SwitchTimeoutError(
                    f"Forwarding counter/probe check timed out for flow '{fid}' on '{pid}': {reason}"
                )
            return ForwardingCheckResult(
                flow_group_id=fid,
                checked_path_id=pid,
                actual_forwarded_path_id=None,
                counter_or_probe_ok=False,
                probe_id=probe_id,
                packet_delta=0,
                timed_out=False,
                error_reason=reason,
            )

        # Trace actual installed rules across the 3 hops (e_src -> c_core -> e_dst)
        matched_entries: list[tuple[int, tuple, SwitchFlowStatsEntry]] = []
        for hop_rule in compiled_plan.ordered_hop_rules:
            table = self._flow_tables[hop_rule.dpid]
            entry = table.get(hop_rule.table_entry_key)
            if (
                entry is None
                or entry.out_port != hop_rule.out_port
                or entry.cookie_int != hop_rule.cookie_int
            ):
                self._record_op(
                    op_type="FORWARDING_CHECK",
                    xid=xid,
                    dpid=hop_rule.dpid,
                    switch_id=hop_rule.switch_id,
                    phase=SwitchPhaseTag.VERIFICATION,
                    flow_group_id=fid,
                    path_id=pid,
                    success=False,
                    error_reason=f"HOP_RULE_MISMATCH_ON_{hop_rule.switch_id}",
                )
                return ForwardingCheckResult(
                    flow_group_id=fid,
                    checked_path_id=pid,
                    actual_forwarded_path_id=None,
                    counter_or_probe_ok=False,
                    probe_id=probe_id,
                    packet_delta=0,
                    error_reason=f"HOP_RULE_MISMATCH_ON_{hop_rule.switch_id}",
                )
            matched_entries.append((hop_rule.dpid, hop_rule.table_entry_key, entry))

        # All 3 hops match: increment simulated packet/byte counters on the 3 traversed rules
        for dpid, key, entry in matched_entries:
            self._flow_tables[dpid][key] = replace(
                entry,
                packet_count=entry.packet_count + 1,
                byte_count=entry.byte_count + 1200,
            )

        eff_probe_id = probe_id or f"mock_probe:{fid}:{pid}:{xid}"
        self._record_op(
            op_type="FORWARDING_CHECK",
            xid=xid,
            dpid=src_dpid,
            switch_id=src_sw,
            phase=SwitchPhaseTag.VERIFICATION,
            flow_group_id=fid,
            path_id=pid,
            success=True,
        )
        return ForwardingCheckResult(
            flow_group_id=fid,
            checked_path_id=pid,
            actual_forwarded_path_id=pid,
            counter_or_probe_ok=True,
            probe_id=eff_probe_id,
            packet_delta=1,
        )

    def get_operation_log(self) -> tuple[SwitchOperationLogEntry, ...]:
        """Return the chronological log of all switch operations."""
        return tuple(self._op_log)

    def read_port_desc(self, dpid: int) -> tuple[SwitchPortDescEntry, ...]:
        """Return simulated OpenFlow 1.3 port descriptions for `dpid` from `TopologyRegistry`."""
        sw = self._topology.get_switch_by_dpid(dpid)
        ports: list[SwitchPortDescEntry] = []
        seen_ports: set[int] = set()
        for lk in self._topology.get_all_links():
            if lk.from_node == sw.switch_id and lk.from_port not in seen_ports:
                seen_ports.add(lk.from_port)
                ports.append(
                    SwitchPortDescEntry(
                        dpid=dpid,
                        switch_id=sw.switch_id,
                        port_no=lk.from_port,
                        name=f"{sw.switch_id}-eth{lk.from_port}",
                        is_live=True,
                    )
                )
        return tuple(sorted(ports, key=lambda p: p.port_no))

