"""
Ryu / OS-Ken OpenFlow 1.3 Switch Driver (`SwitchDriverProtocol` implementation).

Isolated from the pure-Python SDN core (`TopologyRegistry`, `RouteCompiler`,
`StateManager`, `RuleInstaller`, `RuleVerifier`, `DecisionHandler`, `SnapshotCreator`).
Supports both `ryu` and `os_ken` runtimes via lazy imports, while remaining
testable on Windows using fake datapath/event/reply objects (`PASS_MOCK_ADAPTER`).

Never fabricates `LIVE_OVS` verification evidence: `verify_forwarding_counter_or_probe`
requires real OpenFlow `FlowStats` rule presence AND real counter delta or active
probe confirmation before returning `counter_or_probe_ok=True`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import threading
import time
from typing import Any, Callable, Mapping, Optional, Sequence

from sdn.controller.topology_registry import TopologyRegistry
from sdn.flow_management.flow_matcher import FlowMatchPolicy, FlowMatchSpec
from sdn.flow_management.route_compiler import CompiledPathPlan, OpenFlowRuleSpec
from sdn.flow_management.switch_driver import (
    BarrierReceipt,
    FlowModCommand,
    FlowModReceipt,
    ForwardingCheckResult,
    SwitchDriverError,
    SwitchDriverProtocol,
    SwitchFlowStatsEntry,
    SwitchOperationLogEntry,
    SwitchPhaseTag,
    SwitchTimeoutError,
)
from sdn.schemas.state_models import EvidenceMode


@dataclass(frozen=True)
class RyuDriverConfig:
    """Configuration for `RyuOpenFlowDriver` OpenFlow 1.3 wire behavior."""

    barrier_timeout_s: float = 2.0
    flow_stats_timeout_s: float = 2.0
    min_counter_packet_delta: int = 1
    strict_dpid_validation: bool = True
    hard_timeout_s: int = 0
    idle_timeout_s: int = 0


@dataclass
class _PendingBarrier:
    dpid: int
    xid: int
    phase: SwitchPhaseTag
    event: Any
    confirmed: bool = False
    error_reason: Optional[str] = None


@dataclass
class _PendingFlowStats:
    dpid: int
    xid: int
    event: Any
    raw_stats: list[Any] = field(default_factory=list)
    error_reason: Optional[str] = None


def _default_event_factory() -> Any:
    """
    Create a synchronization event compatible with eventlet (if monkey-patched)
    or standard Python `threading.Event`.
    """
    try:
        import eventlet.event  # type: ignore
        import eventlet.patcher  # type: ignore

        if eventlet.patcher.is_monkey_patched("thread"):
            return _EventletEventAdapter(eventlet.event.Event())
    except ImportError:
        pass
    return threading.Event()


class _EventletEventAdapter:
    """Adapts `eventlet.event.Event` to the `.set()` / `.wait(timeout=...)` API."""

    def __init__(self, ev: Any) -> None:
        self._ev = ev

    def set(self) -> None:
        if not self._ev.ready():
            self._ev.send(True)

    def wait(self, timeout: Optional[float] = None) -> bool:
        if timeout is None:
            self._ev.wait()
            return True
        try:
            import eventlet.timeout  # type: ignore

            with eventlet.timeout.Timeout(timeout, False):
                self._ev.wait()
            return bool(self._ev.ready())
        except ImportError:
            self._ev.wait()
            return bool(self._ev.ready())


def _extract_ofp_out_port(stat: Any, ofproto: Any) -> Optional[int]:
    """Extract the first `OFPActionOutput.port` from an OF1.3 `OFPFlowStats` entry."""
    instructions = getattr(stat, "instructions", None) or ()
    apply_type = getattr(ofproto, "OFPIT_APPLY_ACTIONS", 4)
    for inst in instructions:
        inst_type = getattr(inst, "type", None)
        if inst_type is not None and inst_type != apply_type:
            continue
        for action in getattr(inst, "actions", None) or ():
            port = getattr(action, "port", None)
            if port is not None:
                return int(port)
    # Fallback for simplified mock stats objects exposing out_port directly
    direct = getattr(stat, "out_port", None)
    return int(direct) if direct is not None else None


def _ofp_match_to_flow_match_spec(
    raw_match: Any,
    *,
    flow_group_id: str,
    cookie_int: int,
) -> Optional[FlowMatchSpec]:
    """Convert a Ryu `OFPMatch`, dict, or `FlowMatchSpec` into `FlowMatchSpec`."""
    if isinstance(raw_match, FlowMatchSpec):
        return raw_match
    if raw_match is None:
        return None
    # Ryu OFPMatch supports `in` / `__getitem__` / `get` or `.to_jsondict()`
    get_fn = getattr(raw_match, "get", None)
    if not callable(get_fn):
        return None
    in_port = get_fn("in_port")
    eth_type = get_fn("eth_type")
    ipv4_src = get_fn("ipv4_src")
    ipv4_dst = get_fn("ipv4_dst")
    if in_port is None or eth_type is None or ipv4_src is None or ipv4_dst is None:
        return None
    ip_proto = get_fn("ip_proto")
    udp_dst = get_fn("udp_dst")
    policy = (
        FlowMatchPolicy.L4_UDP_DPORT
        if udp_dst is not None
        else FlowMatchPolicy.L3_IPV4_PAIR
    )
    return FlowMatchSpec(
        flow_group_id=flow_group_id,
        flow_cookie_hex=f"0x{cookie_int:X}",
        flow_cookie_int=cookie_int,
        in_port=int(in_port),
        eth_type=int(eth_type),
        ipv4_src=str(ipv4_src),
        ipv4_dst=str(ipv4_dst),
        match_policy=policy,
        ip_proto=int(ip_proto) if ip_proto is not None else None,
        udp_dst=int(udp_dst) if udp_dst is not None else None,
    )


class RyuOpenFlowDriver(SwitchDriverProtocol):
    """
    Live OpenFlow 1.3 driver implementing `SwitchDriverProtocol` for Ryu / OS-Ken.

    Thread/eventlet-safe design:
    - Ryu event handlers (`handle_barrier_reply`, `handle_flow_stats_reply`,
      `handle_error_msg`) never block; they update pending waiters and signal
      events immediately so the OpenFlow receive loop remains responsive.
    - All 6 canonical switches (`DPID 1..6`) are tracked in `_datapaths`.
    """

    def __init__(
        self,
        topology_registry: TopologyRegistry,
        config: Optional[RyuDriverConfig] = None,
        *,
        event_factory: Optional[Callable[[], Any]] = None,
        active_probe_callback: Optional[
            Callable[[CompiledPathPlan, Optional[str]], ForwardingCheckResult]
        ] = None,
    ) -> None:
        self._topology = topology_registry
        self._config = config or RyuDriverConfig()
        self._event_factory = event_factory or _default_event_factory
        self._active_probe_callback = active_probe_callback

        self._lock = threading.RLock()
        self._datapaths: dict[int, Any] = {}
        self._expected_dpids: frozenset[int] = frozenset(
            sw.dpid for sw in self._topology.get_all_switches()
        )
        self._xid_counter: int = 0
        self._seq_counter: int = 0
        self._op_log: list[SwitchOperationLogEntry] = []

        self._pending_barriers: dict[tuple[int, int], _PendingBarrier] = {}
        self._pending_flow_stats: dict[tuple[int, int], _PendingFlowStats] = {}
        self._recent_flowmod_errors: dict[tuple[int, int], str] = {}
        self._flowmod_xids_by_dpid: dict[int, list[int]] = {}
        self._baseline_packet_counts: dict[tuple[int, str, str], int] = {}
        self._disconnect_events: list[tuple[int, str]] = []

    @property
    def evidence_mode(self) -> EvidenceMode:
        return EvidenceMode.LIVE_OVS

    @property
    def config(self) -> RyuDriverConfig:
        return self._config

    @property
    def connected_dpids(self) -> tuple[int, ...]:
        with self._lock:
            return tuple(sorted(self._datapaths.keys()))

    @property
    def all_expected_switches_connected(self) -> bool:
        with self._lock:
            return self._expected_dpids.issubset(self._datapaths.keys())

    @property
    def disconnect_events(self) -> tuple[tuple[int, str], ...]:
        with self._lock:
            return tuple(self._disconnect_events)

    # -------------------------------------------------------------------------
    # Datapath Connection Lifecycle
    # -------------------------------------------------------------------------

    def register_datapath(self, datapath: Any) -> bool:
        """
        Register an OpenFlow 1.3 switch datapath.
        Returns `True` if this replaced a duplicate/existing datapath for `dpid`.
        """
        dpid = int(getattr(datapath, "id"))
        if self._config.strict_dpid_validation and dpid not in self._expected_dpids:
            raise SwitchDriverError(
                f"Unexpected switch DPID {dpid}; expected one of {sorted(self._expected_dpids)}."
            )
        ofproto = getattr(datapath, "ofproto", None)
        ofp_version = getattr(ofproto, "OFP_VERSION", None)
        if ofp_version is not None and int(ofp_version) != 0x04:
            raise SwitchDriverError(
                f"Switch DPID {dpid} negotiated OpenFlow version {ofp_version:#x}, "
                "but OpenFlow 1.3 (0x04) is required."
            )
        with self._lock:
            replaced = dpid in self._datapaths
            self._datapaths[dpid] = datapath
            if replaced:
                self._clear_baseline_counts_for_dpid(dpid)
                self._fail_pending_waiters_for_dpid(
                    dpid, reason=f"DUPLICATE_SWITCH_RECONNECT_DPID_{dpid}"
                )
            return replaced

    def unregister_datapath(self, dpid: int, reason: str = "SWITCH_DISCONNECTED") -> None:
        """Unregister a disconnected switch and fail any pending barrier/stats waiters."""
        with self._lock:
            dpid_int = int(dpid)
            self._datapaths.pop(dpid_int, None)
            self._clear_baseline_counts_for_dpid(dpid_int)
            self._disconnect_events.append((dpid_int, reason))
            self._fail_pending_waiters_for_dpid(dpid_int, reason=reason)

    def _clear_baseline_counts_for_dpid(self, dpid: int) -> None:
        for k in [k for k in self._baseline_packet_counts if k[0] == dpid]:
            self._baseline_packet_counts.pop(k, None)

    def _fail_pending_waiters_for_dpid(self, dpid: int, *, reason: str) -> None:
        for (p_dpid, xid), waiter in list(self._pending_barriers.items()):
            if p_dpid == dpid:
                waiter.confirmed = False
                waiter.error_reason = reason
                waiter.event.set()
        for (p_dpid, xid), waiter in list(self._pending_flow_stats.items()):
            if p_dpid == dpid:
                waiter.error_reason = reason
                waiter.event.set()

    def _get_datapath(self, dpid: int) -> Any:
        with self._lock:
            dp = self._datapaths.get(int(dpid))
        if dp is None:
            raise SwitchDriverError(f"Switch DPID {dpid} is not connected.")
        return dp

    def _next_xid(self, datapath: Any, msg: Any) -> int:
        if hasattr(datapath, "set_xid"):
            try:
                xid = datapath.set_xid(msg)
                if xid is not None:
                    return int(xid)
            except Exception:
                pass
        with self._lock:
            self._xid_counter += 1
            xid = self._xid_counter
        try:
            setattr(msg, "xid", xid)
        except Exception:
            pass
        return xid

    def _record_log(
        self,
        *,
        op_type: str,
        xid: int,
        dpid: int,
        switch_id: str,
        phase: SwitchPhaseTag,
        flow_group_id: Optional[str],
        path_id: Optional[str],
        hop_stage: Any,
        out_port: Optional[int],
        success: bool,
        timed_out: bool = False,
        error_reason: Optional[str] = None,
        command: Optional[FlowModCommand] = None,
    ) -> None:
        with self._lock:
            self._seq_counter += 1
            self._op_log.append(
                SwitchOperationLogEntry(
                    seq=self._seq_counter,
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

    def _switch_id_for_dpid(self, dpid: int) -> str:
        try:
            return self._topology.get_switch_by_dpid(dpid).switch_id
        except Exception:
            return f"dpid:{dpid}"

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
        dp = self._get_datapath(rule.dpid)
        ofproto = dp.ofproto
        parser = dp.ofproto_parser

        if command == FlowModCommand.ADD:
            ofp_cmd = ofproto.OFPFC_ADD
        elif command == FlowModCommand.MODIFY_STRICT:
            ofp_cmd = ofproto.OFPFC_MODIFY_STRICT
        elif command == FlowModCommand.DELETE_STRICT:
            ofp_cmd = ofproto.OFPFC_DELETE_STRICT
        else:
            raise SwitchDriverError(f"Unsupported FlowModCommand: {command!r}")

        match = parser.OFPMatch(**rule.match.to_openflow_match_dict())
        actions = [parser.OFPActionOutput(rule.out_port)]
        instructions = [
            parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)
        ]

        out_port_field = (
            rule.out_port
            if command == FlowModCommand.DELETE_STRICT
            else getattr(ofproto, "OFPP_ANY", 0xFFFFFFFF)
        )
        out_group_field = getattr(ofproto, "OFPG_ANY", 0xFFFFFFFF)
        cookie_mask = (
            0xFFFFFFFFFFFFFFFF
            if command in (FlowModCommand.MODIFY_STRICT, FlowModCommand.DELETE_STRICT)
            else 0
        )

        msg = parser.OFPFlowMod(
            datapath=dp,
            cookie=rule.cookie_int,
            cookie_mask=cookie_mask,
            table_id=rule.table_id,
            command=ofp_cmd,
            idle_timeout=self._config.idle_timeout_s,
            hard_timeout=self._config.hard_timeout_s,
            priority=rule.priority,
            buffer_id=getattr(ofproto, "OFP_NO_BUFFER", 0xFFFFFFFF),
            out_port=out_port_field,
            out_group=out_group_field,
            flags=getattr(ofproto, "OFPFF_SEND_FLOW_REM", 0),
            match=match,
            instructions=instructions,
        )
        xid = self._next_xid(dp, msg)
        with self._lock:
            self._flowmod_xids_by_dpid.setdefault(rule.dpid, []).append(xid)

        try:
            accepted = dp.send_msg(msg)
            if accepted is False:
                err = f"SEND_MSG_FAILED_DPID_{rule.dpid}"
                self._record_log(
                    op_type="FLOW_MOD",
                    xid=xid,
                    dpid=rule.dpid,
                    switch_id=rule.switch_id,
                    phase=phase,
                    flow_group_id=rule.flow_group_id,
                    path_id=rule.path_id,
                    hop_stage=rule.hop_stage,
                    out_port=rule.out_port,
                    success=False,
                    error_reason=err,
                    command=command,
                )
                return FlowModReceipt(
                    xid=xid,
                    dpid=rule.dpid,
                    switch_id=rule.switch_id,
                    command=command,
                    flow_group_id=rule.flow_group_id,
                    path_id=rule.path_id,
                    hop_stage=rule.hop_stage,
                    out_port=rule.out_port,
                    phase=phase,
                    accepted=False,
                    error_reason=err,
                )
        except Exception as exc:
            err = f"FLOWMOD_SEND_EXCEPTION:{exc}"
            self._record_log(
                op_type="FLOW_MOD",
                xid=xid,
                dpid=rule.dpid,
                switch_id=rule.switch_id,
                phase=phase,
                flow_group_id=rule.flow_group_id,
                path_id=rule.path_id,
                hop_stage=rule.hop_stage,
                out_port=rule.out_port,
                success=False,
                error_reason=err,
                command=command,
            )
            return FlowModReceipt(
                xid=xid,
                dpid=rule.dpid,
                switch_id=rule.switch_id,
                command=command,
                flow_group_id=rule.flow_group_id,
                path_id=rule.path_id,
                hop_stage=rule.hop_stage,
                out_port=rule.out_port,
                phase=phase,
                accepted=False,
                error_reason=err,
            )

        self._record_log(
            op_type="FLOW_MOD",
            xid=xid,
            dpid=rule.dpid,
            switch_id=rule.switch_id,
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
            switch_id=rule.switch_id,
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
        dp = self._get_datapath(dpid)
        parser = dp.ofproto_parser
        switch_id = self._switch_id_for_dpid(dpid)

        msg = parser.OFPBarrierRequest(dp)
        xid = self._next_xid(dp, msg)
        waiter = _PendingBarrier(
            dpid=dpid,
            xid=xid,
            phase=phase,
            event=self._event_factory(),
        )
        with self._lock:
            self._pending_barriers[(dpid, xid)] = waiter

        try:
            sent = dp.send_msg(msg)
            if sent is False:
                with self._lock:
                    self._pending_barriers.pop((dpid, xid), None)
                err = f"BARRIER_SEND_FAILED_DPID_{dpid}"
                self._record_log(
                    op_type="BARRIER",
                    xid=xid,
                    dpid=dpid,
                    switch_id=switch_id,
                    phase=phase,
                    flow_group_id=None,
                    path_id=None,
                    hop_stage=None,
                    out_port=None,
                    success=False,
                    error_reason=err,
                )
                return BarrierReceipt(
                    xid=xid,
                    dpid=dpid,
                    switch_id=switch_id,
                    phase=phase,
                    confirmed=False,
                    error_reason=err,
                )
            signaled = waiter.event.wait(timeout=self._config.barrier_timeout_s)
        finally:
            with self._lock:
                self._pending_barriers.pop((dpid, xid), None)
                recent_err = self._pop_recent_flowmod_error_locked(dpid)

        if not signaled:
            err = f"BARRIER_TIMEOUT_DPID_{dpid}_XID_{xid}"
            self._record_log(
                op_type="BARRIER",
                xid=xid,
                dpid=dpid,
                switch_id=switch_id,
                phase=phase,
                flow_group_id=None,
                path_id=None,
                hop_stage=None,
                out_port=None,
                success=False,
                timed_out=True,
                error_reason=err,
            )
            return BarrierReceipt(
                xid=xid,
                dpid=dpid,
                switch_id=switch_id,
                phase=phase,
                confirmed=False,
                timed_out=True,
                error_reason=err,
            )

        if waiter.error_reason is not None or recent_err is not None or not waiter.confirmed:
            err = waiter.error_reason or recent_err or "BARRIER_NOT_CONFIRMED"
            self._record_log(
                op_type="BARRIER",
                xid=xid,
                dpid=dpid,
                switch_id=switch_id,
                phase=phase,
                flow_group_id=None,
                path_id=None,
                hop_stage=None,
                out_port=None,
                success=False,
                error_reason=err,
            )
            return BarrierReceipt(
                xid=xid,
                dpid=dpid,
                switch_id=switch_id,
                phase=phase,
                confirmed=False,
                error_reason=err,
            )

        self._record_log(
            op_type="BARRIER",
            xid=xid,
            dpid=dpid,
            switch_id=switch_id,
            phase=phase,
            flow_group_id=None,
            path_id=None,
            hop_stage=None,
            out_port=None,
            success=True,
        )
        return BarrierReceipt(
            xid=xid,
            dpid=dpid,
            switch_id=switch_id,
            phase=phase,
            confirmed=True,
        )

    def _pop_recent_flowmod_error_locked(self, dpid: int) -> Optional[str]:
        xids = self._flowmod_xids_by_dpid.pop(dpid, [])
        for xid in xids:
            err = self._recent_flowmod_errors.pop((dpid, xid), None)
            if err is not None:
                return err
        return None

    def read_flow_stats(
        self,
        dpid: int,
        *,
        flow_group_id: Optional[str] = None,
        cookie_int: Optional[int] = None,
    ) -> tuple[SwitchFlowStatsEntry, ...]:
        dp = self._get_datapath(dpid)
        ofproto = dp.ofproto
        parser = dp.ofproto_parser
        switch_id = self._switch_id_for_dpid(dpid)

        eff_cookie = cookie_int
        if eff_cookie is None and flow_group_id is not None:
            eff_cookie = int(flow_group_id, 16)

        cookie_val = eff_cookie if eff_cookie is not None else 0
        cookie_mask = 0xFFFFFFFFFFFFFFFF if eff_cookie is not None else 0

        msg = parser.OFPFlowStatsRequest(
            datapath=dp,
            flags=0,
            table_id=getattr(ofproto, "OFPTT_ALL", 0xFF),
            out_port=getattr(ofproto, "OFPP_ANY", 0xFFFFFFFF),
            out_group=getattr(ofproto, "OFPG_ANY", 0xFFFFFFFF),
            cookie=cookie_val,
            cookie_mask=cookie_mask,
            match=parser.OFPMatch(),
        )
        xid = self._next_xid(dp, msg)
        waiter = _PendingFlowStats(dpid=dpid, xid=xid, event=self._event_factory())
        with self._lock:
            self._pending_flow_stats[(dpid, xid)] = waiter

        try:
            sent = dp.send_msg(msg)
            if sent is False:
                raise SwitchDriverError(f"FlowStatsRequest send failed on DPID {dpid}.")
            signaled = waiter.event.wait(timeout=self._config.flow_stats_timeout_s)
        finally:
            with self._lock:
                self._pending_flow_stats.pop((dpid, xid), None)

        if not signaled:
            self._record_log(
                op_type="FLOW_STATS",
                xid=xid,
                dpid=dpid,
                switch_id=switch_id,
                phase=SwitchPhaseTag.VERIFICATION,
                flow_group_id=flow_group_id,
                path_id=None,
                hop_stage=None,
                out_port=None,
                success=False,
                timed_out=True,
                error_reason=f"FLOW_STATS_TIMEOUT_DPID_{dpid}",
            )
            raise SwitchTimeoutError(
                f"FlowStats timeout on switch '{switch_id}' (DPID={dpid}, xid={xid})."
            )

        if waiter.error_reason is not None:
            self._record_log(
                op_type="FLOW_STATS",
                xid=xid,
                dpid=dpid,
                switch_id=switch_id,
                phase=SwitchPhaseTag.VERIFICATION,
                flow_group_id=flow_group_id,
                path_id=None,
                hop_stage=None,
                out_port=None,
                success=False,
                error_reason=waiter.error_reason,
            )
            raise SwitchDriverError(waiter.error_reason)

        entries: list[SwitchFlowStatsEntry] = []
        for stat in waiter.raw_stats:
            stat_cookie = int(getattr(stat, "cookie", 0))
            fg_id = flow_group_id or f"0x{stat_cookie:X}"
            if flow_group_id is not None and stat_cookie != int(flow_group_id, 16):
                continue
            out_port = _extract_ofp_out_port(stat, ofproto)
            if out_port is None:
                continue
            match_spec = _ofp_match_to_flow_match_spec(
                getattr(stat, "match_spec", getattr(stat, "match", None)),
                flow_group_id=fg_id,
                cookie_int=stat_cookie,
            )
            if match_spec is None:
                continue
            entries.append(
                SwitchFlowStatsEntry(
                    dpid=dpid,
                    switch_id=switch_id,
                    table_id=int(getattr(stat, "table_id", 0)),
                    priority=int(getattr(stat, "priority", 0)),
                    cookie_int=stat_cookie,
                    cookie_hex=f"0x{stat_cookie:X}",
                    flow_group_id=fg_id,
                    path_id="",  # Resolved by RuleVerifier from exact match/out_port
                    match=match_spec,
                    out_port=out_port,
                    packet_count=int(getattr(stat, "packet_count", 0)),
                    byte_count=int(getattr(stat, "byte_count", 0)),
                )
            )

        self._record_log(
            op_type="FLOW_STATS",
            xid=xid,
            dpid=dpid,
            switch_id=switch_id,
            phase=SwitchPhaseTag.VERIFICATION,
            flow_group_id=flow_group_id,
            path_id=None,
            hop_stage=None,
            out_port=None,
            success=True,
        )
        return tuple(entries)

    def snapshot_flow_counters(
        self, compiled_plan: CompiledPathPlan
    ) -> dict[tuple[int, str, str], int]:
        """Record current packet_count per hop as baseline before measuring counter delta."""
        counts: dict[tuple[int, str, str], int] = {}
        for hop in compiled_plan.ordered_hop_rules:
            stats = self.read_flow_stats(
                hop.dpid,
                flow_group_id=hop.flow_group_id,
                cookie_int=hop.cookie_int,
            )
            matched = [
                s
                for s in stats
                if s.out_port == hop.out_port
                and (
                    s.match is None
                    or (s.table_id, s.priority, s.match.match_tuple_key) == hop.table_entry_key
                )
            ]
            pkt_sum = sum(s.packet_count for s in matched)
            key = (hop.dpid, hop.flow_group_id, compiled_plan.path_id)
            counts[key] = pkt_sum
            with self._lock:
                self._baseline_packet_counts[key] = pkt_sum
        return counts

    def verify_forwarding_counter_or_probe(
        self,
        compiled_plan: CompiledPathPlan,
        *,
        probe_id: Optional[str] = None,
    ) -> ForwardingCheckResult:
        """
        Verify real data-plane forwarding on `compiled_plan`:
        1. If `active_probe_callback` is configured, run real active probe and require confirmation.
        2. Otherwise, read OpenFlow `FlowStats` counters across all 3 hops and verify
           a real positive `packet_count` delta (`>= min_counter_packet_delta`) compared
           to the baseline (never returning `counter_or_probe_ok=True` when counters are 0).
        """
        if self._active_probe_callback is not None:
            res = self._active_probe_callback(compiled_plan, probe_id)
            self._record_log(
                op_type="FORWARDING_CHECK",
                xid=0,
                dpid=compiled_plan.source_edge_rule.dpid,
                switch_id=compiled_plan.source_edge_rule.switch_id,
                phase=SwitchPhaseTag.VERIFICATION,
                flow_group_id=compiled_plan.flow_group_id,
                path_id=compiled_plan.path_id,
                hop_stage=None,
                out_port=compiled_plan.source_edge_rule.out_port,
                success=res.counter_or_probe_ok,
                timed_out=res.timed_out,
                error_reason=res.error_reason,
            )
            return res

        hop_deltas: list[int] = []
        for hop in compiled_plan.ordered_hop_rules:
            stats = self.read_flow_stats(
                hop.dpid,
                flow_group_id=hop.flow_group_id,
                cookie_int=hop.cookie_int,
            )
            matched = [
                s
                for s in stats
                if s.out_port == hop.out_port
                and (
                    s.match is None
                    or (s.table_id, s.priority, s.match.match_tuple_key) == hop.table_entry_key
                )
            ]
            if not matched:
                err = f"MISSING_HOP_RULE_ON_{hop.switch_id}_OUTPORT_{hop.out_port}"
                return ForwardingCheckResult(
                    flow_group_id=compiled_plan.flow_group_id,
                    checked_path_id=compiled_plan.path_id,
                    actual_forwarded_path_id=None,
                    counter_or_probe_ok=False,
                    probe_id=probe_id,
                    packet_delta=0,
                    error_reason=err,
                )
            pkt_now = sum(s.packet_count for s in matched)
            key = (hop.dpid, hop.flow_group_id, compiled_plan.path_id)
            with self._lock:
                prev = self._baseline_packet_counts.get(key, 0)
                self._baseline_packet_counts[key] = pkt_now
            hop_deltas.append(max(0, pkt_now - prev))

        min_delta = min(hop_deltas) if hop_deltas else 0
        ok = min_delta >= self._config.min_counter_packet_delta
        err_reason = None if ok else "NO_REAL_COUNTER_DELTA_OR_ACTIVE_PROBE"
        self._record_log(
            op_type="FORWARDING_CHECK",
            xid=0,
            dpid=compiled_plan.source_edge_rule.dpid,
            switch_id=compiled_plan.source_edge_rule.switch_id,
            phase=SwitchPhaseTag.VERIFICATION,
            flow_group_id=compiled_plan.flow_group_id,
            path_id=compiled_plan.path_id,
            hop_stage=None,
            out_port=compiled_plan.source_edge_rule.out_port,
            success=ok,
            error_reason=err_reason,
        )
        return ForwardingCheckResult(
            flow_group_id=compiled_plan.flow_group_id,
            checked_path_id=compiled_plan.path_id,
            actual_forwarded_path_id=compiled_plan.path_id if ok else None,
            counter_or_probe_ok=ok,
            probe_id=probe_id,
            packet_delta=min_delta,
            error_reason=err_reason,
        )

    def get_operation_log(self) -> tuple[SwitchOperationLogEntry, ...]:
        with self._lock:
            return tuple(self._op_log)

    # -------------------------------------------------------------------------
    # Ryu / OS-Ken Event Callbacks (Non-Blocking)
    # -------------------------------------------------------------------------

    def handle_barrier_reply(self, ev: Any) -> None:
        msg = getattr(ev, "msg", ev)
        dp = getattr(msg, "datapath", None)
        dpid = int(getattr(dp, "id", getattr(ev, "dpid", 0)))
        xid = int(getattr(msg, "xid", 0))
        with self._lock:
            waiter = self._pending_barriers.get((dpid, xid))
            if waiter is not None:
                waiter.confirmed = True
                waiter.event.set()

    def handle_flow_stats_reply(self, ev: Any) -> None:
        msg = getattr(ev, "msg", ev)
        dp = getattr(msg, "datapath", None)
        dpid = int(getattr(dp, "id", getattr(ev, "dpid", 0)))
        xid = int(getattr(msg, "xid", 0))
        ofproto = getattr(dp, "ofproto", None) if dp is not None else None
        more_flag = getattr(ofproto, "OFPMPF_REPLY_MORE", 0x0001)
        flags = int(getattr(msg, "flags", 0))
        body = getattr(msg, "body", None) or ()

        with self._lock:
            waiter = self._pending_flow_stats.get((dpid, xid))
            if waiter is None:
                return
            waiter.raw_stats.extend(body)
            if (flags & more_flag) == 0:
                waiter.event.set()

    def handle_error_msg(self, ev: Any) -> None:
        msg = getattr(ev, "msg", ev)
        dp = getattr(msg, "datapath", None)
        dpid = int(getattr(dp, "id", getattr(ev, "dpid", 0)))
        xid = int(getattr(msg, "xid", 0))
        err_type = getattr(msg, "type", "UNKNOWN")
        err_code = getattr(msg, "code", "UNKNOWN")
        err_str = f"OFP_ERROR(dpid={dpid},xid={xid},type={err_type},code={err_code})"

        with self._lock:
            b_waiter = self._pending_barriers.get((dpid, xid))
            if b_waiter is not None:
                b_waiter.confirmed = False
                b_waiter.error_reason = err_str
                b_waiter.event.set()
                return

            fs_waiter = self._pending_flow_stats.get((dpid, xid))
            if fs_waiter is not None:
                fs_waiter.error_reason = err_str
                fs_waiter.event.set()
                return

            self._recent_flowmod_errors[(dpid, xid)] = err_str
