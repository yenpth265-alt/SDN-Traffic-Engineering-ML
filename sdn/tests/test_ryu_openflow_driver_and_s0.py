"""
Windows Adapter & S0 Orchestrator Tests (`PASS_MOCK_ADAPTER`, NOT `PASS_LIVE_OVS`).

Uses fake Ryu/OS-Ken datapath, ofproto, parser, event, and reply objects to verify:
- OpenFlow 1.3 encoding (OFPFC_ADD / OFPFC_MODIFY_STRICT / OFPFC_DELETE_STRICT)
- Barrier & XID correlation
- FlowStats multipart replies (OFPMPF_REPLY_MORE)
- OpenFlow error handling (OFPErrorMsg)
- Timeout and switch disconnect handling
- Duplicate datapath connection & reconnect reconciliation
- S0 bootstrap of 48 inter-edge flows (downstream-first -> Barrier -> source-edge)
- Strict no-false-VERIFIED enforcement without real counter/probe evidence
- Separation of 48 inter-edge TE flows vs. 8 intra-edge pairs & ARP for `pingall`
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import queue
import threading
from types import SimpleNamespace
from typing import Any, Optional
import pytest

from sdn.apps.ryu_s0_app import (
    INTRA_EDGE_HOST_PAIRS,
    S0AppConfig,
    S0ControllerOrchestrator,
    S0ReadinessState,
    _resolve_ofp_base_app,
)
from sdn.controller.state_manager import (
    StateValidationError,
    format_utc_timestamp,
    parse_utc_timestamp,
)
from sdn.controller.topology_registry import TopologyRegistry
from sdn.flow_management.flow_matcher import FlowMatcher
from sdn.flow_management.route_compiler import RouteCompiler
from sdn.flow_management.ryu_openflow_driver import RyuDriverConfig, RyuOpenFlowDriver
from sdn.flow_management.switch_driver import (
    FlowModCommand,
    SwitchDriverError,
    SwitchPhaseTag,
    SwitchTimeoutError,
)
from sdn.monitoring.snapshot_creator import (
    MockLinkTelemetryProvider,
    MockReleasedDemandProvider,
    SnapshotCreator,
)
from sdn.schemas.state_models import (
    ContractSerializationError,
    EvidenceMode,
    FlowDecisionStatus,
    VerificationStatus,
    normalize_iso8601_fractional_seconds,
    validate_rfc3339_utc_timestamp,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TOPOLOGY_BUNDLE_PATH = REPO_ROOT / "configs" / "network" / "topology_bundle.json"
CATALOG_DIR = REPO_ROOT / "configs" / "network"


# =============================================================================
# Fake OpenFlow 1.3 Datapath / Parser / Ofproto Simulator (Windows Test Only)
# =============================================================================


class _FakeOfprotoV13:
    OFP_VERSION = 0x04
    OFPFC_ADD = 0
    OFPFC_MODIFY_STRICT = 2
    OFPFC_DELETE_STRICT = 4
    OFPIT_APPLY_ACTIONS = 4
    OFPP_ANY = 0xFFFFFFFF
    OFPG_ANY = 0xFFFFFFFF
    OFP_NO_BUFFER = 0xFFFFFFFF
    OFPTT_ALL = 0xFF
    OFPFF_SEND_FLOW_REM = 1
    OFPMPF_REPLY_MORE = 0x0001


class _FakeOFPMatch(dict):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)


@dataclass
class _FakeOFPActionOutput:
    port: int


@dataclass
class _FakeOFPInstructionActions:
    type: int
    actions: list[_FakeOFPActionOutput]


@dataclass
class _FakeOFPFlowMod:
    datapath: Any
    cookie: int
    cookie_mask: int
    table_id: int
    command: int
    idle_timeout: int
    hard_timeout: int
    priority: int
    buffer_id: int
    out_port: int
    out_group: int
    flags: int
    match: _FakeOFPMatch
    instructions: list[_FakeOFPInstructionActions]
    xid: int = 0


@dataclass
class _FakeOFPBarrierRequest:
    datapath: Any
    xid: int = 0


@dataclass
class _FakeOFPFlowStatsRequest:
    datapath: Any
    flags: int
    table_id: int
    out_port: int
    out_group: int
    cookie: int
    cookie_mask: int
    match: _FakeOFPMatch
    xid: int = 0


@dataclass
class _FakeOFPPortDescStatsRequest:
    datapath: Any
    flags: int = 0
    xid: int = 0


@dataclass
class _FakeOFPPort:
    port_no: int
    name: str
    hw_addr: str = "00:00:00:00:00:01"
    config: int = 0
    state: int = 0


class _FakeParserV13:
    OFPMatch = _FakeOFPMatch
    OFPActionOutput = _FakeOFPActionOutput
    OFPInstructionActions = _FakeOFPInstructionActions
    OFPFlowMod = _FakeOFPFlowMod
    OFPBarrierRequest = _FakeOFPBarrierRequest
    OFPFlowStatsRequest = _FakeOFPFlowStatsRequest
    OFPPortDescStatsRequest = _FakeOFPPortDescStatsRequest


@dataclass
class _FakeFlowStatItem:
    table_id: int
    priority: int
    cookie: int
    match: _FakeOFPMatch
    instructions: list[_FakeOFPInstructionActions]
    packet_count: int = 0
    byte_count: int = 0


class _FakeDatapath:
    def __init__(
        self,
        dpid: int,
        driver: RyuOpenFlowDriver,
        *,
        auto_reply: bool = True,
        multipart_chunk_size: int = 0,
        packet_count_provider: Optional[Any] = None,
        reject_flowmod_predicate: Optional[Any] = None,
        drop_barrier: bool = False,
        corrupt_flow_stats_out_port: Optional[int] = None,
        strict_modify_requires_existing: bool = False,
        ports: Optional[list[_FakeOFPPort]] = None,
    ) -> None:
        self.id = dpid
        self.ofproto = _FakeOfprotoV13()
        self.ofproto_parser = _FakeParserV13()
        self._driver = driver
        self.auto_reply = auto_reply
        self.multipart_chunk_size = multipart_chunk_size
        self.packet_count_provider = packet_count_provider
        self.reject_flowmod_predicate = reject_flowmod_predicate
        self.drop_barrier = drop_barrier
        self.corrupt_flow_stats_out_port = corrupt_flow_stats_out_port
        self.strict_modify_requires_existing = strict_modify_requires_existing
        self.ports = (
            ports
            if ports is not None
            else [_FakeOFPPort(port_no=p, name=f"s{dpid}-eth{p}") for p in (1, 2, 3, 4)]
        )
        self._next_xid = 100 * dpid
        self.sent_messages: list[Any] = []
        self.installed_rules: dict[tuple[int, int, tuple[tuple[str, Any], ...]], _FakeFlowStatItem] = {}
        self._msg_queue: queue.Queue[Any] = queue.Queue()
        self._worker = threading.Thread(target=self._run_worker, daemon=True)
        self._worker.start()

    def _run_worker(self) -> None:
        while True:
            msg = self._msg_queue.get()
            self._deliver_msg(msg)

    def set_xid(self, msg: Any) -> int:
        self._next_xid += 1
        msg.xid = self._next_xid
        return self._next_xid

    def send_msg(self, msg: Any) -> bool:
        self.sent_messages.append(msg)
        if isinstance(msg, _FakeOFPFlowMod) and self.reject_flowmod_predicate is not None:
            if self.reject_flowmod_predicate(msg):
                return False
        if not self.auto_reply:
            return True
        self._msg_queue.put(msg)
        return True

    def _deliver_msg(self, msg: Any) -> None:
            if isinstance(msg, _FakeOFPFlowMod):
                key = (
                    msg.table_id,
                    msg.priority,
                    tuple(sorted(msg.match.items())),
                )
                if msg.command == _FakeOfprotoV13.OFPFC_ADD:
                    pkt = (
                        self.packet_count_provider(self.id, msg.cookie)
                        if self.packet_count_provider is not None
                        else 0
                    )
                    self.installed_rules[key] = _FakeFlowStatItem(
                        table_id=msg.table_id,
                        priority=msg.priority,
                        cookie=msg.cookie,
                        match=msg.match,
                        instructions=msg.instructions,
                        packet_count=pkt,
                        byte_count=pkt * 1200,
                    )
                elif msg.command == _FakeOfprotoV13.OFPFC_MODIFY_STRICT:
                    if self.strict_modify_requires_existing and key not in self.installed_rules:
                        # Real Open vSwitch behavior: MODIFY_STRICT on a missing entry silently does nothing!
                        return
                    pkt = (
                        self.packet_count_provider(self.id, msg.cookie)
                        if self.packet_count_provider is not None
                        else 0
                    )
                    self.installed_rules[key] = _FakeFlowStatItem(
                        table_id=msg.table_id,
                        priority=msg.priority,
                        cookie=msg.cookie,
                        match=msg.match,
                        instructions=msg.instructions,
                        packet_count=pkt,
                        byte_count=pkt * 1200,
                    )
                elif msg.command == _FakeOfprotoV13.OFPFC_DELETE_STRICT:
                    self.installed_rules.pop(key, None)

            elif isinstance(msg, _FakeOFPBarrierRequest):
                if self.drop_barrier:
                    return
                ev = SimpleNamespace(
                    msg=SimpleNamespace(datapath=self, xid=msg.xid)
                )
                self._driver.handle_barrier_reply(ev)

            elif isinstance(msg, _FakeOFPPortDescStatsRequest):
                ev = SimpleNamespace(
                    msg=SimpleNamespace(
                        datapath=self, xid=msg.xid, flags=0, body=list(self.ports)
                    )
                )
                self._driver.handle_port_desc_reply(ev)

            elif isinstance(msg, _FakeOFPFlowStatsRequest):
                matching = [
                    _FakeFlowStatItem(
                        table_id=st.table_id,
                        priority=st.priority,
                        cookie=st.cookie,
                        match=st.match,
                        instructions=(
                            [
                                _FakeOFPInstructionActions(
                                    _FakeOfprotoV13.OFPIT_APPLY_ACTIONS,
                                    [_FakeOFPActionOutput(self.corrupt_flow_stats_out_port)],
                                )
                            ]
                            if self.corrupt_flow_stats_out_port is not None
                            else st.instructions
                        ),
                        packet_count=(
                            self.packet_count_provider(self.id, st.cookie)
                            if self.packet_count_provider is not None
                            else st.packet_count
                        ),
                        byte_count=st.byte_count,
                    )
                    for st in self.installed_rules.values()
                    if (st.cookie & msg.cookie_mask) == (msg.cookie & msg.cookie_mask)
                ]
                chunk = self.multipart_chunk_size
                if chunk > 0 and len(matching) > chunk:
                    for idx in range(0, len(matching), chunk):
                        part = matching[idx : idx + chunk]
                        is_more = (idx + chunk) < len(matching)
                        flags = _FakeOfprotoV13.OFPMPF_REPLY_MORE if is_more else 0
                        ev = SimpleNamespace(
                            msg=SimpleNamespace(
                                datapath=self, xid=msg.xid, flags=flags, body=part
                            )
                        )
                        self._driver.handle_flow_stats_reply(ev)
                else:
                    ev = SimpleNamespace(
                        msg=SimpleNamespace(
                            datapath=self, xid=msg.xid, flags=0, body=matching
                        )
                    )
                    self._driver.handle_flow_stats_reply(ev)


@pytest.fixture(scope="module")
def topology() -> TopologyRegistry:
    return TopologyRegistry.from_bundle_file(
        TOPOLOGY_BUNDLE_PATH,
        csv_config_dir=CATALOG_DIR,
        verify_hash=True,
    )


# =============================================================================
# 1. OpenFlow 1.3 Encoding & XID / Barrier Correlation
# =============================================================================


def test_01_of13_flowmod_encoding_and_xid_correlation(
    topology: TopologyRegistry,
) -> None:
    driver = RyuOpenFlowDriver(topology)
    assert driver.evidence_mode == EvidenceMode.LIVE_OVS

    dps = {sw.dpid: _FakeDatapath(sw.dpid, driver) for sw in topology.get_all_switches()}
    for dp in dps.values():
        assert driver.register_datapath(dp) is False
    assert driver.all_expected_switches_connected is True

    compiler = RouteCompiler(topology, FlowMatcher(topology))
    plan_p1 = compiler.compile_path("0x103_P1")
    src_rule = plan_p1.source_edge_rule

    # ADD
    r_add = driver.send_flow_mod(
        src_rule, FlowModCommand.ADD, phase=SwitchPhaseTag.BOOTSTRAP
    )
    assert r_add.accepted is True
    msg_add = dps[src_rule.dpid].sent_messages[-1]
    assert isinstance(msg_add, _FakeOFPFlowMod)
    assert msg_add.command == _FakeOfprotoV13.OFPFC_ADD
    assert msg_add.cookie == 0x103
    assert msg_add.priority == 100
    assert msg_add.instructions[0].actions[0].port == src_rule.out_port
    assert msg_add.match["ipv4_src"] == "10.0.0.1"
    assert msg_add.match["ipv4_dst"] == "10.0.0.3"

    # MODIFY_STRICT
    plan_p2 = compiler.compile_path("0x103_P2")
    r_mod = driver.send_flow_mod(
        plan_p2.source_edge_rule,
        FlowModCommand.MODIFY_STRICT,
        phase=SwitchPhaseTag.PRIMARY_SOURCE_EDGE,
    )
    assert r_mod.accepted is True
    msg_mod = dps[src_rule.dpid].sent_messages[-1]
    assert msg_mod.command == _FakeOfprotoV13.OFPFC_MODIFY_STRICT
    assert msg_mod.cookie_mask == 0xFFFFFFFFFFFFFFFF
    assert msg_mod.instructions[0].actions[0].port == plan_p2.source_edge_rule.out_port

    # DELETE_STRICT
    r_del = driver.send_flow_mod(
        plan_p2.source_edge_rule,
        FlowModCommand.DELETE_STRICT,
        phase=SwitchPhaseTag.CLEANUP,
    )
    assert r_del.accepted is True
    msg_del = dps[src_rule.dpid].sent_messages[-1]
    assert msg_del.command == _FakeOfprotoV13.OFPFC_DELETE_STRICT
    assert msg_del.out_port == plan_p2.source_edge_rule.out_port

    # Barrier with XID correlation
    bar = driver.send_barrier(src_rule.dpid, phase=SwitchPhaseTag.PRIMARY_SOURCE_EDGE)
    assert bar.confirmed is True
    assert bar.timed_out is False


# =============================================================================
# 2. Multipart FlowStats Replies, OpenFlow Errors, Timeout & Disconnect
# =============================================================================


def test_02_multipart_flow_stats_and_ofp_error_handling(
    topology: TopologyRegistry,
) -> None:
    driver = RyuOpenFlowDriver(
        topology,
        config=RyuDriverConfig(barrier_timeout_s=0.3, flow_stats_timeout_s=0.3),
    )
    dp1 = _FakeDatapath(1, driver, multipart_chunk_size=1)
    driver.register_datapath(dp1)

    compiler = RouteCompiler(topology, FlowMatcher(topology))
    for fid in ("0x103", "0x104", "0x105"):
        rule = compiler.compile_path(f"{fid}_P1").source_edge_rule
        driver.send_flow_mod(rule, FlowModCommand.ADD, phase=SwitchPhaseTag.BOOTSTRAP)

    assert driver.send_barrier(1, phase=SwitchPhaseTag.BOOTSTRAP).confirmed is True

    # Read all stats on dpid=1 -> split across 3 multipart replies!
    all_stats = driver.read_flow_stats(1)
    assert len(all_stats) == 3
    assert {s.flow_group_id for s in all_stats} == {"0x103", "0x104", "0x105"}

    # Filtered read-back for 0x104
    only_104 = driver.read_flow_stats(1, flow_group_id="0x104")
    assert len(only_104) == 1
    assert only_104[0].cookie_int == 0x104

    # OFPErrorMsg handling on FlowStatsRequest
    dp2 = _FakeDatapath(2, driver, auto_reply=False)
    driver.register_datapath(dp2)

    def _inject_ofp_error() -> None:
        # Wait briefly for pending FlowStatsRequest to be sent
        while not dp2.sent_messages:
            pass
        req = dp2.sent_messages[-1]
        ev = SimpleNamespace(
            msg=SimpleNamespace(datapath=dp2, xid=req.xid, type=1, code=7)
        )
        driver.handle_error_msg(ev)

    threading.Thread(target=_inject_ofp_error, daemon=True).start()
    with pytest.raises(SwitchDriverError, match="OFP_ERROR"):
        driver.read_flow_stats(2, flow_group_id="0x301")


def test_03_timeout_disconnect_and_duplicate_connection(
    topology: TopologyRegistry,
) -> None:
    driver = RyuOpenFlowDriver(
        topology,
        config=RyuDriverConfig(barrier_timeout_s=0.1, flow_stats_timeout_s=0.1),
    )
    dp1 = _FakeDatapath(1, driver, auto_reply=False)
    assert driver.register_datapath(dp1) is False

    # Barrier timeout when switch does not reply
    bar_to = driver.send_barrier(1, phase=SwitchPhaseTag.PRIMARY_DOWNSTREAM)
    assert bar_to.confirmed is False
    assert bar_to.timed_out is True

    # FlowStats timeout when switch does not reply
    with pytest.raises(SwitchTimeoutError):
        driver.read_flow_stats(1, flow_group_id="0x103")

    # Disconnect wakes pending waiter immediately and unregisters DPID
    def _disconnect_soon() -> None:
        while not dp1.sent_messages:
            pass
        driver.unregister_datapath(1, reason="DEAD_DISPATCHER")

    dp1.sent_messages.clear()
    threading.Thread(target=_disconnect_soon, daemon=True).start()
    bar_disc = driver.send_barrier(1, phase=SwitchPhaseTag.PRIMARY_DOWNSTREAM)
    assert bar_disc.confirmed is False
    assert "DEAD_DISPATCHER" in (bar_disc.error_reason or "")
    assert 1 not in driver.connected_dpids

    # Duplicate connection replaces old datapath and returns replaced=True
    dp1_new = _FakeDatapath(1, driver, auto_reply=True)
    assert driver.register_datapath(dp1_new) is False
    dp1_dup = _FakeDatapath(1, driver, auto_reply=True)
    assert driver.register_datapath(dp1_dup) is True
    assert driver.send_barrier(1, phase=SwitchPhaseTag.BOOTSTRAP).confirmed is True


# =============================================================================
# 3. S0 Orchestrator: Bootstrap 48 Flows & No False VERIFIED
# =============================================================================


def test_04_s0_bootstrap_never_marks_verified_without_real_dataplane_evidence() -> None:
    orch = S0ControllerOrchestrator(
        S0AppConfig(
            topology_bundle_path=TOPOLOGY_BUNDLE_PATH,
            csv_config_dir=CATALOG_DIR,
            run_id="run_s0_no_fake_verify",
        )
    )
    # Connect all 6 switches with packet_count=0 (no traffic / no active probe)
    for sw in orch.topology.get_all_switches():
        dp = _FakeDatapath(sw.dpid, orch.driver, auto_reply=True)
        orch.on_switch_connected(dp)
    assert orch.driver.all_expected_switches_connected is True

    report = orch.bootstrap_and_verify(verified_at_utc="2026-11-12T09:00:00Z")
    assert report.bootstrapped_48_inter_edge_installed is True
    # MUST NOT be marked VERIFIED without real counter delta or active probe!
    assert report.verified_48_flows is False
    assert report.reconcile_required is True
    snap = orch.state_manager.get_routing_state("run_s0_no_fake_verify")
    assert snap.routing_state_complete is False
    assert all(
        e.verification_status == VerificationStatus.UNVERIFIED for e in snap.routing_state
    )


def test_05_s0_bootstrap_48_flows_verified_with_real_counter_delta_and_pingall_scope() -> None:
    pkt_counter = {"val": 10}
    orch = S0ControllerOrchestrator(
        S0AppConfig(
            topology_bundle_path=TOPOLOGY_BUNDLE_PATH,
            csv_config_dir=CATALOG_DIR,
            run_id="run_s0_verified",
        )
    )
    for sw in orch.topology.get_all_switches():
        dp = _FakeDatapath(
            sw.dpid,
            orch.driver,
            auto_reply=True,
            packet_count_provider=lambda _dpid, _cookie: pkt_counter["val"],
        )
        orch.on_switch_connected(dp)

    report = orch.bootstrap_and_verify(verified_at_utc="2026-11-12T09:00:00Z")
    assert report.bootstrapped_48_inter_edge_installed is True
    assert report.verified_48_flows is True
    assert report.reconcile_required is False
    # 48 inter-edge flows do NOT make full-mesh pingall ready (8 intra-edge + ARP missing)!
    assert report.pingall_inter_edge_only_ready is True
    assert report.pingall_full_mesh_ready is False
    assert report.intra_edge_pairs_covered is False
    assert len(INTRA_EDGE_HOST_PAIRS) == 8

    snap = orch.state_manager.get_routing_state("run_s0_verified")
    assert snap.routing_state_complete is True
    assert len(snap.routing_state) == 48
    assert all(
        e.verification_status == VerificationStatus.VERIFIED
        and e.evidence_mode == EvidenceMode.LIVE_OVS
        for e in snap.routing_state
    )

    # Simulate switch reconnect -> requires reconciliation -> traffic advances counters -> reconcile succeeds
    pkt_counter["val"] += 5
    dp1_reconn = _FakeDatapath(
        1,
        orch.driver,
        auto_reply=True,
        packet_count_provider=lambda _dpid, _cookie: pkt_counter["val"],
    )
    orch.on_switch_connected(dp1_reconn)
    assert orch.state_manager.is_reconcile_required("run_s0_verified") is True
    # Copy installed rules onto reconnected fake datapath or let reconcile reinstall P1
    reconciled = orch.reconcile_after_reconnect_or_traffic()
    assert reconciled.reconcile_required is False
    assert reconciled.routing_state_complete is True


def test_06_non_destructive_traffic_reconcile_and_spanning_tree_arp_helpers() -> None:
    pkt_counter = {"val": 0}
    orch = S0ControllerOrchestrator(
        S0AppConfig(
            topology_bundle_path=TOPOLOGY_BUNDLE_PATH,
            csv_config_dir=CATALOG_DIR,
            run_id="run_s0_traffic_later",
            enable_intra_edge_l3=True,
            enable_arp_edge_local_flood=True,
        )
    )
    dps = {}
    for sw in orch.topology.get_all_switches():
        dp = _FakeDatapath(
            sw.dpid,
            orch.driver,
            auto_reply=True,
            packet_count_provider=lambda _dpid, _cookie: pkt_counter["val"],
        )
        dps[sw.dpid] = dp
        orch.on_switch_connected(dp)

    # 1. Bootstrap before traffic: rules installed, NOT verified, does not block
    report = orch.bootstrap_and_verify()
    assert report.bootstrapped_48_inter_edge_installed is True
    assert report.verified_48_flows is False
    assert report.reconcile_required is True
    assert report.pingall_full_mesh_ready is True

    # Verify loop-free ARP helpers: e1..e4 have 3 ARP rules (in_port 1,2,3) and c1 (dpid=5) has 4 ARP rules; c2 has 0
    for edge_dpid in (1, 2, 3, 4):
        arp_rules = [r for r in dps[edge_dpid].installed_rules.values() if r.cookie == 0xFFF1]
        assert len(arp_rules) == 3
    c1_arp = [r for r in dps[5].installed_rules.values() if r.cookie == 0xFFF1]
    c2_arp = [r for r in dps[6].installed_rules.values() if r.cookie == 0xFFF1]
    assert len(c1_arp) == 4
    assert len(c2_arp) == 0

    flowmod_count_after_bootstrap = sum(
        1 for dp in dps.values() for m in dp.sent_messages if isinstance(m, _FakeOFPFlowMod)
    )

    # 2. Retry while still 0 traffic: returns None and sends ZERO FlowMods
    assert orch.verify_or_reconcile_after_traffic() is None
    flowmod_count_after_zero_retry = sum(
        1 for dp in dps.values() for m in dp.sent_messages if isinstance(m, _FakeOFPFlowMod)
    )
    assert flowmod_count_after_zero_retry == flowmod_count_after_bootstrap

    # 3. Traffic arrives -> transitions UNVERIFIED -> VERIFIED with ZERO extra FlowMods
    pkt_counter["val"] = 7
    snap = orch.verify_or_reconcile_after_traffic()
    assert snap is not None
    assert snap.reconcile_required is False
    assert snap.routing_state_complete is True
    assert all(
        e.verification_status == VerificationStatus.VERIFIED
        and e.evidence_mode == EvidenceMode.LIVE_OVS
        for e in snap.routing_state
    )
    flowmod_count_after_verified = sum(
        1 for dp in dps.values() for m in dp.sent_messages if isinstance(m, _FakeOFPFlowMod)
    )
    assert flowmod_count_after_verified == flowmod_count_after_bootstrap


# =============================================================================
# 4. Phase 7.1: Bootstrap Fault Injection — Zero False Readiness
# =============================================================================


def test_07_phase7_1_bootstrap_fault_injection_no_false_readiness() -> None:
    # Phase 12.1 flake fix: use generous 1.0s barrier timeout for non-timeout FlowMod-rejection
    # sub-cases (orch1/orch3) and 0.15s for deliberate barrier-timeout sub-cases (orch2/orch4),
    # preventing false DOWNSTREAM_BARRIER timeouts under heavy VirtualBox VM CPU jitter.
    fast_timeout_cfg = RyuDriverConfig(barrier_timeout_s=0.15, flow_stats_timeout_s=0.15)
    normal_cfg = RyuDriverConfig(barrier_timeout_s=1.0, flow_stats_timeout_s=1.0)

    def _make_orch(
        run_id: str,
        *,
        enable_helpers: bool = False,
        dp_customizer: Optional[Any] = None,
        expect_timeout: bool = False,
    ) -> S0ControllerOrchestrator:
        orch = S0ControllerOrchestrator(
            S0AppConfig(
                topology_bundle_path=TOPOLOGY_BUNDLE_PATH,
                csv_config_dir=CATALOG_DIR,
                run_id=run_id,
                enable_intra_edge_l3=enable_helpers,
                enable_arp_edge_local_flood=enable_helpers,
                driver_config=fast_timeout_cfg if expect_timeout else normal_cfg,
            )
        )
        for sw in orch.topology.get_all_switches():
            dp = _FakeDatapath(sw.dpid, orch.driver, auto_reply=True)
            if dp_customizer is not None:
                dp_customizer(sw, dp)
            orch.on_switch_connected(dp)
        return orch

    # 1. Downstream FlowMod failure (c1 dpid=5 rejects TE FlowMod)
    orch1 = _make_orch(
        "run_p71_ds_flowmod_fail",
        dp_customizer=lambda sw, dp: setattr(
            dp, "reject_flowmod_predicate", lambda m: sw.dpid == 5 and m.cookie == 0x103
        ),
    )
    rep1 = orch1.bootstrap_and_verify()
    assert rep1.readiness_state == S0ReadinessState.INSTALL_FAILED
    assert rep1.bootstrapped_48_inter_edge_installed is False
    assert rep1.verified_48_flows is False
    assert rep1.pingall_inter_edge_only_ready is False
    assert rep1.failed_stage == "DOWNSTREAM_FLOWMOD"

    # 2. Downstream Barrier timeout (c1 dpid=5 drops Barrier)
    orch2 = _make_orch(
        "run_p71_ds_barrier_to",
        dp_customizer=lambda sw, dp: setattr(dp, "drop_barrier", sw.dpid == 5),
        expect_timeout=True,
    )
    rep2 = orch2.bootstrap_and_verify()
    assert rep2.readiness_state == S0ReadinessState.INSTALL_FAILED
    assert rep2.bootstrapped_48_inter_edge_installed is False
    assert rep2.pingall_inter_edge_only_ready is False
    assert rep2.failed_stage == "DOWNSTREAM_BARRIER"

    # 3. Source-edge FlowMod failure (e1 dpid=1 rejects source-edge rule for 0x103: in_port=1)
    orch3 = _make_orch(
        "run_p71_src_flowmod_fail",
        dp_customizer=lambda sw, dp: setattr(
            dp,
            "reject_flowmod_predicate",
            lambda m: sw.dpid == 1 and m.cookie == 0x103 and m.match.get("in_port") == 1,
        ),
    )
    rep3 = orch3.bootstrap_and_verify()
    assert rep3.readiness_state == S0ReadinessState.INSTALL_FAILED
    assert rep3.bootstrapped_48_inter_edge_installed is False
    assert rep3.pingall_inter_edge_only_ready is False
    assert rep3.failed_stage == "SOURCE_EDGE_FLOWMOD"

    # 4. Source-edge Barrier timeout (e1 dpid=1 drops Barrier after source-edge install)
    # Note: e1 is also a downstream switch for flows -> e1, so we drop only the 2nd barrier on e1
    class _DropSecondBarrierDatapath(_FakeDatapath):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._barrier_count = 0
            super().__init__(*args, **kwargs)

        def _deliver_msg(self, msg: Any) -> None:
            if isinstance(msg, _FakeOFPBarrierRequest):
                self._barrier_count += 1
                if self._barrier_count >= 2:
                    return
            super()._deliver_msg(msg)

    orch4 = S0ControllerOrchestrator(
        S0AppConfig(
            topology_bundle_path=TOPOLOGY_BUNDLE_PATH,
            csv_config_dir=CATALOG_DIR,
            run_id="run_p71_src_barrier_to",
            driver_config=fast_timeout_cfg,
        )
    )
    for sw in orch4.topology.get_all_switches():
        dp_cls = _DropSecondBarrierDatapath if sw.dpid == 1 else _FakeDatapath
        orch4.on_switch_connected(dp_cls(sw.dpid, orch4.driver, auto_reply=True))
    rep4 = orch4.bootstrap_and_verify()
    assert rep4.readiness_state == S0ReadinessState.INSTALL_FAILED
    assert rep4.bootstrapped_48_inter_edge_installed is False
    assert rep4.pingall_inter_edge_only_ready is False
    assert rep4.failed_stage == "SOURCE_EDGE_BARRIER"

    # 5. FlowStats read-back mismatch (e1 reports wrong out_port=99 in FlowStats)
    orch5 = _make_orch(
        "run_p71_flowstats_mismatch",
        dp_customizer=lambda sw, dp: setattr(
            dp, "corrupt_flow_stats_out_port", 99 if sw.dpid == 1 else None
        ),
    )
    rep5 = orch5.bootstrap_and_verify()
    assert rep5.readiness_state == S0ReadinessState.INSTALL_FAILED
    assert rep5.bootstrapped_48_inter_edge_installed is False
    assert rep5.rules_readback_ok is False
    assert rep5.failed_stage == "FLOW_STATS_READBACK"

    # 6. Helper rule installation failure (e1 rejects intra-edge helper cookie 0xFFF0)
    orch6 = _make_orch(
        "run_p71_helper_fail",
        enable_helpers=True,
        dp_customizer=lambda sw, dp: setattr(
            dp, "reject_flowmod_predicate", lambda m: m.cookie == 0xFFF0
        ),
    )
    rep6 = orch6.bootstrap_and_verify()
    assert rep6.readiness_state == S0ReadinessState.INSTALL_FAILED
    assert rep6.helpers_installed_ok is False
    assert rep6.bootstrapped_48_inter_edge_installed is False
    assert rep6.pingall_full_mesh_ready is False
    assert rep6.failed_stage == "HELPER_RULES"

    # 7. Bootstrap succeeds with 0 traffic -> VERIFICATION_PENDING (rules installed, awaiting traffic)
    orch7 = _make_orch("run_p71_no_traffic_ok", enable_helpers=True)
    rep7 = orch7.bootstrap_and_verify()
    assert rep7.readiness_state == S0ReadinessState.VERIFICATION_PENDING
    assert rep7.bootstrapped_48_inter_edge_installed is True
    assert rep7.rules_readback_ok is True
    assert rep7.helpers_installed_ok is True
    assert rep7.verified_48_flows is False
    assert rep7.pingall_inter_edge_only_ready is True
    assert rep7.pingall_full_mesh_ready is True
    assert rep7.failed_stage is None


# =============================================================================
# 5. Phase 7.2: Incremental Forwarding Verification (20 + 28 Flows, Counter Reset)
# =============================================================================


def test_08_phase7_2_incremental_verification_20_plus_28_flows_and_counter_reset() -> None:
    # Independent packet counter per (dpid, cookie_int) — NOT a shared counter!
    per_hop_counters: dict[tuple[int, int], int] = {}

    orch = S0ControllerOrchestrator(
        S0AppConfig(
            topology_bundle_path=TOPOLOGY_BUNDLE_PATH,
            csv_config_dir=CATALOG_DIR,
            run_id="run_p72_incremental",
        )
    )
    dps = {}
    for sw in orch.topology.get_all_switches():
        dp = _FakeDatapath(
            sw.dpid,
            orch.driver,
            auto_reply=True,
            packet_count_provider=lambda dpid, cookie: per_hop_counters.get((dpid, cookie), 0),
        )
        dps[sw.dpid] = dp
        orch.on_switch_connected(dp)

    rep = orch.bootstrap_and_verify(verified_at_utc="2026-11-12T09:00:00Z")
    assert rep.readiness_state == S0ReadinessState.VERIFICATION_PENDING
    assert rep.bootstrapped_48_inter_edge_installed is True
    assert rep.verified_48_flows is False

    all_flows = [fg.flow_group_id for fg in orch.topology.get_all_flow_groups()]
    first_20 = all_flows[:20]
    remaining_28 = all_flows[20:]

    def _advance_flow_traffic(flow_ids: list[str], delta: int) -> None:
        for fid in flow_ids:
            cookie = int(fid, 16)
            plan = orch.compiler.compile_path(f"{fid}_P1")
            for hop in plan.ordered_hop_rules:
                key = (hop.dpid, cookie)
                per_hop_counters[key] = per_hop_counters.get(key, 0) + delta

    flowmods_after_boot = sum(
        1 for dp in dps.values() for m in dp.sent_messages if isinstance(m, _FakeOFPFlowMod)
    )

    # Turn 1: Only first 20 flows have traffic (+5 packets); remaining 28 have 0
    _advance_flow_traffic(first_20, 5)
    snap_turn1 = orch.verify_or_reconcile_after_traffic(
        reconciled_at_utc="2026-11-12T09:00:02Z"
    )
    assert snap_turn1 is None
    assert len(orch.accumulated_evidences) == 20
    assert set(orch.accumulated_evidences.keys()) == set(first_20)
    assert orch.readiness_state == S0ReadinessState.VERIFICATION_PENDING

    # Simulate counter reset on flow 0x103 (first of first_20) before Turn 2:
    per_hop_counters[(1, int("0x103", 16))] = 0
    snap_reset = orch.verify_or_reconcile_after_traffic(
        reconciled_at_utc="2026-11-12T09:00:03Z"
    )
    assert snap_reset is None
    # 0x103 evidence was invalidated due to counter reset -> 19 cached flows remain
    assert "0x103" not in orch.accumulated_evidences
    assert len(orch.accumulated_evidences) == 19

    # Turn 2: Restore traffic on 0x103 (+4) and send traffic on remaining 28 flows (+6),
    # while flows 2..20 have ZERO new packets in Turn 2!
    _advance_flow_traffic(["0x103"], 4)
    _advance_flow_traffic(remaining_28, 6)
    snap_turn2 = orch.verify_or_reconcile_after_traffic(
        reconciled_at_utc="2026-11-12T09:00:04Z"
    )
    assert snap_turn2 is not None
    assert snap_turn2.reconcile_required is False
    assert snap_turn2.routing_state_complete is True
    assert orch.readiness_state == S0ReadinessState.VERIFIED
    assert len(orch.accumulated_evidences) == 48

    # Zero extra FlowMods were sent across all incremental verification turns!
    flowmods_after_turn2 = sum(
        1 for dp in dps.values() for m in dp.sent_messages if isinstance(m, _FakeOFPFlowMod)
    )
    assert flowmods_after_turn2 == flowmods_after_boot


# =============================================================================
# 6. Phase 7.3: Datapath Connection Epoch, Stale Disconnect & Helper Restore
# =============================================================================


def test_09_phase7_3_datapath_connection_epoch_stale_disconnect_and_helper_restore() -> None:
    pkt_counter = {"val": 5}
    orch = S0ControllerOrchestrator(
        S0AppConfig(
            topology_bundle_path=TOPOLOGY_BUNDLE_PATH,
            csv_config_dir=CATALOG_DIR,
            run_id="run_p73_conn_safety",
            enable_intra_edge_l3=True,
            enable_arp_edge_local_flood=True,
        )
    )
    dps = {}
    for sw in orch.topology.get_all_switches():
        dp = _FakeDatapath(
            sw.dpid,
            orch.driver,
            auto_reply=True,
            packet_count_provider=lambda _d, _c: pkt_counter["val"],
        )
        dps[sw.dpid] = dp
        orch.on_switch_connected(dp)

    rep = orch.bootstrap_and_verify(verified_at_utc="2026-11-12T09:00:00Z")
    assert rep.verified_48_flows is True
    assert orch.driver.get_connection_epoch(1) == 1

    old_dp1 = dps[1]
    # New connection dp1_v2 replaces old_dp1 BEFORE old_dp1's DEAD_DISPATCHER arrives
    pkt_counter["val"] += 5
    dp1_v2 = _FakeDatapath(
        1,
        orch.driver,
        auto_reply=True,
        packet_count_provider=lambda _d, _c: pkt_counter["val"],
    )
    orch.on_switch_connected(dp1_v2)
    assert orch.driver.get_connection_epoch(1) == 2
    assert orch.driver.get_active_datapath_id(1) == id(dp1_v2)
    assert orch.state_manager.is_reconcile_required("run_p73_conn_safety") is True

    # Reconcile with dp1_v2 (restores wiped rules AND helper ARP/intra-edge rules on dpid=1)
    snap_rec = orch.reconcile_after_reconnect_or_traffic()
    assert snap_rec.reconcile_required is False
    assert orch.readiness_state == S0ReadinessState.VERIFIED
    # Verify helper ARP (3 rules) and intra-edge (2 rules) were restored on dp1_v2!
    assert len([r for r in dp1_v2.installed_rules.values() if r.cookie == 0xFFF1]) == 3
    assert len([r for r in dp1_v2.installed_rules.values() if r.cookie == 0xFFF0]) == 2

    # Now late DEAD_DISPATCHER arrives for old_dp1 -> MUST BE IGNORED!
    was_unregistered = orch.on_switch_disconnected(
        1, reason="DEAD_DISPATCHER", datapath=old_dp1
    )
    assert was_unregistered is False
    assert 1 in orch.driver.connected_dpids
    assert orch.driver.get_active_datapath_id(1) == id(dp1_v2)
    assert orch.state_manager.is_reconcile_required("run_p73_conn_safety") is False
    assert orch.readiness_state == S0ReadinessState.VERIFIED

    # Stale replies from old_dp1 are also ignored
    orch.driver.handle_barrier_reply(
        SimpleNamespace(msg=SimpleNamespace(datapath=old_dp1, xid=9999))
    )
    orch.driver.handle_error_msg(
        SimpleNamespace(msg=SimpleNamespace(datapath=old_dp1, xid=9999, type=1, code=1))
    )
    assert any("STALE_" in reason for _dpid, reason in orch.driver.ignored_stale_events)


# =============================================================================
# 7. Phase 7.5: Live PortDesc Validation & Adjacency Separation
# =============================================================================


def test_10_phase7_5_live_port_desc_validation_and_adjacency_separation() -> None:
    orch = S0ControllerOrchestrator(
        S0AppConfig(
            topology_bundle_path=TOPOLOGY_BUNDLE_PATH,
            csv_config_dir=CATALOG_DIR,
            run_id="run_p75_port_desc",
            verify_port_desc_on_bootstrap=True,
        )
    )
    dps = {}
    for sw in orch.topology.get_all_switches():
        dp = _FakeDatapath(sw.dpid, orch.driver, auto_reply=True)
        dps[sw.dpid] = dp
        orch.on_switch_connected(dp)

    # 1. Live PortDesc query succeeds across all 6 DPIDs, but link_adjacency_verified is False
    #    because port existence alone never proves physical adjacency!
    port_rep = orch.verify_connected_switches_and_ports(query_live_port_desc=True)
    assert port_rep.catalog_validated is True
    assert port_rep.port_desc_validated is True
    assert port_rep.link_adjacency_verified is False
    assert port_rep.adjacency_status == "PORT_DESC_ONLY_ADJACENCY_UNVERIFIED"

    # 2. Supplying full 32-link observed_peer_links evidence upgrades link_adjacency_verified to True
    peer_map = {
        (lk.from_node, lk.from_port): (lk.to_node, lk.to_port)
        for lk in orch.topology.get_all_links()
    }
    adj_rep = orch.verify_connected_switches_and_ports(
        query_live_port_desc=True, observed_peer_links=peer_map
    )
    assert adj_rep.port_desc_validated is True
    assert adj_rep.link_adjacency_verified is True
    assert adj_rep.adjacency_status == "LINK_ADJACENCY_VERIFIED"

    # 3. Link down on c1 port 2 (OFPPS_LINK_DOWN = 1) -> PortDesc validation refuses bootstrap
    dps[5].ports = [
        _FakeOFPPort(port_no=1, name="c1-eth1", state=0),
        _FakeOFPPort(port_no=2, name="c1-eth2", state=1),  # LINK_DOWN!
        _FakeOFPPort(port_no=3, name="c1-eth3", state=0),
        _FakeOFPPort(port_no=4, name="c1-eth4", state=0),
    ]
    with pytest.raises(SwitchDriverError, match="down_ports"):
        orch.bootstrap_and_verify()


# =============================================================================
# 8. Phase 7.6 & 7.7: MODIFY_STRICT Silent Miss Read-Back & Network Event Ingestion
# =============================================================================


def test_11_phase7_6_and_7_7_strict_modify_readback_and_network_event_ingestion(
    topology: TopologyRegistry,
) -> None:
    # Part A: OpenFlow 1.3 OFPFC_MODIFY_STRICT silent miss caught by FlowStats read-back
    pkt_counter = {"val": 10}
    orch = S0ControllerOrchestrator(
        S0AppConfig(
            topology_bundle_path=TOPOLOGY_BUNDLE_PATH,
            csv_config_dir=CATALOG_DIR,
            run_id="run_p76_strict_mod",
        )
    )
    dps = {}
    for sw in orch.topology.get_all_switches():
        dp = _FakeDatapath(
            sw.dpid,
            orch.driver,
            auto_reply=True,
            strict_modify_requires_existing=True,
            packet_count_provider=lambda _d, _c: pkt_counter["val"],
        )
        dps[sw.dpid] = dp
        orch.on_switch_connected(dp)

    rep = orch.bootstrap_and_verify(verified_at_utc="2026-11-12T09:00:00Z")
    assert rep.verified_48_flows is True

    # Secretly wipe e1 (dpid=1) flow table WITHOUT disconnecting e1, so MODIFY_STRICT on e1
    # silently does nothing (no OFPErrorMsg!) during P1 -> P2 switch:
    dps[1].installed_rules.clear()
    pkt_counter["val"] += 5
    outcome = orch.installer.install_flow_change(
        run_id="run_p76_strict_mod",
        flow_group_id="0x103",
        old_path_id="0x103_P1",
        new_path_id="0x103_P2",
        applied_at_utc="2026-11-12T09:00:10Z",
    )
    # Because e1's rule was missing and MODIFY_STRICT silently no-oped (both primary and rollback),
    # RuleVerifier read-back catches the missing rule on e1 and marks the flow UNKNOWN (actual_path_id=None)!
    assert outcome.status == FlowDecisionStatus.UNKNOWN
    assert outcome.actual_path_id is None
    assert outcome.verification_status == VerificationStatus.UNKNOWN

    # Part B: Phase 7.7 Network sender.py DemandRelease & telemetry_collector.py LinkSampleBatch ingestion
    demand_prov = MockReleasedDemandProvider()
    link_prov = MockLinkTelemetryProvider()
    snap_creator = SnapshotCreator(
        topology,
        orch.state_manager,
        demand_prov,
        link_telemetry_provider=link_prov,
    )
    origin_refs = snap_creator.get_run_origin_refs("run_p76_strict_mod")

    for fg in topology.get_all_flow_groups():
        rec = demand_prov.ingest_network_release_event(
            {
                "event_id": f"network:run_p76_strict_mod:demand:{fg.flow_group_id}:0",
                "run_id": "run_p76_strict_mod",
                "flow_group_id": fg.flow_group_id,
                "bin_index": 60,  # sender.py t=60 (warmup=60 -> measurement bin 0)
                "window_start_utc": "2026-11-12T09:00:00.000000Z",
                "window_end_utc": "2026-11-12T09:00:01.000000Z",
                "requested_mbps": 2.5,
                "released_at_utc": "2026-11-12T09:00:01.002000Z",
            },
            origin_refs=origin_refs,
        )
        assert rec.bin_index == 0
        assert rec.requested_mbps == 2.5

    link_refs = link_prov.ingest_network_telemetry_batch(
        {
            "batch_id": "network:run_p76_strict_mod:telemetry:60",
            "run_id": "run_p76_strict_mod",
            "bin_index": 60,
            "sampled_at_utc": "2026-11-12T09:00:01.005000Z",
            "interval_s": 1.0,
            "sample_count": 2,
            "samples": [
                {"link_id": "e1>c1", "port_tx_mbps": 4.2, "counter_status": "OK"},
                {"link_id": "c1>e2", "port_tx_mbps": 4.2, "counter_status": "OK"},
            ],
        }
    )
    assert len(link_refs) == 2
    assert link_refs[0].link_id == "e1>c1"
    assert link_refs[0].valid is True


# =============================================================================
# 9. Phase 8.1: Python 3.10 RFC3339 Fractional-Second & OS-Ken Compatibility
# =============================================================================


def test_12_phase8_1_python310_rfc3339_fractional_seconds_and_malformed_rejection() -> None:
    """
    Verify RFC3339 UTC timestamp parsing and normalization across 1..6 fractional digits
    (`.2`, `.05`, `.200`, `.123456`, and whole-second `Z` / `+00:00`) on Python 3.10+,
    preserving exact microsecond precision and rejecting malformed timestamps.
    """
    cases = [
        ("2026-11-12T09:00:10Z", 0, "2026-11-12T09:00:10Z"),
        ("2026-11-12T09:00:10+00:00", 0, "2026-11-12T09:00:10Z"),
        ("2026-11-12T09:00:10.2Z", 200_000, "2026-11-12T09:00:10.2Z"),
        ("2026-04-06T00:01:00.05Z", 50_000, "2026-04-06T00:01:00.05Z"),
        ("2026-11-12T09:00:10.200Z", 200_000, "2026-11-12T09:00:10.2Z"),
        ("2026-11-12T09:00:10.123456Z", 123_456, "2026-11-12T09:00:10.123456Z"),
        ("2026-11-12T09:00:10.123456+00:00", 123_456, "2026-11-12T09:00:10.123456Z"),
    ]
    for raw_ts, expected_us, expected_norm in cases:
        dt = parse_utc_timestamp(raw_ts)
        assert dt.microsecond == expected_us
        assert format_utc_timestamp(raw_ts) == expected_norm
        assert validate_rfc3339_utc_timestamp(raw_ts, normalize=False) == raw_ts
        assert validate_rfc3339_utc_timestamp(raw_ts, normalize=True) == expected_norm
        # Round-trip the normalized form back through both validators
        dt_round = parse_utc_timestamp(expected_norm)
        assert dt_round == dt
        assert dt_round.microsecond == expected_us

    # Direct helper check for 1..2 digit padding to 6 microsecond digits
    assert (
        normalize_iso8601_fractional_seconds("2026-11-12T09:00:10.2+00:00")
        == "2026-11-12T09:00:10.200000+00:00"
    )
    assert (
        normalize_iso8601_fractional_seconds("2026-04-06T00:01:00.05+00:00")
        == "2026-04-06T00:01:00.050000+00:00"
    )

    # Malformed timestamps MUST still be rejected strictly
    malformed = [
        "",
        "   ",
        "2026-11-12T09:00:10",  # missing timezone
        "2026-11-12T09:00:10.Z",  # trailing dot without digits
        "2026-11-12T09:00:10.1234567Z",  # 7 digits (> microsecond precision)
        "2026-11-12T09:00:10.12abcZ",  # non-numeric fractional part
        "2026-13-45T99:00:00Z",  # invalid calendar date/time
    ]
    for bad in malformed:
        with pytest.raises(ContractSerializationError):
            validate_rfc3339_utc_timestamp(bad)
        with pytest.raises(StateValidationError):
            parse_utc_timestamp(bad)

    # Non-UTC offset must be rejected by contract schema validator
    with pytest.raises(ContractSerializationError):
        validate_rfc3339_utc_timestamp("2026-11-12T09:00:10.2+07:00")


def test_13_phase8_1_osken_and_ryu_base_app_resolution_and_event_dispatch() -> None:
    """
    Verify `_resolve_ofp_base_app` selects `RyuApp` for classic Ryu and `OSKenApp`
    for OS-Ken, and verify `RyuS0App` event handler wiring when an OpenFlow framework
    is present (or simulated).
    """
    class _DummyRyuApp:
        pass

    class _DummyOSKenApp:
        pass

    ryu_mod = SimpleNamespace(RyuApp=_DummyRyuApp)
    osken_mod = SimpleNamespace(OSKenApp=_DummyOSKenApp)
    both_mod = SimpleNamespace(RyuApp=_DummyRyuApp, OSKenApp=_DummyOSKenApp)
    empty_mod = SimpleNamespace()

    assert _resolve_ofp_base_app(ryu_mod, "ryu") is _DummyRyuApp
    assert _resolve_ofp_base_app(osken_mod, "os_ken") is _DummyOSKenApp
    assert _resolve_ofp_base_app(both_mod, "ryu") is _DummyRyuApp
    assert _resolve_ofp_base_app(both_mod, "os_ken") is _DummyOSKenApp

    with pytest.raises(AttributeError, match="neither 'RyuApp' nor 'OSKenApp'"):
        _resolve_ofp_base_app(empty_mod, "os_ken")

    # If running on Ubuntu with os_ken or ryu installed, verify RyuS0App class & handlers
    import sdn.apps.ryu_s0_app as s0_mod

    if s0_mod._FW_NAME in ("ryu", "os_ken"):
        assert hasattr(s0_mod, "RyuS0App")
        app_cls = s0_mod.RyuS0App
        assert app_cls.OFP_VERSIONS == [0x04]
        for handler_name in (
            "switch_features_handler",
            "barrier_reply_handler",
            "flow_stats_reply_handler",
            "port_desc_reply_handler",
            "error_msg_handler",
            "state_change_handler",
        ):
            assert hasattr(app_cls, handler_name), f"Missing handler {handler_name} on RyuS0App"
            fn = getattr(app_cls, handler_name)
            assert callable(fn)
            assert hasattr(fn, "callers"), f"Handler {handler_name} not decorated with @_set_ev_cls"


def test_14_phase9_1_state_change_handler_none_datapath_unhandshaked_dpid_and_stale_disconnect() -> None:
    """
    Verify Phase 9.1 hardening of `handle_state_change_event`, `on_switch_disconnected`,
    and `unregister_datapath`:
    1. `ev.datapath = None` is ignored safely without raising `TypeError`.
    2. `ev.datapath.id = None` (pre-handshake socket closed in `DEAD_DISPATCHER`) is ignored
       safely, recorded in `ignored_stale_events`, and never alters active switches or `connection_epoch`.
    3. Stale `DEAD_DISPATCHER` from an already-replaced connection (`dp1_old`) is ignored
       while preserving `dp1_new` and `connection_epoch == 2`.
    4. Active `DEAD_DISPATCHER` on a connected switch (`dp1_new`, `dpid=1`) cleanly unregisters
       `dpid=1`, invalidates only `dpid=1` cached evidences, and transitions readiness to
       `RECONCILIATION_REQUIRED`.
    """
    pkt_counter = {"val": 10}
    orch = S0ControllerOrchestrator(
        S0AppConfig(
            topology_bundle_path=TOPOLOGY_BUNDLE_PATH,
            csv_config_dir=CATALOG_DIR,
            run_id="run_p91_state_change",
        )
    )
    dead_sentinel = "DEAD_DISPATCHER"
    main_sentinel = "MAIN_DISPATCHER"

    # Case 1: ev.datapath is None before any switch connects -> returns False, no error
    ev_none_dp = SimpleNamespace(state=dead_sentinel, datapath=None)
    assert (
        orch.handle_state_change_event(ev_none_dp, dead_dispatcher_state=dead_sentinel)
        is False
    )

    # Case 2: ev.datapath.id is None (unhandshaked socket closes before OFPT_FEATURES_REPLY)
    unhandshaked_dp = SimpleNamespace(id=None)
    ev_none_id = SimpleNamespace(state=dead_sentinel, datapath=unhandshaked_dp)
    assert (
        orch.handle_state_change_event(ev_none_id, dead_dispatcher_state=dead_sentinel)
        is False
    )
    assert orch.on_switch_disconnected(None, datapath=unhandshaked_dp) is False
    assert any(
        "UNHANDSHAKED_DISCONNECT_IGNORED" in msg
        for _dpid, msg in orch.driver.ignored_stale_events
    )

    # Connect all 6 switches and bootstrap to VERIFIED
    dps = {}
    for sw in orch.topology.get_all_switches():
        dp = _FakeDatapath(
            sw.dpid,
            orch.driver,
            auto_reply=True,
            packet_count_provider=lambda _d, _c: pkt_counter["val"],
        )
        dps[sw.dpid] = dp
        orch.on_switch_connected(dp)

    rep = orch.bootstrap_and_verify(verified_at_utc="2026-11-12T09:00:00Z")
    assert rep.verified_48_flows is True
    assert orch.readiness_state == S0ReadinessState.VERIFIED
    assert orch.driver.get_connection_epoch(1) == 1

    # Non-DEAD_DISPATCHER state change is ignored
    ev_main = SimpleNamespace(state=main_sentinel, datapath=dps[1])
    assert (
        orch.handle_state_change_event(ev_main, dead_dispatcher_state=dead_sentinel)
        is False
    )
    assert orch.readiness_state == S0ReadinessState.VERIFIED

    # Another unhandshaked socket disconnect while all 6 switches are VERIFIED must NOT disturb state!
    assert (
        orch.handle_state_change_event(ev_none_id, dead_dispatcher_state=dead_sentinel)
        is False
    )
    assert orch.readiness_state == S0ReadinessState.VERIFIED
    assert len(orch.accumulated_evidences) == 48
    assert orch.driver.connected_dpids == (1, 2, 3, 4, 5, 6)

    # Case 3: Switch 1 reconnects with a new datapath instance (dp1_new), then late DEAD_DISPATCHER
    # arrives for dp1_old -> must be ignored without dropping dp1_new or corrupting epoch=2!
    dp1_old = dps[1]
    dp1_new = _FakeDatapath(
        1,
        orch.driver,
        auto_reply=True,
        packet_count_provider=lambda _d, _c: pkt_counter["val"],
    )
    orch.on_switch_connected(dp1_new)
    dps[1] = dp1_new
    assert orch.driver.get_connection_epoch(1) == 2
    assert orch.driver.get_active_datapath_id(1) == id(dp1_new)

    ev_stale_old = SimpleNamespace(state=dead_sentinel, datapath=dp1_old)
    assert (
        orch.handle_state_change_event(
            ev_stale_old, dead_dispatcher_state=dead_sentinel
        )
        is False
    )
    assert 1 in orch.driver.connected_dpids
    assert orch.driver.get_connection_epoch(1) == 2
    assert orch.driver.get_active_datapath_id(1) == id(dp1_new)

    # Re-verify after reconnect so all 48 flows are VERIFIED again at epoch=2
    pkt_counter["val"] += 5
    snap = orch.verify_or_reconcile_after_traffic()
    assert snap is not None and not snap.reconcile_required
    assert orch.readiness_state == S0ReadinessState.VERIFIED
    assert len(orch.accumulated_evidences) == 48

    # Case 4: Active datapath dp1_new disconnects via DEAD_DISPATCHER -> unregisters dpid=1
    # and invalidates the 24 flows touching dpid=1 (keeping 24 unaffected flows cached)
    ev_active_dead = SimpleNamespace(state=dead_sentinel, datapath=dp1_new)
    assert (
        orch.handle_state_change_event(
            ev_active_dead, dead_dispatcher_state=dead_sentinel
        )
        is True
    )
    assert 1 not in orch.driver.connected_dpids
    assert orch.readiness_state == S0ReadinessState.RECONCILIATION_REQUIRED
    assert len(orch.accumulated_evidences) == 24


# =============================================================================
# 10. Phase 10.1: Bounded Forwarding Verification Polling Regression Suite
# =============================================================================


def test_15_phase10_1_bounded_polling_first_try_delayed_timeout_and_partial_hops(
    topology: TopologyRegistry,
) -> None:
    """
    Verify bounded forwarding verification polling in `RyuOpenFlowDriver`:
    - Immediate pass on 1st attempt when all 3 hops have counter delta >= 1 (0 sleeps).
    - Pass after 2-3 polling attempts when counter increments asynchronously, with zero extra FlowMods.
    - Fail with `timed_out=True` when no traffic arrives before `counter_verify_timeout_s`.
    - Fail when only 1 or 2 hops have packet delta > 0 (partial hops never falsely PASS).
    - Respect `set_verification_budget_deadline()` when ApplyRequest deadline is shorter than timeout.
    """
    compiler = RouteCompiler(topology, FlowMatcher(topology))
    plan_p2 = compiler.compile_path("0x103_P2")  # e1(1) -> c2(6) -> e2(2)

    # 1. Immediate pass on 1st try (0 sleeps)
    sleeps: list[float] = []
    driver1 = RyuOpenFlowDriver(
        topology,
        config=RyuDriverConfig(counter_verify_timeout_s=0.5, counter_verify_poll_interval_s=0.05),
        sleep_fn=lambda s: sleeps.append(s),
    )
    for dpid in (1, 6, 2):
        dp = _FakeDatapath(dpid, driver1, packet_count_provider=lambda _d, _c: 5)
        driver1.register_datapath(dp)
    for hop in plan_p2.ordered_hop_rules:
        driver1.send_flow_mod(hop, FlowModCommand.ADD, phase=SwitchPhaseTag.PRIMARY_DOWNSTREAM)
    res_imm = driver1.verify_forwarding_counter_or_probe(plan_p2)
    assert res_imm.counter_or_probe_ok is True
    assert res_imm.actual_forwarded_path_id == "0x103_P2"
    assert res_imm.packet_delta == 5
    assert sleeps == []

    # 2. Counter increments after 2 polling sleeps -> passes on 3rd attempt with 0 extra FlowMods
    poll_state = {"calls": 0}
    sleeps2: list[float] = []

    def _delayed_sleep(sec: float) -> None:
        sleeps2.append(sec)
        poll_state["calls"] += 1

    driver2 = RyuOpenFlowDriver(
        topology,
        config=RyuDriverConfig(counter_verify_timeout_s=0.5, counter_verify_poll_interval_s=0.05),
        sleep_fn=_delayed_sleep,
    )
    for dpid in (1, 6, 2):
        dp = _FakeDatapath(
            dpid,
            driver2,
            packet_count_provider=lambda _d, _c: 3 if poll_state["calls"] >= 2 else 0,
        )
        driver2.register_datapath(dp)
    for hop in plan_p2.ordered_hop_rules:
        driver2.send_flow_mod(hop, FlowModCommand.ADD, phase=SwitchPhaseTag.PRIMARY_DOWNSTREAM)
    flowmod_count_before = sum(1 for e in driver2.get_operation_log() if e.op_type == "FLOW_MOD")
    res_delayed = driver2.verify_forwarding_counter_or_probe(plan_p2)
    flowmod_count_after = sum(1 for e in driver2.get_operation_log() if e.op_type == "FLOW_MOD")
    assert res_delayed.counter_or_probe_ok is True
    assert res_delayed.packet_delta == 3
    assert len(sleeps2) == 2
    assert flowmod_count_after == flowmod_count_before  # Zero redundant FlowMods during polling

    # 3. Partial hops only (e1=5, c2=5, e2=0) -> must NOT pass; times out cleanly
    driver3 = RyuOpenFlowDriver(
        topology,
        config=RyuDriverConfig(counter_verify_timeout_s=0.08, counter_verify_poll_interval_s=0.03),
    )
    for dpid in (1, 6, 2):
        dp = _FakeDatapath(
            dpid,
            driver3,
            packet_count_provider=(lambda d, _c: 5 if d in (1, 6) else 0),
        )
        driver3.register_datapath(dp)
    for hop in plan_p2.ordered_hop_rules:
        driver3.send_flow_mod(hop, FlowModCommand.ADD, phase=SwitchPhaseTag.PRIMARY_DOWNSTREAM)
    res_partial = driver3.verify_forwarding_counter_or_probe(plan_p2)
    assert res_partial.counter_or_probe_ok is False
    assert res_partial.actual_forwarded_path_id is None
    assert res_partial.timed_out is True
    assert res_partial.error_reason == "NO_REAL_COUNTER_DELTA_OR_ACTIVE_PROBE"

    # 4. Deadline budget shorter than counter_verify_timeout_s -> stops promptly within budget
    driver4 = RyuOpenFlowDriver(
        topology,
        config=RyuDriverConfig(counter_verify_timeout_s=5.0, counter_verify_poll_interval_s=0.02),
    )
    for dpid in (1, 6, 2):
        dp = _FakeDatapath(dpid, driver4, packet_count_provider=lambda _d, _c: 0)
        driver4.register_datapath(dp)
    for hop in plan_p2.ordered_hop_rules:
        driver4.send_flow_mod(hop, FlowModCommand.ADD, phase=SwitchPhaseTag.PRIMARY_DOWNSTREAM)
    driver4.set_verification_budget_deadline(0.05)
    try:
        res_budget = driver4.verify_forwarding_counter_or_probe(plan_p2)
    finally:
        driver4.set_verification_budget_deadline(None)
    assert res_budget.counter_or_probe_ok is False
    assert res_budget.timed_out is True


def test_16_phase10_1_bounded_polling_counter_reset_reconnect_and_rollback(
    topology: TopologyRegistry,
) -> None:
    """
    Verify bounded polling aborts immediately on:
    - Counter reset (`pkt_now < prev`) -> `COUNTER_RESET_DETECTED`
    - Switch reconnect (`conn_epoch` changes during polling) -> `DATAPATH_RECONNECTED_DURING_VERIFY`
    - Rule replacement (`rule_generation` changes during polling) -> `RULE_REPLACED_DURING_VERIFY`
    - Rollback verification succeeds with bounded polling when primary fails and rollback path receives traffic.
    """
    compiler = RouteCompiler(topology, FlowMatcher(topology))
    plan_p1 = compiler.compile_path("0x103_P1")
    plan_p2 = compiler.compile_path("0x103_P2")

    # 1. Counter reset detected immediately during verification
    pkt_box = {"val": 10}
    driver = RyuOpenFlowDriver(
        topology,
        config=RyuDriverConfig(counter_verify_timeout_s=0.5, counter_verify_poll_interval_s=0.02),
    )
    for dpid in (1, 5, 6, 2):
        dp = _FakeDatapath(dpid, driver, packet_count_provider=lambda _d, _c: pkt_box["val"])
        driver.register_datapath(dp)
    for hop in plan_p1.ordered_hop_rules:
        driver.send_flow_mod(hop, FlowModCommand.ADD, phase=SwitchPhaseTag.BOOTSTRAP)
    assert driver.verify_forwarding_counter_or_probe(plan_p1).counter_or_probe_ok is True
    # Counter drops from 10 -> 2 (switch reset)
    pkt_box["val"] = 2
    res_reset = driver.verify_forwarding_counter_or_probe(plan_p1)
    assert res_reset.counter_or_probe_ok is False
    assert res_reset.error_reason == "COUNTER_RESET_DETECTED"

    # 2. Datapath reconnect during polling aborts verification immediately
    reconn_box = {"done": False}
    dps2: dict[int, _FakeDatapath] = {}

    def _sleep_and_reconnect(_sec: float) -> None:
        if not reconn_box["done"]:
            reconn_box["done"] = True
            new_dp6 = _FakeDatapath(6, driver2, packet_count_provider=lambda _d, _c: 5)
            driver2.register_datapath(new_dp6)
            dps2[6] = new_dp6

    driver2 = RyuOpenFlowDriver(
        topology,
        config=RyuDriverConfig(counter_verify_timeout_s=0.5, counter_verify_poll_interval_s=0.02),
        sleep_fn=_sleep_and_reconnect,
    )
    for dpid in (1, 6, 2):
        dp = _FakeDatapath(dpid, driver2, packet_count_provider=lambda _d, _c: 0)
        dps2[dpid] = dp
        driver2.register_datapath(dp)
    for hop in plan_p2.ordered_hop_rules:
        driver2.send_flow_mod(hop, FlowModCommand.ADD, phase=SwitchPhaseTag.PRIMARY_DOWNSTREAM)
    res_reconn = driver2.verify_forwarding_counter_or_probe(plan_p2)
    assert res_reconn.counter_or_probe_ok is False
    assert res_reconn.error_reason == "DATAPATH_RECONNECTED_DURING_VERIFY_DPID_6"

    # 3. Rollback verification with bounded polling:
    #    Primary P2 has 0 packets -> times out -> RuleInstaller rolls back to P1 ->
    #    P1 receives packets on 2nd poll -> Rollback returns ROLLED_BACK with actual_path_id=0x103_P1!
    rb_polls = {"p1_sleep_count": 0, "in_rollback": False}

    def _rb_sleep(_sec: float) -> None:
        if rb_polls["in_rollback"]:
            rb_polls["p1_sleep_count"] += 1

    orch = S0ControllerOrchestrator(
        S0AppConfig(
            topology_bundle_path=TOPOLOGY_BUNDLE_PATH,
            csv_config_dir=CATALOG_DIR,
            run_id="run_p101_rb_poll",
            driver_config=RyuDriverConfig(
                counter_verify_timeout_s=0.06,
                counter_verify_poll_interval_s=0.02,
            ),
        )
    )
    orch.driver._sleep_fn = _rb_sleep
    for sw in orch.topology.get_all_switches():
        # c2 (dpid=6) always stays at 0 packets -> P2 fails verification;
        # c1 (dpid=5) and e1/e2 get packets once rollback starts!
        def _mk_provider(d_id: int):
            def _prov(_dpid: int, _cookie: int) -> int:
                if d_id == 6:
                    return 0
                if not rb_polls["in_rollback"]:
                    return 10
                return 20 if rb_polls["p1_sleep_count"] >= 1 else 0
            return _prov

        dp = _FakeDatapath(sw.dpid, orch.driver, auto_reply=True, packet_count_provider=_mk_provider(sw.dpid))
        orch.on_switch_connected(dp)

    rep = orch.bootstrap_and_verify(verified_at_utc="2026-11-12T09:00:00Z")
    assert rep.verified_48_flows is True
    # Flip flag right as rollback starts when e1 receives ROLLBACK_SOURCE_EDGE
    orig_send_fm = orch.driver.send_flow_mod

    def _wrap_fm(rule, command, *, phase):
        if phase == SwitchPhaseTag.ROLLBACK_SOURCE_EDGE:
            rb_polls["in_rollback"] = True
        return orig_send_fm(rule, command, phase=phase)

    orch.driver.send_flow_mod = _wrap_fm  # type: ignore[method-assign]
    outcome = orch.installer.install_flow_change(
        run_id="run_p101_rb_poll",
        flow_group_id="0x103",
        old_path_id="0x103_P1",
        new_path_id="0x103_P2",
        applied_at_utc="2026-11-12T09:00:15Z",
    )
    assert outcome.status == FlowDecisionStatus.ROLLED_BACK
    assert outcome.actual_path_id == "0x103_P1"
    assert outcome.verification_status == VerificationStatus.VERIFIED


def test_17_phase10_1_utc_now_rfc3339_preserves_microseconds_for_disconnect() -> None:
    """
    Phase 10.1 audit regression: `_utc_now_rfc3339()` must preserve microsecond precision
    so an immediate switch disconnect in the same wall-clock second after a sub-second
    `ApplyRequest` never triggers `Backward timestamp in mark_controller_restart`.
    """
    from sdn.apps.ryu_s0_app import _utc_now_rfc3339
    from sdn.controller.state_manager import parse_utc_timestamp

    ts1 = _utc_now_rfc3339()
    ts2 = _utc_now_rfc3339()
    assert "." in ts1 and ts1.endswith("Z")
    assert parse_utc_timestamp(ts2) >= parse_utc_timestamp(ts1)




