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
from sdn.schemas.state_models import EvidenceMode, VerificationStatus

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


class _FakeParserV13:
    OFPMatch = _FakeOFPMatch
    OFPActionOutput = _FakeOFPActionOutput
    OFPInstructionActions = _FakeOFPInstructionActions
    OFPFlowMod = _FakeOFPFlowMod
    OFPBarrierRequest = _FakeOFPBarrierRequest
    OFPFlowStatsRequest = _FakeOFPFlowStatsRequest


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
    ) -> None:
        self.id = dpid
        self.ofproto = _FakeOfprotoV13()
        self.ofproto_parser = _FakeParserV13()
        self._driver = driver
        self.auto_reply = auto_reply
        self.multipart_chunk_size = multipart_chunk_size
        self.packet_count_provider = packet_count_provider
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
                if msg.command in (_FakeOfprotoV13.OFPFC_ADD, _FakeOfprotoV13.OFPFC_MODIFY_STRICT):
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
                ev = SimpleNamespace(
                    msg=SimpleNamespace(datapath=self, xid=msg.xid)
                )
                self._driver.handle_barrier_reply(ev)

            elif isinstance(msg, _FakeOFPFlowStatsRequest):
                matching = [
                    _FakeFlowStatItem(
                        table_id=st.table_id,
                        priority=st.priority,
                        cookie=st.cookie,
                        match=st.match,
                        instructions=st.instructions,
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
