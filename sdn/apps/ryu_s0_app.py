"""
Ryu / OS-Ken S0 Application for Ubuntu + Mininet + Open vSwitch (`UBUNTU_REQUIRED`).

Can be launched on Ubuntu with either:
  - `ryu-manager --ofp-tcp-listen-port 6633 sdn/apps/ryu_s0_app.py`
  - `osken-manager --ofp-tcp-listen-port 6633 sdn/apps/ryu_s0_app.py`

On Windows (without `ryu` or `os_ken` installed), `S0ControllerOrchestrator` can be
imported and tested directly with fake datapath/event/reply objects (`PASS_MOCK_ADAPTER`).

Important `pingall` & Traffic Scope Note:
- The 48 canonical inter-edge flows (`0x103..0x806`) cover ONLY inter-edge host pairs.
- They do NOT include the 8 directed intra-edge host pairs on the same edge switch:
  `h1<->h2` (`e1`), `h3<->h4` (`e2`), `h5<->h6` (`e3`), `h7<->h8` (`e4`).
- Therefore, `pingall` 0% loss MUST NOT be claimed from the 48 inter-edge flows alone;
  `pingall` requires static ARP (or controlled ARP handling) AND intra-edge forwarding.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import threading
from typing import Any, Callable, Optional

from sdn.controller.decision_handler import DecisionHandler, DecisionHandlerConfig
from sdn.controller.state_manager import StateManager
from sdn.controller.topology_registry import TopologyRegistry
from sdn.flow_management.flow_matcher import FlowMatcher, FlowMatcherConfig
from sdn.flow_management.route_compiler import RouteCompiler
from sdn.flow_management.rule_installer import RuleInstaller
from sdn.flow_management.ryu_openflow_driver import RyuDriverConfig, RyuOpenFlowDriver
from sdn.flow_management.switch_driver import SwitchDriverError
from sdn.route_verification.rule_verifier import RuleVerifier, RuleVerifierConfig
from sdn.schemas.state_models import RoutingStateSnapshot, VerificationEvidence

# 8 directed intra-edge host pairs (excluded from the 48 canonical inter-edge TE flows)
INTRA_EDGE_HOST_PAIRS: tuple[tuple[str, str, str, int, int], ...] = (
    ("e1", "h1", "h2", 1, 2),
    ("e1", "h2", "h1", 2, 1),
    ("e2", "h3", "h4", 1, 2),
    ("e2", "h4", "h3", 2, 1),
    ("e3", "h5", "h6", 1, 2),
    ("e3", "h6", "h5", 2, 1),
    ("e4", "h7", "h8", 1, 2),
    ("e4", "h8", "h7", 2, 1),
)

INTRA_EDGE_HELPER_COOKIE = 0xFFF0
ARP_HELPER_COOKIE = 0xFFF1
HELPER_RULE_PRIORITY = 50


def _utc_now_rfc3339() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class S0AppConfig:
    """Configuration for the S0 Ryu/OS-Ken bootstrap orchestrator."""

    topology_bundle_path: Path = Path("configs/network/topology_bundle.json")
    csv_config_dir: Optional[Path] = Path("configs/network")
    run_id: str = "run_s0_01"
    preinstall_both_downstream_branches: bool = True
    enable_intra_edge_l3: bool = False
    enable_arp_edge_local_flood: bool = False
    reconcile_poll_interval_s: float = 2.0
    driver_config: RyuDriverConfig = RyuDriverConfig()
    matcher_config: FlowMatcherConfig = FlowMatcherConfig()
    verifier_config: RuleVerifierConfig = RuleVerifierConfig()

    @classmethod
    def from_env(cls) -> "S0AppConfig":
        """Build `S0AppConfig` from environment variables for Ubuntu `ryu-manager`/`osken-manager`."""
        def _env_bool(name: str, default: bool) -> bool:
            raw = os.environ.get(name)
            if raw is None:
                return default
            return raw.strip().lower() in ("1", "true", "yes", "on")

        return cls(
            run_id=os.environ.get("SDN_RUN_ID", "s0_live_run").strip() or "s0_live_run",
            preinstall_both_downstream_branches=_env_bool("SDN_PREINSTALL_BOTH_DOWNSTREAM", True),
            enable_intra_edge_l3=_env_bool("SDN_ENABLE_INTRA_EDGE_L3", False),
            enable_arp_edge_local_flood=_env_bool("SDN_ENABLE_ARP_EDGE_LOCAL_FLOOD", False),
            reconcile_poll_interval_s=float(os.environ.get("SDN_RECONCILE_POLL_INTERVAL_S", "2.0")),
        )


@dataclass(frozen=True)
class S0BootstrapReport:
    """Structured result of S0 switch verification, rule bootstrap, and verification."""

    run_id: str
    topology_version: str
    topology_hash: str
    connected_dpids: tuple[int, ...]
    bootstrapped_48_inter_edge_installed: bool
    verified_48_flows: bool
    state_version: int
    reconcile_required: bool
    pingall_inter_edge_only_ready: bool
    pingall_full_mesh_ready: bool
    intra_edge_pairs_covered: bool
    arp_handling_configured: bool
    note: str


class S0ControllerOrchestrator:
    """
    Pure-Python S0 orchestrator coordinating `TopologyRegistry`, `RyuOpenFlowDriver`,
    `RouteCompiler`, `RuleVerifier`, `RuleInstaller`, `StateManager`, and `DecisionHandler`.

    Eventlet-safe: `bootstrap_and_verify()` is designed to run in a background greenlet/thread
    and never blocks Ryu's OpenFlow receive loop.
    """

    def __init__(
        self,
        config: Optional[S0AppConfig] = None,
        *,
        active_probe_callback: Optional[Callable[..., Any]] = None,
        event_factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        self._config = config or S0AppConfig()
        bundle_path = Path(self._config.topology_bundle_path)
        csv_dir = (
            Path(self._config.csv_config_dir)
            if self._config.csv_config_dir is not None
            else bundle_path.parent
        )

        # 1. Load & validate canonical Network topology + hash
        self._topology = TopologyRegistry.from_bundle_file(
            bundle_path,
            csv_config_dir=csv_dir,
            verify_hash=True,
        )

        # 2. Initialize RyuOpenFlowDriver + SDN core modules (integration fail-closed mode)
        self._driver = RyuOpenFlowDriver(
            self._topology,
            config=self._config.driver_config,
            event_factory=event_factory,
            active_probe_callback=active_probe_callback,
        )
        self._matcher = FlowMatcher(self._topology, self._config.matcher_config)
        self._compiler = RouteCompiler(self._topology, self._matcher)
        self._verifier = RuleVerifier(
            self._compiler, self._driver, self._config.verifier_config
        )
        self._installer = RuleInstaller(self._compiler, self._driver, self._verifier)
        self._state_manager = StateManager(self._topology)
        self._decision_handler = DecisionHandler(
            self._topology,
            self._state_manager,
            self._installer,
            config=DecisionHandlerConfig.for_integration(),
        )

        self._lock = threading.RLock()
        self._bootstrapped = False
        self._run_initialized = False
        self._last_report: Optional[S0BootstrapReport] = None

    @property
    def topology(self) -> TopologyRegistry:
        return self._topology

    @property
    def driver(self) -> RyuOpenFlowDriver:
        return self._driver

    @property
    def state_manager(self) -> StateManager:
        return self._state_manager

    @property
    def decision_handler(self) -> DecisionHandler:
        return self._decision_handler

    @property
    def installer(self) -> RuleInstaller:
        return self._installer

    @property
    def verifier(self) -> RuleVerifier:
        return self._verifier

    @property
    def last_report(self) -> Optional[S0BootstrapReport]:
        return self._last_report

    def on_switch_connected(self, datapath: Any) -> bool:
        """
        Handle switch handshake / features reply.
        Returns `True` if all 6 expected switches (`DPID 1..6`) are now connected.
        If the run was already initialized and a switch reconnected, marks controller
        restart / reconciliation required so stale state is never trusted.
        """
        replaced = self._driver.register_datapath(datapath)
        dpid = int(getattr(datapath, "id"))
        with self._lock:
            if self._run_initialized and replaced:
                self._decision_handler.notify_controller_restart(
                    self._config.run_id, _utc_now_rfc3339()
                )
        return self._driver.all_expected_switches_connected

    def on_switch_disconnected(self, dpid: int, reason: str = "DEAD_DISPATCHER") -> None:
        """Handle switch disconnect: fail waiters and require reconciliation if active."""
        self._driver.unregister_datapath(dpid, reason=reason)
        with self._lock:
            if self._run_initialized and not self._state_manager.is_reconcile_required(
                self._config.run_id
            ):
                self._decision_handler.notify_controller_restart(
                    self._config.run_id, _utc_now_rfc3339()
                )

    def verify_connected_switches_and_ports(
        self, port_map_by_dpid: Optional[dict[int, set[int]]] = None
    ) -> None:
        """
        Verify all 6 canonical DPIDs (`1..6`) are connected and, when port descriptions
        are provided, that every canonical switch port from `configs/network/links.csv`
        exists on the switch.
        """
        expected_dpids = {sw.dpid for sw in self._topology.get_all_switches()}
        connected = set(self._driver.connected_dpids)
        missing = sorted(expected_dpids - connected)
        if missing:
            raise SwitchDriverError(
                f"Cannot bootstrap S0: missing switch DPIDs {missing}; connected={sorted(connected)}."
            )
        if port_map_by_dpid is not None:
            for lk in self._topology.get_all_links():
                if lk.from_node in {sw.switch_id for sw in self._topology.get_all_switches()}:
                    dpid = self._topology.get_switch(lk.from_node).dpid
                    ports = port_map_by_dpid.get(dpid, set())
                    if lk.from_port not in ports:
                        raise SwitchDriverError(
                            f"Port mismatch on switch '{lk.from_node}' (dpid={dpid}): "
                            f"expected port {lk.from_port} for link '{lk.link_id}', found {sorted(ports)}."
                        )

    def _send_helper_flow_mod(
        self,
        dp: Any,
        *,
        cookie: int,
        match: Any,
        actions: list[Any],
    ) -> None:
        ofproto = dp.ofproto
        parser = dp.ofproto_parser
        inst = [
            parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)
        ]
        msg = parser.OFPFlowMod(
            datapath=dp,
            cookie=cookie,
            cookie_mask=0,
            table_id=0,
            command=ofproto.OFPFC_ADD,
            idle_timeout=0,
            hard_timeout=0,
            priority=HELPER_RULE_PRIORITY,
            buffer_id=getattr(ofproto, "OFP_NO_BUFFER", 0xFFFFFFFF),
            out_port=getattr(ofproto, "OFPP_ANY", 0xFFFFFFFF),
            out_group=getattr(ofproto, "OFPG_ANY", 0xFFFFFFFF),
            flags=0,
            match=match,
            instructions=inst,
        )
        dp.send_msg(msg)

    def install_optional_s0_pingall_helpers(self) -> int:
        """
        Install optional intra-edge L3 rules (8 pairs) and/or controlled ARP rules
        ONLY when explicitly enabled in `S0AppConfig`.
        Uses dedicated helper cookies (`0xFFF0`, `0xFFF1`) and lower priority (`50`)
        so it never interferes with the 48 canonical inter-edge TE flows (`0x103..0x806`).
        """
        installed_count = 0
        if not (self._config.enable_intra_edge_l3 or self._config.enable_arp_edge_local_flood):
            return 0

        for sw in self._topology.get_all_switches():
            if sw.role != "edge":
                continue
            dp = self._driver._get_datapath(sw.dpid)
            parser = dp.ofproto_parser

            if self._config.enable_intra_edge_l3:
                for edge_id, src_host, dst_host, in_p, out_p in INTRA_EDGE_HOST_PAIRS:
                    if edge_id != sw.switch_id:
                        continue
                    src_ip = self._topology.get_host(src_host).ip
                    dst_ip = self._topology.get_host(dst_host).ip
                    match = parser.OFPMatch(
                        in_port=in_p,
                        eth_type=0x0800,
                        ipv4_src=src_ip,
                        ipv4_dst=dst_ip,
                    )
                    self._send_helper_flow_mod(
                        dp,
                        cookie=INTRA_EDGE_HELPER_COOKIE,
                        match=match,
                        actions=[parser.OFPActionOutput(out_p)],
                    )
                    installed_count += 1

            if self._config.enable_arp_edge_local_flood:
                # Loop-free spanning-tree ARP across edge switches + core `c1` ONLY (never `c2`):
                # - Host ports on edge <-> sibling host port + uplink port to `c1`
                # - Uplink port from `c1` on edge -> local host ports only (never back up)
                host_ports = sorted(
                    lk.from_port
                    for lk in self._topology.get_all_links()
                    if lk.from_node == sw.switch_id and lk.to_node.startswith("h")
                )
                c1_uplink = next(
                    lk.from_port
                    for lk in self._topology.get_all_links()
                    if lk.from_node == sw.switch_id and lk.to_node == "c1"
                )
                for hp in host_ports:
                    out_ports = [p for p in host_ports if p != hp] + [c1_uplink]
                    match = parser.OFPMatch(in_port=hp, eth_type=0x0806)
                    self._send_helper_flow_mod(
                        dp,
                        cookie=ARP_HELPER_COOKIE,
                        match=match,
                        actions=[parser.OFPActionOutput(p) for p in out_ports],
                    )
                    installed_count += 1

                # From c1 down to local hosts only
                if host_ports:
                    match = parser.OFPMatch(in_port=c1_uplink, eth_type=0x0806)
                    self._send_helper_flow_mod(
                        dp,
                        cookie=ARP_HELPER_COOKIE,
                        match=match,
                        actions=[parser.OFPActionOutput(p) for p in host_ports],
                    )
                    installed_count += 1

        if self._config.enable_arp_edge_local_flood:
            # Core switch `c1` forwards ARP to all other edge-facing ports (`c2` is untouched -> 0 loops)
            c1_sw = self._topology.get_switch("c1")
            c1_dp = self._driver._get_datapath(c1_sw.dpid)
            c1_parser = c1_dp.ofproto_parser
            c1_edge_ports = sorted(
                lk.from_port
                for lk in self._topology.get_all_links()
                if lk.from_node == "c1" and lk.to_node.startswith("e")
            )
            for in_p in c1_edge_ports:
                out_ports = [p for p in c1_edge_ports if p != in_p]
                match = c1_parser.OFPMatch(in_port=in_p, eth_type=0x0806)
                self._send_helper_flow_mod(
                    c1_dp,
                    cookie=ARP_HELPER_COOKIE,
                    match=match,
                    actions=[c1_parser.OFPActionOutput(p) for p in out_ports],
                )
                installed_count += 1

        return installed_count

    def bootstrap_and_verify(
        self,
        *,
        verified_at_utc: Optional[str] = None,
        port_map_by_dpid: Optional[dict[int, set[int]]] = None,
    ) -> S0BootstrapReport:
        """
        Execute the full S0 bootstrap sequence once all 6 switches are connected:
        1. Verify DPIDs (`1..6`), `topology_hash`, and optional port mappings.
        2. Install downstream P1/P2 branches + Barrier, then source-edge P1 + Barrier,
           then read back `FlowStats` and verify real data-plane forwarding across all 48 flows.
        3. Only initialize `StateManager` as `VERIFIED` if all 48 flows actually produced
           verified `VerificationEvidence` (never fake `VERIFIED` from `Barrier` alone!).
           Otherwise initialize as `UNVERIFIED` (`reconcile_required=True`).
        """
        ts = verified_at_utc or _utc_now_rfc3339()
        self.verify_connected_switches_and_ports(port_map_by_dpid=port_map_by_dpid)

        self.install_optional_s0_pingall_helpers()

        evidences: dict[str, VerificationEvidence] = {}
        bootstrap_error: Optional[str] = None
        try:
            evidences = self._installer.bootstrap_initial_paths(
                run_id=self._config.run_id,
                verified_at_utc=ts,
                preinstall_both_downstream_branches=self._config.preinstall_both_downstream_branches,
            )
        except Exception as exc:
            bootstrap_error = str(exc)

        all_48_verified = len(evidences) == 48 and bootstrap_error is None
        with self._lock:
            if not self._run_initialized:
                snap = self._state_manager.initialize_run(
                    run_id=self._config.run_id,
                    bootstrapped_at_utc=ts,
                    verification_evidences=evidences if all_48_verified else None,
                )
                self._run_initialized = True
            else:
                snap = self._state_manager.get_routing_state(self._config.run_id)
            self._bootstrapped = True

        pingall_full = bool(
            self._config.enable_intra_edge_l3 and self._config.enable_arp_edge_local_flood
        )
        note = (
            "All 48 inter-edge flows VERIFIED via FlowStats + data-plane evidence."
            if all_48_verified
            else (
                f"48 inter-edge flows installed on P1 but NOT marked VERIFIED "
                f"(awaiting real data-plane counter/probe evidence): {bootstrap_error or 'INCOMPLETE_EVIDENCE'}"
            )
        )
        report = S0BootstrapReport(
            run_id=self._config.run_id,
            topology_version=self._topology.topology_version,
            topology_hash=self._topology.topology_hash,
            connected_dpids=self._driver.connected_dpids,
            bootstrapped_48_inter_edge_installed=True,
            verified_48_flows=all_48_verified,
            state_version=snap.state_version,
            reconcile_required=snap.reconcile_required,
            pingall_inter_edge_only_ready=True,
            pingall_full_mesh_ready=pingall_full,
            intra_edge_pairs_covered=self._config.enable_intra_edge_l3,
            arp_handling_configured=self._config.enable_arp_edge_local_flood,
            note=note,
        )
        self._last_report = report
        return report

    def verify_or_reconcile_after_traffic(
        self, *, reconciled_at_utc: Optional[str] = None
    ) -> Optional[RoutingStateSnapshot]:
        """
        Non-destructive verification/reconciliation pass after traffic starts or switch reconnect:
        1. Reads back `FlowStats` + counter/probe evidence across all 48 flows.
        2. Reinstalls P1 ONLY for flows whose rules are actually missing from OVS
           (`not res.rule_readback_ok`), never reinstalling flows whose rules are already present.
        3. If any flow has rules installed (`rule_readback_ok=True`) but still awaits real
           traffic/probe evidence (`not is_verified`), returns `None` without mutating OVS rules
           so a background retry loop can re-check once traffic arrives.
        4. When all 48 flows produce `VERIFIED` evidence, commits `reconcile_run_state()`.
        """
        ts = reconciled_at_utc or _utc_now_rfc3339()
        discovered = self._verifier.discover_all_active_paths(self._config.run_id, ts)
        evidences: dict[str, VerificationEvidence] = {}
        missing_rule_flows: dict[str, str] = {}
        awaiting_traffic: list[str] = []

        for fg in self._topology.get_all_flow_groups():
            fid = fg.flow_group_id
            res = discovered[fid]
            if res.is_verified and res.evidence is not None:
                evidences[fid] = res.evidence
            elif not res.rule_readback_ok:
                missing_rule_flows[fid] = self._topology.get_initial_candidate_path(fid).path_id
            else:
                awaiting_traffic.append(fid)

        if missing_rule_flows:
            try:
                reinstalled = self._installer.reinstall_and_verify_paths(
                    run_id=self._config.run_id,
                    target_paths_by_flow=missing_rule_flows,
                    verified_at_utc=ts,
                )
                evidences.update(reinstalled)
            except RuntimeError:
                return None

        if awaiting_traffic or len(evidences) != len(self._topology.get_all_flow_groups()):
            return None

        return self._state_manager.reconcile_run_state(
            run_id=self._config.run_id,
            verified_evidences=evidences,
            reconciled_at_utc=ts,
            reconciliation_id="reconcile_s0",
        )

    def reconcile_after_reconnect_or_traffic(
        self, *, reconciled_at_utc: Optional[str] = None
    ) -> RoutingStateSnapshot:
        """
        Discover and verify all 48 active paths from live switches after traffic starts
        or after a switch/controller reconnect (`AC-S03`, `AC-S10`).
        Raises `RuntimeError` if any flow still lacks real counter/probe evidence.
        """
        snap = self.verify_or_reconcile_after_traffic(reconciled_at_utc=reconciled_at_utc)
        if snap is None:
            raise RuntimeError(
                f"Run '{self._config.run_id}' is not yet VERIFIED across all 48 flows "
                "(awaiting real counter delta or active probe evidence)."
            )
        return snap


# -----------------------------------------------------------------------------
# Lazy Ryu / OS-Ken Entrypoint (`ryu-manager` / `osken-manager`)
# -----------------------------------------------------------------------------

def _import_ofp_framework() -> tuple[Any, Any, Any, Any, Any, Any]:
    """Import either `ryu` or `os_ken` dynamically."""
    for pkg in ("ryu", "os_ken"):
        try:
            app_manager = __import__(f"{pkg}.base.app_manager", fromlist=["app_manager"])
            ofp_event = __import__(f"{pkg}.controller.ofp_event", fromlist=["ofp_event"])
            handler = __import__(f"{pkg}.controller.handler", fromlist=["handler"])
            ofproto_v1_3 = __import__(
                f"{pkg}.ofproto.ofproto_v1_3", fromlist=["ofproto_v1_3"]
            )
            hub = __import__(f"{pkg}.lib.hub", fromlist=["hub"])
            return app_manager, ofp_event, handler, ofproto_v1_3, hub, pkg
        except ImportError:
            continue
    raise ImportError(
        "Neither 'ryu' nor 'os_ken' is installed in this Python environment. "
        "Run on Ubuntu Linux with ryu-manager or osken-manager."
    )


try:
    _app_manager, _ofp_event, _handler, _ofproto_v1_3, _hub, _FW_NAME = _import_ofp_framework()
    _BASE_APP = _app_manager.RyuApp
    _set_ev_cls = _handler.set_ev_cls
    _CONFIG_DISPATCHER = _handler.CONFIG_DISPATCHER
    _MAIN_DISPATCHER = _handler.MAIN_DISPATCHER
    _DEAD_DISPATCHER = _handler.DEAD_DISPATCHER
    _OFP_VERSIONS = [_ofproto_v1_3.OFP_VERSION]
except ImportError:
    _BASE_APP = object
    _ofp_event = None
    _set_ev_cls = None
    _CONFIG_DISPATCHER = None
    _MAIN_DISPATCHER = None
    _DEAD_DISPATCHER = None
    _hub = None
    _FW_NAME = "none"
    _OFP_VERSIONS = [0x04]


if _set_ev_cls is not None and _ofp_event is not None:

    class RyuS0App(_BASE_APP):  # type: ignore[misc, valid-type]
        """Ryu / OS-Ken OpenFlow 1.3 S0 Controller Application."""

        OFP_VERSIONS = _OFP_VERSIONS

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.orchestrator = S0ControllerOrchestrator(S0AppConfig.from_env())
            self._bootstrap_started = False
            self._reconcile_loop_running = False

        @_set_ev_cls(_ofp_event.EventOFPSwitchFeatures, _CONFIG_DISPATCHER)
        def switch_features_handler(self, ev: Any) -> None:
            dp = ev.msg.datapath
            all_ready = self.orchestrator.on_switch_connected(dp)
            if all_ready and not self._bootstrap_started:
                self._bootstrap_started = True
                # Spawn in background greenlet so OF receive loop is NEVER blocked!
                _hub.spawn(self._run_bootstrap)
            elif all_ready and self._bootstrap_started and not self._reconcile_loop_running:
                self._reconcile_loop_running = True
                _hub.spawn(self._reconcile_until_verified)

        def _run_bootstrap(self) -> None:
            report = self.orchestrator.bootstrap_and_verify()
            self.logger.info("S0 Bootstrap Report: %s", report)
            if report.reconcile_required and not self._reconcile_loop_running:
                self._reconcile_loop_running = True
                _hub.spawn(self._reconcile_until_verified)

        def _reconcile_until_verified(self) -> None:
            poll_s = max(0.5, float(self.orchestrator._config.reconcile_poll_interval_s))
            try:
                while self.orchestrator.state_manager.is_reconcile_required(
                    self.orchestrator._config.run_id
                ):
                    _hub.sleep(poll_s)
                    if not self.orchestrator.driver.all_expected_switches_connected:
                        continue
                    snap = self.orchestrator.verify_or_reconcile_after_traffic()
                    if snap is not None and not snap.reconcile_required:
                        self.logger.info(
                            "S0 Reconciled & VERIFIED all 48 flows (state_version=%d).",
                            snap.state_version,
                        )
                        break
            finally:
                self._reconcile_loop_running = False

        @_set_ev_cls(_ofp_event.EventOFPBarrierReply, _MAIN_DISPATCHER)
        def barrier_reply_handler(self, ev: Any) -> None:
            self.orchestrator.driver.handle_barrier_reply(ev)

        @_set_ev_cls(_ofp_event.EventOFPFlowStatsReply, _MAIN_DISPATCHER)
        def flow_stats_reply_handler(self, ev: Any) -> None:
            self.orchestrator.driver.handle_flow_stats_reply(ev)

        @_set_ev_cls(_ofp_event.EventOFPErrorMsg, [_CONFIG_DISPATCHER, _MAIN_DISPATCHER])
        def error_msg_handler(self, ev: Any) -> None:
            self.orchestrator.driver.handle_error_msg(ev)

        @_set_ev_cls(_ofp_event.EventOFPStateChange, [_MAIN_DISPATCHER, _DEAD_DISPATCHER])
        def state_change_handler(self, ev: Any) -> None:
            if ev.state == _DEAD_DISPATCHER and ev.datapath is not None:
                self.orchestrator.on_switch_disconnected(ev.datapath.id)
