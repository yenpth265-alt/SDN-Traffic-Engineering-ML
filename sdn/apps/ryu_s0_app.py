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

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
import os
from pathlib import Path
import threading
from typing import Any, Callable, Mapping, Optional

from sdn.controller.decision_handler import DecisionHandler, DecisionHandlerConfig
from sdn.controller.state_manager import (
    StateManager,
    format_utc_timestamp,
    parse_utc_timestamp,
)
from sdn.controller.topology_registry import (
    PortAndAdjacencyValidationReport,
    TopologyRegistry,
)
from sdn.flow_management.flow_matcher import FlowMatcher, FlowMatcherConfig
from sdn.flow_management.route_compiler import CompiledPathPlan, RouteCompiler
from sdn.flow_management.rule_installer import BootstrapInstallReport, RuleInstaller
from sdn.flow_management.ryu_openflow_driver import RyuDriverConfig, RyuOpenFlowDriver
from sdn.flow_management.switch_driver import SwitchDriverError, SwitchPhaseTag
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
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class S0ReadinessState(str, Enum):
    """Explicit S0 controller lifecycle & readiness states (`Phase 7.1`)."""

    WAITING_FOR_SWITCHES = "WAITING_FOR_SWITCHES"
    BOOTSTRAPPING = "BOOTSTRAPPING"
    INSTALL_FAILED = "INSTALL_FAILED"
    RULES_INSTALLED = "RULES_INSTALLED"
    VERIFICATION_PENDING = "VERIFICATION_PENDING"
    VERIFIED = "VERIFIED"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"


@dataclass(frozen=True)
class CachedFlowVerificationEvidence:
    """
    Per-flow incremental verification evidence bound to rule generation and switch
    connection epoch across all 3 hops (`Phase 7.2`).
    """

    run_id: str
    flow_group_id: str
    path_id: str
    rule_generation_by_dpid: dict[int, int]
    conn_epoch_by_dpid: dict[int, int]
    last_packet_count_by_dpid: dict[int, int]
    verified_at_utc: str
    evidence: VerificationEvidence


@dataclass(frozen=True)
class S0AppConfig:
    """Configuration for the S0 Ryu/OS-Ken bootstrap orchestrator."""

    topology_bundle_path: Path = Path("configs/network/topology_bundle.json")
    csv_config_dir: Optional[Path] = Path("configs/network")
    run_id: str = "run_s0_01"
    preinstall_both_downstream_branches: bool = True
    enable_intra_edge_l3: bool = False
    enable_arp_edge_local_flood: bool = False
    verify_port_desc_on_bootstrap: bool = False
    reconcile_poll_interval_s: float = 2.0
    max_reconcile_attempts: int = 150
    max_evidence_age_s: float = 300.0
    driver_config: RyuDriverConfig = RyuDriverConfig()
    matcher_config: FlowMatcherConfig = FlowMatcherConfig()
    verifier_config: RuleVerifierConfig = RuleVerifierConfig()
    northbound_enable: bool = False
    northbound_host: str = "127.0.0.1"
    northbound_port: int = 18080
    northbound_auth_token: Optional[str] = None
    northbound_sync_wait_ms: int = 300
    idempotency_db_path: Optional[Path] = None
    data_endpoint_url: Optional[str] = None
    data_outbox_dir: Optional[Path] = None

    @classmethod
    def from_env(cls) -> "S0AppConfig":
        """Build `S0AppConfig` from environment variables for Ubuntu `ryu-manager`/`osken-manager`."""
        def _env_bool(name: str, default: bool) -> bool:
            raw = os.environ.get(name)
            if raw is None:
                return default
            return raw.strip().lower() in ("1", "true", "yes", "on")

        def _env_opt_str(name: str) -> Optional[str]:
            raw = os.environ.get(name)
            if raw is None or not raw.strip():
                return None
            return raw.strip()

        idem_raw = _env_opt_str("SDN_IDEMPOTENCY_DB_PATH")
        outbox_raw = _env_opt_str("SDN_DATA_OUTBOX_DIR")

        return cls(
            run_id=os.environ.get("SDN_RUN_ID", "s0_live_run").strip() or "s0_live_run",
            preinstall_both_downstream_branches=_env_bool("SDN_PREINSTALL_BOTH_DOWNSTREAM", True),
            enable_intra_edge_l3=_env_bool("SDN_ENABLE_INTRA_EDGE_L3", False),
            enable_arp_edge_local_flood=_env_bool("SDN_ENABLE_ARP_EDGE_LOCAL_FLOOD", False),
            verify_port_desc_on_bootstrap=_env_bool("SDN_VERIFY_PORT_DESC", False),
            reconcile_poll_interval_s=float(os.environ.get("SDN_RECONCILE_POLL_INTERVAL_S", "2.0")),
            max_reconcile_attempts=int(os.environ.get("SDN_MAX_RECONCILE_ATTEMPTS", "150")),
            max_evidence_age_s=float(os.environ.get("SDN_MAX_EVIDENCE_AGE_S", "300.0")),
            driver_config=RyuDriverConfig(
                counter_verify_timeout_s=float(
                    os.environ.get("SDN_COUNTER_VERIFY_TIMEOUT_S", "1.0")
                ),
                counter_verify_poll_interval_s=float(
                    os.environ.get("SDN_COUNTER_VERIFY_POLL_INTERVAL_S", "0.04")
                ),
            ),
            northbound_enable=_env_bool("SDN_NORTHBOUND_ENABLE", False),
            northbound_host=os.environ.get("SDN_NORTHBOUND_HOST", "127.0.0.1").strip() or "127.0.0.1",
            northbound_port=int(os.environ.get("SDN_NORTHBOUND_PORT", "18080")),
            northbound_auth_token=_env_opt_str("SDN_NORTHBOUND_TOKEN"),
            northbound_sync_wait_ms=int(os.environ.get("SDN_NORTHBOUND_SYNC_WAIT_MS", "300")),
            idempotency_db_path=Path(idem_raw) if idem_raw else None,
            data_endpoint_url=_env_opt_str("SDN_DATA_ENDPOINT_URL"),
            data_outbox_dir=Path(outbox_raw) if outbox_raw else None,
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
    readiness_state: S0ReadinessState = S0ReadinessState.VERIFIED
    rules_readback_ok: bool = False
    helpers_installed_ok: bool = True
    failed_stage: Optional[str] = None
    verified_flow_count: int = 0
    pending_traffic_flow_count: int = 0
    port_validation_report: Optional[PortAndAdjacencyValidationReport] = None


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
        self._idempotency_store = None
        if self._config.idempotency_db_path is not None:
            from sdn.adapters.sqlite_idempotency_store import SQLiteBatchIdempotencyStore

            self._idempotency_store = SQLiteBatchIdempotencyStore(
                self._config.idempotency_db_path
            )
        self._decision_handler = DecisionHandler(
            self._topology,
            self._state_manager,
            self._installer,
            config=DecisionHandlerConfig.for_integration(),
            idempotency_store=self._idempotency_store,
        )
        self._demand_provider = None
        self._snapshot_creator = None
        self._data_publisher = None
        if (
            self._config.northbound_enable
            or self._config.data_endpoint_url is not None
            or self._config.data_outbox_dir is not None
        ):
            from sdn.adapters.data_event_publisher import (
                DataEventPublisher,
                DataPublisherConfig,
            )
            from sdn.adapters.http_control_api import HTTPDemandReleaseProvider
            from sdn.monitoring.snapshot_creator import SnapshotCreator

            self._demand_provider = HTTPDemandReleaseProvider()
            self._snapshot_creator = SnapshotCreator(
                self._topology,
                self._state_manager,
                self._demand_provider,
            )
            self._demand_provider._origin_refs_lookup = (
                lambda rid: self._snapshot_creator.get_run_origin_refs(rid)
                if self._snapshot_creator is not None
                else None
            )
            if (
                self._config.data_endpoint_url is not None
                or self._config.data_outbox_dir is not None
            ):
                self._data_publisher = DataEventPublisher(
                    DataPublisherConfig(
                        endpoint_base_url=self._config.data_endpoint_url,
                        outbox_dir=self._config.data_outbox_dir,
                        auth_token=self._config.northbound_auth_token,
                    )
                )

        self._lock = threading.RLock()
        self._worker_lock = threading.Lock()
        self._bootstrapped = False
        self._run_initialized = False
        self._readiness_state = S0ReadinessState.WAITING_FOR_SWITCHES
        self._last_report: Optional[S0BootstrapReport] = None
        self._cached_flow_evidences: dict[str, CachedFlowVerificationEvidence] = {}
        self._reconcile_attempt_count = 0

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
    def data_publisher(self) -> Any:
        return self._data_publisher

    def build_northbound_http_app(self) -> Any:
        """Build the `SDNNorthboundHTTPApp` WSGI callable wired to this orchestrator."""
        from sdn.adapters.http_control_api import (
            HTTPDemandReleaseProvider,
            NorthboundAPIConfig,
            SDNNorthboundHTTPApp,
        )
        from sdn.monitoring.snapshot_creator import SnapshotCreator

        if self._demand_provider is None:
            self._demand_provider = HTTPDemandReleaseProvider()
        if self._snapshot_creator is None:
            self._snapshot_creator = SnapshotCreator(
                self._topology, self._state_manager, self._demand_provider
            )
            self._demand_provider._origin_refs_lookup = (
                lambda rid: self._snapshot_creator.get_run_origin_refs(rid)
                if self._snapshot_creator is not None
                else None
            )

        def _health_extra() -> dict[str, Any]:
            rid = self._config.run_id
            sv = None
            rec_req = None
            in_flight = None
            try:
                snap = self._state_manager.get_routing_state(rid)
                sv = snap.state_version
                rec_req = snap.reconcile_required
                in_flight = snap.in_flight_batch_id
            except Exception:
                pass
            return {
                "connected_dpids": list(self._driver.connected_dpids),
                "readiness_state": self._readiness_state.value,
                "verified_cached": len(self._cached_flow_evidences),
                "state_version": sv,
                "reconcile_required": rec_req,
                "in_flight_batch_id": in_flight,
            }

        return SDNNorthboundHTTPApp(
            state_manager=self._state_manager,
            decision_handler=self._decision_handler,
            snapshot_creator=self._snapshot_creator,
            demand_provider=self._demand_provider,
            data_publisher=self._data_publisher,
            config=NorthboundAPIConfig(
                bind_host=self._config.northbound_host,
                bind_port=self._config.northbound_port,
                auth_token=self._config.northbound_auth_token,
                default_sync_wait_ms=self._config.northbound_sync_wait_ms,
                default_run_id=self._config.run_id,
            ),
            health_provider=_health_extra,
        )

    @property
    def compiler(self) -> RouteCompiler:
        return self._compiler

    @property
    def verifier(self) -> RuleVerifier:
        return self._verifier

    @property
    def last_report(self) -> Optional[S0BootstrapReport]:
        return self._last_report

    @property
    def readiness_state(self) -> S0ReadinessState:
        with self._lock:
            return self._readiness_state

    @property
    def accumulated_evidences(self) -> dict[str, VerificationEvidence]:
        with self._lock:
            return {k: v.evidence for k, v in self._cached_flow_evidences.items()}

    def invalidate_flow_evidence(
        self,
        flow_group_id: Optional[str] = None,
        *,
        dpid: Optional[int] = None,
    ) -> None:
        """
        Invalidate accumulated verification evidence:
        - If `flow_group_id` is given: invalidate that specific flow.
        - If `dpid` is given: invalidate all flows whose cached path traverses `dpid`.
        - If neither is given: invalidate all 48 cached flow evidences.
        """
        with self._lock:
            if flow_group_id is not None:
                self._cached_flow_evidences.pop(flow_group_id, None)
                return
            if dpid is not None:
                to_drop = [
                    fid
                    for fid, item in self._cached_flow_evidences.items()
                    if int(dpid) in item.conn_epoch_by_dpid
                ]
                for fid in to_drop:
                    self._cached_flow_evidences.pop(fid, None)
                return
            self._cached_flow_evidences.clear()

    def on_switch_connected(self, datapath: Any) -> bool:
        """
        Handle switch handshake / features reply.
        Returns `True` if all 6 expected switches (`DPID 1..6`) are now connected.
        If the run was already initialized and a switch reconnected, invalidates cached
        evidence for that DPID and marks controller restart / reconciliation required.
        """
        replaced = self._driver.register_datapath(datapath)
        dpid = int(getattr(datapath, "id"))
        conn_epoch = self._driver.get_connection_epoch(dpid)
        with self._lock:
            if self._run_initialized and (replaced or conn_epoch > 1):
                self.invalidate_flow_evidence(dpid=dpid)
                self._readiness_state = S0ReadinessState.RECONCILIATION_REQUIRED
                if not self._state_manager.is_reconcile_required(self._config.run_id):
                    self._decision_handler.notify_controller_restart(
                        self._config.run_id, _utc_now_rfc3339()
                    )
        return self._driver.all_expected_switches_connected

    def on_switch_disconnected(
        self,
        dpid: Optional[int],
        reason: str = "DEAD_DISPATCHER",
        *,
        datapath: Optional[Any] = None,
    ) -> bool:
        """
        Handle switch disconnect: fail waiters and require reconciliation if active.
        Returns `False` if the disconnect came from an unhandshaked socket (`dpid is None`)
        or a stale/already-replaced datapath (`Phase 7.3`, `Phase 9.1`).
        """
        eff_dpid = dpid if dpid is not None else getattr(datapath, "id", None)
        unregistered = self._driver.unregister_datapath(
            eff_dpid, reason=reason, datapath=datapath
        )
        if not unregistered or eff_dpid is None:
            return False
        dpid_int = int(eff_dpid)
        with self._lock:
            self.invalidate_flow_evidence(dpid=dpid_int)
            if self._run_initialized:
                self._readiness_state = S0ReadinessState.RECONCILIATION_REQUIRED
                if not self._state_manager.is_reconcile_required(self._config.run_id):
                    self._decision_handler.notify_controller_restart(
                        self._config.run_id, _utc_now_rfc3339()
                    )
            else:
                self._readiness_state = S0ReadinessState.WAITING_FOR_SWITCHES
        return True

    def handle_state_change_event(
        self,
        ev: Any,
        *,
        dead_dispatcher_state: Any = None,
    ) -> bool:
        """
        Safely process a Ryu / OS-Ken `EventOFPStateChange` event (`Phase 9.1`):
        - Ignores non-`DEAD_DISPATCHER` transitions.
        - Ignores `ev.datapath is None` or pre-handshake `ev.datapath.id is None` without raising `TypeError`.
        - Ignores stale disconnects from an already-replaced datapath connection while preserving `connection_epoch`.
        - Unregisters and marks reconciliation required when an active connected datapath disconnects.
        """
        state = getattr(ev, "state", None)
        if dead_dispatcher_state is not None and state != dead_dispatcher_state:
            return False
        dp = getattr(ev, "datapath", None)
        if dp is None:
            return False
        dpid = getattr(dp, "id", None)
        return self.on_switch_disconnected(dpid, reason="DEAD_DISPATCHER", datapath=dp)

    def verify_connected_switches_and_ports(
        self,
        port_map_by_dpid: Optional[Mapping[int, Any]] = None,
        *,
        query_live_port_desc: bool = False,
        observed_peer_links: Optional[Mapping[tuple[str, int], tuple[str, int]]] = None,
    ) -> PortAndAdjacencyValidationReport:
        """
        Verify all 6 canonical DPIDs (`1..6`) are connected and validate switch ports
        against `configs/network/links.csv` (`Phase 7.5`).
        """
        expected_dpids = {sw.dpid for sw in self._topology.get_all_switches()}
        connected = set(self._driver.connected_dpids)
        missing = sorted(expected_dpids - connected)
        if missing:
            raise SwitchDriverError(
                f"Cannot bootstrap S0: missing switch DPIDs {missing}; connected={sorted(connected)}."
            )

        eff_port_map: Optional[Mapping[int, Any]] = port_map_by_dpid
        if eff_port_map is None and (
            query_live_port_desc or self._config.verify_port_desc_on_bootstrap
        ):
            queried: dict[int, Any] = {}
            for sw in self._topology.get_all_switches():
                queried[sw.dpid] = self._driver.read_port_desc(sw.dpid)
            eff_port_map = queried

        if eff_port_map is not None:
            report = self._topology.validate_live_port_descriptions(
                eff_port_map,
                observed_peer_links=observed_peer_links,
            )
            if not report.port_desc_validated:
                raise SwitchDriverError(
                    f"Port validation failed: missing_switches={report.missing_switches}, "
                    f"missing_ports={report.missing_ports_by_switch}, "
                    f"down_ports={report.down_ports_by_switch}."
                )
            if observed_peer_links is not None and not report.link_adjacency_verified:
                raise SwitchDriverError(
                    f"Link adjacency validation failed: {report.adjacency_mismatches}."
                )
            return report

        return PortAndAdjacencyValidationReport(
            catalog_validated=True,
            port_desc_validated=False,
            link_adjacency_verified=False,
            adjacency_status="CATALOG_ONLY_PORT_DESC_NOT_QUERIED",
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
        accepted = dp.send_msg(msg)
        if accepted is False:
            raise SwitchDriverError(
                f"Helper FlowMod (cookie={cookie:#x}) rejected on DPID {getattr(dp, 'id', '?')}."
            )

    def install_optional_s0_pingall_helpers(
        self, *, target_dpids: Optional[set[int]] = None
    ) -> int:
        """
        Install optional intra-edge L3 rules (8 pairs) and/or controlled ARP rules
        ONLY when explicitly enabled in `S0AppConfig`.
        Uses dedicated helper cookies (`0xFFF0`, `0xFFF1`) and lower priority (`50`)
        so it never interferes with the 48 canonical inter-edge TE flows (`0x103..0x806`).
        Confirms installation with a Barrier on every touched switch (`Phase 7.1`, `Phase 7.3`).
        """
        installed_count = 0
        if not (self._config.enable_intra_edge_l3 or self._config.enable_arp_edge_local_flood):
            return 0

        touched_dpids: set[int] = set()
        for sw in self._topology.get_all_switches():
            if sw.role != "edge":
                continue
            if target_dpids is not None and sw.dpid not in target_dpids:
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
                    touched_dpids.add(sw.dpid)

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
                    touched_dpids.add(sw.dpid)

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
                    touched_dpids.add(sw.dpid)

        if self._config.enable_arp_edge_local_flood:
            # Core switch `c1` forwards ARP to all other edge-facing ports (`c2` is untouched -> 0 loops)
            c1_sw = self._topology.get_switch("c1")
            if target_dpids is None or c1_sw.dpid in target_dpids:
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
                    touched_dpids.add(c1_sw.dpid)

        for dpid in sorted(touched_dpids):
            b_rec = self._driver.send_barrier(dpid, phase=SwitchPhaseTag.BOOTSTRAP)
            if not b_rec.confirmed:
                raise SwitchDriverError(
                    f"Helper Barrier failed on switch '{b_rec.switch_id}' (dpid={dpid}): {b_rec.error_reason}"
                )

        return installed_count

    def _record_cached_flow_evidence(
        self,
        *,
        run_id: str,
        compiled_plan: CompiledPathPlan,
        evidence: VerificationEvidence,
        verified_at_utc: str,
        packet_count_by_dpid: Optional[dict[int, int]] = None,
    ) -> CachedFlowVerificationEvidence:
        rule_gens: dict[int, int] = {}
        conn_epochs: dict[int, int] = {}
        pkt_counts: dict[int, int] = {}

        for hop in compiled_plan.ordered_hop_rules:
            rule_gens[hop.dpid] = self._driver.get_rule_generation(hop)
            conn_epochs[hop.dpid] = self._driver.get_connection_epoch(hop.dpid)
            if packet_count_by_dpid is not None and hop.dpid in packet_count_by_dpid:
                pkt_counts[hop.dpid] = packet_count_by_dpid[hop.dpid]
            else:
                key = (hop.dpid, hop.flow_group_id, compiled_plan.path_id)
                with self._driver._lock:
                    pkt_counts[hop.dpid] = self._driver._baseline_packet_counts.get(key, 0)

        cached = CachedFlowVerificationEvidence(
            run_id=run_id,
            flow_group_id=compiled_plan.flow_group_id,
            path_id=compiled_plan.path_id,
            rule_generation_by_dpid=rule_gens,
            conn_epoch_by_dpid=conn_epochs,
            last_packet_count_by_dpid=pkt_counts,
            verified_at_utc=verified_at_utc,
            evidence=evidence,
        )
        with self._lock:
            self._cached_flow_evidences[compiled_plan.flow_group_id] = cached
        return cached

    def _validate_cached_flow_evidence(
        self,
        *,
        run_id: str,
        compiled_plan: CompiledPathPlan,
        now_utc: str,
    ) -> Optional[VerificationEvidence]:
        """
        Validate whether previously accumulated `CachedFlowVerificationEvidence` for
        `compiled_plan.flow_group_id` is still valid at `now_utc` (`Phase 7.2`):
        - Same `run_id` and `path_id`
        - Age `<= max_evidence_age_s`
        - Switch connection epoch unchanged on all 3 hops
        - Rule generation unchanged on all 3 hops
        - Live `FlowStats` confirms all 3 hop rules still exist with matching `out_port`
        - Live `packet_count` on all 3 hops has not reset (`pkt_now >= last_packet_count`)
        """
        fid = compiled_plan.flow_group_id
        with self._lock:
            cached = self._cached_flow_evidences.get(fid)
        if cached is None:
            return None
        if cached.run_id != run_id or cached.path_id != compiled_plan.path_id:
            self.invalidate_flow_evidence(fid)
            return None

        now_dt = parse_utc_timestamp(format_utc_timestamp(now_utc))
        ver_dt = parse_utc_timestamp(cached.verified_at_utc)
        age_s = (now_dt - ver_dt).total_seconds()
        if age_s < 0 or age_s > self._config.max_evidence_age_s:
            self.invalidate_flow_evidence(fid)
            return None

        updated_pkts: dict[int, int] = {}
        for hop in compiled_plan.ordered_hop_rules:
            if (
                self._driver.get_connection_epoch(hop.dpid)
                != cached.conn_epoch_by_dpid.get(hop.dpid)
            ):
                self.invalidate_flow_evidence(fid)
                return None
            if (
                self._driver.get_rule_generation(hop)
                != cached.rule_generation_by_dpid.get(hop.dpid)
            ):
                self.invalidate_flow_evidence(fid)
                return None

            try:
                entries = self._driver.read_flow_stats(
                    hop.dpid,
                    flow_group_id=fid,
                    cookie_int=hop.cookie_int,
                )
            except Exception:
                self.invalidate_flow_evidence(fid)
                return None

            matched = [
                e
                for e in entries
                if (e.table_id, e.priority, e.match.match_tuple_key)
                == hop.table_entry_key
                and e.cookie_int == hop.cookie_int
                and e.out_port == hop.out_port
            ]
            if not matched:
                self.invalidate_flow_evidence(fid)
                return None

            if self._driver._active_probe_callback is None:
                pkt_now = sum(e.packet_count for e in matched)
                prev_pkt = cached.last_packet_count_by_dpid.get(hop.dpid, 0)
                if (
                    pkt_now < prev_pkt
                    or pkt_now < self._driver.config.min_counter_packet_delta
                ):
                    # Counter reset detected! Reset baseline to pkt_now and invalidate cached evidence
                    key = (hop.dpid, fid, compiled_plan.path_id)
                    with self._driver._lock:
                        self._driver._baseline_packet_counts[key] = pkt_now
                    self.invalidate_flow_evidence(fid)
                    return None
                updated_pkts[hop.dpid] = pkt_now
                key = (hop.dpid, fid, compiled_plan.path_id)
                with self._driver._lock:
                    self._driver._baseline_packet_counts[key] = pkt_now

        if updated_pkts or cached.evidence.verified_at_utc != now_utc:
            refreshed_ev = replace(cached.evidence, verified_at_utc=now_utc)
            with self._lock:
                if fid in self._cached_flow_evidences:
                    self._cached_flow_evidences[fid] = CachedFlowVerificationEvidence(
                        run_id=cached.run_id,
                        flow_group_id=cached.flow_group_id,
                        path_id=cached.path_id,
                        rule_generation_by_dpid=cached.rule_generation_by_dpid,
                        conn_epoch_by_dpid=cached.conn_epoch_by_dpid,
                        last_packet_count_by_dpid=updated_pkts or cached.last_packet_count_by_dpid,
                        verified_at_utc=now_utc,
                        evidence=refreshed_ev,
                    )
            return refreshed_ev
        return cached.evidence

    def bootstrap_and_verify(
        self,
        *,
        verified_at_utc: Optional[str] = None,
        port_map_by_dpid: Optional[Mapping[int, Any]] = None,
        query_live_port_desc: bool = False,
        observed_peer_links: Optional[Mapping[tuple[str, int], tuple[str, int]]] = None,
    ) -> S0BootstrapReport:
        """
        Execute the full S0 bootstrap sequence once all 6 switches are connected (`Phase 7.1`):
        1. Verify DPIDs (`1..6`), `topology_hash`, and optional `PortDesc` / port mappings.
        2. Install optional intra-edge / ARP helpers (with Barrier confirmation).
        3. Install downstream P1/P2 branches + Barrier, then source-edge P1 + Barrier,
           then read back `FlowStats` across all 48 flows (`install_initial_paths_and_readback`).
        4. Never report `bootstrapped_48_inter_edge_installed=True` or `pingall_*_ready=True`
           if any FlowMod, Barrier, FlowStats read-back, or helper installation failed!
        5. Only mark `VERIFIED` (`verified_48_flows=True`) when all 48 flows produced real
           counter/probe `VerificationEvidence`.
        """
        with self._worker_lock:
            ts = format_utc_timestamp(verified_at_utc or _utc_now_rfc3339())
            with self._lock:
                self._readiness_state = S0ReadinessState.BOOTSTRAPPING
                self.invalidate_flow_evidence()

            port_report = self.verify_connected_switches_and_ports(
                port_map_by_dpid=port_map_by_dpid,
                query_live_port_desc=query_live_port_desc,
                observed_peer_links=observed_peer_links,
            )

            helpers_ok = True
            helper_err: Optional[str] = None
            try:
                self.install_optional_s0_pingall_helpers()
            except Exception as exc:
                helpers_ok = False
                helper_err = str(exc)

            if not helpers_ok:
                with self._lock:
                    if not self._run_initialized:
                        snap = self._state_manager.initialize_run(
                            run_id=self._config.run_id,
                            bootstrapped_at_utc=ts,
                            verification_evidences=None,
                        )
                        self._run_initialized = True
                    else:
                        self._decision_handler.notify_controller_restart(
                            self._config.run_id, ts
                        )
                        snap = self._state_manager.get_routing_state(self._config.run_id)
                    self._readiness_state = S0ReadinessState.INSTALL_FAILED

                report = S0BootstrapReport(
                    run_id=self._config.run_id,
                    topology_version=self._topology.topology_version,
                    topology_hash=self._topology.topology_hash,
                    connected_dpids=self._driver.connected_dpids,
                    bootstrapped_48_inter_edge_installed=False,
                    verified_48_flows=False,
                    state_version=snap.state_version,
                    reconcile_required=snap.reconcile_required,
                    pingall_inter_edge_only_ready=False,
                    pingall_full_mesh_ready=False,
                    intra_edge_pairs_covered=False,
                    arp_handling_configured=False,
                    note=f"S0 helper rule installation failed: {helper_err}",
                    readiness_state=S0ReadinessState.INSTALL_FAILED,
                    rules_readback_ok=False,
                    helpers_installed_ok=False,
                    failed_stage="HELPER_RULES",
                    verified_flow_count=0,
                    pending_traffic_flow_count=48,
                    port_validation_report=port_report,
                )
                self._last_report = report
                return report

            # During initial S0 bootstrap (before traffic starts), do not block 48x1.0s
            # waiting on zero counters; use immediate read-back and let the non-blocking
            # reconcile loop verify flows incrementally once traffic arrives.
            self._driver.set_verification_budget_deadline(0.0)
            try:
                install_rep: BootstrapInstallReport = (
                    self._installer.install_initial_paths_and_readback(
                        run_id=self._config.run_id,
                        installed_at_utc=ts,
                        preinstall_both_downstream_branches=self._config.preinstall_both_downstream_branches,
                    )
                )
            finally:
                self._driver.set_verification_budget_deadline(None)

            for fid, ev in install_rep.verified_evidences.items():
                init_pid = self._topology.get_initial_candidate_path(fid).path_id
                plan = self._compiler.compile_path(init_pid)
                self._record_cached_flow_evidence(
                    run_id=self._config.run_id,
                    compiled_plan=plan,
                    evidence=ev,
                    verified_at_utc=ts,
                )

            rules_installed_ok = install_rep.rules_installed_48
            all_48_verified = bool(rules_installed_ok and install_rep.all_48_verified)

            with self._lock:
                if self._run_initialized:
                    if all_48_verified:
                        snap = self._state_manager.reconcile_run_state(
                            run_id=self._config.run_id,
                            verified_evidences=install_rep.verified_evidences,
                            reconciled_at_utc=ts,
                            reconciliation_id="bootstrap_s0",
                        )
                    else:
                        if not self._state_manager.is_reconcile_required(self._config.run_id):
                            self._decision_handler.notify_controller_restart(
                                self._config.run_id, ts
                            )
                        snap = self._state_manager.get_routing_state(self._config.run_id)
                else:
                    snap = self._state_manager.initialize_run(
                        run_id=self._config.run_id,
                        bootstrapped_at_utc=ts,
                        verification_evidences=(
                            install_rep.verified_evidences if all_48_verified else None
                        ),
                    )
                    self._run_initialized = True
                self._bootstrapped = rules_installed_ok

                if not rules_installed_ok:
                    self._readiness_state = S0ReadinessState.INSTALL_FAILED
                elif all_48_verified:
                    self._readiness_state = S0ReadinessState.VERIFIED
                else:
                    self._readiness_state = S0ReadinessState.VERIFICATION_PENDING
                current_readiness = self._readiness_state

            pingall_inter_edge = bool(rules_installed_ok)
            pingall_full = bool(
                rules_installed_ok
                and helpers_ok
                and self._config.enable_intra_edge_l3
                and self._config.enable_arp_edge_local_flood
            )
            if not rules_installed_ok:
                note = (
                    f"S0 rule installation FAILED at stage '{install_rep.failed_stage}': "
                    f"{install_rep.error_detail}"
                )
            elif all_48_verified:
                note = "All 48 inter-edge flows VERIFIED via FlowStats + data-plane evidence."
            else:
                note = (
                    f"48 inter-edge flows installed and FlowStats read-back confirmed on P1 "
                    f"({len(install_rep.verified_evidences)}/48 verified, "
                    f"{len(install_rep.pending_traffic_flow_ids)}/48 awaiting traffic)."
                )

            report = S0BootstrapReport(
                run_id=self._config.run_id,
                topology_version=self._topology.topology_version,
                topology_hash=self._topology.topology_hash,
                connected_dpids=self._driver.connected_dpids,
                bootstrapped_48_inter_edge_installed=rules_installed_ok,
                verified_48_flows=all_48_verified,
                state_version=snap.state_version,
                reconcile_required=snap.reconcile_required,
                pingall_inter_edge_only_ready=pingall_inter_edge,
                pingall_full_mesh_ready=pingall_full,
                intra_edge_pairs_covered=bool(
                    rules_installed_ok and helpers_ok and self._config.enable_intra_edge_l3
                ),
                arp_handling_configured=bool(
                    rules_installed_ok and helpers_ok and self._config.enable_arp_edge_local_flood
                ),
                note=note,
                readiness_state=current_readiness,
                rules_readback_ok=install_rep.flow_stats_readback_ok,
                helpers_installed_ok=helpers_ok,
                failed_stage=install_rep.failed_stage,
                verified_flow_count=len(install_rep.verified_evidences),
                pending_traffic_flow_count=len(install_rep.pending_traffic_flow_ids),
                port_validation_report=port_report,
            )
            self._last_report = report
            return report

    def verify_or_reconcile_after_traffic(
        self, *, reconciled_at_utc: Optional[str] = None
    ) -> Optional[RoutingStateSnapshot]:
        """
        Incremental, non-destructive verification/reconciliation pass (`Phase 7.2`, `Phase 7.3`):
        1. Re-validates any previously accumulated `CachedFlowVerificationEvidence` against
           live `FlowStats`, switch connection epochs, rule generations, counter resets, and TTL.
        2. For remaining flows, checks live `FlowStats` + counter/probe evidence:
           - If verified: caches `CachedFlowVerificationEvidence`.
           - If rules are missing/corrupted (`not rule_readback_ok`): records in `missing_rule_flows`
             to reinstall P1 (and restores optional helper rules on affected switches).
           - If rules are present (`rule_readback_ok=True`) and only awaiting traffic: does NOT
             reinstall or send any `FlowMod`.
        3. When all 48 flows have valid `CachedFlowVerificationEvidence`, performs a final
           consistency check across all 48 evidences and commits `StateManager.reconcile_run_state()`.
        """
        with self._worker_lock:
            ts = format_utc_timestamp(reconciled_at_utc or _utc_now_rfc3339())
            if not self._driver.all_expected_switches_connected:
                return None

            # In non-blocking background reconcile ticks, check counters immediately per poll
            # cycle rather than blocking 48x1.0s across idle flows.
            self._driver.set_verification_budget_deadline(0.0)
            try:
                return self._verify_or_reconcile_after_traffic_locked(ts)
            finally:
                self._driver.set_verification_budget_deadline(None)

    def _verify_or_reconcile_after_traffic_locked(
        self, ts: str
    ) -> Optional[RoutingStateSnapshot]:
        if True:
            evidences: dict[str, VerificationEvidence] = {}
            missing_rule_flows: dict[str, str] = {}
            missing_rule_dpids: set[int] = set()
            awaiting_traffic: list[str] = []

            for fg in self._topology.get_all_flow_groups():
                fid = fg.flow_group_id
                init_pid = self._topology.get_initial_candidate_path(fid).path_id
                init_plan = self._compiler.compile_path(init_pid)

                # Step 1: Check if accumulated evidence for `fid` is still valid
                cached_ev = self._validate_cached_flow_evidence(
                    run_id=self._config.run_id,
                    compiled_plan=init_plan,
                    now_utc=ts,
                )
                if cached_ev is not None:
                    evidences[fid] = cached_ev
                    continue

                # Step 2: Check live switch state for `fid`
                res = self._verifier.discover_active_flow_path(
                    self._config.run_id, fid, ts
                )
                if res.is_verified and res.evidence is not None and res.actual_path_id is not None:
                    act_plan = self._compiler.compile_path(res.actual_path_id)
                    self._record_cached_flow_evidence(
                        run_id=self._config.run_id,
                        compiled_plan=act_plan,
                        evidence=res.evidence,
                        verified_at_utc=ts,
                    )
                    evidences[fid] = res.evidence
                elif not res.rule_readback_ok:
                    missing_rule_flows[fid] = init_pid
                    for hop in init_plan.ordered_hop_rules:
                        missing_rule_dpids.add(hop.dpid)
                else:
                    awaiting_traffic.append(fid)

            # Step 3: Reinstall ONLY flows whose rules were actually missing/wiped
            if missing_rule_flows:
                if (
                    self._config.enable_intra_edge_l3
                    or self._config.enable_arp_edge_local_flood
                ):
                    try:
                        self.install_optional_s0_pingall_helpers(
                            target_dpids=missing_rule_dpids
                        )
                    except Exception:
                        with self._lock:
                            self._readiness_state = S0ReadinessState.INSTALL_FAILED
                        return None

                # Reinstall missing flow rules via two-phase install + Barrier, then check verification
                reinstall_xids: dict[str, list[int]] = {
                    fid: [] for fid in missing_rule_flows
                }
                ds_dpids: set[int] = set()
                src_dpids: set[int] = set()
                try:
                    for fid, target_pid in missing_rule_flows.items():
                        t_plan = self._compiler.compile_path(target_pid)
                        plans = (
                            self._compiler.compile_flow_candidates(fid)
                            if self._config.preinstall_both_downstream_branches
                            else (t_plan,)
                        )
                        for pl in plans:
                            for ds_rule in pl.downstream_rules:
                                rec = self._driver.send_flow_mod(
                                    ds_rule,
                                    self._installer.switch_driver.send_flow_mod.__defaults__  # type: ignore[attr-defined]
                                    if False
                                    else __import__(
                                        "sdn.flow_management.switch_driver",
                                        fromlist=["FlowModCommand"],
                                    ).FlowModCommand.ADD,
                                    phase=SwitchPhaseTag.BOOTSTRAP,
                                )
                                reinstall_xids[fid].append(rec.xid)
                                ds_dpids.add(ds_rule.dpid)
                                if not rec.accepted:
                                    raise SwitchDriverError(rec.error_reason or "DS_FLOWMOD_FAILED")
                    for dpid in sorted(ds_dpids):
                        b_rec = self._driver.send_barrier(dpid, phase=SwitchPhaseTag.BOOTSTRAP)
                        if not b_rec.confirmed:
                            raise SwitchDriverError(b_rec.error_reason or "DS_BARRIER_FAILED")

                    for fid, target_pid in missing_rule_flows.items():
                        t_plan = self._compiler.compile_path(target_pid)
                        src_rule = t_plan.source_edge_rule
                        rec = self._driver.send_flow_mod(
                            src_rule,
                            __import__(
                                "sdn.flow_management.switch_driver",
                                fromlist=["FlowModCommand"],
                            ).FlowModCommand.ADD,
                            phase=SwitchPhaseTag.BOOTSTRAP,
                        )
                        reinstall_xids[fid].append(rec.xid)
                        src_dpids.add(src_rule.dpid)
                        if not rec.accepted:
                            raise SwitchDriverError(rec.error_reason or "SRC_FLOWMOD_FAILED")
                    for dpid in sorted(src_dpids):
                        b_rec = self._driver.send_barrier(dpid, phase=SwitchPhaseTag.BOOTSTRAP)
                        if not b_rec.confirmed:
                            raise SwitchDriverError(b_rec.error_reason or "SRC_BARRIER_FAILED")
                except Exception:
                    with self._lock:
                        self._readiness_state = S0ReadinessState.INSTALL_FAILED
                    return None

                for fid, target_pid in missing_rule_flows.items():
                    t_plan = self._compiler.compile_path(target_pid)
                    v_res = self._verifier.verify_flow_path(
                        run_id=self._config.run_id,
                        flow_group_id=fid,
                        expected_path_id=target_pid,
                        verified_at_utc=ts,
                        barrier_confirmed=True,
                        xid_refs=reinstall_xids[fid],
                    )
                    if not v_res.rule_readback_ok:
                        with self._lock:
                            self._readiness_state = S0ReadinessState.INSTALL_FAILED
                        return None
                    if v_res.is_verified and v_res.evidence is not None:
                        self._record_cached_flow_evidence(
                            run_id=self._config.run_id,
                            compiled_plan=t_plan,
                            evidence=v_res.evidence,
                            verified_at_utc=ts,
                        )
                        evidences[fid] = v_res.evidence
                    else:
                        awaiting_traffic.append(fid)

            total_flows = len(self._topology.get_all_flow_groups())
            if awaiting_traffic or len(evidences) != total_flows:
                with self._lock:
                    if self._readiness_state != S0ReadinessState.INSTALL_FAILED:
                        self._readiness_state = S0ReadinessState.VERIFICATION_PENDING
                return None

            # Final pre-commit validation across all 48 accumulated evidences
            for fg in self._topology.get_all_flow_groups():
                fid = fg.flow_group_id
                ev = evidences.get(fid)
                if ev is None:
                    return None
                plan = self._compiler.compile_path(ev.verified_path_id)
                refreshed_ev = self._validate_cached_flow_evidence(
                    run_id=self._config.run_id,
                    compiled_plan=plan,
                    now_utc=ts,
                )
                if refreshed_ev is None:
                    with self._lock:
                        self._readiness_state = S0ReadinessState.VERIFICATION_PENDING
                    return None
                evidences[fid] = refreshed_ev

            snap = self._state_manager.reconcile_run_state(
                run_id=self._config.run_id,
                verified_evidences=evidences,
                reconciled_at_utc=ts,
                reconciliation_id="reconcile_s0",
            )
            with self._lock:
                self._readiness_state = S0ReadinessState.VERIFIED
                self._bootstrapped = True
            return snap

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
                f"(verified={len(self.accumulated_evidences)}/48, readiness={self.readiness_state.value})."
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


def _resolve_ofp_base_app(app_manager: Any, fw_name: str = "") -> Any:
    """
    Resolve the OpenFlow controller application base class across both classic Ryu
    (`ryu.base.app_manager.RyuApp`) and OS-Ken (`os_ken.base.app_manager.OSKenApp`).
    Raises `AttributeError` if the loaded `app_manager` module provides neither.
    """
    if fw_name == "os_ken":
        candidates = ("OSKenApp", "RyuApp")
    else:
        candidates = ("RyuApp", "OSKenApp")
    for attr in candidates:
        base_cls = getattr(app_manager, attr, None)
        if base_cls is not None:
            return base_cls
    raise AttributeError(
        f"Module '{getattr(app_manager, '__name__', app_manager)}' has neither 'RyuApp' nor 'OSKenApp'."
    )


try:
    _app_manager, _ofp_event, _handler, _ofproto_v1_3, _hub, _FW_NAME = _import_ofp_framework()
    _BASE_APP = _resolve_ofp_base_app(_app_manager, _FW_NAME)
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
            self.northbound_app = None
            self._northbound_thread = None
            if self.orchestrator._config.northbound_enable:
                self.northbound_app = self.orchestrator.build_northbound_http_app()
                self._northbound_thread = _hub.spawn(self._serve_northbound_http)

        def _serve_northbound_http(self) -> None:
            import eventlet  # type: ignore[import-not-found]
            from eventlet import wsgi  # type: ignore[import-not-found]

            host = self.orchestrator._config.northbound_host
            port = self.orchestrator._config.northbound_port
            self.logger.info("Starting SDN Northbound HTTP API on http://%s:%d", host, port)
            sock = eventlet.listen((host, port))
            wsgi.server(sock, self.northbound_app, log_output=False)

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
            max_attempts = max(1, int(self.orchestrator._config.max_reconcile_attempts))
            attempts = 0
            try:
                while (
                    self.orchestrator.state_manager.is_reconcile_required(
                        self.orchestrator._config.run_id
                    )
                    and attempts < max_attempts
                ):
                    _hub.sleep(poll_s)
                    if not self.orchestrator.driver.all_expected_switches_connected:
                        continue
                    attempts += 1
                    snap = self.orchestrator.verify_or_reconcile_after_traffic()
                    if snap is not None and not snap.reconcile_required:
                        self.logger.info(
                            "S0 Reconciled & VERIFIED all 48 flows (state_version=%d).",
                            snap.state_version,
                        )
                        break
                    self.logger.info(
                        "S0 Verification pending (%d/48 flows verified, attempt %d/%d).",
                        len(self.orchestrator.accumulated_evidences),
                        attempts,
                        max_attempts,
                    )
            finally:
                self._reconcile_loop_running = False

        @_set_ev_cls(_ofp_event.EventOFPBarrierReply, _MAIN_DISPATCHER)
        def barrier_reply_handler(self, ev: Any) -> None:
            self.orchestrator.driver.handle_barrier_reply(ev)

        @_set_ev_cls(_ofp_event.EventOFPFlowStatsReply, _MAIN_DISPATCHER)
        def flow_stats_reply_handler(self, ev: Any) -> None:
            self.orchestrator.driver.handle_flow_stats_reply(ev)

        if hasattr(_ofp_event, "EventOFPPortDescStatsReply"):
            @_set_ev_cls(_ofp_event.EventOFPPortDescStatsReply, _MAIN_DISPATCHER)
            def port_desc_reply_handler(self, ev: Any) -> None:
                self.orchestrator.driver.handle_port_desc_reply(ev)

        @_set_ev_cls(_ofp_event.EventOFPErrorMsg, [_CONFIG_DISPATCHER, _MAIN_DISPATCHER])
        def error_msg_handler(self, ev: Any) -> None:
            self.orchestrator.driver.handle_error_msg(ev)

        @_set_ev_cls(_ofp_event.EventOFPStateChange, [_MAIN_DISPATCHER, _DEAD_DISPATCHER])
        def state_change_handler(self, ev: Any) -> None:
            self.orchestrator.handle_state_change_event(
                ev, dead_dispatcher_state=_DEAD_DISPATCHER
            )
