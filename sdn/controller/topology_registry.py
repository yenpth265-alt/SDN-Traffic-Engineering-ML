"""
TopologyRegistry for the SDN Controller (Integration Guide v1.4 compliant).

Loads, validates, and indexes the canonical Network TopologyBundle and CSV catalogs
without hard-coding lookup data or requiring Linux-only dependencies (Ryu/Mininet/OVS).
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


class TopologyValidationError(ValueError):
    """Raised when TopologyBundle or CSV catalog violates v1.4 contract invariants."""


class TopologyLookupError(KeyError):
    """Raised when a flow, path, switch, host, or link lookup fails or uses legacy IDs."""


_CANONICAL_FLOW_ID_RE = re.compile(r"^0x[1-8]0[1-8]$")
_CANONICAL_PATH_ID_RE = re.compile(r"^(0x[1-8]0[1-8])_(P[12])$")
_CANONICAL_LINK_ID_RE = re.compile(r"^([a-z][0-9]+)>([a-z][0-9]+)$")
_HOST_ID_RE = re.compile(r"^h([1-8])$")
_EDGE_ID_RE = re.compile(r"^e([1-4])$")
_CORE_ID_RE = re.compile(r"^c([1-2])$")


@dataclass(frozen=True)
class HostInfo:
    """Canonical host definition from TopologyBundle."""

    host_id: str
    ip: str
    mac: str
    edge_switch: str
    edge_port: int


@dataclass(frozen=True)
class SwitchInfo:
    """Canonical OpenFlow switch definition from TopologyBundle."""

    switch_id: str
    role: str
    dpid: int
    dpid_hex: str
    openflow_version: str


@dataclass(frozen=True)
class DirectedLink:
    """Canonical directed link definition from TopologyBundle."""

    link_id: str
    from_node: str
    to_node: str
    from_port: int
    to_port: int
    link_type: str
    is_fabric: bool
    capacity_mbps: float
    delay_ms: float


@dataclass(frozen=True)
class FlowGroup:
    """Canonical directed inter-edge flow group definition."""

    stt: int
    flow_group_id: str
    flow_cookie: str
    src_host: str
    dst_host: str
    src_ip: str
    dst_ip: str
    src_edge: str
    dst_edge: str
    udp_port: int

    @property
    def numeric_id(self) -> int:
        """Numeric value of canonical hex flow_group_id (e.g. 0x103 -> 259)."""
        return int(self.flow_group_id, 16)

    @property
    def cookie_int(self) -> int:
        """Numeric OpenFlow cookie value."""
        return int(self.flow_cookie, 16)


@dataclass(frozen=True)
class CandidatePath:
    """Canonical candidate path (P1 via c1 or P2 via c2) for a flow group."""

    path_id: str
    flow_group_id: str
    path_label: str
    core_switch: str
    hop_count: int
    nominal_delay_ms: float
    ordered_link_ids: tuple[str, ...]
    fabric_link_ids: tuple[str, ...]


@dataclass(frozen=True)
class InitialPathEntry:
    """Canonical initial path assignment for a flow group."""

    flow_group_id: str
    initial_path_id: str
    path_label: str


@dataclass(frozen=True)
class SwitchHop:
    """Resolved per-switch hop instructions (in_port -> out_port) along a candidate path."""

    hop_index: int
    switch_id: str
    dpid: int
    role: str
    in_port: int
    out_port: int
    ingress_link_id: str
    egress_link_id: str


def compute_network_topology_hash(bundle_data: Mapping[str, Any]) -> str:
    """
    Compute the SHA-256 topology_hash using the exact canonical algorithm
    defined in network/topology/exporter.py.
    """
    required_core_keys = (
        "schema_version",
        "topology_version",
        "hosts",
        "switches",
        "links",
        "flow_groups",
        "candidate_paths",
        "initial_paths",
    )
    missing = [k for k in required_core_keys if k not in bundle_data]
    if missing:
        raise TopologyValidationError(
            f"Cannot compute topology_hash; missing required core payload keys: {missing}"
        )

    core_payload = {k: bundle_data[k] for k in required_core_keys}
    canonical_bytes = json.dumps(
        core_payload, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(canonical_bytes).hexdigest()


class TopologyRegistry:
    """
    Authoritative SDN Controller registry for topology, flow groups, candidate paths,
    directed links, initial paths, and switch port mappings.
    """

    EXPECTED_HOST_COUNT = 8
    EXPECTED_SWITCH_COUNT = 6
    EXPECTED_DIRECTED_LINK_COUNT = 32
    EXPECTED_FABRIC_LINK_COUNT = 16
    EXPECTED_FLOW_GROUP_COUNT = 48
    EXPECTED_CANDIDATE_PATH_COUNT = 96
    EXPECTED_INITIAL_PATH_COUNT = 48
    EXPECTED_PATH_FABRIC_INCIDENCE_COUNT = 192
    EXPECTED_INCIDENCES_PER_FABRIC_LINK = 12

    def __init__(
        self,
        *,
        schema_version: str,
        topology_version: str,
        topology_hash: str,
        ovs_port_mapping_verified: bool,
        hosts: Sequence[HostInfo],
        switches: Sequence[SwitchInfo],
        links: Sequence[DirectedLink],
        flow_groups: Sequence[FlowGroup],
        candidate_paths: Sequence[CandidatePath],
        initial_paths: Sequence[InitialPathEntry],
    ) -> None:
        self.schema_version = schema_version
        self.topology_version = topology_version
        self.topology_hash = topology_hash
        self.ovs_port_mapping_verified = ovs_port_mapping_verified

        self._hosts_by_id: dict[str, HostInfo] = {h.host_id: h for h in hosts}
        self._switches_by_id: dict[str, SwitchInfo] = {s.switch_id: s for s in switches}
        self._switches_by_dpid: dict[int, SwitchInfo] = {s.dpid: s for s in switches}
        self._links_by_id: dict[str, DirectedLink] = {lk.link_id: lk for lk in links}
        self._links_by_nodes: dict[tuple[str, str], DirectedLink] = {
            (lk.from_node, lk.to_node): lk for lk in links
        }
        self._flows_by_id: dict[str, FlowGroup] = {
            fg.flow_group_id: fg for fg in flow_groups
        }
        self._flows_by_hosts: dict[tuple[str, str], FlowGroup] = {
            (fg.src_host, fg.dst_host): fg for fg in flow_groups
        }
        self._paths_by_id: dict[str, CandidatePath] = {
            cp.path_id: cp for cp in candidate_paths
        }
        self._paths_by_flow: dict[str, dict[str, CandidatePath]] = {}
        for cp in candidate_paths:
            self._paths_by_flow.setdefault(cp.flow_group_id, {})[cp.path_label] = cp

        self._initial_paths_by_flow: dict[str, InitialPathEntry] = {
            ip.flow_group_id: ip for ip in initial_paths
        }

        self._switch_hops_by_path: dict[str, tuple[SwitchHop, ...]] = {}
        for cp in candidate_paths:
            self._switch_hops_by_path[cp.path_id] = self._compile_switch_hops(cp)

    @classmethod
    def from_bundle_dict(
        cls,
        bundle_data: Mapping[str, Any],
        *,
        verify_hash: bool = True,
    ) -> "TopologyRegistry":
        """Load and validate TopologyRegistry from a parsed TopologyBundle dictionary."""
        if not isinstance(bundle_data, Mapping):
            raise TopologyValidationError("TopologyBundle must be a JSON object / mapping.")

        required_top_keys = (
            "schema_version",
            "topology_version",
            "topology_hash",
            "hosts",
            "switches",
            "links",
            "flow_groups",
            "candidate_paths",
            "initial_paths",
        )
        missing_keys = [k for k in required_top_keys if k not in bundle_data]
        if missing_keys:
            raise TopologyValidationError(
                f"TopologyBundle missing required top-level keys: {missing_keys}"
            )

        schema_version = str(bundle_data["schema_version"]).strip()
        topology_version = str(bundle_data["topology_version"]).strip()
        topology_hash = str(bundle_data["topology_hash"]).strip()
        if not schema_version or not topology_version or not topology_hash:
            raise TopologyValidationError(
                "schema_version, topology_version, and topology_hash must be non-empty strings."
            )

        if verify_hash:
            computed_hash = compute_network_topology_hash(bundle_data)
            if computed_hash != topology_hash:
                raise TopologyValidationError(
                    f"TopologyBundle hash mismatch: expected '{topology_hash}', "
                    f"computed '{computed_hash}'."
                )

        hosts = cls._parse_and_validate_hosts(bundle_data["hosts"])
        switches = cls._parse_and_validate_switches(bundle_data["switches"], hosts)
        links = cls._parse_and_validate_links(bundle_data["links"], hosts, switches)
        flow_groups = cls._parse_and_validate_flow_groups(
            bundle_data["flow_groups"], hosts, switches
        )
        candidate_paths = cls._parse_and_validate_candidate_paths(
            bundle_data["candidate_paths"], flow_groups, switches, links
        )
        initial_paths = cls._parse_and_validate_initial_paths(
            bundle_data["initial_paths"], flow_groups, candidate_paths
        )

        if "summary" in bundle_data and bundle_data["summary"] is not None:
            cls._validate_summary_block(bundle_data["summary"])

        ovs_verified = bool(bundle_data.get("ovs_port_mapping_verified", False))

        return cls(
            schema_version=schema_version,
            topology_version=topology_version,
            topology_hash=topology_hash,
            ovs_port_mapping_verified=ovs_verified,
            hosts=hosts,
            switches=switches,
            links=links,
            flow_groups=flow_groups,
            candidate_paths=candidate_paths,
            initial_paths=initial_paths,
        )

    @classmethod
    def from_bundle_file(
        cls,
        bundle_path: str | Path,
        *,
        csv_config_dir: str | Path | None = None,
        verify_hash: bool = True,
    ) -> "TopologyRegistry":
        """
        Load and validate TopologyRegistry from a TopologyBundle JSON file.
        If csv_config_dir is provided, also cross-verifies consistency against
        flow_groups.csv, candidate_paths.csv, initial_paths.csv, and links.csv.
        """
        path = Path(bundle_path)
        if not path.is_file():
            raise TopologyValidationError(f"TopologyBundle file not found: {path}")

        try:
            raw_text = path.read_text(encoding="utf-8")
            bundle_data = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise TopologyValidationError(
                f"Invalid JSON in TopologyBundle file {path}: {exc}"
            ) from exc

        registry = cls.from_bundle_dict(bundle_data, verify_hash=verify_hash)

        if csv_config_dir is not None:
            registry.verify_against_csv_catalogs(csv_config_dir)

        return registry

    def verify_against_csv_catalogs(self, csv_config_dir: str | Path) -> None:
        """
        Cross-check loaded TopologyBundle data against the 4 Network CSV files:
        links.csv, flow_groups.csv, candidate_paths.csv, and initial_paths.csv.
        """
        config_dir = Path(csv_config_dir)
        required_csvs = (
            "links.csv",
            "flow_groups.csv",
            "candidate_paths.csv",
            "initial_paths.csv",
        )
        for filename in required_csvs:
            csv_path = config_dir / filename
            if not csv_path.is_file():
                raise TopologyValidationError(
                    f"Required CSV catalog file missing: {csv_path}"
                )

        # 1. Verify links.csv
        with (config_dir / "links.csv").open(encoding="utf-8", newline="") as f:
            csv_links = list(csv.DictReader(f))
        if len(csv_links) != len(self._links_by_id):
            raise TopologyValidationError(
                f"links.csv row count ({len(csv_links)}) does not match bundle ({len(self._links_by_id)})."
            )
        for row in csv_links:
            lid = row["link_id"].strip()
            if lid not in self._links_by_id:
                raise TopologyValidationError(
                    f"Link '{lid}' in links.csv is missing from TopologyBundle."
                )
            lk = self._links_by_id[lid]
            if (
                lk.from_node != row["from_node"].strip()
                or lk.to_node != row["to_node"].strip()
                or lk.from_port != int(row["from_port"])
                or lk.to_port != int(row["to_port"])
                or lk.link_type != row["link_type"].strip()
                or lk.is_fabric != bool(int(row["is_fabric"]))
                or abs(lk.capacity_mbps - float(row["capacity_mbps"])) > 1e-9
                or abs(lk.delay_ms - float(row["delay_ms"])) > 1e-9
            ):
                raise TopologyValidationError(
                    f"Link '{lid}' attributes in links.csv do not match TopologyBundle."
                )

        # 2. Verify flow_groups.csv
        with (config_dir / "flow_groups.csv").open(encoding="utf-8", newline="") as f:
            csv_flows = list(csv.DictReader(f))
        if len(csv_flows) != len(self._flows_by_id):
            raise TopologyValidationError(
                f"flow_groups.csv row count ({len(csv_flows)}) does not match bundle ({len(self._flows_by_id)})."
            )
        for row in csv_flows:
            fg_id = row["flow_group_id"].strip()
            if fg_id not in self._flows_by_id:
                raise TopologyValidationError(
                    f"Flow '{fg_id}' in flow_groups.csv is missing from TopologyBundle."
                )
            fg = self._flows_by_id[fg_id]
            if (
                fg.stt != int(row["stt"])
                or fg.flow_cookie != row["flow_cookie"].strip()
                or fg.src_host != row["src_host"].strip()
                or fg.dst_host != row["dst_host"].strip()
                or fg.src_ip != row["src_ip"].strip()
                or fg.dst_ip != row["dst_ip"].strip()
                or fg.src_edge != row["src_edge"].strip()
                or fg.dst_edge != row["dst_edge"].strip()
                or fg.udp_port != int(row["udp_port"])
            ):
                raise TopologyValidationError(
                    f"Flow '{fg_id}' attributes in flow_groups.csv do not match TopologyBundle."
                )

        # 3. Verify candidate_paths.csv
        with (config_dir / "candidate_paths.csv").open(encoding="utf-8", newline="") as f:
            csv_paths = list(csv.DictReader(f))
        if len(csv_paths) != len(self._paths_by_id):
            raise TopologyValidationError(
                f"candidate_paths.csv row count ({len(csv_paths)}) does not match bundle ({len(self._paths_by_id)})."
            )
        for row in csv_paths:
            pid = row["path_id"].strip()
            if pid not in self._paths_by_id:
                raise TopologyValidationError(
                    f"Path '{pid}' in candidate_paths.csv is missing from TopologyBundle."
                )
            cp = self._paths_by_id[pid]
            ordered = tuple(
                x.strip() for x in row["ordered_link_ids"].split(";") if x.strip()
            )
            fabric = tuple(
                x.strip() for x in row["fabric_link_ids"].split(";") if x.strip()
            )
            if (
                cp.flow_group_id != row["flow_group_id"].strip()
                or cp.path_label != row["path_label"].strip()
                or cp.core_switch != row["core_switch"].strip()
                or cp.hop_count != int(row["hop_count"])
                or abs(cp.nominal_delay_ms - float(row["nominal_delay_ms"])) > 1e-9
                or cp.ordered_link_ids != ordered
                or cp.fabric_link_ids != fabric
            ):
                raise TopologyValidationError(
                    f"Path '{pid}' attributes in candidate_paths.csv do not match TopologyBundle."
                )

        # 4. Verify initial_paths.csv
        with (config_dir / "initial_paths.csv").open(encoding="utf-8", newline="") as f:
            csv_init = list(csv.DictReader(f))
        if len(csv_init) != len(self._initial_paths_by_flow):
            raise TopologyValidationError(
                f"initial_paths.csv row count ({len(csv_init)}) does not match bundle ({len(self._initial_paths_by_flow)})."
            )
        for row in csv_init:
            fg_id = row["flow_group_id"].strip()
            if fg_id not in self._initial_paths_by_flow:
                raise TopologyValidationError(
                    f"Initial path for flow '{fg_id}' in initial_paths.csv is missing from TopologyBundle."
                )
            ip = self._initial_paths_by_flow[fg_id]
            if (
                ip.initial_path_id != row["initial_path_id"].strip()
                or ip.path_label != row["path_label"].strip()
            ):
                raise TopologyValidationError(
                    f"Initial path for flow '{fg_id}' in initial_paths.csv does not match TopologyBundle."
                )

    # -------------------------------------------------------------------------
    # Validation Helpers
    # -------------------------------------------------------------------------

    @classmethod
    def _validate_summary_block(cls, summary: Mapping[str, Any]) -> None:
        if not isinstance(summary, Mapping):
            raise TopologyValidationError("summary field must be a mapping.")
        expected_summary = {
            "host_count": cls.EXPECTED_HOST_COUNT,
            "switch_count": cls.EXPECTED_SWITCH_COUNT,
            "directed_link_count": cls.EXPECTED_DIRECTED_LINK_COUNT,
            "fabric_directed_link_count": cls.EXPECTED_FABRIC_LINK_COUNT,
            "flow_group_count": cls.EXPECTED_FLOW_GROUP_COUNT,
            "candidate_path_count": cls.EXPECTED_CANDIDATE_PATH_COUNT,
            "path_fabric_incidence_count": cls.EXPECTED_PATH_FABRIC_INCIDENCE_COUNT,
        }
        for key, expected_val in expected_summary.items():
            actual_val = summary.get(key)
            if actual_val != expected_val:
                raise TopologyValidationError(
                    f"Summary invariant mismatch for '{key}': expected {expected_val}, got {actual_val}."
                )

    @classmethod
    def _parse_and_validate_hosts(
        cls, raw_hosts: Any
    ) -> list[HostInfo]:
        if not isinstance(raw_hosts, list):
            raise TopologyValidationError("hosts must be a list.")
        if len(raw_hosts) != cls.EXPECTED_HOST_COUNT:
            raise TopologyValidationError(
                f"Expected {cls.EXPECTED_HOST_COUNT} hosts, got {len(raw_hosts)}."
            )

        hosts: list[HostInfo] = []
        seen_ids: set[str] = set()
        seen_ips: set[str] = set()
        seen_macs: set[str] = set()
        seen_edge_ports: set[tuple[str, int]] = set()

        required_fields = ("host_id", "ip", "mac", "edge_switch", "edge_port")
        for idx, item in enumerate(raw_hosts):
            if not isinstance(item, Mapping):
                raise TopologyValidationError(f"Host at index {idx} must be an object.")
            missing = [k for k in required_fields if k not in item or item[k] is None]
            if missing:
                raise TopologyValidationError(
                    f"Host at index {idx} missing fields: {missing}"
                )

            host_id = str(item["host_id"]).strip()
            ip = str(item["ip"]).strip()
            mac = str(item["mac"]).strip().lower()
            edge_switch = str(item["edge_switch"]).strip()
            edge_port = int(item["edge_port"])

            m_host = _HOST_ID_RE.match(host_id)
            if not m_host:
                raise TopologyValidationError(
                    f"Invalid host_id '{host_id}': expected h1..h8."
                )
            host_num = int(m_host.group(1))
            expected_edge = f"e{(host_num - 1) // 2 + 1}"
            expected_port = (host_num - 1) % 2 + 1
            if edge_switch != expected_edge or edge_port != expected_port:
                raise TopologyValidationError(
                    f"Host '{host_id}' attachment ({edge_switch}, port {edge_port}) "
                    f"violates v1.4 Section 3.1 mapping (expected {expected_edge}, port {expected_port})."
                )

            if host_id in seen_ids:
                raise TopologyValidationError(f"Duplicate host_id '{host_id}'.")
            if ip in seen_ips:
                raise TopologyValidationError(f"Duplicate host IP '{ip}'.")
            if mac in seen_macs:
                raise TopologyValidationError(f"Duplicate host MAC '{mac}'.")
            if (edge_switch, edge_port) in seen_edge_ports:
                raise TopologyValidationError(
                    f"Duplicate host attachment on ({edge_switch}, port {edge_port})."
                )

            seen_ids.add(host_id)
            seen_ips.add(ip)
            seen_macs.add(mac)
            seen_edge_ports.add((edge_switch, edge_port))

            hosts.append(
                HostInfo(
                    host_id=host_id,
                    ip=ip,
                    mac=mac,
                    edge_switch=edge_switch,
                    edge_port=edge_port,
                )
            )
        return hosts

    @classmethod
    def _parse_and_validate_switches(
        cls, raw_switches: Any, hosts: Sequence[HostInfo]
    ) -> list[SwitchInfo]:
        if not isinstance(raw_switches, list):
            raise TopologyValidationError("switches must be a list.")
        if len(raw_switches) != cls.EXPECTED_SWITCH_COUNT:
            raise TopologyValidationError(
                f"Expected {cls.EXPECTED_SWITCH_COUNT} switches, got {len(raw_switches)}."
            )

        switches: list[SwitchInfo] = []
        seen_ids: set[str] = set()
        seen_dpids: set[int] = set()
        expected_dpid_map = {"e1": 1, "e2": 2, "e3": 3, "e4": 4, "c1": 5, "c2": 6}
        required_fields = (
            "switch_id",
            "role",
            "dpid",
            "dpid_hex",
            "openflow_version",
        )

        for idx, item in enumerate(raw_switches):
            if not isinstance(item, Mapping):
                raise TopologyValidationError(f"Switch at index {idx} must be an object.")
            missing = [k for k in required_fields if k not in item or item[k] is None]
            if missing:
                raise TopologyValidationError(
                    f"Switch at index {idx} missing fields: {missing}"
                )

            switch_id = str(item["switch_id"]).strip()
            role = str(item["role"]).strip()
            dpid = int(item["dpid"])
            dpid_hex = str(item["dpid_hex"]).strip()
            of_version = str(item["openflow_version"]).strip()

            if switch_id not in expected_dpid_map:
                raise TopologyValidationError(
                    f"Invalid switch_id '{switch_id}': expected one of {list(expected_dpid_map)}."
                )
            if switch_id in seen_ids:
                raise TopologyValidationError(f"Duplicate switch_id '{switch_id}'.")
            if dpid in seen_dpids:
                raise TopologyValidationError(f"Duplicate switch dpid '{dpid}'.")

            expected_dpid = expected_dpid_map[switch_id]
            if dpid != expected_dpid or int(dpid_hex, 16) != expected_dpid:
                raise TopologyValidationError(
                    f"Switch '{switch_id}' DPID mismatch: got dpid={dpid} ({dpid_hex}), "
                    f"expected {expected_dpid} (0x{expected_dpid:x})."
                )

            expected_role = "edge" if switch_id.startswith("e") else "core"
            if role != expected_role:
                raise TopologyValidationError(
                    f"Switch '{switch_id}' role mismatch: expected '{expected_role}', got '{role}'."
                )
            if of_version != "1.3":
                raise TopologyValidationError(
                    f"Switch '{switch_id}' openflow_version must be '1.3', got '{of_version}'."
                )

            seen_ids.add(switch_id)
            seen_dpids.add(dpid)
            switches.append(
                SwitchInfo(
                    switch_id=switch_id,
                    role=role,
                    dpid=dpid,
                    dpid_hex=dpid_hex,
                    openflow_version=of_version,
                )
            )

        # Ensure every host's edge_switch exists
        for h in hosts:
            if h.edge_switch not in seen_ids:
                raise TopologyValidationError(
                    f"Host '{h.host_id}' references unknown edge_switch '{h.edge_switch}'."
                )

        return switches

    @classmethod
    def _parse_and_validate_links(
        cls,
        raw_links: Any,
        hosts: Sequence[HostInfo],
        switches: Sequence[SwitchInfo],
    ) -> list[DirectedLink]:
        if not isinstance(raw_links, list):
            raise TopologyValidationError("links must be a list.")
        if len(raw_links) != cls.EXPECTED_DIRECTED_LINK_COUNT:
            raise TopologyValidationError(
                f"Expected {cls.EXPECTED_DIRECTED_LINK_COUNT} directed links, got {len(raw_links)}."
            )

        hosts_by_id = {h.host_id: h for h in hosts}
        switches_by_id = {s.switch_id: s for s in switches}
        valid_nodes = set(hosts_by_id) | set(switches_by_id)

        links: list[DirectedLink] = []
        seen_link_ids: set[str] = set()
        seen_node_Pairs: set[tuple[str, str]] = set()
        seen_tx_ports: set[tuple[str, int]] = set()
        seen_rx_ports: set[tuple[str, int]] = set()

        required_fields = (
            "link_id",
            "from_node",
            "to_node",
            "from_port",
            "to_port",
            "link_type",
            "is_fabric",
            "capacity_mbps",
            "delay_ms",
        )

        for idx, item in enumerate(raw_links):
            if not isinstance(item, Mapping):
                raise TopologyValidationError(f"Link at index {idx} must be an object.")
            missing = [k for k in required_fields if k not in item or item[k] is None]
            if missing:
                raise TopologyValidationError(
                    f"Link at index {idx} missing fields: {missing}"
                )

            link_id = str(item["link_id"]).strip()
            from_node = str(item["from_node"]).strip()
            to_node = str(item["to_node"]).strip()
            from_port = int(item["from_port"])
            to_port = int(item["to_port"])
            link_type = str(item["link_type"]).strip()
            is_fabric = bool(item["is_fabric"])
            capacity_mbps = float(item["capacity_mbps"])
            delay_ms = float(item["delay_ms"])

            m_link = _CANONICAL_LINK_ID_RE.match(link_id)
            if not m_link:
                raise TopologyValidationError(
                    f"Non-canonical link_id '{link_id}': expected '<from_node>><to_node>' (e.g. 'e1>c1')."
                )
            if m_link.group(1) != from_node or m_link.group(2) != to_node:
                raise TopologyValidationError(
                    f"link_id '{link_id}' does not match endpoints '{from_node}' -> '{to_node}'."
                )

            if from_node not in valid_nodes or to_node not in valid_nodes:
                raise TopologyValidationError(
                    f"Link '{link_id}' references unknown node(s): '{from_node}', '{to_node}'."
                )

            if link_id in seen_link_ids:
                raise TopologyValidationError(f"Duplicate link_id '{link_id}'.")
            if (from_node, to_node) in seen_node_Pairs:
                raise TopologyValidationError(
                    f"Duplicate directed link between '{from_node}' and '{to_node}'."
                )

            if capacity_mbps <= 0:
                raise TopologyValidationError(
                    f"Link '{link_id}' capacity_mbps must be > 0, got {capacity_mbps}."
                )
            if delay_ms <= 0:
                raise TopologyValidationError(
                    f"Link '{link_id}' delay_ms must be > 0, got {delay_ms}."
                )

            # Check port uniqueness on switches (hosts use port 0 in Network catalog)
            if from_node in switches_by_id:
                if (from_node, from_port) in seen_tx_ports:
                    raise TopologyValidationError(
                        f"Duplicate switch egress port ({from_node}, port {from_port}) on link '{link_id}'."
                    )
                seen_tx_ports.add((from_node, from_port))
            if to_node in switches_by_id:
                if (to_node, to_port) in seen_rx_ports:
                    raise TopologyValidationError(
                        f"Duplicate switch ingress port ({to_node}, port {to_port}) on link '{link_id}'."
                    )
                seen_rx_ports.add((to_node, to_port))

            # Validate Leaf-Spine topology rules & port mapping per Section 3.1
            from_is_host = from_node in hosts_by_id
            to_is_host = to_node in hosts_by_id
            if from_is_host or to_is_host:
                if is_fabric or link_type != "host_edge":
                    raise TopologyValidationError(
                        f"Host link '{link_id}' must have link_type='host_edge' and is_fabric=False."
                    )
                host_obj = hosts_by_id[from_node] if from_is_host else hosts_by_id[to_node]
                edge_id = to_node if from_is_host else from_node
                sw_port = to_port if from_is_host else from_port
                host_port = from_port if from_is_host else to_port
                if edge_id != host_obj.edge_switch or sw_port != host_obj.edge_port or host_port != 0:
                    raise TopologyValidationError(
                        f"Host-edge link '{link_id}' port mapping mismatch with host '{host_obj.host_id}'."
                    )
            else:
                # Switch-to-switch link: must be edge <-> core (no edge-edge or core-core per AC-N01)
                sw_from = switches_by_id[from_node]
                sw_to = switches_by_id[to_node]
                if sw_from.role == sw_to.role:
                    raise TopologyValidationError(
                        f"Forbidden {sw_from.role}-{sw_to.role} link '{link_id}' (AC-N01 violation)."
                    )
                if not is_fabric or link_type != "fabric":
                    raise TopologyValidationError(
                        f"Switch-to-switch link '{link_id}' must have link_type='fabric' and is_fabric=True."
                    )
                edge_sw = sw_from if sw_from.role == "edge" else sw_to
                core_sw = sw_to if sw_from.role == "edge" else sw_from
                edge_port_no = from_port if sw_from.role == "edge" else to_port
                core_port_no = to_port if sw_from.role == "edge" else from_port

                expected_edge_port = 3 if core_sw.switch_id == "c1" else 4
                edge_index = int(edge_sw.switch_id[1:])  # e1..e4 -> 1..4
                expected_core_port = edge_index

                if edge_port_no != expected_edge_port or core_port_no != expected_core_port:
                    raise TopologyValidationError(
                        f"Fabric link '{link_id}' violates Section 3.1 port mapping: "
                        f"got ({edge_sw.switch_id}:{edge_port_no} <-> {core_sw.switch_id}:{core_port_no}), "
                        f"expected ({edge_sw.switch_id}:{expected_edge_port} <-> {core_sw.switch_id}:{expected_core_port})."
                    )

            seen_link_ids.add(link_id)
            seen_node_Pairs.add((from_node, to_node))
            links.append(
                DirectedLink(
                    link_id=link_id,
                    from_node=from_node,
                    to_node=to_node,
                    from_port=from_port,
                    to_port=to_port,
                    link_type=link_type,
                    is_fabric=is_fabric,
                    capacity_mbps=capacity_mbps,
                    delay_ms=delay_ms,
                )
            )

        fabric_links = [lk for lk in links if lk.is_fabric]
        if len(fabric_links) != cls.EXPECTED_FABRIC_LINK_COUNT:
            raise TopologyValidationError(
                f"Expected {cls.EXPECTED_FABRIC_LINK_COUNT} fabric directed links, got {len(fabric_links)}."
            )

        # Verify reverse link symmetry
        links_by_pair = {(lk.from_node, lk.to_node): lk for lk in links}
        for lk in links:
            rev = links_by_pair.get((lk.to_node, lk.from_node))
            if rev is None:
                raise TopologyValidationError(
                    f"Directed link '{lk.link_id}' is missing reverse link '{lk.to_node}>{lk.from_node}'."
                )
            if rev.from_port != lk.to_port or rev.to_port != lk.from_port:
                raise TopologyValidationError(
                    f"Reverse link '{rev.link_id}' port numbers do not mirror '{lk.link_id}'."
                )

        return links

    @classmethod
    def _parse_and_validate_flow_groups(
        cls,
        raw_flows: Any,
        hosts: Sequence[HostInfo],
        switches: Sequence[SwitchInfo],
    ) -> list[FlowGroup]:
        if not isinstance(raw_flows, list):
            raise TopologyValidationError("flow_groups must be a list.")
        if len(raw_flows) != cls.EXPECTED_FLOW_GROUP_COUNT:
            raise TopologyValidationError(
                f"Expected {cls.EXPECTED_FLOW_GROUP_COUNT} flow groups, got {len(raw_flows)}."
            )

        hosts_by_id = {h.host_id: h for h in hosts}
        switches_by_id = {s.switch_id: s for s in switches}

        flow_groups: list[FlowGroup] = []
        seen_flow_ids: set[str] = set()
        seen_host_pairs: set[tuple[str, str]] = set()
        seen_udp_ports: set[int] = set()
        seen_stts: set[int] = set()

        required_fields = (
            "stt",
            "flow_group_id",
            "flow_cookie",
            "src_host",
            "dst_host",
            "src_ip",
            "dst_ip",
            "src_edge",
            "dst_edge",
            "udp_port",
        )

        for idx, item in enumerate(raw_flows):
            if not isinstance(item, Mapping):
                raise TopologyValidationError(
                    f"FlowGroup at index {idx} must be an object."
                )
            missing = [k for k in required_fields if k not in item or item[k] is None]
            if missing:
                raise TopologyValidationError(
                    f"FlowGroup at index {idx} missing fields: {missing}"
                )

            stt = int(item["stt"])
            flow_group_id = str(item["flow_group_id"]).strip()
            flow_cookie = str(item["flow_cookie"]).strip()
            src_host = str(item["src_host"]).strip()
            dst_host = str(item["dst_host"]).strip()
            src_ip = str(item["src_ip"]).strip()
            dst_ip = str(item["dst_ip"]).strip()
            src_edge = str(item["src_edge"]).strip()
            dst_edge = str(item["dst_edge"]).strip()
            udp_port = int(item["udp_port"])

            if not _CANONICAL_FLOW_ID_RE.match(flow_group_id):
                raise TopologyValidationError(
                    f"Non-canonical flow_group_id '{flow_group_id}': expected hex format '0x{{src*256+dst:X}}' (e.g. '0x103')."
                )
            if flow_cookie != flow_group_id:
                raise TopologyValidationError(
                    f"Flow '{flow_group_id}' has mismatched flow_cookie '{flow_cookie}'."
                )

            if src_host not in hosts_by_id or dst_host not in hosts_by_id:
                raise TopologyValidationError(
                    f"Flow '{flow_group_id}' references unknown host(s): '{src_host}', '{dst_host}'."
                )
            if src_edge not in switches_by_id or dst_edge not in switches_by_id:
                raise TopologyValidationError(
                    f"Flow '{flow_group_id}' references unknown edge switch(es): '{src_edge}', '{dst_edge}'."
                )

            src_h = hosts_by_id[src_host]
            dst_h = hosts_by_id[dst_host]

            src_num = int(src_host[1:])
            dst_num = int(dst_host[1:])
            expected_hex_id = f"0x{src_num * 256 + dst_num:X}"
            if flow_group_id != expected_hex_id:
                raise TopologyValidationError(
                    f"Flow '{flow_group_id}' does not match formula 0x{{src*256+dst:X}} for {src_host}->{dst_host} (expected '{expected_hex_id}')."
                )

            if src_h.edge_switch != src_edge or dst_h.edge_switch != dst_edge:
                raise TopologyValidationError(
                    f"Flow '{flow_group_id}' edge switches ({src_edge}->{dst_edge}) do not match host attachments ({src_h.edge_switch}->{dst_h.edge_switch})."
                )
            if src_edge == dst_edge:
                raise TopologyValidationError(
                    f"Same-edge flow '{flow_group_id}' ({src_host}->{dst_host} on {src_edge}) is forbidden in F (48 inter-edge flow groups)."
                )
            if src_h.ip != src_ip or dst_h.ip != dst_ip:
                raise TopologyValidationError(
                    f"Flow '{flow_group_id}' IP addresses ({src_ip}->{dst_ip}) do not match host catalog ({src_h.ip}->{dst_h.ip})."
                )

            if flow_group_id in seen_flow_ids:
                raise TopologyValidationError(
                    f"Duplicate flow_group_id '{flow_group_id}'."
                )
            if (src_host, dst_host) in seen_host_pairs:
                raise TopologyValidationError(
                    f"Duplicate flow group for host pair ({src_host}, {dst_host})."
                )
            if udp_port in seen_udp_ports:
                raise TopologyValidationError(
                    f"Duplicate udp_port {udp_port} on flow '{flow_group_id}'."
                )
            if stt in seen_stts:
                raise TopologyValidationError(
                    f"Duplicate stt {stt} on flow '{flow_group_id}'."
                )

            seen_flow_ids.add(flow_group_id)
            seen_host_pairs.add((src_host, dst_host))
            seen_udp_ports.add(udp_port)
            seen_stts.add(stt)

            flow_groups.append(
                FlowGroup(
                    stt=stt,
                    flow_group_id=flow_group_id,
                    flow_cookie=flow_cookie,
                    src_host=src_host,
                    dst_host=dst_host,
                    src_ip=src_ip,
                    dst_ip=dst_ip,
                    src_edge=src_edge,
                    dst_edge=dst_edge,
                    udp_port=udp_port,
                )
            )

        # Verify ascending order by numeric hex ID per Guide Section 3.2
        numeric_ids = [fg.numeric_id for fg in flow_groups]
        if numeric_ids != sorted(numeric_ids):
            raise TopologyValidationError(
                "flow_groups must be ordered by ascending numeric value of hex flow_group_id."
            )

        return flow_groups

    @classmethod
    def _parse_and_validate_candidate_paths(
        cls,
        raw_paths: Any,
        flow_groups: Sequence[FlowGroup],
        switches: Sequence[SwitchInfo],
        links: Sequence[DirectedLink],
    ) -> list[CandidatePath]:
        if not isinstance(raw_paths, list):
            raise TopologyValidationError("candidate_paths must be a list.")
        if len(raw_paths) != cls.EXPECTED_CANDIDATE_PATH_COUNT:
            raise TopologyValidationError(
                f"Expected {cls.EXPECTED_CANDIDATE_PATH_COUNT} candidate paths, got {len(raw_paths)}."
            )

        flows_by_id = {fg.flow_group_id: fg for fg in flow_groups}
        switches_by_id = {s.switch_id: s for s in switches}
        links_by_id = {lk.link_id: lk for lk in links}

        candidate_paths: list[CandidatePath] = []
        seen_path_ids: set[str] = set()
        seen_flow_labels: set[tuple[str, str]] = set()
        fabric_incidence_counter: dict[str, int] = {
            lk.link_id: 0 for lk in links if lk.is_fabric
        }
        total_fabric_incidences = 0

        required_fields = (
            "path_id",
            "flow_group_id",
            "path_label",
            "core_switch",
            "hop_count",
            "nominal_delay_ms",
            "ordered_link_ids",
            "fabric_link_ids",
        )

        for idx, item in enumerate(raw_paths):
            if not isinstance(item, Mapping):
                raise TopologyValidationError(
                    f"CandidatePath at index {idx} must be an object."
                )
            missing = [k for k in required_fields if k not in item or item[k] is None]
            if missing:
                raise TopologyValidationError(
                    f"CandidatePath at index {idx} missing fields: {missing}"
                )

            path_id = str(item["path_id"]).strip()
            flow_group_id = str(item["flow_group_id"]).strip()
            path_label = str(item["path_label"]).strip()
            core_switch = str(item["core_switch"]).strip()
            hop_count = int(item["hop_count"])
            nominal_delay_ms = float(item["nominal_delay_ms"])

            if not isinstance(item["ordered_link_ids"], (list, tuple)) or not isinstance(
                item["fabric_link_ids"], (list, tuple)
            ):
                raise TopologyValidationError(
                    f"Path '{path_id}' ordered_link_ids and fabric_link_ids must be lists."
                )
            ordered_link_ids = tuple(str(x).strip() for x in item["ordered_link_ids"])
            fabric_link_ids = tuple(str(x).strip() for x in item["fabric_link_ids"])

            m_path = _CANONICAL_PATH_ID_RE.match(path_id)
            if not m_path:
                raise TopologyValidationError(
                    f"Non-canonical path_id '{path_id}': expected '<flow_group_id>_P1' or '<flow_group_id>_P2' (e.g. '0x103_P1')."
                )
            if m_path.group(1) != flow_group_id or m_path.group(2) != path_label:
                raise TopologyValidationError(
                    f"path_id '{path_id}' does not match flow_group_id='{flow_group_id}' and path_label='{path_label}'."
                )

            if flow_group_id not in flows_by_id:
                raise TopologyValidationError(
                    f"Path '{path_id}' references unknown flow_group_id '{flow_group_id}'."
                )
            if path_id in seen_path_ids:
                raise TopologyValidationError(f"Duplicate path_id '{path_id}'.")
            if (flow_group_id, path_label) in seen_flow_labels:
                raise TopologyValidationError(
                    f"Duplicate path_label '{path_label}' for flow '{flow_group_id}'."
                )

            expected_core = "c1" if path_label == "P1" else "c2"
            if core_switch != expected_core or core_switch not in switches_by_id:
                raise TopologyValidationError(
                    f"Path '{path_id}' with label '{path_label}' must traverse core_switch '{expected_core}', got '{core_switch}'."
                )

            if hop_count != 4 or len(ordered_link_ids) != 4:
                raise TopologyValidationError(
                    f"Path '{path_id}' must have hop_count=4 and 4 ordered_link_ids, got hop_count={hop_count}, len={len(ordered_link_ids)}."
                )

            # Validate path continuity, node sequence, loop-freeness, and delay
            fg = flows_by_id[flow_group_id]
            expected_nodes = (
                fg.src_host,
                fg.src_edge,
                core_switch,
                fg.dst_edge,
                fg.dst_host,
            )
            visited_nodes = [expected_nodes[0]]
            derived_fabric_links: list[str] = []
            delay_sum = 0.0

            for hop_idx, lid in enumerate(ordered_link_ids):
                if lid not in links_by_id:
                    raise TopologyValidationError(
                        f"Path '{path_id}' hop {hop_idx} references unknown link_id '{lid}'."
                    )
                lk = links_by_id[lid]
                exp_from = expected_nodes[hop_idx]
                exp_to = expected_nodes[hop_idx + 1]
                if lk.from_node != exp_from or lk.to_node != exp_to:
                    raise TopologyValidationError(
                        f"Path '{path_id}' discontinuity at hop {hop_idx}: link '{lid}' is "
                        f"{lk.from_node}->{lk.to_node}, expected {exp_from}->{exp_to}."
                    )
                if lk.to_node in visited_nodes:
                    raise TopologyValidationError(
                        f"Path '{path_id}' contains a loop revisiting node '{lk.to_node}'."
                    )
                visited_nodes.append(lk.to_node)
                delay_sum += lk.delay_ms
                if lk.is_fabric:
                    derived_fabric_links.append(lid)

            if tuple(derived_fabric_links) != fabric_link_ids or len(fabric_link_ids) != 2:
                raise TopologyValidationError(
                    f"Path '{path_id}' fabric_link_ids {fabric_link_ids} do not match derived fabric links {tuple(derived_fabric_links)}."
                )

            if abs(nominal_delay_ms - delay_sum) > 1e-6:
                raise TopologyValidationError(
                    f"Path '{path_id}' nominal_delay_ms ({nominal_delay_ms}) does not match sum of link delays ({delay_sum})."
                )

            for flid in fabric_link_ids:
                fabric_incidence_counter[flid] = (
                    fabric_incidence_counter.get(flid, 0) + 1
                )
                total_fabric_incidences += 1

            seen_path_ids.add(path_id)
            seen_flow_labels.add((flow_group_id, path_label))
            candidate_paths.append(
                CandidatePath(
                    path_id=path_id,
                    flow_group_id=flow_group_id,
                    path_label=path_label,
                    core_switch=core_switch,
                    hop_count=hop_count,
                    nominal_delay_ms=nominal_delay_ms,
                    ordered_link_ids=ordered_link_ids,
                    fabric_link_ids=fabric_link_ids,
                )
            )

        # Every flow must have both P1 and P2
        for fg in flow_groups:
            for lbl in ("P1", "P2"):
                if (fg.flow_group_id, lbl) not in seen_flow_labels:
                    raise TopologyValidationError(
                        f"Flow '{fg.flow_group_id}' is missing candidate path '{lbl}'."
                    )

        if total_fabric_incidences != cls.EXPECTED_PATH_FABRIC_INCIDENCE_COUNT:
            raise TopologyValidationError(
                f"Expected {cls.EXPECTED_PATH_FABRIC_INCIDENCE_COUNT} path-fabric-link incidences, got {total_fabric_incidences}."
            )

        for flid, count in fabric_incidence_counter.items():
            if count != cls.EXPECTED_INCIDENCES_PER_FABRIC_LINK:
                raise TopologyValidationError(
                    f"Fabric link '{flid}' appears in {count} candidate paths, expected {cls.EXPECTED_INCIDENCES_PER_FABRIC_LINK}."
                )

        return candidate_paths

    @classmethod
    def _parse_and_validate_initial_paths(
        cls,
        raw_init_paths: Any,
        flow_groups: Sequence[FlowGroup],
        candidate_paths: Sequence[CandidatePath],
    ) -> list[InitialPathEntry]:
        if not isinstance(raw_init_paths, list):
            raise TopologyValidationError("initial_paths must be a list.")
        if len(raw_init_paths) != cls.EXPECTED_INITIAL_PATH_COUNT:
            raise TopologyValidationError(
                f"Expected {cls.EXPECTED_INITIAL_PATH_COUNT} initial paths, got {len(raw_init_paths)}."
            )

        flows_by_id = {fg.flow_group_id: fg for fg in flow_groups}
        paths_by_id = {cp.path_id: cp for cp in candidate_paths}

        initial_paths: list[InitialPathEntry] = []
        seen_flows: set[str] = set()
        required_fields = ("flow_group_id", "initial_path_id", "path_label")

        for idx, item in enumerate(raw_init_paths):
            if not isinstance(item, Mapping):
                raise TopologyValidationError(
                    f"InitialPath at index {idx} must be an object."
                )
            missing = [k for k in required_fields if k not in item or item[k] is None]
            if missing:
                raise TopologyValidationError(
                    f"InitialPath at index {idx} missing fields: {missing}"
                )

            flow_group_id = str(item["flow_group_id"]).strip()
            initial_path_id = str(item["initial_path_id"]).strip()
            path_label = str(item["path_label"]).strip()

            if flow_group_id not in flows_by_id:
                raise TopologyValidationError(
                    f"InitialPath references unknown flow_group_id '{flow_group_id}'."
                )
            if flow_group_id in seen_flows:
                raise TopologyValidationError(
                    f"Duplicate initial_path entry for flow '{flow_group_id}'."
                )
            if initial_path_id not in paths_by_id:
                raise TopologyValidationError(
                    f"InitialPath for flow '{flow_group_id}' references unknown path_id '{initial_path_id}'."
                )
            cp = paths_by_id[initial_path_id]
            if cp.flow_group_id != flow_group_id or cp.path_label != path_label:
                raise TopologyValidationError(
                    f"InitialPath '{initial_path_id}' mismatch with flow_group_id='{flow_group_id}' or path_label='{path_label}'."
                )

            seen_flows.add(flow_group_id)
            initial_paths.append(
                InitialPathEntry(
                    flow_group_id=flow_group_id,
                    initial_path_id=initial_path_id,
                    path_label=path_label,
                )
            )

        if len(seen_flows) != len(flows_by_id):
            missing_flows = set(flows_by_id) - seen_flows
            raise TopologyValidationError(
                f"initial_paths missing entries for flow groups: {sorted(missing_flows)}"
            )

        return initial_paths

    def _compile_switch_hops(self, cp: CandidatePath) -> tuple[SwitchHop, ...]:
        """
        Compile the ordered directed links of a candidate path into per-switch
        (in_port, out_port) instructions for the 3 traversed switches (src_edge, core, dst_edge).
        """
        hops: list[SwitchHop] = []
        # A 4-link path [h_src>e_src, e_src>core, core>e_dst, e_dst>h_dst]
        # traverses 3 switches at consecutive link pairs (0,1), (1,2), (2,3).
        for idx in range(len(cp.ordered_link_ids) - 1):
            in_link = self._links_by_id[cp.ordered_link_ids[idx]]
            out_link = self._links_by_id[cp.ordered_link_ids[idx + 1]]
            sw = self._switches_by_id[in_link.to_node]
            hops.append(
                SwitchHop(
                    hop_index=idx,
                    switch_id=sw.switch_id,
                    dpid=sw.dpid,
                    role=sw.role,
                    in_port=in_link.to_port,
                    out_port=out_link.from_port,
                    ingress_link_id=in_link.link_id,
                    egress_link_id=out_link.link_id,
                )
            )
        return tuple(hops)

    # -------------------------------------------------------------------------
    # Public Lookup API
    # -------------------------------------------------------------------------

    def get_flow_group(self, flow_group_id: str) -> FlowGroup:
        """
        Look up a canonical FlowGroup by ID (e.g. '0x103').
        Explicitly rejects legacy IDs such as 'h1_h3', 'h1->h3', or 'f13'.
        """
        if not isinstance(flow_group_id, str) or not _CANONICAL_FLOW_ID_RE.match(
            flow_group_id
        ):
            raise TopologyLookupError(
                f"Invalid or legacy flow_group_id '{flow_group_id}': must use canonical Network hex ID (e.g. '0x103')."
            )
        fg = self._flows_by_id.get(flow_group_id)
        if fg is None:
            raise TopologyLookupError(
                f"Flow group '{flow_group_id}' not found in catalog."
            )
        return fg

    def get_flow_by_hosts(self, src_host: str, dst_host: str) -> FlowGroup:
        """Look up the directed FlowGroup for (src_host, dst_host)."""
        fg = self._flows_by_hosts.get((src_host, dst_host))
        if fg is None:
            raise TopologyLookupError(
                f"No inter-edge flow group found for ({src_host} -> {dst_host})."
            )
        return fg

    def get_all_flow_groups(self) -> list[FlowGroup]:
        """Return all 48 FlowGroups sorted by ascending numeric value of hex flow_group_id."""
        return sorted(self._flows_by_id.values(), key=lambda f: f.numeric_id)

    def get_candidate_path(self, path_id: str) -> CandidatePath:
        """
        Look up a CandidatePath by canonical path_id (e.g. '0x103_P1' or '0x103_P2').
        Explicitly rejects bare labels ('P1', 'P2') or legacy IDs ('h1_h3_c1').
        """
        if not isinstance(path_id, str) or not _CANONICAL_PATH_ID_RE.match(path_id):
            raise TopologyLookupError(
                f"Invalid or legacy path_id '{path_id}': must use canonical '<flow_group_id>_P1' or '<flow_group_id>_P2' (e.g. '0x103_P1')."
            )
        cp = self._paths_by_id.get(path_id)
        if cp is None:
            raise TopologyLookupError(f"Candidate path '{path_id}' not found in catalog.")
        return cp

    def get_candidate_paths_for_flow(
        self, flow_group_id: str
    ) -> tuple[CandidatePath, CandidatePath]:
        """Return (P1, P2) CandidatePath tuple for the given canonical flow_group_id."""
        self.get_flow_group(flow_group_id)
        by_label = self._paths_by_flow[flow_group_id]
        return (by_label["P1"], by_label["P2"])

    def get_path_by_label(self, flow_group_id: str, path_label: str) -> CandidatePath:
        """Look up a CandidatePath by canonical flow_group_id and path_label ('P1' or 'P2')."""
        self.get_flow_group(flow_group_id)
        if path_label not in ("P1", "P2"):
            raise TopologyLookupError(
                f"Invalid path_label '{path_label}': expected 'P1' or 'P2'."
            )
        return self._paths_by_flow[flow_group_id][path_label]

    def get_initial_path(self, flow_group_id: str) -> InitialPathEntry:
        """Look up the InitialPathEntry for a canonical flow_group_id."""
        self.get_flow_group(flow_group_id)
        return self._initial_paths_by_flow[flow_group_id]

    def get_initial_candidate_path(self, flow_group_id: str) -> CandidatePath:
        """Return the resolved CandidatePath corresponding to the flow's initial path."""
        entry = self.get_initial_path(flow_group_id)
        return self._paths_by_id[entry.initial_path_id]

    def get_switch(self, switch_id: str) -> SwitchInfo:
        """Look up a SwitchInfo by switch_id ('e1'..'e4', 'c1', 'c2')."""
        sw = self._switches_by_id.get(switch_id)
        if sw is None:
            raise TopologyLookupError(f"Switch '{switch_id}' not found in catalog.")
        return sw

    def get_switch_by_dpid(self, dpid: int) -> SwitchInfo:
        """Look up a SwitchInfo by integer OpenFlow DPID (1..6)."""
        sw = self._switches_by_dpid.get(dpid)
        if sw is None:
            raise TopologyLookupError(f"Switch with dpid={dpid} not found in catalog.")
        return sw

    def get_all_switches(self) -> list[SwitchInfo]:
        """Return all 6 SwitchInfo objects sorted by DPID."""
        return sorted(self._switches_by_id.values(), key=lambda s: s.dpid)

    def get_host(self, host_id: str) -> HostInfo:
        """Look up a HostInfo by host_id ('h1'..'h8')."""
        h = self._hosts_by_id.get(host_id)
        if h is None:
            raise TopologyLookupError(f"Host '{host_id}' not found in catalog.")
        return h

    def get_all_hosts(self) -> list[HostInfo]:
        """Return all 8 HostInfo objects sorted by host number."""
        return sorted(self._hosts_by_id.values(), key=lambda h: int(h.host_id[1:]))

    def get_link(self, link_id: str) -> DirectedLink:
        """
        Look up a DirectedLink by canonical link_id (e.g. 'e1>c1').
        Explicitly rejects legacy IDs such as 'e1_to_c1' or 'e1-c1'.
        """
        if not isinstance(link_id, str) or not _CANONICAL_LINK_ID_RE.match(link_id):
            raise TopologyLookupError(
                f"Invalid or legacy link_id '{link_id}': must use canonical directed link ID '<from_node>><to_node>' (e.g. 'e1>c1')."
            )
        lk = self._links_by_id.get(link_id)
        if lk is None:
            raise TopologyLookupError(f"Directed link '{link_id}' not found in catalog.")
        return lk

    def get_fabric_links(self) -> list[DirectedLink]:
        """Return all 16 directed fabric links."""
        return [lk for lk in self._links_by_id.values() if lk.is_fabric]

    def get_all_links(self) -> list[DirectedLink]:
        """Return all 32 directed links."""
        return list(self._links_by_id.values())

    def get_port_between(self, from_node: str, to_node: str) -> tuple[int, int]:
        """Return (from_port, to_port) for the directed link from_node -> to_node."""
        lk = self._links_by_nodes.get((from_node, to_node))
        if lk is None:
            raise TopologyLookupError(
                f"No directed link exists from '{from_node}' to '{to_node}'."
            )
        return (lk.from_port, lk.to_port)

    def get_switch_hops(self, path_id: str) -> tuple[SwitchHop, ...]:
        """
        Return the ordered tuple of SwitchHop (src_edge, core_switch, dst_edge)
        with resolved (in_port, out_port) for a canonical path_id (e.g. '0x103_P1').
        """
        self.get_candidate_path(path_id)
        return self._switch_hops_by_path[path_id]

