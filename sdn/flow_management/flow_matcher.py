"""
FlowMatcher for SDN Controller Phase 3 (Integration Guide v1.4 & Testing v2 compliant).

Responsibilities:
- Build canonical OpenFlow 1.3 match specifications (FlowMatchSpec) from TopologyRegistry's
  FlowGroup catalog without hard-coding IPs, UDP ports, or cookies.
- Enforce S2 and AC-N02: flow_cookie (e.g. 0x103) is an OpenFlow rule management identifier
  and NEVER substitutes for packet match fields (eth_type, ipv4_src, ipv4_dst, in_port).
- Isolate the [OPEN / MUST-LOCK] L3 vs. L4 packet matching choice behind a configurable
  FlowMatchPolicy (provisional default: L3_IPV4_PAIR + in_port, so both UDP traffic and
  per-flow probes share the same IP pair rule per AC-N06 / AC-S01).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from sdn.controller.topology_registry import FlowGroup, TopologyRegistry


ETH_TYPE_IPV4 = 0x0800
IP_PROTO_UDP = 17


class FlowMatchPolicy(str, Enum):
    """
    Configurable OpenFlow 1.3 match granularity policy ([MUST-LOCK] item).

    - L3_IPV4_PAIR: Match on (in_port, eth_type=0x0800, ipv4_src, ipv4_dst).
      Allows both UDP data packets and ICMP/UDP active probes between (src_ip, dst_ip)
      to traverse the exact same OpenFlow rules (AC-N06, AC-S01).
    - L4_UDP_DPORT: Match on (in_port, eth_type=0x0800, ip_proto=17, ipv4_src, ipv4_dst, udp_dst).
      Matches the exact per-flow UDP destination port from flow_groups.csv (e.g. 5103 for 0x103).
    """

    L3_IPV4_PAIR = "L3_IPV4_PAIR"
    L4_UDP_DPORT = "L4_UDP_DPORT"


@dataclass(frozen=True)
class FlowMatcherConfig:
    """
    Configuration for FlowMatcher.
    Marked MUST_LOCK_PROVISIONAL_DEFAULT until L3/L4 and probe matching policy is frozen.
    """

    match_policy: FlowMatchPolicy = FlowMatchPolicy.L3_IPV4_PAIR
    policy_status: str = "MUST_LOCK_PROVISIONAL_DEFAULT"


@dataclass(frozen=True)
class FlowMatchSpec:
    """
    Abstract OpenFlow 1.3 packet match specification for a canonical flow group at a switch hop.

    Note (S2, AC-N02): flow_cookie_hex / flow_cookie_int are carried as rule management
    metadata and are NEVER used as a substitute for packet match fields.
    """

    flow_group_id: str
    flow_cookie_hex: str
    flow_cookie_int: int
    in_port: int
    eth_type: int
    ipv4_src: str
    ipv4_dst: str
    match_policy: FlowMatchPolicy
    ip_proto: Optional[int] = None
    udp_dst: Optional[int] = None

    def to_openflow_match_dict(self) -> dict[str, int | str]:
        """
        Return the canonical OpenFlow 1.3 match dictionary (compatible with Ryu OFPMatch kwargs).
        """
        match_dict: dict[str, int | str] = {
            "in_port": self.in_port,
            "eth_type": self.eth_type,
            "ipv4_src": self.ipv4_src,
            "ipv4_dst": self.ipv4_dst,
        }
        if self.ip_proto is not None:
            match_dict["ip_proto"] = self.ip_proto
        if self.udp_dst is not None:
            match_dict["udp_dst"] = self.udp_dst
        return match_dict

    @property
    def match_tuple_key(self) -> tuple[int, int, str, str, Optional[int], Optional[int]]:
        """Hashable key uniquely identifying the packet match fields in a switch flow table."""
        return (
            self.in_port,
            self.eth_type,
            self.ipv4_src,
            self.ipv4_dst,
            self.ip_proto,
            self.udp_dst,
        )


class FlowMatcher:
    """
    Constructs canonical FlowMatchSpec objects for the 48 inter-edge flow groups
    backed by TopologyRegistry.
    """

    def __init__(
        self,
        topology_registry: TopologyRegistry,
        config: Optional[FlowMatcherConfig] = None,
    ) -> None:
        self._topology = topology_registry
        self._config = config or FlowMatcherConfig()

    @property
    def topology(self) -> TopologyRegistry:
        return self._topology

    @property
    def config(self) -> FlowMatcherConfig:
        return self._config

    def build_match_for_hop(
        self,
        flow_group_id: str,
        in_port: int,
        *,
        policy_override: Optional[FlowMatchPolicy] = None,
    ) -> FlowMatchSpec:
        """
        Build the OpenFlow 1.3 FlowMatchSpec for a canonical flow_group_id (e.g. '0x103')
        entering a switch on `in_port`.
        """
        if not isinstance(in_port, int) or in_port <= 0:
            raise ValueError(f"Switch in_port must be a positive integer, got {in_port!r}.")

        fg: FlowGroup = self._topology.get_flow_group(flow_group_id)
        policy = policy_override or self._config.match_policy
        if not isinstance(policy, FlowMatchPolicy):
            raise ValueError(f"Invalid FlowMatchPolicy: {policy!r}")

        if policy == FlowMatchPolicy.L4_UDP_DPORT:
            return FlowMatchSpec(
                flow_group_id=fg.flow_group_id,
                flow_cookie_hex=fg.flow_cookie,
                flow_cookie_int=fg.cookie_int,
                in_port=in_port,
                eth_type=ETH_TYPE_IPV4,
                ipv4_src=fg.src_ip,
                ipv4_dst=fg.dst_ip,
                match_policy=policy,
                ip_proto=IP_PROTO_UDP,
                udp_dst=fg.udp_port,
            )

        return FlowMatchSpec(
            flow_group_id=fg.flow_group_id,
            flow_cookie_hex=fg.flow_cookie,
            flow_cookie_int=fg.cookie_int,
            in_port=in_port,
            eth_type=ETH_TYPE_IPV4,
            ipv4_src=fg.src_ip,
            ipv4_dst=fg.dst_ip,
            match_policy=policy,
            ip_proto=None,
            udp_dst=None,
        )
