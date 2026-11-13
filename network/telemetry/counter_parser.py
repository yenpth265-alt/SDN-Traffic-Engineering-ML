#!/usr/bin/env python3
"""
OVS Port Counter Parser (v1.4 Contract Compliant)
Executes and parses `ovs-ofctl -O OpenFlow13 dump-ports <switch>` to extract per-port
TX counters (packets, bytes, drops, errors) and maps them to canonical directed links.

Rules enforced per v1.4 Section 3.3 & 5.1 (N4):
- Reads ONLY TX counters at the sending switch port of each directed link.
- Never sums TX and RX on the same link.
- Gracefully handles counter resets/wraps and initial readings.
"""

import re
import subprocess
from typing import Any

PORT_HEADER_RE = re.compile(r"^\s*port\s+(\d+|LOCAL):", re.IGNORECASE)
TX_LINE_RE = re.compile(
    r"tx\s+pkts=(\d+|\?),\s*bytes=(\d+|\?),\s*drop=(\d+|\?),\s*errs=(\d+|\?)",
    re.IGNORECASE,
)


def parse_dump_ports_output(raw_text: str) -> dict[int, dict[str, int]]:
    """
    Parses raw text from `ovs-ofctl -O OpenFlow13 dump-ports <switch>`.
    Returns a mapping: {port_no (int): {"tx_packets": int, "tx_bytes": int, "tx_drops": int, "tx_errors": int}}.
    """
    ports: dict[int, dict[str, int]] = {}
    current_port: int | None = None

    for line in raw_text.splitlines():
        m_port = PORT_HEADER_RE.match(line)
        if m_port:
            port_str = m_port.group(1).upper()
            if port_str == "LOCAL":
                current_port = None
            else:
                current_port = int(port_str)
            continue

        if current_port is not None:
            m_tx = TX_LINE_RE.search(line)
            if m_tx:
                def _to_int(val: str) -> int:
                    return 0 if val == "?" else int(val)

                ports[current_port] = {
                    "tx_packets": _to_int(m_tx.group(1)),
                    "tx_bytes": _to_int(m_tx.group(2)),
                    "tx_drops": _to_int(m_tx.group(3)),
                    "tx_errors": _to_int(m_tx.group(4)),
                }
                current_port = None

    return ports


def read_switch_ports(switch_name: str, openflow_version: str = "OpenFlow13") -> dict[int, dict[str, int]]:
    """Runs `ovs-ofctl` for a single switch and returns parsed TX port counters."""
    cmd = ["ovs-ofctl", "-O", openflow_version, "dump-ports", switch_name]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return parse_dump_ports_output(res.stdout)


def compute_link_delta(
    prev_sample: dict[str, Any] | None,
    curr_tx_bytes: int,
    curr_tx_packets: int,
    curr_tx_drops: int,
    curr_ts: float,
    capacity_mbps: float,
    wrap_bits: int = 64,
) -> dict[str, Any]:
    """
    Computes delta bytes, actual elapsed time, port_tx_mbps, and utilization from consecutive TX readings.
    Formula: port_tx_mbps = (8 * delta_bytes) / (delta_t_actual * 1e6).
    """
    if prev_sample is None:
        return {
            "delta_bytes": 0,
            "delta_packets": 0,
            "delta_drops": 0,
            "delta_t_s": 0.0,
            "port_tx_mbps": 0.0,
            "utilization": 0.0,
            "counter_status": "INITIAL",
        }

    delta_t = curr_ts - prev_sample["ts"]
    if delta_t <= 0.0:
        return {
            "delta_bytes": 0,
            "delta_packets": 0,
            "delta_drops": 0,
            "delta_t_s": 0.0,
            "port_tx_mbps": 0.0,
            "utilization": 0.0,
            "counter_status": "INVALID_DT",
        }

    prev_bytes = prev_sample["tx_bytes"]
    if curr_tx_bytes >= prev_bytes:
        delta_bytes = curr_tx_bytes - prev_bytes
        status = "OK"
    else:
        max_val = 1 << wrap_bits
        if prev_bytes > (max_val - (1 << 40)):
            delta_bytes = (max_val - prev_bytes) + curr_tx_bytes
            status = "WRAPPED"
        else:
            delta_bytes = curr_tx_bytes
            status = "RESET"

    delta_packets = max(0, curr_tx_packets - prev_sample["tx_packets"])
    delta_drops = max(0, curr_tx_drops - prev_sample["tx_drops"])
    port_tx_mbps = (8.0 * delta_bytes) / (delta_t * 1e6)
    utilization = port_tx_mbps / capacity_mbps if capacity_mbps > 0 else 0.0

    return {
        "delta_bytes": delta_bytes,
        "delta_packets": delta_packets,
        "delta_drops": delta_drops,
        "delta_t_s": round(delta_t, 6),
        "port_tx_mbps": round(port_tx_mbps, 6),
        "utilization": round(utilization, 6),
        "counter_status": status,
    }