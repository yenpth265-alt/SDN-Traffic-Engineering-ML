
#!/usr/bin/env python3
"""Shared utilities and binary packet header definitions for UDP traffic generator/receiver (v1.4 compliant)."""

import csv
import json
import os
import struct
from datetime import datetime, timezone

# Binary UDP header: stt (uint16), send_bin (int32), seq (uint32), send_time (float64)
HEADER = struct.Struct("!HiId")


def iso_utc(ts: float) -> str:
    """Formats a Unix timestamp into an ISO-8601 UTC string."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_utc(val) -> float:
    """Parses either a numeric Unix timestamp or an ISO-8601 UTC string into a float timestamp."""
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).timestamp()


def load_manifest(path: str) -> dict:
    """Loads run_manifest.json and normalizes timing fields."""
    with open(path, encoding="utf-8") as f:
        man = json.load(f)

    if "t0" not in man:
        for key in ("traffic_started_at_utc", "run_start_utc", "run_started_at_utc"):
            if key in man:
                man["t0"] = parse_utc(man[key])
                break
    else:
        man["t0"] = float(man["t0"])

    man.setdefault("warmup", int(man.get("warmup_s", 60)))
    man.setdefault("measurement", int(man.get("measurement_s", 360)))
    man.setdefault("drain", int(man.get("drain_s", 5)))
    return man


def load_flow_groups(config_dir: str = "config") -> list[dict]:
    """Loads the 48 canonical cross-edge flow groups with hex IDs (e.g., 0x103)."""
    path = os.path.join(config_dir, "flow_groups.csv")
    groups = []
    with open(path, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            fg_id = r["flow_group_id"].strip()
            groups.append({
                "stt": int(r["stt"]),
                "flow_group_id": fg_id,
                "flow_cookie": r.get("flow_cookie", fg_id).strip(),
                "src_host": r["src_host"].strip(),
                "dst_host": r["dst_host"].strip(),
                "src_ip": r["src_ip"].strip(),
                "dst_ip": r["dst_ip"].strip(),
                "src_edge": r["src_edge"].strip(),
                "dst_edge": r["dst_edge"].strip(),
                "udp_port": int(r["udp_port"]),
            })
    return groups