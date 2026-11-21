#!/usr/bin/env python3
"""Shared utilities and binary packet header definitions for UDP traffic generator/receiver (v1.4 compliant)."""

import csv
import hashlib
import json
import os
import struct
from datetime import datetime, timezone
from pathlib import Path

# Binary UDP header: stt (uint16), send_bin (int32), seq (uint32), send_time (float64)
HEADER = struct.Struct("!HiId")

# Các trường thay đổi theo từng lượt chạy (không đưa vào config_hash)
VOLATILE_MANIFEST_KEYS = {
    "run_id",
    "schedule_id",
    "seed",
    "split",
    "run_start_utc",
    "traffic_started_at_utc",
    "run_started_at_utc",
    "t0",
    "status",
    "quality_flags",
    "config_hash",
}


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


def _pick_phase_seconds(man: dict, keys: tuple[str, ...], default: int) -> int:
    """Reads phase duration from manifest supporting both '*_seconds' and '*_s' keys."""
    for k in keys:
        if k in man and man[k] is not None:
            return int(man[k])
    phases = man.get("phases")
    if isinstance(phases, dict):
        for k in keys:
            if k in phases and phases[k] is not None:
                return int(phases[k])
    return default


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

    man["warmup"] = _pick_phase_seconds(man, ("warmup_seconds", "warmup_s"), 60)
    man["measurement"] = _pick_phase_seconds(man, ("measurement_seconds", "measurement_s"), 360)
    man["drain"] = _pick_phase_seconds(man, ("drain_seconds", "drain_s"), 10)
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


def compute_config_hash(
    config_paths: list[str | os.PathLike],
    manifest: dict | None = None,
    base_dir: str | os.PathLike | None = None,
    prefix: str = "sha256:",
    short_len: int | None = None,
) -> str:
    """Computes a normalized SHA-256 hash from config files and optional static manifest fields."""
    hasher = hashlib.sha256()
    root = Path(base_dir).resolve() if base_dir else Path.cwd().resolve()

    normalized_files: list[tuple[str, Path]] = []
    for p in config_paths:
        full_path = Path(p)
        if not full_path.is_absolute():
            full_path = (root / full_path).resolve()
        try:
            rel_key = full_path.relative_to(root).as_posix()
        except ValueError:
            rel_key = full_path.as_posix()
        normalized_files.append((rel_key, full_path))

    normalized_files.sort(key=lambda item: item[0])

    for rel_key, full_path in normalized_files:
        raw_bytes = full_path.read_bytes()
        norm_bytes = raw_bytes.replace(b"\r\n", b"\n")
        hasher.update(f"FILE:{rel_key}\n".encode("utf-8"))
        hasher.update(norm_bytes)
        hasher.update(b"\n---END_FILE---\n")

    if manifest is not None:
        static_manifest = {
            k: v for k, v in manifest.items() if k not in VOLATILE_MANIFEST_KEYS
        }
        manifest_bytes = json.dumps(
            static_manifest,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        hasher.update(b"MANIFEST_STATIC\n")
        hasher.update(manifest_bytes)

    digest = hasher.hexdigest()
    if short_len is not None and short_len > 0:
        digest = digest[:short_len]
    return f"{prefix}{digest}"
