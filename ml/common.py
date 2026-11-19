"""Shared validation and artifact I/O. No DB or network access."""
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

SPLITS = ("train", "validation", "test")
FEATURE_VERSION = "demand_lags_stats_flow_v1"
ROW_FIELDS = ("run_id", "flow_group_id", "issued_at", "forecast_start",
              "forecast_end", "predicted_mbps", "source")


class ContractError(ValueError):
    """Invalid data/config: caller must skip, never silently repair."""


def require(condition, message):
    if not condition:
        raise ContractError(message)


def utc(value):
    require(isinstance(value, str), "Timestamp must be an explicit UTC string")
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractError(f"Invalid timestamp: {value}") from exc
    require(dt.tzinfo is not None and dt.utcoffset().total_seconds() == 0,
            "Timestamp must include UTC timezone")
    return dt


def stamp(dt=None):
    return (dt or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat(
        timespec="microseconds").replace("+00:00", "Z")


def digest(value):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"),
                     ensure_ascii=False, allow_nan=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def read_json(path):
    with open(path, encoding="utf-8-sig") as f:
        return json.load(f)


def write_json(path, data):
    """Atomic complete file; never expose a partially written forecast."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, allow_nan=False)
            f.write("\n")
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def new_directory(path):
    path = Path(path)
    require(not path.exists() or not any(path.iterdir()),
            f"Output directory is not empty: {path}. Use a new versioned directory.")
    path.mkdir(parents=True, exist_ok=True)
    return path


def whole(value, name, minimum=0):
    require(not isinstance(value, bool) and isinstance(value, (int, float))
            and math.isfinite(value) and value == int(value) and value >= minimum,
            f"{name} must be an integer >= {minimum}")
    return int(value)


def numbers(values, name, nonnegative=True):
    a = np.asarray(values, dtype=np.float64)
    require(np.isfinite(a).all(), f"{name} contains missing/NaN/infinite values")
    if nonnegative:
        require((a >= 0).all(), f"{name} contains negative demand")
    return a


def catalog(path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    ids = sorted((r["flow_group_id"] for r in rows), key=lambda x: int(x, 16))
    expected = sorted((hex(s * 256 + d) for s in range(1, 9) for d in range(1, 9)
                       if (s - 1) // 2 != (d - 1) // 2), key=lambda x: int(x, 16))
    require(ids == expected, "Catalog must contain exactly 48 canonical cross-edge hex IDs")
    for r in rows:
        s, d = int(r["src_host"][1:]), int(r["dst_host"][1:])
        require(r["flow_group_id"] == hex(s * 256 + d), "Flow ID/host mismatch")
    return ids


def split_check(runs):
    """Check identities BEFORE generating windows, including replay/family overlap."""
    require(runs and len({r["run_id"] for r in runs}) == len(runs),
            "Run IDs must be unique and nonempty")
    for field in ("schedule_id", "parent_family_id", "seed"):
        ownership = {}
        for r in runs:
            require(r["split"] in SPLITS, "Unknown split")
            require(r[field] is not None and str(r[field]) != "", f"Missing {field}")
            key = str(r[field])
            require(key not in ownership or ownership[key] == r["split"],
                    f"Split leakage in {field}={key}")
            ownership[key] = r["split"]
    seeds = {s: {str(r["seed"]) for r in runs if r["split"] == s} for s in SPLITS}
    total = sum(map(len, seeds.values()))
    require(total > 0 and total % 20 == 0,
            "70/15/15 exact seed split requires a multiple of 20 unique seeds")
    require([len(seeds[s]) for s in SPLITS] == [total * 14 // 20, total * 3 // 20,
                                              total * 3 // 20],
            f"Expected 70/15/15 by seed; got {[len(seeds[s]) for s in SPLITS]}")
    return {s: sorted(seeds[s]) for s in SPLITS}
