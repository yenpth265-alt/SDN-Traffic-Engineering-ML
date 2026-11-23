"""Explicitly synthetic fixtures. Never use these as project benchmark evidence."""
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
import numpy as np
import yaml

from ml.common import catalog, digest, stamp, write_json


def create_demo_inputs(directory, catalog_path):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    ids = catalog(catalog_path)
    runs = []
    with open(directory / "history.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run_id", "flow_group_id", "time_offset_s", "requested_mbps"])
        for seed in range(1, 21):
            start = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=seed)
            split = "train" if seed <= 10 else "validation" if seed <= 15 else "test"
            run = {
                "run_id": f"demo_{seed}", "schedule_id": f"demo_schedule_{seed}",
                "parent_family_id": f"demo_family_{seed}", "scenario_id": "SYNTHETIC",
                "seed": seed, "split": split, "run_started_at_utc": stamp(start),
                "traffic_started_at_utc": stamp(start),
                "measurement_started_at_utc": stamp(start + timedelta(seconds=10)),
                "measurement_end_utc": stamp(start + timedelta(seconds=40)),
            }
            runs.append(run)
            rng = np.random.default_rng(seed)
            for t in range(40):
                for i, fg in enumerate(ids):
                    value = 0.5 + i / 120 + seed / 500 + 0.15 * np.sin(t / 5 + i)
                    w.writerow([run["run_id"], fg, t, float(value + rng.uniform(0, 0.03))])
    write_json(directory / "runs.json", {"delta_s": 1, "synthetic": True, "runs": runs})
    config = {
        "lookback_candidates": [3, 5], "horizon_candidates": [3, 5],
        "epsilon_candidates": [0, 0.1], "ma_window_candidates": [2, 3, 5],
        "random_state": 2026, "n_jobs": 1, "max_train_samples": 2000,
        "rf_candidates": [{"n_estimators": 8, "max_depth": 6, "min_samples_leaf": 2}],
        "baseline_epsilon": 0,
    }
    (directory / "training.yaml").write_text(yaml.safe_dump(config), encoding="utf-8")
    return config


def synthetic_snapshot(cfg, metadata, cutoff=None):
    """TEST/DEMO ONLY: production snapshots must be created by SDN and served by Data."""
    cutoff = cutoff or (datetime.now(timezone.utc) - timedelta(milliseconds=100))
    asof = cutoff + timedelta(milliseconds=50)
    ids, L = cfg["flow_group_ids"], cfg["lookback_L"]
    snapshot = {
        "snapshot_id": "synthetic_snapshot", "run_id": "synthetic_online",
        "history_cutoff_utc": stamp(cutoff), "snapshot_as_of_utc": stamp(asof),
        "config_version": "synthetic", "config_hash": "synthetic_config",
        "topology_version": "synthetic", "topology_hash": "synthetic_topology",
        "flow_group_ids": ids, "state_version": 0, "quality_status": "VALID",
        "routing_state_complete": True, "missing_groups": [], "unknown_flow_group_ids": [],
        "routing_state": [{"flow_group_id": fg, "path_id": fg + "_P1",
                           "verification_status": "VERIFIED", "state_version": 0} for fg in ids],
        "history": [],
    }
    for fg in ids:
        for i in range(L):
            start = cutoff - timedelta(seconds=L - i)
            end = start + timedelta(seconds=1)
            snapshot["history"].append({
                "flow_group_id": fg, "window_start_utc": stamp(start), "window_end_utc": stamp(end),
                "requested_mbps": float(i + 2), "released_at_utc": stamp(end),
                "available_at_utc": stamp(end + timedelta(milliseconds=10)),
                "valid": True, "reason": None, "release_record_id": f"synthetic:{fg}:{i}",
            })
    snapshot["content_hash"] = digest(snapshot)
    context = {
        "run_id": snapshot["run_id"], "snapshot_id": snapshot["snapshot_id"],
        "snapshot_content_hash": snapshot["content_hash"],
        "config_hash": snapshot["config_hash"], "topology_hash": snapshot["topology_hash"],
        "ml_config_sha256": metadata["ml_config_sha256"], "model_sha256": metadata["model_sha256"],
        "measurement_started_at_utc": stamp(cutoff - timedelta(seconds=100)),
        "measurement_end_utc": stamp(cutoff + timedelta(seconds=260)),
        "max_snapshot_age_s": 2.0, "forecast_timeout_ms": 5000,
        "fallback_policy": "skip", "fallback_epsilon": 0.0,
    }
    return snapshot, context
