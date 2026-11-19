"""File adapter for IF04. Data may call prepare_bundle or supply the same bundle.

No online data access. Offline CSV must be an explicit export from Data, with
run origins and a preassigned split manifest. Never auto-split overlapping windows.
"""
from pathlib import Path
import hashlib
import numpy as np
import pandas as pd

from ml.common import (SPLITS, catalog, digest, file_hash, new_directory,
                       numbers, read_json, require, split_check, utc, whole, write_json)

FORMAT = "training_bundle_history_v1"


def prepare_bundle(history_path, runs_path, catalog_path, out, max_lookback=60,
                   max_horizon=10, stride=5):
    L, H, stride = (whole(x, n, 1) for x, n in
                    [(max_lookback, "max_lookback"), (max_horizon, "max_horizon"),
                     (stride, "stride")])
    manifest = read_json(runs_path)
    require(manifest.get("delta_s") == 1, "Only contiguous 1-second bins are supported")
    runs = manifest["runs"]
    split_seeds = split_check(runs)
    ids = catalog(catalog_path)
    frame = pd.read_csv(history_path, dtype={"run_id": str, "flow_group_id": str})
    required = {"run_id", "flow_group_id", "time_offset_s", "requested_mbps"}
    require(required <= set(frame.columns), f"History CSV needs {sorted(required)}")
    require(not frame[list(required)].isna().any().any(), "History has missing values")
    require(set(frame.run_id) == {r["run_id"] for r in runs}, "CSV/run manifest mismatch")
    require(set(frame.flow_group_id) == set(ids), "Expected all 48 canonical flows")
    require(not frame.duplicated(["run_id", "flow_group_id", "time_offset_s"]).any(),
            "Duplicate demand observation")
    offsets = numbers(frame.time_offset_s, "time_offset_s")
    require(np.equal(offsets, np.floor(offsets)).all(), "Offsets must be whole seconds")
    frame["time_offset_s"] = offsets.astype(np.int64)
    frame["requested_mbps"] = numbers(frame.requested_mbps, "requested_mbps")
    chunks = {s: {"X": [], "y": [], "meta": []} for s in SPLITS}
    fingerprints = {}
    recorded_runs = []
    for run in runs:
        rid, split = run["run_id"], run["split"]
        for key in ("run_id", "schedule_id", "parent_family_id", "scenario_id"):
            require(isinstance(run[key], str) and bool(run[key]), f"Missing {key}")
        traffic, measurement, end = (utc(run[k]) for k in
            ("traffic_started_at_utc", "measurement_started_at_utc", "measurement_end_utc"))
        require(utc(run["run_started_at_utc"]) <= traffic <= measurement < end,
                f"Invalid run origins: {rid}")
        warmup = whole((measurement - traffic).total_seconds(), "warmup")
        duration = whole((end - measurement).total_seconds(), "measurement length", 1)
        sub = frame[frame.run_id == rid]
        for field in ("schedule_id", "split", "scenario_id"):
            if field in sub:
                require(sub[field].eq(run[field]).all(), f"CSV {field} mismatch for {rid}")
        matrix = sub.pivot(index="time_offset_s", columns="flow_group_id",
                           values="requested_mbps").reindex(columns=ids).sort_index()
        # No unknown tail, gaps, implicit zeroes or missing groups.
        require(matrix.index.tolist() == list(range(warmup + duration)),
                f"Expected complete traffic bins [0,{warmup + duration}) for {rid}")
        values = numbers(matrix.to_numpy(), f"demand for {rid}")
        fingerprint = hashlib.sha256(np.asarray(values, dtype="<f8").tobytes()).hexdigest()
        require(fingerprint not in fingerprints or fingerprints[fingerprint] == split,
                "Identical demand series renamed/replayed across different splits")
        fingerprints[fingerprint] = split
        ts = np.arange(max(L, warmup), warmup + duration - H + 1, stride, dtype=int)
        require(len(ts) > 0, f"No eligible windows for {rid}; need more history/labels")
        ch = chunks[split]
        for col, fg in enumerate(ids):
            windows = np.lib.stride_tricks.sliding_window_view(values[:, col], L + H)
            selected = windows[ts - L]
            ch["X"].append(selected[:, :L])
            ch["y"].append(selected[:, L:])
            for t in ts:
                cutoff = int(t - warmup)
                ch["meta"].append({
                    "sample_id": f"{rid}:{fg}:{cutoff}", "run_id": rid,
                    "schedule_id": run["schedule_id"], "parent_family_id": run["parent_family_id"],
                    "scenario_id": run["scenario_id"], "seed": str(run["seed"]), "split": split,
                    "flow_group_id": fg, "cutoff": cutoff,
                    "measurement_started_at_utc": run["measurement_started_at_utc"],
                })
        recorded_runs.append({**run, "series_fingerprint": fingerprint})
    out = new_directory(out)
    files = {}
    counts = {}
    for split, ch in chunks.items():
        meta = pd.DataFrame(ch["meta"])
        require(not meta.empty, f"No {split} samples")
        x, y = np.concatenate(ch["X"]), np.concatenate(ch["y"])
        np.savez_compressed(out / f"{split}.npz", X=x, y_vector=y,
                            sample_ids=meta.sample_id.to_numpy(dtype=str))
        meta.to_csv(out / f"{split}.csv", index=False)
        counts[split] = len(meta)
        for suffix in ("npz", "csv"):
            name = f"{split}.{suffix}"
            files[name] = file_hash(out / name)
    info = {
        "format": FORMAT, "delta_s": 1, "target": "requested_mbps",
        "X_schema": "oldest_to_newest_demand_only", "max_lookback": L, "max_horizon": H,
        "dtypes": {"X": "float64", "y_vector": "float64", "sample_ids": "unicode"},
        "stride": stride, "flow_group_ids": ids, "split_seeds": split_seeds,
        "runs": recorded_runs, "counts": counts, "files": files,
        "source_hashes": {"history": file_hash(history_path), "runs": file_hash(runs_path),
                          "catalog": file_hash(catalog_path)},
        "data_provenance": manifest.get("provenance", {
            "kind": "synthetic_demo" if manifest.get("synthetic") else "data_export",
        }),
    }
    write_json(out / "bundle.json", info)
    return info


class TrainingBundle:
    def __init__(self, directory):
        self.path = Path(directory)
        self.info = read_json(self.path / "bundle.json")
        self.sha256 = file_hash(self.path / "bundle.json")
        b = self.info
        require(b["format"] == FORMAT and b["delta_s"] == 1
                and b["target"] == "requested_mbps"
                and b["X_schema"] == "oldest_to_newest_demand_only", "Unknown TrainingBundle schema")
        split_check(b["runs"])
        require(len(set(b["flow_group_ids"])) == 48, "Bundle needs 48 unique flow IDs")
        expected = sorted((hex(s * 256 + d) for s in range(1, 9) for d in range(1, 9)
                           if (s - 1) // 2 != (d - 1) // 2), key=lambda x: int(x, 16))
        require(b["flow_group_ids"] == expected, "Noncanonical flow order")
        owners = {}
        for r in b["runs"]:
            fp = r["series_fingerprint"]
            require(fp not in owners or owners[fp] == r["split"], "Series fingerprint leakage")
            owners[fp] = r["split"]
        self.runs = {r["run_id"]: r for r in b["runs"]}

    def load(self, split):
        """Load only requested split; train() never loads test X or labels."""
        require(split in SPLITS, "Unknown split")
        for suffix in ("csv", "npz"):
            name = f"{split}.{suffix}"
            require(file_hash(self.path / name) == self.info["files"][name],
                    f"TrainingBundle hash mismatch: {name}")
        meta = pd.read_csv(self.path / f"{split}.csv", dtype=str)
        require(not meta.isna().any().any(), "Missing sample metadata")
        with np.load(self.path / f"{split}.npz", allow_pickle=False) as arr:
            x, y = numbers(arr["X"], "X"), numbers(arr["y_vector"], "y_vector")
            sample_ids = arr["sample_ids"].astype(str).tolist()
        require(len(sample_ids) == len(set(sample_ids)), "Duplicate array sample_id")
        require(meta.sample_id.is_unique and set(meta.sample_id) == set(sample_ids),
                "X/y/sample_metadata must join one-to-one on sample_id")
        meta = meta.set_index("sample_id").loc[sample_ids].reset_index()
        require(x.shape == (len(meta), self.info["max_lookback"])
                and y.shape == (len(meta), self.info["max_horizon"]), "Bundle array shape mismatch")
        require(len(meta) == self.info["counts"][split] and len(meta) > 0, "Sample count mismatch")
        require(meta.split.eq(split).all(), "Mixed splits in bundle")
        for rid, group in meta.groupby("run_id"):
            require(rid in self.runs, f"Unknown run {rid}")
            run = self.runs[rid]
            for k in ("schedule_id", "parent_family_id", "seed", "scenario_id", "split",
                      "measurement_started_at_utc"):
                require(group[k].eq(str(run[k])).all(), f"Metadata mismatch: {k}")
            cutoffs = pd.to_numeric(group.cutoff, errors="raise")
            duration = (utc(run["measurement_end_utc"]) -
                        utc(run["measurement_started_at_utc"])).total_seconds()
            require(((cutoffs >= 0) & (cutoffs + self.info["max_horizon"] <= duration)
                     & (cutoffs == np.floor(cutoffs))).all(), "Invalid label interval")
            require(not group.duplicated(["flow_group_id", "cutoff"]).any(), "Duplicate window")
            for _, tick in group.groupby("cutoff"):
                require(set(tick.flow_group_id) == set(self.info["flow_group_ids"])
                        and len(tick) == 48, "Incomplete 48-flow support")
        return x, y, meta
