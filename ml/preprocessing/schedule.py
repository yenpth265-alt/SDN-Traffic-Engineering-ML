"""Import a Network demand plan for OFFLINE learning, never online snapshots."""
import csv
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import io
import math
from pathlib import Path
import zipfile

from ml.common import (SPLITS, catalog, file_hash, new_directory, read_json,
                       require, split_check, stamp, whole, write_json)


@contextmanager
def schedule_csv(path):
    """Stream CSV or the sole CSV member of a ZIP without extracting files."""
    path = Path(path)
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as archive:
            members = [n for n in archive.namelist() if n.lower().endswith(".csv")]
            require(len(members) == 1, "Schedule ZIP must contain exactly one CSV")
            with archive.open(members[0]) as raw:
                with io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as stream:
                    yield stream
    else:
        with path.open(encoding="utf-8-sig", newline="") as stream:
            yield stream


def import_schedule(schedule_path, seed_manifest_path, catalog_path, out,
                    warmup_seconds, measurement_seconds):
    """Preserve declared splits/demand; require explicit offline time boundaries.

    UTCs below are an artificial coordinate origin, NOT recorded execution times.
    No release/availability/route evidence is manufactured by this adapter.
    """
    warmup = whole(warmup_seconds, "warmup_seconds")
    duration = whole(measurement_seconds, "measurement_seconds", 1)
    total = warmup + duration
    ids = catalog(catalog_path)
    positions = {fg: i for i, fg in enumerate(ids)}
    manifest = read_json(seed_manifest_path)
    allocations = {}
    for allocation in manifest["seed_allocations"]:
        seed = whole(allocation["seed"], "seed")
        require(seed not in allocations, "Duplicate seed allocation")
        require(allocation["split"] in SPLITS, "Unknown manifest split")
        allocations[seed] = allocation["split"]
    require(allocations, "Empty seed allocations")
    out = new_directory(out)
    history_path = out / "history.csv"
    staging_path = out / "history.csv.partial"
    schedules = {}
    row_count = 0
    origin = datetime(2000, 1, 1, tzinfo=timezone.utc)
    try:
        with schedule_csv(schedule_path) as source, staging_path.open(
                "w", encoding="utf-8", newline="") as target:
            reader = csv.DictReader(source)
            required = {"schedule_id", "scenario_id", "seed", "split", "time_offset_s",
                        "flow_group_id", "requested_mbps"}
            require(required <= set(reader.fieldnames or []),
                    f"Schedule CSV needs {sorted(required)}")
            writer = csv.writer(target)
            writer.writerow(["run_id", "flow_group_id", "time_offset_s", "requested_mbps"])
            for line, row in enumerate(reader, 2):
                sid, scenario = row["schedule_id"], row["scenario_id"]
                require(bool(sid) and bool(scenario), f"Missing schedule/scenario at row {line}")
                seed = whole(float(row["seed"]), "schedule seed")
                split = row["split"]
                require(seed in allocations and allocations[seed] == split,
                        f"Split mismatch for {sid}, seed {seed}: CSV={split}, "
                        f"manifest={allocations.get(seed)}; source labels are never rewritten")
                family = row.get("parent_family_id", f"seed_family_{seed}")
                require(bool(family), f"Missing parent_family_id for {sid}")
                identity = (scenario, seed, split, family)
                if sid not in schedules:
                    schedules[sid] = {"identity": identity, "seen": bytearray(total * 48),
                                      "count": 0}
                entry = schedules[sid]
                require(entry["identity"] == identity, f"Inconsistent schedule metadata: {sid}")
                t = whole(float(row["time_offset_s"]), "time_offset_s")
                fg = row["flow_group_id"]
                require(fg in positions, f"Unknown flow {fg} in {sid}")
                require(t < total, f"Schedule {sid} has bins outside [0,{total})")
                index = t * 48 + positions[fg]
                require(not entry["seen"][index], f"Duplicate schedule bin: {sid}/{fg}/{t}")
                demand = float(row["requested_mbps"])
                require(math.isfinite(demand) and demand >= 0,
                        f"Invalid requested_mbps at row {line}")
                entry["seen"][index] = 1
                entry["count"] += 1
                row_count += 1
                writer.writerow([f"offline_{sid}", fg, t, row["requested_mbps"]])
        runs = []
        for sid, entry in sorted(schedules.items()):
            require(entry["count"] == total * 48,
                    f"Schedule {sid} needs {total * 48} rows ({total}s x 48 flows); "
                    f"got {entry['count']}. Check warmup/measurement; missing is not zero.")
            scenario, seed, split, family = entry["identity"]
            runs.append({
                "run_id": f"offline_{sid}", "schedule_id": sid, "scenario_id": scenario,
                "parent_family_id": family, "seed": seed, "split": split,
                "run_started_at_utc": stamp(origin), "traffic_started_at_utc": stamp(origin),
                "measurement_started_at_utc": stamp(origin + timedelta(seconds=warmup)),
                "measurement_end_utc": stamp(origin + timedelta(seconds=total)),
            })
        split_seeds = split_check(runs)
        for scenario in {r["scenario_id"] for r in runs}:
            require({r["seed"] for r in runs if r["scenario_id"] == scenario} == set(allocations),
                    f"Scenario {scenario} does not cover all allocated seeds")
        provenance = {
            "kind": "offline_network_schedule", "online_eligible": False,
            "time_origin_kind": "artificial_utc_coordinate_not_execution_time",
            "schedule_sha256": file_hash(schedule_path),
            "seed_manifest_sha256": file_hash(seed_manifest_path),
            "warmup_seconds": warmup, "measurement_seconds": duration,
            "family_policy": "CSV parent_family_id when present; otherwise conservatively group by seed. "
                             "Network must confirm family independence before benchmark.",
            "note": "Requested demand plan only; no measured network delivery, release or route evidence. "
                    "Timing arguments are explicit offline assumptions, not a change to Network config.",
        }
        staging_path.replace(history_path)
        write_json(out / "runs.json", {"delta_s": 1, "provenance": provenance, "runs": runs})
        report = {"rows": row_count, "schedules": len(runs), "flow_count": 48,
                  "traffic_bins": total, "split_seeds": split_seeds,
                  "history_sha256": file_hash(history_path), "provenance": provenance}
        write_json(out / "import_report.json", report)
        return report
    finally:
        staging_path.unlink(missing_ok=True)
