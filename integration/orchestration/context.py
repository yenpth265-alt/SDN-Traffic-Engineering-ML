"""Explicit Network run metadata and Data READY forecast adapters."""
from datetime import timedelta
import csv
import math

from data.ingestion.management_api import RunCreate
from ml.common import require, stamp, utc
from ml.common import file_hash
from ml.preprocessing.schedule import schedule_csv


def manifest_to_run(manifest, topology, selected, *, git_commit, config_version):
    """Map actual launch timestamps; never reuse the sample manifest's old date."""
    phases = manifest.get("phases", {})
    warmup = phases.get("warmup_seconds", manifest.get("warmup_s", 60))
    measurement = phases.get("measurement_seconds", manifest.get("measurement_s", 360))
    traffic = utc(manifest["traffic_started_at_utc"])
    start = utc(manifest["run_started_at_utc"])
    require(manifest.get("topology_version", topology["topology_version"]) == topology["topology_version"],
            "Manifest topology version differs from the loaded bundle")
    require(manifest.get("config_version", config_version) == config_version,
            "Manifest config version differs from Network configuration")
    method = manifest.get("method_id", manifest.get("method"))
    require(method in {"M0", "M1", "M2", "M3", "M4"}, "Missing or invalid method")
    body = {
        "run_id": manifest["run_id"], "topology_id": topology["topology_hash"],
        "schedule_id": manifest.get("schedule_id"),
        "mode": manifest.get("mode", "oracle" if method == "M4" else "live"),
        "method_id": method, "run_started_at_utc": stamp(start),
        "traffic_started_at_utc": stamp(traffic),
        "measurement_started_at_utc": stamp(traffic + timedelta(seconds=warmup)),
        "warmup_s": warmup, "measurement_s": measurement, "schema_version": "1.0",
        "config_version": config_version, "config_hash": manifest["config_hash"],
        "ml_config_hash": selected["ml_config_sha256"], "lookback_bins": selected["lookback_L"],
        "horizon_bins": selected["horizon_H"], "model_version": selected["model_version"],
        "git_commit": git_commit, "manifest": manifest,
    }
    return RunCreate.model_validate(body).model_dump(mode="json")


def ready_forecast_context(ready, run, snapshot, routing, *, tick_at_utc):
    """Enrich the envelope from trusted run/snapshot metadata, preserving all seven row fields."""
    snapshot = snapshot.get("sdn", snapshot)
    require(ready.get("status") == "READY", "Forecast is not READY")
    require(ready.get("run_id") == run["run_id"] == snapshot.get("run_id") == routing.get("run_id"),
            "Forecast/run/snapshot/routing run mismatch")
    require(ready.get("snapshot_id") == snapshot.get("snapshot_id"), "Forecast snapshot mismatch")
    require(snapshot.get("quality_status") == "VALID", "Snapshot is not VALID")
    require(snapshot.get("state_version") == routing.get("state_version"), "Routing changed since snapshot")
    require(snapshot.get("config_hash") == run["config_hash"], "Snapshot config hash mismatch")
    require(snapshot.get("config_version") == run["config_version"], "Snapshot config version mismatch")
    require(snapshot.get("topology_hash") == run["topology_id"], "Snapshot topology mismatch")
    require(snapshot.get("lookback_L") == run["lookback_bins"], "Snapshot lookback mismatch")
    cutoff, horizon = snapshot["cutoff_offset_s"], run["horizon_bins"]
    require(type(cutoff) is int and type(horizon) is int and horizon > 0, "Invalid cutoff/horizon")
    origin = utc(run["measurement_started_at_utc"])
    end = origin + timedelta(seconds=cutoff + horizon)
    require(utc(tick_at_utc) < end, "Forecast has expired")
    require(cutoff + horizon <= run["measurement_s"], "Forecast exceeds measurement")
    result = dict(ready)
    result.update(
        config_hash=run["config_hash"], config_version=run["config_version"],
        topology_hash=snapshot["topology_hash"], topology_version=snapshot["topology_version"],
        measurement_started_at_utc=run["measurement_started_at_utc"], delta_s=1,
        history_cutoff_utc=snapshot["history_cutoff_utc"], snapshot_as_of_utc=snapshot["snapshot_as_of_utc"],
        forecast_start=cutoff, forecast_end=cutoff + horizon,
        forecast_start_utc=stamp(origin + timedelta(seconds=cutoff)), forecast_end_utc=stamp(end),
        apply_deadline_utc=stamp(end), tick_at_utc=tick_at_utc,
    )
    return result


class OracleProvider:
    """Explicit M4-only future schedule access; M1-M3 never construct this provider."""
    def __init__(self, run, schedule_file):
        require(run["method_id"] == "M4" and run["mode"] == "oracle", "Oracle data is reserved for M4")
        require(file_hash(schedule_file) == run["manifest"]["schedule_sha256"], "Oracle schedule hash mismatch")
        self.values = {}
        with schedule_csv(schedule_file) as stream:
            for row in csv.DictReader(stream):
                if row["schedule_id"] != run["schedule_id"]:
                    continue
                index = int(row["time_offset_s"])
                value = float(row["requested_mbps"])
                key = (index, row["flow_group_id"])
                require(key not in self.values and math.isfinite(value) and value >= 0,
                        "Duplicate or invalid oracle schedule row")
                self.values[key] = value

    def __call__(self, run, snapshot, now):
        require(run["method_id"] == "M4" and run["mode"] == "oracle", "Oracle data is reserved for M4")
        cutoff = snapshot["cutoff_offset_s"]
        rows = [{"run_id": run["run_id"], "flow_group_id": group, "issued_at": stamp(now),
                 "forecast_start": index, "forecast_end": index + 1,
                 "predicted_mbps": self.values[(index + run["warmup_s"], group)], "source": "oracle_schedule"}
                for group in snapshot["flow_group_ids"] for index in range(cutoff, cutoff + run["horizon_bins"])]
        return {"status": "READY", "run_id": run["run_id"], "snapshot_id": snapshot["snapshot_id"], "rows": rows}
