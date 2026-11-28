"""Online inference: consume an immutable snapshot; emit seven fields only."""
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
import time
import numpy as np

from ml.baselines import moving_average, persistence
from ml.common import (ContractError, ROW_FIELDS, digest, numbers, require,
                       stamp, utc, whole)
from ml.model_registry import load_artifact
from ml.preprocessing.features import build_features
from ml.training.train import rf_predict
from ml.validation.metrics import calibrate


def unwrap_snapshot(snapshot):
    """Accept flat snapshots or canonical Data API v1.4 envelopes (`{"sdn": ...}`)."""
    require(isinstance(snapshot, Mapping), "Snapshot must be a JSON object")
    if "sdn" in snapshot:
        require(set(snapshot) == {"sdn"} and isinstance(snapshot["sdn"], Mapping),
                "Canonical snapshot envelope must contain only the 'sdn' object")
        return snapshot["sdn"]
    return snapshot


def validate_snapshot(snapshot, context, cfg, metadata, now):
    snapshot = unwrap_snapshot(snapshot)
    for key in ("config_version", "topology_version"):
        require(isinstance(snapshot.get(key), str) and snapshot[key], f"Missing snapshot {key}")
    for key in ("run_id", "snapshot_id", "config_hash", "topology_hash"):
        require(isinstance(context.get(key), str) and context[key], f"Missing context {key}")
        require(snapshot.get(key) == context[key], f"Snapshot/context mismatch: {key}")
    for key in ("ml_config_sha256", "model_sha256"):
        require(context.get(key) == metadata[key], f"Frozen artifact mismatch: {key}")
    expected_hash = digest({k: v for k, v in snapshot.items() if k != "content_hash"})
    require(snapshot.get("content_hash") == expected_hash
            and context.get("snapshot_content_hash") == expected_hash,
            "Snapshot content hash mismatch")
    require(snapshot.get("quality_status") == "VALID", "INVALID snapshot quality")
    ids = cfg["flow_group_ids"]
    require(snapshot.get("flow_group_ids") == ids, "Snapshot flow order mismatch")
    require(snapshot.get("routing_state_complete") is True
            and snapshot.get("missing_groups") == []
            and snapshot.get("unknown_flow_group_ids") == [], "Incomplete/unknown snapshot state")
    state_version = whole(snapshot["state_version"], "state_version")
    routes = snapshot["routing_state"]
    require(len(routes) == 48 and {r["flow_group_id"] for r in routes} == set(ids),
            "Routing state must cover all 48 flows")
    for r in routes:
        require(r["verification_status"] == "VERIFIED" and r["state_version"] == state_version
                and r["path_id"] in (r["flow_group_id"] + "_P1", r["flow_group_id"] + "_P2"),
                "Unverified/inconsistent routing state")
    L, H = cfg["lookback_L"], cfg["horizon_H"]
    if "delta_s" in snapshot:
        require(whole(snapshot["delta_s"], "delta_s", 1) == 1, "Unsupported snapshot delta_s")
    if "lookback_L" in snapshot:
        require(whole(snapshot["lookback_L"], "lookback_L", 1) == L, "Snapshot lookback_L mismatch")
    origin, end = utc(context["measurement_started_at_utc"]), utc(context["measurement_end_utc"])
    cutoff, asof = utc(snapshot["history_cutoff_utc"]), utc(snapshot["snapshot_as_of_utc"])
    require(origin <= cutoff <= asof <= now, "Invalid snapshot cutoff/availability time")
    anchor = whole((cutoff - origin).total_seconds(), "forecast_start")
    if "cutoff_offset_s" in snapshot:
        require(whole(snapshot["cutoff_offset_s"], "cutoff_offset_s") == anchor,
                "Snapshot cutoff_offset_s mismatch")
    if "origin_refs" in snapshot:
        origins = snapshot["origin_refs"]
        require(isinstance(origins, Mapping), "Snapshot origin_refs must be an object")
        require(utc(origins.get("measurement_started_at_utc")) == origin,
                "Snapshot origin_refs.measurement_started_at_utc mismatch")
        if "traffic_started_at_utc" in origins and "warmup_s" in origins:
            warmup_s = whole(origins["warmup_s"], "origin_refs.warmup_s")
            require(utc(origins["traffic_started_at_utc"]) + timedelta(seconds=warmup_s) == origin,
                    "Snapshot origin_refs warmup mismatch")
        if "delta_s" in origins:
            require(whole(origins["delta_s"], "origin_refs.delta_s", 1) == 1,
                    "Unsupported origin_refs.delta_s")
    require(cutoff + timedelta(seconds=H) <= end, "FORECAST_OUTSIDE_MEASUREMENT")
    max_age = float(context["max_snapshot_age_s"])
    timeout_ms = float(context["forecast_timeout_ms"])
    require(np.isfinite(max_age) and max_age >= 0 and np.isfinite(timeout_ms) and timeout_ms > 0,
            "Explicit finite age and latency policies are required")
    require(context["fallback_policy"] in ("skip", "persistence"), "Unknown fallback policy")
    numbers([context["fallback_epsilon"]], "fallback_epsilon")
    require((now - cutoff).total_seconds() <= max_age, "STALE_HISTORY")
    require(now < cutoff + timedelta(seconds=H), "FORECAST_EXPIRED")
    rows = snapshot["history"]
    require(len(rows) == 48 * L, "History must contain exactly 48 * L observations")
    positions = {fg: i for i, fg in enumerate(ids)}
    matrix = np.full((48, L), np.nan)
    release_ids = set()
    start = cutoff - timedelta(seconds=L)
    for r in rows:
        fg = r["flow_group_id"]
        require(fg in positions and r.get("valid") is True, "Invalid history observation")
        require(r.get("run_id", context["run_id"]) == context["run_id"], "Mixed history runs")
        ws, we = utc(r["window_start_utc"]), utc(r["window_end_utc"])
        released, available = utc(r["released_at_utc"]), utc(r["available_at_utc"])
        require(start <= ws < we <= cutoff and (we - ws).total_seconds() == 1,
                "History window outside [t-L,t) or not one second")
        require(we <= released <= available <= asof, "Future/unavailable history")
        col = whole((ws - start).total_seconds(), "history bin")
        require(col < L and np.isnan(matrix[positions[fg], col]), "Duplicate history bin")
        if "bin_index" in r:
            require(whole(r["bin_index"], "history bin_index") == anchor - L + col,
                    "History bin_index does not match measurement-relative window")
        record = r["release_record_id"]
        require(isinstance(record, str) and record and record not in release_ids,
                "Missing/duplicate release_record_id")
        release_ids.add(record)
        value = numbers([r["requested_mbps"]], "requested_mbps")[0]
        matrix[positions[fg], col] = value
    numbers(matrix, "complete causal history")
    return matrix, anchor, cutoff


class Predictor:
    """Load once, reuse per tick. Failures raise ContractError and emit no rows.

    last_audit is local diagnostic data, never attached to PredictionRows.
    Timeout is checked before publishing; a synchronous RF call is not forcibly
    interrupted. The runner can impose a hard process deadline if needed.
    """
    def __init__(self, artifact_path):
        self.model, self.config, self.metadata = load_artifact(artifact_path)
        self.last_audit = {}
        self.counts = {"eligible": 0, "native": 0, "fallback": 0, "skipped": 0}

    def predict_batch(self, snapshot, context, method="rf", clock=None):
        clock = clock or (lambda: datetime.now(timezone.utc))
        started = time.perf_counter()
        self.counts["eligible"] += 1
        audit = {"method": method, "snapshot_id": None,
                 "status": "INVALID", "fallback": False}
        self.last_audit = audit
        try:
            inner = unwrap_snapshot(snapshot)
            audit["snapshot_id"] = inner.get("snapshot_id")
            require(method in ("rf", "persistence", "moving_average"), "Unknown method")
            now = clock()
            history, anchor, cutoff = validate_snapshot(
                snapshot, context, self.config, self.metadata, now)
            cfg, H = self.config, self.config["horizon_H"]
            if method == "rf":
                try:
                    x = build_features(history, cfg["flow_group_ids"],
                                       cfg["flow_group_ids"], cfg["lookback_L"])
                    raw = rf_predict(self.model, x, H)
                    final = calibrate(raw, cfg["epsilon"])
                    source = cfg["source"]
                except Exception as exc:
                    if context["fallback_policy"] != "persistence":
                        raise ContractError(f"FORECAST_ERROR: {exc}") from exc
                    # Rebuild the ENTIRE batch, never mix sources per group.
                    final = calibrate(persistence.predict(history, H), context["fallback_epsilon"])
                    source = "fallback_persistence"
                    audit.update(fallback=True, fallback_reason=str(exc))
            elif method == "persistence":
                final = calibrate(persistence.predict(history, H), cfg["baseline_epsilon"])
                source = cfg["baseline_sources"][method]
            else:
                final = calibrate(moving_average.predict(history, H, cfg["ma_window"]),
                                  cfg["baseline_epsilon"])
                source = cfg["baseline_sources"][method]
            require(final.shape == (48, H), "Invalid complete forecast shape")
            # Construct the complete batch before selecting the real issue time.
            rows = [
                dict(zip(ROW_FIELDS, (context["run_id"], fg, "", int(anchor + h),
                                     int(anchor + h + 1), float(final[i, h]), source)))
                for i, fg in enumerate(cfg["flow_group_ids"]) for h in range(H)
            ]
            issue_time = clock()
            elapsed = (time.perf_counter() - started) * 1000
            require(elapsed <= float(context["forecast_timeout_ms"]), "FORECAST_TIMEOUT")
            require(issue_time >= now, "Clock moved backwards")
            require((issue_time - cutoff).total_seconds() <= float(context["max_snapshot_age_s"]),
                    "STALE_HISTORY_AT_ISSUE")
            require(issue_time < cutoff + timedelta(seconds=H), "FORECAST_EXPIRED")
            issued = stamp(issue_time)
            for row in rows:
                row["issued_at"] = issued
            audit.update(status="OK", source=source, row_count=len(rows), issued_at=issued,
                         forecast_ms=elapsed)
            self.counts["fallback" if audit["fallback"] else "native"] += 1
            return rows
        except Exception as exc:
            self.counts["skipped"] += 1
            audit.update(reason=str(exc), forecast_ms=(time.perf_counter() - started) * 1000)
            if isinstance(exc, ContractError):
                raise
            raise ContractError(f"FORECAST_INVALID: {exc}") from exc
        finally:
            audit["counts"] = dict(self.counts)
