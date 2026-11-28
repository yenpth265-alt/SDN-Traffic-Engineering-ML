"""Build synthetic v1.4 test payloads from actual PostgreSQL evidence.

This helper is for API rejection/replay tests, not a live producer capture.
The SDN PostgreSQL suite separately exercises SnapshotCreator itself.
"""
from datetime import datetime, timedelta

from data.ingestion import online_store as store


def canonical_snapshot(compact):
    def utc(value):
        return datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value

    def stamp(value):
        return value.isoformat().replace("+00:00", "Z") if value is not None else None

    with store.transaction() as cursor:
        run = store.lock_run(cursor, compact["run_id"])
        cursor.execute("SELECT * FROM public.topologies WHERE topology_id=%s", (run["topology_id"],))
        topology = cursor.fetchone()
        as_of = utc(compact["snapshot_as_of_utc"])
        origin = run["measurement_started_at_utc"]
        cutoff = int((utc(compact["history_cutoff_utc"]) - origin).total_seconds())
        cursor.execute("SELECT * FROM public.demand_observations WHERE run_id=%s", (run["run_id"],))
        evidence = {(r["flow_group_id"], r["bin_index"]): dict(r) for r in cursor}
        history = []
        for source in compact["history"]:
            row = dict(source)
            stored = evidence.get((row["flow_group_id"], row["bin_index"]), {})
            row["bin_index"] -= run["warmup_s"]
            row.setdefault("window_start_utc", stamp(origin + timedelta(seconds=row["bin_index"])))
            row.setdefault("window_end_utc", stamp(origin + timedelta(seconds=row["bin_index"] + 1)))
            row.setdefault("released_at_utc", stamp(stored.get("released_at_utc")))
            row.setdefault("release_record_id", stored.get("event_id"))
            row.setdefault("valid", row.get("requested_mbps") is not None)
            row.setdefault("reason", None)
            # Compact Data history already includes timestamps and event_id.
            history.append({k: row.get(k) for k in (
                "flow_group_id", "bin_index", "window_start_utc", "window_end_utc",
                "requested_mbps", "released_at_utc", "available_at_utc", "valid",
                "reason", "release_record_id")})
        routes = [{
            "run_id": run["run_id"], "flow_group_id": r["flow_group_id"],
            "path_id": r["path_id"], "state_version": compact["state_version"],
            "confirmed_at": r["confirmed_at_utc"], "last_successful_change_at": None,
            "verification_status": r["verification_status"], "config_version": run["config_version"],
            "evidence_mode": "API_TEST", "last_optimization_change_at": None,
        } for r in compact["routing_state"]]
        cursor.execute("""SELECT h.*,
            (SELECT v2.available_at_utc FROM public.routing_history h2
             JOIN public.routing_versions v2 USING(run_id,state_version)
             WHERE h2.run_id=h.run_id AND h2.flow_group_id=h.flow_group_id
               AND h2.valid_from_utc=h.valid_to_utc LIMIT 1) AS closed_available_at_utc
            FROM public.routing_history h
            JOIN public.routing_versions v USING(run_id,state_version)
            WHERE h.run_id=%s AND h.valid_from_utc<=%s AND v.available_at_utc<=%s""",
                       (run["run_id"], as_of, as_of))
        intervals = [{
            "run_id": run["run_id"], "flow_group_id": row["flow_group_id"], "path_id": row["path_id"],
            "valid_from": stamp(row["valid_from_utc"]),
            "valid_to": stamp(row["valid_to_utc"]) if (row["valid_to_utc"] and row["valid_to_utc"] <= as_of
                and (row["closed_available_at_utc"] is None or row["closed_available_at_utc"] <= as_of)) else None,
            "status": "VERIFIED", "batch_id": None, "evidence_mode": "API_TEST",
        } for row in cursor]
    intervals.sort(key=lambda r: (r["flow_group_id"], r["valid_from"], r["path_id"] or ""))
    groups = sorted(r["flow_group_id"] for r in routes)
    unknown = [r["flow_group_id"] for r in routes if r["verification_status"] != "VERIFIED"]
    full = {
        "run_id": run["run_id"], "snapshot_id": compact["snapshot_id"],
        "snapshot_as_of_utc": compact["snapshot_as_of_utc"],
        "history_cutoff_utc": compact["history_cutoff_utc"], "cutoff_offset_s": cutoff,
        "lookback_L": run["lookback_bins"], "delta_s": 1, "state_version": compact["state_version"],
        "config_version": run["config_version"], "config_hash": run["config_hash"],
        "topology_version": topology["topology_version"], "topology_hash": topology["topology_hash"],
        "origin_refs": {**{k: stamp(run[k]) for k in (
            "run_started_at_utc", "traffic_started_at_utc", "measurement_started_at_utc")},
            "warmup_s": run["warmup_s"], "delta_s": 1},
        "flow_group_ids": groups, "history": history, "routing_state": routes,
        "routing_history": intervals, "link_sample_refs": [],
        "quality_status": compact["quality_status"], "missing_groups": [],
        "routing_state_complete": True, "unknown_flow_group_ids": unknown,
        "provenance": {"producer": "synthetic_api_test", "demand_channel_status": "AVAILABLE",
                       "demand_provider_name": "PostgreSQL", "schema_version": compact["schema_version"],
                       "evidence_mode": "API_TEST"},
        "reason": None, "expected_bin_count": len(groups) * run["lookback_bins"],
        "valid_bin_count": sum(r["valid"] for r in history),
    }
    full["content_hash"] = store.snapshot_content_hash(full)
    return {"sdn": full}
