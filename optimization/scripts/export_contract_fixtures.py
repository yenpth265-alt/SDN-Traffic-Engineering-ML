import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from optimization.config_loader import load_optimization_config
from optimization.load_estimator import load_topology_from_configs
from optimization.planner import run_planner


def export_fixtures(out_dir: str = "optimization/fixtures/sdn_v14") -> None:
    topology = load_topology_from_configs("configs/network")
    config = load_optimization_config("configs/optimization/optimization.yaml")
    now_dt = datetime(2026, 11, 26, 5, 0, 0, tzinfo=timezone.utc)
    f_start_utc = now_dt.isoformat().replace("+00:00", "Z")
    deadline_utc = (now_dt + timedelta(seconds=4)).isoformat().replace("+00:00", "Z")

    routes = []
    for fg in topology["F"]:
        p1 = topology["P"][fg][0]
        routes.append(
            {
                "flow_group_id": fg,
                "current_path": p1,
                "actual_path_id": p1,
                "verification_status": "VERIFIED",
                "last_successful_change_at": None,
            }
        )

    routing_state = {
        "run_id": "run_interoperability_01",
        "state_version": 2,
        "routing_state_complete": True,
        "reconcile_required": False,
        "in_flight_batch_id": None,
        "data_sync_gate": {"synced": True, "pending_count": 0},
        "routing_state": routes,
    }

    congested_fgs = [fg for fg in topology["F"] if len(topology["P"][fg]) >= 2][:2]
    preds = {
        fg: {"predicted_mbps_array": [65.0, 65.0, 65.0] if fg in congested_fgs else [0.5, 0.5, 0.5]}
        for fg in topology["F"]
    }

    forecast_data = {
        "run_id": "run_interoperability_01",
        "tick_id": 100,
        "snapshot_id": "snap_interoperability_001",
        "forecast_batch_id": "fc_interoperability_100",
        "status": "READY",
        "forecast_start": 100,
        "forecast_end": 105,
        "measurement_started_at_utc": "2026-11-26T04:58:20Z",
        "delta_s": 1,
        "snapshot_as_of_utc": f_start_utc,
        "history_cutoff_utc": f_start_utc,
        "apply_deadline_utc": deadline_utc,
        "predictions": preds,
    }

    res_reroute = run_planner(
        forecast_data,
        routing_state,
        [0.91, 0.93, 0.96],
        config,
        topology_override=topology,
        now_utc=now_dt,
        decision_batch_id="dec_interoperability_tick_100",
        tick_id=100,
    )

    res_b0 = run_planner(
        forecast_data,
        routing_state,
        [0.91, 0.93, 0.96],
        dict(config, B=0),
        topology_override=topology,
        now_utc=now_dt,
        decision_batch_id="dec_interoperability_b0_tick_101",
        tick_id=101,
    )

    target = Path(out_dir)
    target.mkdir(parents=True, exist_ok=True)
    (target / "decision_batch_48_flows.json").write_text(
        json.dumps(res_reroute["decision_batch"], indent=2), encoding="utf-8"
    )
    (target / "apply_request_b_flows.json").write_text(
        json.dumps(res_reroute["apply_request"], indent=2), encoding="utf-8"
    )
    (target / "decision_batch_b0_kept.json").write_text(
        json.dumps(res_b0["decision_batch"], indent=2), encoding="utf-8"
    )
    print(f"Exported 3 contract fixtures to {target.resolve()}")


if __name__ == "__main__":
    export_fixtures()