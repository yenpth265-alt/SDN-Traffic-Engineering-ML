import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from optimization.load_estimator import load_topology_from_configs
from optimization.planner import OptimizationController, run_planner
from optimization.result_serializer import build_decision_wire_payload


def _sample_topology() -> dict[str, Any]:
    """Topology canonical v1.0 (G0-02): flow ID 0x{src}{dst}, path {flow}_P1/_P2, 32 directed links."""
    topology = load_topology_from_configs(NETWORK_CONFIG_DIR)
    if len(topology.get("F", ())) != 48:
        raise RuntimeError(f"Topology canonical phải có 48 flow, nhận {len(topology.get('F', ()))}")
    return topology


def _sample_routing_state(topology: dict[str, Any]) -> dict[str, Any]:
    routes: dict[str, Any] = {}
    for idx, fg in enumerate(topology["F"]):
        cur_p = topology["P"][fg][0]
        routes[fg] = {
            "flow_group_id": fg,
            "path_id": cur_p,
            "verification_status": "VERIFIED",
            "last_successful_change_at": "2026-11-26T00:00:00Z" if idx == 0 else None,
        }
    return {
        "run_id": "run_g0_fixture",
        "state_version": 5,
        "routing_state_complete": True,
        "data_sync_gate": {
            "optimization_tick_eligible": True,
            "synced": True,
            "pending_count": 0,
        },
        "routes": routes,
    }


def _sample_forecast(
    topology: dict[str, Any],
    h_bins: int = 1,
    forecast_start: int = 0,
    forecast_end: int | None = None,
) -> dict[str, Any]:
    resolved_end = (forecast_start + h_bins) if forecast_end is None else forecast_end
    rows: list[dict[str, Any]] = []
    for idx, fg in enumerate(topology["F"]):
        base_demand = HIGH_DEMAND_MBPS if idx < 8 else BASE_DEMAND_MBPS
        for step in range(h_bins):
            rows.append(
                {
                    "flow_group_id": fg,
                    "offset": step,
                    "predicted_mbps": round(base_demand + step * 0.1, 6),
                }
            )
    return {
        "run_id": "run_g0_fixture",
        "forecast_batch_id": "fc_batch_001",
        "snapshot_id": "snap_001",
        "snapshot_as_of_utc": "2026-11-26T14:00:00Z",
        "history_cutoff_utc": "2026-11-26T13:59:00Z",
        "measurement_started_at_utc": "2026-11-26T14:00:00Z",
        "delta_s": 1,
        "forecast_start": forecast_start,
        "forecast_end": resolved_end,
        "status": "READY",
        "rows": rows,
    }


NETWORK_CONFIG_DIR = "configs/network"
HIGH_DEMAND_MBPS = 1.2
BASE_DEMAND_MBPS = 0.2

# Run config hash do Network/Runner cấp; khác optimizer_config_hash (tham số tối ưu).
FIXTURE_RUN_CONFIG_HASH = "sha256:" + "c" * 64


def _sample_config(budget_b: int = 4) -> dict[str, Any]:
    return {
        "config_version": "1.0",
        "config_hash": FIXTURE_RUN_CONFIG_HASH,
        "H": 1,
        "TH": 0.5,
        "N": 1,
        "B": budget_b,
        "T_hold": 30.0,
        "h": 0.0,
        "lambda_weight": 0.001,
        "deadline_ms": 200,
        "apply_guard_ms": 200,
        "w_u": 1.0,
        "w_m": 0.01,
        "w_c": 0.01,
    }


def export_fixtures(out_dir: str = "optimization/fixtures") -> None:
    target = Path(out_dir)
    wire_dir = target / "wire"
    audit_dir = target / "audit"
    target.mkdir(parents=True, exist_ok=True)
    wire_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)

    topo = _sample_topology()
    rs = _sample_routing_state(topo)
    fc = _sample_forecast(topo, h_bins=1)
    now_dt = datetime(2026, 11, 26, 14, 0, 0, tzinfo=timezone.utc)
    tick_at = "2026-11-26T14:00:00Z"
    deadline = "2026-11-26T14:00:00.900Z"

    # 1. Reroute (1..B flows)
    cfg_reroute = _sample_config(budget_b=4)
    ctrl_reroute = OptimizationController(cfg_reroute, topology_override=topo)
    res_reroute = ctrl_reroute.step(
        u_current=0.92,
        forecast_data=fc,
        routing_state=rs,
        now_utc=now_dt,
        decision_batch_id="dec_fixture_reroute_001",
        apply_deadline_utc=deadline,
        tick_id="tick_001",
        tick_at_utc=tick_at,
    )
    if res_reroute.get("status") != "APPLY_REQUESTED" or res_reroute.get("apply_request") is None:
        raise RuntimeError(f"Không tạo được fixture reroute hợp lệ: {res_reroute}")

    wire_reroute = build_decision_wire_payload(res_reroute["decision_batch"])
    apply_reroute = res_reroute["apply_request"]

    # Xuất vào wire/ (CF-42) và giữ bản tương thích tại thư mục gốc fixtures/
    (wire_dir / "decision_batch_wire_reroute.json").write_text(
        json.dumps(wire_reroute, indent=2), encoding="utf-8"
    )
    (wire_dir / "apply_request_reroute.json").write_text(
        json.dumps(apply_reroute, indent=2), encoding="utf-8"
    )
    (target / "decision_batch_wire_reroute.json").write_text(
        json.dumps(wire_reroute, indent=2), encoding="utf-8"
    )
    (target / "apply_request_reroute.json").write_text(
        json.dumps(apply_reroute, indent=2), encoding="utf-8"
    )

    # Xuất audit metadata riêng sang audit/ (CF-42 & CF-16)
    (audit_dir / "decision_batch_audit_reroute.json").write_text(
        json.dumps(res_reroute["decision_batch"], indent=2), encoding="utf-8"
    )
    (audit_dir / "controller_audit_records.json").write_text(
        json.dumps(ctrl_reroute.get_audit_records(), indent=2), encoding="utf-8"
    )

    # 2. Zero budget / KEPT (B = 0)
    cfg_zero = _sample_config(budget_b=0)
    res_zero = run_planner(
        forecast_data=fc,
        routing_state=rs,
        U_current_history=[0.92],
        config=cfg_zero,
        topology_override=topo,
        now_utc=now_dt,
        decision_batch_id="dec_fixture_zero_001",
        apply_deadline_utc=deadline,
        tick_id="tick_002",
        tick_at_utc=tick_at,
    )
    if res_zero.get("status") != "KEPT":
        raise RuntimeError(f"Không tạo được fixture zero-budget hợp lệ: {res_zero}")

    wire_zero = build_decision_wire_payload(res_zero["decision_batch"])
    (wire_dir / "decision_batch_wire_zero_b.json").write_text(
        json.dumps(wire_zero, indent=2), encoding="utf-8"
    )
    (target / "decision_batch_wire_zero_b.json").write_text(
        json.dumps(wire_zero, indent=2), encoding="utf-8"
    )
    (audit_dir / "decision_batch_audit_zero_b.json").write_text(
        json.dumps(res_zero["decision_batch"], indent=2), encoding="utf-8"
    )


if __name__ == "__main__":
    export_fixtures()