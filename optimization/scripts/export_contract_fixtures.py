import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from optimization.planner import OptimizationController, run_planner
from optimization.result_serializer import build_decision_wire_payload


def _sample_topology() -> dict[str, Any]:
    flows = [f"0x{100 + i:03x}" for i in range(48)]
    paths = {fg: [f"p_{fg}_a", f"p_{fg}_b"] for fg in flows}
    links = {
        "l_core_1": {"capacity_mbps": 600.0},
        "l_core_2": {"capacity_mbps": 2000.0},
    }
    path_links: dict[str, list[str]] = {}
    for idx, fg in enumerate(flows):
        path_links[f"p_{fg}_a"] = ["l_core_1"] if idx % 2 == 0 else ["l_core_2"]
        path_links[f"p_{fg}_b"] = ["l_core_2"] if idx % 2 == 0 else ["l_core_1"]

    return {
        "topology_version": "1.0",
        "topology_hash": "sha256:topo_v1_0_fixture",
        "F": flows,
        "P": paths,
        "E": list(links.keys()),
        "C": {"l_core_1": 600.0, "l_core_2": 2000.0},
        "links": links,
        "path_links": path_links,
        "a_fpe": {
            fg: {
                path_id: {e: (1 if e in set(path_links.get(path_id, ())) else 0) for e in links}
                for path_id in paths[fg]
            }
            for fg in flows
        },
    }


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
        base_demand = 180.0 if idx < 8 else 40.0
        for step in range(h_bins):
            rows.append(
                {
                    "flow_group_id": fg,
                    "offset": step,
                    "predicted_mbps": base_demand + step * 2.0,
                }
            )
    return {
        "run_id": "run_g0_fixture",
        "forecast_batch_id": "fc_batch_001",
        "snapshot_id": "snap_001",
        "snapshot_as_of_utc": "2026-11-26T14:00:00Z",
        "history_cutoff_utc": "2026-11-26T13:59:00Z",
        "measurement_started_at_utc": "2026-11-26T14:00:00Z",
        "delta_s": 10.0,
        "forecast_start": forecast_start,
        "forecast_end": resolved_end,
        "status": "READY",
        "rows": rows,
    }


def _sample_config(budget_b: int = 4) -> dict[str, Any]:
    return {
        "config_version": "1.0",
        "H": 1,
        "TH": 0.5,
        "N": 1,
        "B": budget_b,
        "T_hold": 30.0,
        "h": 0.0,
        "lambda_weight": 0.001,
        "deadline_ms": 200,
        "apply_guard_ms": 200,
        "C_e": 1000.0,
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
    deadline = "2026-11-26T14:00:05Z"

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