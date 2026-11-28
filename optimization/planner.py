from collections import deque
from datetime import datetime
from typing import Any, Mapping, Sequence

from optimization.input_validator import (
    normalize_routing_entries,
    validate_forecast,
    validate_routing_state,
)
from optimization.load_estimator import (
    calculate_mean_demand,
    evaluate_routing_utilization,
    load_topology_from_configs,
)
from optimization.milp.model import build_milp
from optimization.result_serializer import build_optimization_result, serialize_apply_request
from optimization.routing_policies.solution_validator import check_hysteresis, get_locked_groups
from optimization.routing_policies.trigger import check_trigger
from optimization.solver.solver import solve_model


def run_planner(
    forecast_data: Mapping[str, Any],
    routing_state: Mapping[str, Any],
    U_current_history: Sequence[float],
    config: Mapping[str, Any],
    topology_override: Mapping[str, Any] | None = None,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    topology = (
        dict(topology_override)
        if topology_override is not None
        else load_topology_from_configs(config.get("network_config_dir", "configs/network"))
    )
    if not topology:
        return {"status": "ERROR", "reason": "Topology data missing"}

    # Đồng bộ topology_version & topology_hash từ topology_bundle.json sang config nếu thiếu
    merged_config = dict(config)
    merged_config.setdefault("topology_version", topology.get("topology_version", "1.0"))
    if topology.get("topology_hash") and not merged_config.get("topology_hash"):
        merged_config["topology_hash"] = topology["topology_hash"]

    F = topology["F"]
    if config.get("ml_deployment_config"):
        from optimization.ml_contract import normalize_forecast
        try:
            forecast_data = normalize_forecast(forecast_data, F, config["ml_deployment_config"])
        except (ValueError, KeyError, TypeError, OSError) as exc:
            return {"status": "FORECAST_INVALID", "reason": str(exc)}
    routes = normalize_routing_entries(routing_state)

    # 1. Kiểm tra routing state trước (chặn UNKNOWN / INCONSISTENT / IN_FLIGHT / sai path)
    state_ok, state_status, state_msg = validate_routing_state(routing_state, F, topology["P"])
    if not state_ok:
        return {"status": state_status, "reason": state_msg}

    # 2. Kiểm tra dữ liệu dự báo hợp lệ & quỹ thời gian trước apply_deadline_utc
    fc_ok, fc_status, fc_msg = validate_forecast(
        forecast_data,
        F,
        deadline_ms=int(merged_config.get("deadline_ms", 200)),
        apply_guard_ms=int(merged_config.get("apply_guard_ms", 200)),
        now_utc=now_utc,
    )
    if not fc_ok:
        return {"status": fc_status, "reason": fc_msg}

    # 3. Kiểm tra Trigger (N chu kỳ liên tiếp vượt ngưỡng TH)
    if not check_trigger(U_current_history, float(merged_config["TH"]), int(merged_config["N"])):
        return {"status": "NO_TRIGGER"}

    # 4. Tính nhu cầu trung bình b_f và đánh giá U_old (= J_old) của tuyến hiện tại trên tải dự báo
    b_f = calculate_mean_demand(forecast_data, F)
    default_ce = float(merged_config.get("C_e", 100.0))
    U_old = evaluate_routing_utilization(topology, b_f, routes, default_ce=default_ce)
    J_old = U_old

    # Nếu trên cửa sổ dự báo H*, tải cực đại của tuyến hiện tại đã dưới ngưỡng TH thì giữ nguyên
    if U_old <= float(merged_config["TH"]):
        return {
            "status": "KEPT",
            "reason": "FORECAST_BELOW_THRESHOLD",
            "U_old": round(U_old, 4),
        }

    # 5. Xác định các nhóm luồng đang chịu Hold-down
    locked_groups = get_locked_groups(routes, float(merged_config["T_hold"]), now_utc=now_utc)

    # 6. Xây dựng & Giải mô hình MILP
    prob, x, y, U = build_milp(topology, b_f, locked_groups, routes, merged_config)
    status, U_new, J_new, y_vals, x_vals, solve_ms = solve_model(
        prob,
        x,
        y,
        U,
        F,
        topology["P"],
        int(merged_config["deadline_ms"]),
        topology=topology,
        b_f=b_f,
        routing_state=routes,
        locked_groups=locked_groups,
        config=merged_config,
    )

    if status not in ["Optimal", "Feasible"]:
        return {"status": "KEPT", "reason": f"Solver status: {status}"}

    # 7. Kiểm định ngưỡng cải thiện Hysteresis (h)
    n_changes = sum(y_vals.values())
    if not check_hysteresis(J_old, J_new, n_changes, float(merged_config["h"])):
        return {
            "status": "KEPT",
            "reason": "NO_IMPROVEMENT",
            "U_old": round(U_old, 4),
            "U_new": round(U_new, 4),
            "J_old": round(J_old, 4),
            "J_new": round(J_new, 4),
        }

    # 8. Tạo OptimizationResult và đóng gói ApplyRequest chuẩn SDN v1.4
    opt_result = build_optimization_result(
        topology,
        forecast_data,
        routing_state,
        merged_config,
        U_old,
        U_new,
        J_old,
        J_new,
        status,
        solve_ms,
        x_vals,
        y_vals,
    )
    apply_req = serialize_apply_request(opt_result, forecast_data, merged_config)

    return {
        "status": "APPLY_REQUESTED",
        "optimization_result": opt_result,
        "apply_request": apply_req,
    }


class OptimizationController:
    """
    Wrapper có trạng thái cho Runner/Orchestrator: tự lưu cửa sổ lịch sử U_current_history
    và tự reset chuỗi trigger khi dữ liệu bị gián đoạn/stale.
    """

    def __init__(self, config: Mapping[str, Any], topology_override: Mapping[str, Any] | None = None) -> None:
        self.config = dict(config)
        self.topology = (
            dict(topology_override)
            if topology_override is not None
            else load_topology_from_configs(self.config.get("network_config_dir", "configs/network"))
        )
        n_win = max(1, int(self.config.get("N", 3)))
        self._u_history: deque[float] = deque(maxlen=n_win)

    def step(
        self,
        u_current: float,
        forecast_data: Mapping[str, Any],
        routing_state: Mapping[str, Any],
        now_utc: datetime | None = None,
    ) -> dict[str, Any]:
        self._u_history.append(float(u_current))
        res = run_planner(
            forecast_data=forecast_data,
            routing_state=routing_state,
            U_current_history=list(self._u_history),
            config=self.config,
            topology_override=self.topology,
            now_utc=now_utc,
        )
        # Nếu trạng thái mạng không xác định hoặc dữ liệu quá hạn -> xóa lịch sử trigger để tránh kích hoạt giả
        if res["status"] in ("STATE_UNKNOWN", "STATE_INCOMPLETE", "DATA_STALE", "FORECAST_INVALID"):
            self._u_history.clear()
        return res
