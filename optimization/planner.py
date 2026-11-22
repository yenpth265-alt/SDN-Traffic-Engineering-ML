import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from topology_config import TOPOLOGY_DATA
except ImportError:
    TOPOLOGY_DATA = None

from input_validator import validate_forecast, validate_routing_state
from load_estimator import calculate_mean_demand
from routing_policies.trigger import check_trigger
from routing_policies.solution_validator import get_locked_groups, check_hysteresis
from milp.model import build_milp
from solver.solver import solve_model
from result_serializer import build_optimization_result, serialize_apply_request

def run_planner(forecast_data, routing_state, U_current_history, config, topology_override=None):
    topology = topology_override if topology_override else TOPOLOGY_DATA
    if not topology:
        return {"status": "ERROR", "reason": "Topology data missing"}
        
    F = topology['F']
    routes = routing_state.get('routes', routing_state)
    
    # 1. Kiểm tra routing state trước (chặn UNKNOWN/INCONSISTENT)
    state_ok, state_status, state_msg = validate_routing_state(routing_state, F)
    if not state_ok:
        return {"status": state_status, "reason": state_msg}

    # 2. Kiểm tra dữ liệu dự báo hợp lệ & chưa quá hạn
    fc_ok, fc_status, fc_msg = validate_forecast(forecast_data, F)
    if not fc_ok:
        return {"status": fc_status, "reason": fc_msg}

    # 3. Kiểm tra Trigger (N chu kỳ liên tiếp vượt TH)
    if not check_trigger(U_current_history, config['TH'], config['N']):
        return {"status": "NO_TRIGGER"}
        
    # 4. Tự tính Mean H (b_f) cho 48 flows
    b_f = calculate_mean_demand(forecast_data, F)
    
    # 5. Xác định các flow đang chịu Hold-down
    locked_groups = get_locked_groups(routes, config['T_hold'])
    
    # 6. Xây dựng & Giải mô hình MILP
    prob, x, y, U = build_milp(topology, b_f, locked_groups, routes, config)
    status, U_new, J_new, y_vals, x_vals, solve_ms = solve_model(
        prob, x, y, U, F, topology['P'], config['deadline_ms']
    )
    
    if status not in ['Optimal', 'Feasible']:
        return {"status": "KEPT", "reason": f"Solver status: {status}"}
         
    # 7. Kiểm định ngưỡng cải thiện Hysteresis (h)
    J_old = U_current_history[-1]
    n_changes = sum(y_vals.values())
    if not check_hysteresis(J_old, J_new, n_changes, config['h']):
        return {
            "status": "KEPT",
            "reason": "NO_IMPROVEMENT",
            "J_old": round(J_old, 4),
            "J_new": round(J_new, 4)
        }
        
    # 8. Tạo OptimizationResult và chuyển đổi sang ApplyRequest chuẩn SDN
    opt_result = build_optimization_result(
        topology, forecast_data, routing_state, config,
        J_old, U_new, J_old, J_new, status, solve_ms, x_vals, y_vals
    )
    apply_req = serialize_apply_request(opt_result, forecast_data, config)
    
    return {
        "status": "APPLY_REQUESTED",
        "optimization_result": opt_result,
        "apply_request": apply_req
    }