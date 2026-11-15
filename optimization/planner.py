import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from topology_config import TOPOLOGY_DATA
except ImportError:
    TOPOLOGY_DATA = None

from .input_validator import validate_forecast
from .load_estimator import calculate_mean_demand
from .trigger import check_trigger
from .solution_validator import get_locked_groups, check_hysteresis
from .model import build_milp
from .solver import solve_model
from .result_serializer import serialize_apply_request

def run_planner(forecast_data, routing_state, U_current_history, config, topology_override=None):
    """
    Hàm entry point thực hiện toàn bộ vòng lặp tối ưu trong một control tick:
    1. Kiểm tra Trigger -> 2. Validate Input -> 3. Tính Mean H (b_f) -> 
    4. Hold-down -> 5. Dựng MILP -> 6. Giải Solver -> 7. Kiểm định Hysteresis -> 8. Serializer JSON.
    """
    topology = topology_override if topology_override else TOPOLOGY_DATA
    if not topology:
        return {"status": "ERROR", "reason": "Topology data is not provided or loaded."}
        
    F = topology['F']
    
    # Bước 1: Kiểm tra điều kiện Trigger
    if not check_trigger(U_current_history, config['TH'], config['N']):
        return {"status": "NO_TRIGGER_NEEDED"}
        
    # Bước 2: Kiểm tra tính hợp lệ của dữ liệu dự báo (Fallback Policy)
    is_valid, err_status, err_msg = validate_forecast(forecast_data, F)
    if not is_valid:
        return {"status": err_status, "reason": err_msg}
        
    # Bước 3: Tự tính Mean H (b_f) cho từng nhóm lưu lượng
    b_f = calculate_mean_demand(forecast_data, F)
    
    # Bước 4: Xác định các nhóm bị khóa bởi Hold-down
    locked_groups = get_locked_groups(routing_state, config['T_hold'])
    
    # Bước 5: Xây dựng mô hình MILP
    prob, x, y, U = build_milp(topology, b_f, locked_groups, routing_state, config)
    
    # Bước 6: Kích hoạt bộ giải (Solver)
    status, U_new, J_new, y_vals, x_vals, solve_ms = solve_model(
        prob, x, y, U, F, topology['P'], config['deadline_ms']
    )
    
    if status not in ['Optimal', 'Feasible']:
         return {"status": "TIMEOUT_OR_INFEASIBLE", "solver_status": status}
         
    # Bước 7: Kiểm định điểm Hysteresis
    J_old = U_current_history[-1] if U_current_history else 1.0
    n_changes = sum(y_vals.values())
    if not check_hysteresis(J_old, J_new, n_changes, config['h']):
        return {"status": "HYSTERESIS_REJECTED", "J_old": J_old, "J_new": J_new, "changes_count": n_changes}
        
    # Bước 8: Đóng gói kết quả thành ApplyRequest JSON chuẩn
    return serialize_apply_request(
        topology, forecast_data, routing_state, config, 
        J_old, U_new, J_old, J_new, status, solve_ms, x_vals, y_vals
    )