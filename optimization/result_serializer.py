import time
from datetime import datetime, timezone

def build_optimization_result(topology, forecast_data, routing_state, config,
                              U_old, U_new, J_old, J_new, status, solve_ms, x_vals, y_vals):
    """
    Tạo OptimizationResult: Kết quả phân tích và nghiệm solver (lưu audit/gửi Orchestrator).
    """
    proposed_changes = []
    routes = routing_state.get('routes', routing_state)
    
    for f in topology['F']:
        if y_vals.get(f) == 1:
            old_p = routes[f]['current_path']
            new_p = [p for p in topology['P'][f] if x_vals[f].get(p) == 1][0]
            proposed_changes.append({
                "flow_group_id": f,
                "old_path_id": old_p,
                "new_path_id": new_p
            })

    return {
        "run_id": forecast_data.get('run_id', 'run_001'),
        "snapshot_id": forecast_data.get('snapshot_id', 'snap_default'),
        "forecast_batch_id": forecast_data.get('forecast_batch_id', 'fc_default'),
        "expected_state_version": routing_state.get('state_version', 0),
        "config_version": config.get('version', 'v1.0'),
        "U_old": round(U_old, 4),
        "U_new": round(U_new, 4),
        "J_old": round(J_old, 4),
        "J_new": round(J_new, 4),
        "solver_status": status.upper(),
        "solve_ms": round(solve_ms, 2),
        "changed_flows_count": len(proposed_changes),
        "changes": proposed_changes
    }

def serialize_apply_request(optimization_result, forecast_data, config):
    """
    Chuyển đổi sang ApplyRequest chuẩn SDN Controller (Mục 7.5 & Bước E Mục 13.3).
    Sửa triệt để conflict: dùng decision_batch_id, changes, old_path_id, new_path_id.
    """
    return {
        "run_id": optimization_result["run_id"],
        "decision_batch_id": f"batch_{optimization_result['run_id']}_{int(time.time()*1000)}",
        "snapshot_id": optimization_result["snapshot_id"],
        "expected_state_version": optimization_result["expected_state_version"],
        "config_version": optimization_result["config_version"],
        "forecast_start": forecast_data.get("forecast_start_utc", datetime.now(timezone.utc).isoformat()),
        "forecast_end": forecast_data.get("forecast_end_utc", datetime.now(timezone.utc).isoformat()),
        "apply_deadline_utc": forecast_data.get("apply_deadline_utc", datetime.now(timezone.utc).isoformat()),
        "changes": optimization_result["changes"]
    }