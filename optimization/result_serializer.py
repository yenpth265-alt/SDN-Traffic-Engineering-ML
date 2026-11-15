import time
from datetime import datetime, timezone

def serialize_apply_request(topology, forecast_data, routing_state, config, 
                            U_old, U_new, J_old, J_new, status, solve_ms, x_vals, y_vals):
    """
    Tạo payload ApplyRequest chuẩn gửi cho SDN Controller.
    """
    changes = []
    for f in topology['F']:
        if y_vals.get(f) == 1:
            old_p = routing_state[f]['current_path']
            # Tìm đường mới có x_fp = 1 trong danh sách candidate paths của f
            new_p = [p for p in topology['P'][f] if x_vals[f].get(p) == 1][0]
            
            changes.append({
                "flow_group_id": f,  # Sử dụng chuẩn Hex Network (ví dụ: '0x103')
                "old_path_id": old_p,
                "new_path_id": new_p,
                "status": "PROPOSED",
                "reason": f"U_current > {config['TH']}, solver proposed path change."
            })
            
    return {
        "run_id": forecast_data.get('run_id', 'run_default'),
        "decision_batch_id": f"batch_{int(time.time()*1000)}",
        "snapshot_id": forecast_data.get('snapshot_id', 'snap_default'),
        "config_version": config.get('version', 'v1.0'),
        "expected_state_version": routing_state.get('state_version', 0),
        "demand_source": forecast_data.get('source', 'rf_v1'),
        
        "forecast_start": forecast_data.get('forecast_start_utc', datetime.now(timezone.utc).isoformat()),
        "forecast_end": forecast_data.get('forecast_end_utc', datetime.now(timezone.utc).isoformat()),
        "deadline": forecast_data.get('apply_deadline_utc', datetime.now(timezone.utc).isoformat()),
        "target_interval": f"{config.get('H', 5)}s",
        
        "U_old": round(U_old, 4),
        "U_new": round(U_new, 4),
        "J_old": round(J_old, 4),
        "J_new": round(J_new, 4),
        "solver_status": status.upper(),
        "solve_ms": round(solve_ms, 2),
        "changes": changes
    }