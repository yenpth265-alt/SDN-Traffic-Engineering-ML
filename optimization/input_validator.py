import time

def validate_forecast(forecast_data, flow_groups):
    """Kiểm tra tính hợp lệ và thời hạn của dữ liệu dự báo H bước."""
    current_time = time.time()
    
    for f in flow_groups:
        if f not in forecast_data:
            return False, "FORECAST_INVALID", f"Missing forecast data for flow {f}"
        
        record = forecast_data[f]
        if 'predicted_mbps_array' not in record or not record['predicted_mbps_array']:
            return False, "FORECAST_INVALID", f"Empty forecast array for flow {f}"
        
        end_ts = record.get('forecast_end_ts', float('inf'))
        if current_time > end_ts:
            return False, "DATA_STALE", f"Forecast data expired for flow {f}"
            
    return True, "OK", ""

def validate_routing_state(routing_state, flow_groups):
    """
    Mục 7.4: Không chạy tối ưu nếu routing state của bất kỳ flow nào là UNKNOWN/INCONSISTENT.
    """
    if routing_state.get('batch_status') in ['INCONSISTENT', 'APPLYING', 'RECONCILING']:
        return False, "SKIPPED_IN_FLIGHT", f"Controller state is {routing_state.get('batch_status')}"

    routes = routing_state.get('routes', routing_state)
    for f in flow_groups:
        if f not in routes:
            return False, "STATE_UNKNOWN", f"Missing routing state for flow {f}"
        
        flow_state = routes[f]
        if flow_state.get('status') in ['UNKNOWN', 'INCONSISTENT'] or not flow_state.get('current_path'):
            return False, "STATE_UNKNOWN", f"Unverified/UNKNOWN current path for flow {f}"
            
    return True, "OK", ""