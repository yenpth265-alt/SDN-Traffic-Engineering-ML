import time

def validate_forecast(forecast_data, flow_groups):
    """
    Kiểm tra xem dữ liệu dự báo có đủ 48 nhóm, mảng dự báo không rỗng và chưa hết hạn deadline.
    """
    current_time = time.time()
    
    for f in flow_groups:
        if f not in forecast_data:
            return False, "INVALID_RESULT", f"Missing forecast data for flow group {f}"
        
        record = forecast_data[f]
        if 'predicted_mbps_array' not in record or not record['predicted_mbps_array']:
            return False, "INVALID_RESULT", f"Empty forecast array for flow group {f}"
        
        # Kiểm tra quá hạn thời gian theo chính sách fallback
        end_ts = record.get('forecast_end_ts', float('inf'))
        if current_time > end_ts:
            return False, "TIMEOUT", f"Forecast data expired for flow group {f}"
            
    return True, "OK", ""