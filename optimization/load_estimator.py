def calculate_mean_demand(forecast_data, flow_groups):
    """
    Tự tính trung bình cộng b_f từ mảng predicted_mbps_array gồm H bước.
    Tuyệt đối không áp dụng thêm hệ số epsilon tại đây (epsilon đã được ML xử lý offline).
    """
    b_f = {}
    for f in flow_groups:
        h_values = forecast_data[f]['predicted_mbps_array']
        # b_f = (1 / H) * sum(predicted_mbps)
        b_f[f] = sum(h_values) / len(h_values)
    return b_f 