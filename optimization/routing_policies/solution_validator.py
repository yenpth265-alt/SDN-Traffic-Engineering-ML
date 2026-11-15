import time

def get_locked_groups(routing_state, T_hold):
    """
    Xác định các nhóm lưu lượng đang trong thời gian cấm đổi đường (Hold-down).
    """
    locked_groups = []
    current_time = time.time()
    for f, state in routing_state.items():
        last_change = state.get('last_successful_change_at')
        if last_change and (current_time - last_change) < T_hold:
            locked_groups.append(f)
    return locked_groups

def check_hysteresis(J_old, J_new, n_changes, h):
    """
    Kiểm tra điều kiện Hysteresis: Phương án mới chỉ được chấp nhận 
    khi có ít nhất 1 thay đổi và cải thiện hàm mục tiêu ít nhất tỷ lệ h.
    Công thức: J_new <= (1 - h) * J_old
    """
    if n_changes < 1:
        return False
    if J_new <= (1 - h) * J_old:
        return True
    return False