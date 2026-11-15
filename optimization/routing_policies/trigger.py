def check_trigger(U_current_history, TH, N):
    """
    Kích hoạt tối ưu nếu tải U_current vượt ngưỡng TH liên tục trong N chu kỳ (ticks).
    """
    if len(U_current_history) < N:
        return False
    return all(u > TH for u in U_current_history[-N:])