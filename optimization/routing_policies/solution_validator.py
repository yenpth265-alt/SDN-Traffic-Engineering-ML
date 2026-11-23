from datetime import datetime, timezone


def get_locked_groups(routes, t_hold_sec, now_utc=None):
    """
    Trả về tập các flow_group_id đang nằm trong thời gian khóa tuyến T_hold.
    Hỗ trợ last_successful_change_at dạng None, float timestamp hoặc chuỗi RFC3339 UTC của SDN.
    """
    now_dt = now_utc if now_utc is not None else datetime.now(timezone.utc)
    locked = set()

    for fg_id, info in routes.items():
        last_ts = info.get("last_successful_change_at")
        if last_ts is None:
            continue
        if isinstance(last_ts, (int, float)):
            last_dt = datetime.fromtimestamp(float(last_ts), tz=timezone.utc)
        else:
            last_dt = datetime.fromisoformat(str(last_ts).replace("Z", "+00:00"))
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)

        if (now_dt - last_dt).total_seconds() < float(t_hold_sec):
            locked.add(fg_id)

    return locked


def check_hysteresis(J_old, J_new, n_changes, h):
    """Chỉ chấp nhận đổi tuyến khi có ít nhất 1 luồng đổi và mức cải thiện >= h."""
    if n_changes <= 0:
        return False
    return (J_old - J_new) >= float(h)