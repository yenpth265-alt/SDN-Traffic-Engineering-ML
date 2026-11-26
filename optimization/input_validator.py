import math
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence


def normalize_routing_entries(routing_state: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """
    Chuẩn hóa routing_state từ SDN (dạng list 'routing_state' trong IF08)
    hoặc dạng dict 'routes' về dict {flow_group_id: entry_dict}.
    """
    raw = routing_state.get("routing_state")
    if isinstance(raw, list):
        entries = {str(item["flow_group_id"]): dict(item) for item in raw if "flow_group_id" in item}
        for entry in entries.values():
            if "verification_status" in entry:
                entry["status"] = entry["verification_status"]
            if "path_id" in entry:
                entry["current_path"] = entry["path_id"]
        return entries
    routes = routing_state.get("routes")
    if isinstance(routes, Mapping):
        return {str(k): dict(v) for k, v in routes.items()}
    return {
        str(k): dict(v)
        for k, v in routing_state.items()
        if isinstance(v, Mapping) and str(k).startswith("0x")
    }


def validate_routing_state(
    routing_state: Mapping[str, Any],
    flow_groups: Sequence[str],
    candidate_paths: Mapping[str, Sequence[str]] | None = None,
) -> tuple[bool, str, str]:
    """
    Kiểm tra tính toàn vẹn của RoutingStateSnapshot (IF08) và ánh xạ tuyến hợp lệ.
    """
    if routing_state.get("batch_status") in ("INCONSISTENT", "APPLYING", "RECONCILING"):
        return False, "SKIPPED_IN_FLIGHT", f"Controller state is {routing_state.get('batch_status')}"

    if routing_state.get("in_flight_batch_id") is not None:
        return False, "SKIPPED_IN_FLIGHT", f"Batch in-flight: {routing_state.get('in_flight_batch_id')}"

    if routing_state.get("reconcile_required", False):
        return False, "STATE_UNKNOWN", "SDN yêu cầu reconcile bảng luồng (reconcile_required=True)"

    if "routing_state_complete" in routing_state and not routing_state["routing_state_complete"]:
        return False, "STATE_INCOMPLETE", "SDN chưa hoàn tất thu thập trạng thái 48 luồng"

    state_ver = routing_state.get("state_version")
    if state_ver is not None:
        if isinstance(state_ver, bool) or not isinstance(state_ver, int) or state_ver < 1:
            return False, "INVALID_STATE_VERSION", f"state_version không hợp lệ: {state_ver!r}"

    routes = normalize_routing_entries(routing_state)
    for f in flow_groups:
        if f not in routes:
            return False, "STATE_UNKNOWN", f"Missing routing state for flow {f}"

        flow_state = routes[f]
        status_val = str(flow_state.get("status", "VERIFIED")).upper()
        cur_path = flow_state.get("current_path") or flow_state.get("actual_path_id")
        if status_val in ("UNKNOWN", "INCONSISTENT") or not cur_path:
            return False, "STATE_UNKNOWN", f"Unverified/UNKNOWN current path for flow {f}"

        if candidate_paths is not None and cur_path not in candidate_paths.get(f, ()):
            return False, "PATH_MAPPING_ERROR", f"Current path {cur_path} không thuộc P[{f}]"

    return True, "OK", ""


def validate_forecast(
    forecast_data: Mapping[str, Any],
    flow_groups: Sequence[str],
    deadline_ms: int = 200,
    apply_guard_ms: int = 200,
    now_utc: datetime | None = None,
) -> tuple[bool, str, str]:
    """
    Kiểm tra tính hợp lệ dữ liệu dự báo H bước, timestamp RFC3339 UTC và quỹ thời gian.
    """
    pred_map = forecast_data.get("predictions", forecast_data)
    for f in flow_groups:
        if f not in pred_map:
            return False, "FORECAST_INVALID", f"Missing forecast data for flow {f}"

        record = pred_map[f]
        arr = record.get("predicted_mbps_array") if isinstance(record, Mapping) else record
        if not arr:
            return False, "FORECAST_INVALID", f"Empty forecast array for flow {f}"
        for val in arr:
            if isinstance(val, bool) or not isinstance(val, (int, float)) or not math.isfinite(float(val)) or float(val) < 0.0:
                return False, "FORECAST_INVALID", f"Invalid predicted_mbps value for flow {f}: {val!r}"

    f_start_raw = forecast_data.get("forecast_start") or forecast_data.get("forecast_start_utc")
    f_end_raw = forecast_data.get("forecast_end") or forecast_data.get("forecast_end_utc")
    deadline_raw = forecast_data.get("apply_deadline_utc")

    if f_start_raw and f_end_raw:
        dt_start = datetime.fromisoformat(str(f_start_raw).replace("Z", "+00:00"))
        dt_end = datetime.fromisoformat(str(f_end_raw).replace("Z", "+00:00"))
        if dt_start.tzinfo is None or dt_end.tzinfo is None:
            return False, "INVALID_TIMESTAMP", "forecast_start/forecast_end thiếu múi giờ UTC"
        if dt_end <= dt_start:
            return False, "INVALID_FORECAST_WINDOW", "forecast_end phải > forecast_start"

    if deadline_raw:
        dt_deadline = datetime.fromisoformat(str(deadline_raw).replace("Z", "+00:00"))
        if dt_deadline.tzinfo is None:
            return False, "INVALID_TIMESTAMP", "apply_deadline_utc thiếu múi giờ UTC"
        ref_now = now_utc if now_utc is not None else datetime.now(timezone.utc)
        remaining_ms = (dt_deadline - ref_now).total_seconds() * 1000.0
        if remaining_ms < (deadline_ms + apply_guard_ms):
            return (
                False,
                "DATA_STALE",
                f"Quỹ thời gian còn lại ({remaining_ms:.1f}ms) < {deadline_ms + apply_guard_ms}ms",
            )

    return True, "OK", ""
