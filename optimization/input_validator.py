import math
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence


def resolve_forecast_batch_id(forecast_data: Mapping[str, Any]) -> str | None:
    """Ưu tiên forecast_batch_id, fallback sang batch_id."""
    for key in ("forecast_batch_id", "batch_id"):
        val = forecast_data.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return None


def normalize_routing_entries(routing_state: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    raw = routing_state.get("routing_state")
    if isinstance(raw, list):
        return {str(item["flow_group_id"]): dict(item) for item in raw if "flow_group_id" in item}
    routes = routing_state.get("routes")
    if isinstance(routes, Mapping):
        return {str(k): dict(v) for k, v in routes.items()}
    return {
        str(k): dict(v)
        for k, v in routing_state.items()
        if isinstance(v, Mapping) and str(k).startswith("0x")
    }


def extract_forecast_series(forecast_data: Mapping[str, Any]) -> dict[str, list[Any]]:
    raw = forecast_data.get("rows")
    if raw is None:
        raw = forecast_data.get("predictions", forecast_data)

    if isinstance(raw, list):
        grouped: dict[str, dict[int, Any]] = {}
        for idx, row in enumerate(raw):
            if not isinstance(row, Mapping) or "flow_group_id" not in row:
                continue
            fg = str(row["flow_group_id"])
            offset_raw = row.get("offset", row.get("step", row.get("horizon_step", idx)))
            if isinstance(offset_raw, bool) or not isinstance(offset_raw, int):
                raise ValueError(f"Forecast row offset phải là số nguyên (int), nhận được: {offset_raw!r}")
            by_bin = grouped.setdefault(fg, {})
            if offset_raw in by_bin:
                raise ValueError(f"Trùng bin/offset {offset_raw} cho flow {fg}")
            val = row.get("predicted_mbps", row.get("demand_mbps", row.get("value")))
            by_bin[offset_raw] = val

        series_map: dict[str, list[Any]] = {}
        for fg, by_bin in grouped.items():
            offsets = sorted(by_bin.keys())
            for i in range(1, len(offsets)):
                if offsets[i] != offsets[i - 1] + 1:
                    raise ValueError(
                        f"Bin dự báo không liên tiếp cho flow {fg}: {offsets[i - 1]} -> {offsets[i]}"
                    )
            series_map[fg] = [by_bin[off] for off in offsets]
        return series_map

    if isinstance(raw, Mapping):
        out: dict[str, list[Any]] = {}
        for fg, rec in raw.items():
            if isinstance(rec, Mapping):
                arr = rec.get("predicted_mbps_array")
                if arr is None:
                    arr = rec.get("values")
                out[str(fg)] = list(arr) if isinstance(arr, Sequence) and not isinstance(arr, (str, bytes)) else []
            elif isinstance(rec, Sequence) and not isinstance(rec, (str, bytes)):
                out[str(fg)] = list(rec)
        return out

    return {}


def check_data_sync_gate(routing_state: Mapping[str, Any]) -> tuple[bool, str, str]:
    """
    Bắt buộc data_sync_gate.optimization_tick_eligible == True.
    Thiếu field, false, hoặc Data chưa ACK RoutingState/ApplyResult đều chặn tick.
    """
    gate = routing_state.get("data_sync_gate")
    if not isinstance(gate, Mapping) or gate.get("optimization_tick_eligible") is not True:
        return (
            False,
            "DATA_NOT_SYNCED",
            f"data_sync_gate.optimization_tick_eligible phải là True (nhận được: {gate!r})",
        )

    if (
        gate.get("synced") is False
        or gate.get("is_synced") is False
        or gate.get("allowed") is False
        or gate.get("can_open_tick") is False
    ):
        return False, "DATA_NOT_SYNCED", f"SDN data_sync_gate chặn mở tick mới: {dict(gate)}"

    pending = gate.get("pending_count", 0)
    if isinstance(pending, int) and not isinstance(pending, bool) and pending > 0:
        return False, "DATA_NOT_SYNCED", f"SDN data_sync_gate còn {pending} bản ghi chờ Data ACK"

    if (
        routing_state.get("data_synced") is False
        or routing_state.get("routing_state_synced") is False
        or routing_state.get("data_acked") is False
    ):
        return False, "DATA_NOT_SYNCED", "RoutingState chưa được Data ACK (data_synced=False)"

    sync_status = str(routing_state.get("sync_status", "")).upper()
    if sync_status in ("PENDING", "UNSYNCED", "SYNCING", "OUTBOX_PENDING", "FAILED"):
        return False, "DATA_NOT_SYNCED", f"Data outbox sync_status={sync_status}"

    cur_ver = routing_state.get("state_version", routing_state.get("routing_version"))
    acked_ver = routing_state.get("acked_routing_version", routing_state.get("data_acked_version"))
    if (
        cur_ver is not None
        and acked_ver is not None
        and not isinstance(cur_ver, bool)
        and not isinstance(acked_ver, bool)
        and int(acked_ver) < int(cur_ver)
    ):
        return (
            False,
            "DATA_NOT_SYNCED",
            f"Data mới ACK version {acked_ver}, chưa đồng bộ version hiện tại {cur_ver}",
        )

    return True, "OK", ""


def validate_routing_state(
    routing_state: Mapping[str, Any],
    flow_groups: Sequence[str],
    candidate_paths: Mapping[str, Sequence[str]] | None = None,
) -> tuple[bool, str, str]:
    if routing_state.get("batch_status") in ("INCONSISTENT", "APPLYING", "RECONCILING"):
        return False, "SKIPPED_IN_FLIGHT", f"Controller state is {routing_state.get('batch_status')}"

    if routing_state.get("in_flight_batch_id") is not None:
        return False, "SKIPPED_IN_FLIGHT", f"Batch in-flight: {routing_state.get('in_flight_batch_id')}"

    sync_ok, sync_code, sync_msg = check_data_sync_gate(routing_state)
    if not sync_ok:
        return False, sync_code, sync_msg

    if routing_state.get("reconcile_required", False):
        return False, "STATE_UNKNOWN", "SDN yêu cầu reconcile bảng luồng (reconcile_required=True)"

    if "routing_state_complete" in routing_state and not routing_state["routing_state_complete"]:
        return False, "STATE_INCOMPLETE", "SDN chưa hoàn tất thu thập trạng thái 48 luồng"

    state_ver = routing_state.get("state_version", routing_state.get("routing_version"))
    if state_ver is not None:
        if isinstance(state_ver, bool) or not isinstance(state_ver, int) or state_ver < 1:
            return False, "INVALID_STATE_VERSION", f"state_version/routing_version không hợp lệ: {state_ver!r}"

    routes = normalize_routing_entries(routing_state)
    for f in flow_groups:
        if f not in routes:
            return False, "STATE_UNKNOWN", f"Missing routing state for flow {f}"

        flow_state = routes[f]
        raw_status = flow_state.get("verification_status")
        if raw_status is None:
            raw_status = flow_state.get("status")

        if raw_status is None or not str(raw_status).strip():
            return False, "STATE_UNKNOWN", f"Missing verification_status/status for flow {f}"

        status_val = str(raw_status).strip().upper()
        cur_path = flow_state.get("current_path") or flow_state.get("actual_path_id")
        if status_val != "VERIFIED" or not cur_path:
            return False, "STATE_UNKNOWN", f"Unverified routing state ({status_val}) for flow {f}"

        if candidate_paths is not None and cur_path not in candidate_paths.get(f, ()):
            return False, "PATH_MAPPING_ERROR", f"Current path {cur_path} không thuộc P[{f}]"

    return True, "OK", ""


def _validate_window_bounds(f_start_raw: Any, f_end_raw: Any) -> tuple[bool, str, str]:
    if isinstance(f_start_raw, float) or isinstance(f_end_raw, float):
        return False, "INVALID_FORECAST_OFFSET", "forecast_start/forecast_end phải là số nguyên (int), không chấp nhận float"

    if (
        isinstance(f_start_raw, int)
        and not isinstance(f_start_raw, bool)
        and isinstance(f_end_raw, int)
        and not isinstance(f_end_raw, bool)
    ):
        if f_end_raw <= f_start_raw:
            return False, "INVALID_FORECAST_WINDOW", "forecast_end offset phải lớn hơn forecast_start offset"
        return True, "OK", ""

    if isinstance(f_start_raw, str) and isinstance(f_end_raw, str):
        dt_start = datetime.fromisoformat(f_start_raw.replace("Z", "+00:00"))
        dt_end = datetime.fromisoformat(f_end_raw.replace("Z", "+00:00"))
        if dt_start.tzinfo is None or dt_end.tzinfo is None:
            return False, "INVALID_TIMESTAMP", "forecast_start/forecast_end thiếu múi giờ UTC"
        if dt_end <= dt_start:
            return False, "INVALID_FORECAST_WINDOW", "forecast_end phải lớn hơn forecast_start"
        return True, "OK", ""

    return False, "INVALID_FORECAST_OFFSET", f"Kiểu forecast_start/forecast_end không hợp lệ: {type(f_start_raw).__name__}"


def validate_forecast(
    forecast_data: Mapping[str, Any],
    flow_groups: Sequence[str],
    deadline_ms: int = 200,
    apply_guard_ms: int = 200,
    now_utc: datetime | None = None,
    apply_deadline_utc: str | None = None,
) -> tuple[bool, str, str]:
    batch_status = forecast_data.get("status")
    if batch_status is None or not str(batch_status).strip() or str(batch_status).strip().upper() != "READY":
        return False, "FORECAST_NOT_READY", f"ForecastBatch chưa sẵn sàng (status={batch_status})"

    try:
        series_map = extract_forecast_series(forecast_data)
    except ValueError as exc:
        msg = str(exc)
        code = "INVALID_FORECAST_WINDOW" if "không liên tiếp" in msg else "INVALID_FORECAST_OFFSET"
        return False, code, msg

    expected_h: int | None = None
    for f in flow_groups:
        if f not in series_map:
            return False, "FORECAST_INVALID", f"Missing forecast data for flow {f}"

        arr = series_map[f]
        if not arr:
            return False, "FORECAST_INVALID", f"Empty forecast array for flow {f}"

        if expected_h is None:
            expected_h = len(arr)
        elif len(arr) != expected_h:
            return (
                False,
                "FORECAST_INVALID",
                f"Không đồng nhất horizon H cho flow {f}: {len(arr)} != {expected_h}",
            )

        for val in arr:
            if (
                isinstance(val, bool)
                or not isinstance(val, (int, float))
                or not math.isfinite(float(val))
                or float(val) < 0.0
            ):
                return False, "FORECAST_INVALID", f"Invalid predicted_mbps value for flow {f}: {val!r}"

    f_start_raw = forecast_data.get("forecast_start")
    if f_start_raw is None:
        f_start_raw = forecast_data.get("forecast_start_utc")
    f_end_raw = forecast_data.get("forecast_end")
    if f_end_raw is None:
        f_end_raw = forecast_data.get("forecast_end_utc")

    if f_start_raw is not None and f_end_raw is not None:
        win_ok, win_code, win_msg = _validate_window_bounds(f_start_raw, f_end_raw)
        if not win_ok:
            return False, win_code, win_msg

    deadline_raw = apply_deadline_utc or forecast_data.get("apply_deadline_utc")
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
                f"Quỹ thời gian còn lại ({remaining_ms:.1f}ms) nhỏ hơn {deadline_ms + apply_guard_ms}ms",
            )

    return True, "OK", ""