import math
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence


def resolve_flow_path_id(flow_state: Mapping[str, Any]) -> str | None:
    """Đọc tuyến hiện tại từ path_id (SDN canonical) hoặc current_path / actual_path_id."""
    for key in ("path_id", "current_path", "actual_path_id"):
        val = flow_state.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return None


def resolve_forecast_batch_id(forecast_data: Mapping[str, Any]) -> str | None:
    for key in ("forecast_batch_id", "batch_id"):
        val = forecast_data.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return None


def _normalize_sha256(raw: Any) -> str:
    val = str(raw).strip()
    if not val:
        return ""
    return val if val.startswith("sha256:") else f"sha256:{val}"


def _parse_iso_utc(raw: Any, field_name: str) -> tuple[datetime | None, str]:
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            return None, f"{field_name} thiếu múi giờ UTC"
        return raw.astimezone(timezone.utc), ""
    if not isinstance(raw, str) or not raw.strip():
        return None, f"{field_name} phải là chuỗi thời gian UTC hợp lệ"
    try:
        dt = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except ValueError:
        return None, f"{field_name} sai định dạng ISO-8601: {raw!r}"
    if dt.tzinfo is None:
        return None, f"{field_name} thiếu múi giờ UTC"
    return dt.astimezone(timezone.utc), ""


def normalize_routing_entries(routing_state: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    raw = routing_state.get("routing_state")
    if isinstance(raw, list):
        routes = {str(item["flow_group_id"]): dict(item) for item in raw if "flow_group_id" in item}
    elif isinstance(routing_state.get("routes"), Mapping):
        routes = {str(k): dict(v) for k, v in routing_state["routes"].items()}
    else:
        routes = {str(k): dict(v) for k, v in routing_state.items()
                  if isinstance(v, Mapping) and str(k).startswith("0x")}
    for row in routes.values():
        if row.get("verification_status") is not None:
            row["status"] = row["verification_status"]
        path_id = resolve_flow_path_id(row)
        if path_id is not None:
            row["current_path"] = path_id
    return routes


def _extract_row_bin_offset(row: Mapping[str, Any], fg: str) -> int:
    offset_raw: Any = None
    for key in ("forecast_start", "offset", "step", "horizon_step"):
        if key in row and row[key] is not None:
            offset_raw = row[key]
            break

    if offset_raw is None or isinstance(offset_raw, bool) or not isinstance(offset_raw, int):
        raise ValueError(f"Forecast row cho flow {fg} thiếu hoặc sai kiểu bin integer: {offset_raw!r}")

    if "forecast_end" in row and row["forecast_end"] is not None:
        end_raw = row["forecast_end"]
        if isinstance(end_raw, bool) or not isinstance(end_raw, int) or end_raw != offset_raw + 1:
            raise ValueError(f"Forecast row cho flow {fg} có forecast_end không hợp lệ")

    return offset_raw


def extract_forecast_with_offsets(
    forecast_data: Mapping[str, Any],
) -> tuple[dict[str, list[Any]], dict[str, tuple[int, ...]]]:
    raw = forecast_data.get("rows")
    if raw is None:
        raw = forecast_data.get("predictions", forecast_data)

    if isinstance(raw, list):
        grouped: dict[str, dict[int, Any]] = {}
        for row in raw:
            if not isinstance(row, Mapping) or "flow_group_id" not in row:
                raise ValueError("Forecast row requires flow_group_id")
            fg = str(row["flow_group_id"])
            offset_val = _extract_row_bin_offset(row, fg)
            by_bin = grouped.setdefault(fg, {})
            if offset_val in by_bin:
                raise ValueError(f"Trùng bin/offset {offset_val} cho flow {fg}")
            by_bin[offset_val] = row.get("predicted_mbps", row.get("demand_mbps", row.get("value")))

        series_map: dict[str, list[Any]] = {}
        offset_map: dict[str, tuple[int, ...]] = {}
        for fg, by_bin in grouped.items():
            offsets = sorted(by_bin.keys())
            for i in range(1, len(offsets)):
                if offsets[i] != offsets[i - 1] + 1:
                    raise ValueError(f"Bin dự báo không liên tiếp cho flow {fg}: {offsets[i - 1]} -> {offsets[i]}")
            offset_map[fg] = tuple(offsets)
            series_map[fg] = [by_bin[off] for off in offsets]
        return series_map, offset_map

    if isinstance(raw, Mapping):
        out: dict[str, list[Any]] = {}
        offset_map = {}
        for fg, rec in raw.items():
            if isinstance(rec, Mapping):
                arr = rec.get("predicted_mbps_array") if "predicted_mbps_array" in rec else rec.get("values")
                vals = list(arr) if isinstance(arr, Sequence) and not isinstance(arr, (str, bytes)) else []
                out[str(fg)] = vals
                offset_map[str(fg)] = tuple(range(len(vals)))
            elif isinstance(rec, Sequence) and not isinstance(rec, (str, bytes)):
                vals = list(rec)
                out[str(fg)] = vals
                offset_map[str(fg)] = tuple(range(len(vals)))
        return out, offset_map

    return {}, {}


def extract_forecast_series(forecast_data: Mapping[str, Any]) -> dict[str, list[Any]]:
    series_map, _ = extract_forecast_with_offsets(forecast_data)
    return series_map


def check_data_sync_gate(routing_state: Mapping[str, Any]) -> tuple[bool, str, str]:
    gate = routing_state.get("data_sync_gate")
    if not isinstance(gate, Mapping) or gate.get("optimization_tick_eligible") is not True:
        return False, "DATA_NOT_SYNCED", f"data_sync_gate.optimization_tick_eligible phải là True ({gate!r})"

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
        return False, "DATA_NOT_SYNCED", "RoutingState chưa được Data ACK"

    sync_status = str(routing_state.get("sync_status", "")).upper()
    if sync_status in ("PENDING", "UNSYNCED", "SYNCING", "OUTBOX_PENDING", "FAILED"):
        return False, "DATA_NOT_SYNCED", f"Data outbox sync_status={sync_status}"

    return True, "OK", ""


def validate_context_binding(
    forecast_data: Mapping[str, Any],
    routing_state: Mapping[str, Any],
    topology: Mapping[str, Any],
    config: Mapping[str, Any],
) -> tuple[bool, str, str]:
    """Đối chiếu ràng buộc run_id, snapshot_id, topology/config version & hash (CF-05, CF-11)."""
    fc_run = str(forecast_data.get("run_id") or "").strip()
    rs_run = str(routing_state.get("run_id") or "").strip()
    cfg_run = str(config.get("run_id") or "").strip()
    if not fc_run or not rs_run or not str(forecast_data.get("snapshot_id") or "").strip():
        return False, "MISSING_CONTEXT", "Forecast and routing require run_id; forecast requires snapshot_id"
    if fc_run and rs_run and fc_run != rs_run:
        return False, "CONTEXT_MISMATCH", f"Lệch run_id giữa forecast ({fc_run}) và routing_state ({rs_run})"
    if cfg_run and fc_run and cfg_run != fc_run:
        return False, "CONTEXT_MISMATCH", f"Lệch run_id với cấu hình run ({cfg_run} != {fc_run})"
    if cfg_run and rs_run and cfg_run != rs_run:
        return False, "CONTEXT_MISMATCH", f"Lệch run_id giữa cấu hình ({cfg_run}) và routing_state ({rs_run})"

    fc_snap = str(forecast_data.get("snapshot_id") or "").strip()
    rs_snap = str(routing_state.get("snapshot_id") or "").strip()
    if fc_snap and rs_snap and fc_snap != rs_snap:
        return False, "CONTEXT_MISMATCH", f"Lệch snapshot_id ({fc_snap} != {rs_snap})"

    expected_topo_ver = str(topology.get("topology_version") or "").strip()
    expected_topo_hash = _normalize_sha256(topology.get("topology_hash") or "")
    for src_name, src in (("routing_state", routing_state), ("forecast_data", forecast_data)):
        src_ver = src.get("topology_version")
        if src_ver is not None and str(src_ver).strip() and expected_topo_ver:
            if str(src_ver).strip() != expected_topo_ver:
                return False, "CONTEXT_MISMATCH", f"Lệch topology_version ở {src_name}: {src_ver} != {expected_topo_ver}"
        src_hash = src.get("topology_hash")
        if src_hash is not None and str(src_hash).strip() and expected_topo_hash:
            if _normalize_sha256(src_hash) != expected_topo_hash:
                return False, "CONTEXT_MISMATCH", f"Lệch topology_hash ở {src_name}: {src_hash} != {expected_topo_hash}"

    expected_cfg_ver = str(config.get("config_version") or "").strip()
    expected_cfg_hash = _normalize_sha256(config.get("config_hash") or "")
    for src_name, src in (("routing_state", routing_state), ("forecast_data", forecast_data)):
        c_ver = src.get("config_version")
        if c_ver is not None and str(c_ver).strip() and expected_cfg_ver:
            if str(c_ver).strip() != expected_cfg_ver:
                return False, "CONTEXT_MISMATCH", f"Lệch config_version ở {src_name}: {c_ver} != {expected_cfg_ver}"
        c_hash = src.get("config_hash")
        if c_hash is not None and str(c_hash).strip() and expected_cfg_hash:
            if _normalize_sha256(c_hash) != expected_cfg_hash:
                return False, "CONTEXT_MISMATCH", f"Lệch config_hash ở {src_name}: {c_hash} != {expected_cfg_hash}"

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
        return False, "STATE_UNKNOWN", "SDN yêu cầu reconcile bảng luồng"

    if "routing_state_complete" in routing_state and not routing_state["routing_state_complete"]:
        return False, "STATE_INCOMPLETE", "SDN chưa hoàn tất thu thập trạng thái 48 luồng"

    state_ver = routing_state.get("state_version", routing_state.get("routing_version"))
    if state_ver is None or isinstance(state_ver, bool) or not isinstance(state_ver, int) or state_ver < 1:
        return False, "INVALID_STATE_VERSION", f"Thiếu hoặc sai state_version/routing_version: {state_ver!r}"

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
        cur_path = resolve_flow_path_id(flow_state)
        if status_val != "VERIFIED" or not cur_path:
            return False, "STATE_UNKNOWN", f"Unverified routing state ({status_val}) for flow {f}"

        if candidate_paths is not None and cur_path not in candidate_paths.get(f, ()):
            return False, "PATH_MAPPING_ERROR", f"Current path {cur_path} không thuộc P[{f}]"

    return True, "OK", ""


def _validate_window_bounds(
    f_start_raw: Any,
    f_end_raw: Any,
    expected_bins: int | None = None,
) -> tuple[bool, str, str, datetime | None]:
    if isinstance(f_start_raw, (bool, float)) or isinstance(f_end_raw, (bool, float)):
        return False, "INVALID_FORECAST_OFFSET", "forecast_start/forecast_end không được là bool hoặc float", None

    if isinstance(f_start_raw, int) and isinstance(f_end_raw, int):
        if f_end_raw <= f_start_raw:
            return False, "INVALID_FORECAST_WINDOW", "forecast_end phải lớn hơn forecast_start", None
        if expected_bins is not None and (f_end_raw - f_start_raw) != expected_bins:
            return (
                False,
                "INVALID_FORECAST_WINDOW",
                f"Số bin ({expected_bins}) không khớp cửa sổ [{f_start_raw}, {f_end_raw})",
                None,
            )
        return True, "OK", "", None

    if isinstance(f_start_raw, (str, datetime)) and isinstance(f_end_raw, (str, datetime)):
        dt_start, err_s = _parse_iso_utc(f_start_raw, "forecast_start")
        if dt_start is None:
            return False, "INVALID_TIMESTAMP", err_s, None
        dt_end, err_e = _parse_iso_utc(f_end_raw, "forecast_end")
        if dt_end is None:
            return False, "INVALID_TIMESTAMP", err_e, None
        if dt_end <= dt_start:
            return False, "INVALID_FORECAST_WINDOW", "forecast_end phải lớn hơn forecast_start", None
        return True, "OK", "", dt_end

    return (
        False,
        "INVALID_FORECAST_OFFSET",
        f"Kiểu forecast_start/forecast_end không hợp lệ: {type(f_start_raw).__name__}",
        None,
    )


def validate_forecast(
    forecast_data: Mapping[str, Any],
    flow_groups: Sequence[str],
    deadline_ms: int = 200,
    apply_guard_ms: int = 200,
    now_utc: datetime | None = None,
    apply_deadline_utc: str | None = None,
    expected_horizon: int | None = None,
) -> tuple[bool, str, str]:
    # CF-09: Chỉ từ chối nếu status có mặt và khác READY; chấp nhận forecast đủ dòng không kèm status
    batch_status = forecast_data.get("status")
    if batch_status is not None and str(batch_status).strip():
        if str(batch_status).strip().upper() != "READY":
            return False, "FORECAST_NOT_READY", f"ForecastBatch chưa sẵn sàng (status={batch_status})"

    try:
        series_map, offset_map = extract_forecast_with_offsets(forecast_data)
    except ValueError as exc:
        msg = str(exc)
        code = "INVALID_FORECAST_WINDOW" if "không liên tiếp" in msg else "INVALID_FORECAST_OFFSET"
        return False, code, msg

    if set(series_map) != set(flow_groups):
        return False, "FORECAST_INVALID", "Forecast groups differ from topology"

    expected_offsets: tuple[int, ...] | None = None
    for f in flow_groups:
        if f not in series_map:
            return False, "FORECAST_INVALID", f"Missing forecast data for flow {f}"

        arr = series_map[f]
        if not arr:
            return False, "FORECAST_INVALID", f"Empty forecast array for flow {f}"

        flow_offsets = offset_map.get(f, ())
        if expected_offsets is None:
            expected_offsets = flow_offsets
            # CF-12: Ép kiểm tra Horizon H khớp cấu hình model/run
            if expected_horizon is not None:
                if isinstance(expected_horizon, bool) or not isinstance(expected_horizon, int) or expected_horizon < 1:
                    return False, "FORECAST_INVALID", f"expected_horizon không hợp lệ: {expected_horizon!r}"
                if len(expected_offsets) != expected_horizon:
                    return False, "FORECAST_INVALID", f"Horizon H không khớp ({len(expected_offsets)} != {expected_horizon})"
        elif flow_offsets != expected_offsets:
            return False, "FORECAST_INVALID", f"Tập bin dự báo không đồng nhất cho flow {f}"

        for val in arr:
            if (
                isinstance(val, bool)
                or not isinstance(val, (int, float))
                or not math.isfinite(float(val))
                or float(val) < 0.0
            ):
                return False, "FORECAST_INVALID", f"Invalid predicted_mbps value for flow {f}: {val!r}"

    # CF-13 & CF-14: Kiểm tra window bounds, timestamp an toàn, cấm thiếu context online
    f_start_raw = (
        forecast_data.get("forecast_start")
        if "forecast_start" in forecast_data
        else forecast_data.get("forecast_start_utc")
    )
    f_end_raw = (
        forecast_data.get("forecast_end")
        if "forecast_end" in forecast_data
        else forecast_data.get("forecast_end_utc")
    )
    if f_start_raw is None or f_end_raw is None:
        return False, "MISSING_CONTEXT", "ForecastBatch thiếu forecast_start hoặc forecast_end"

    expected_bins = len(expected_offsets) if expected_offsets is not None else None
    win_ok, win_code, win_msg, dt_window_end = _validate_window_bounds(
        f_start_raw, f_end_raw, expected_bins=expected_bins
    )
    if not win_ok:
        return False, win_code, win_msg

    origin_refs = forecast_data.get("origin_refs") or {}
    delta_s = forecast_data.get("delta_s", origin_refs.get("delta_s", 1))
    if (isinstance(delta_s, bool) or not isinstance(delta_s, (int, float))
            or not math.isfinite(delta_s) or delta_s <= 0):
        return False, "INVALID_FORECAST_WINDOW", "delta_s must be finite and positive"
    origin_raw = forecast_data.get("measurement_started_at_utc") or origin_refs.get("measurement_started_at_utc")
    origin, origin_error = _parse_iso_utc(origin_raw, "measurement_started_at_utc")
    if origin is None:
        return False, "MISSING_CONTEXT", origin_error
    from datetime import timedelta
    if isinstance(f_start_raw, int):
        if f_start_raw < 0:
            return False, "INVALID_FORECAST_WINDOW", "Forecast bins cannot be negative"
        dt_start = origin + timedelta(seconds=f_start_raw * delta_s)
        dt_window_end = origin + timedelta(seconds=f_end_raw * delta_s)
        anchor = f_start_raw
    else:
        dt_start, _ = _parse_iso_utc(f_start_raw, "forecast_start")
        if abs((dt_window_end - dt_start).total_seconds() - expected_bins * delta_s) > 1e-6:
            return False, "INVALID_FORECAST_WINDOW", "UTC span differs from H * delta_s"
        anchor = (dt_start - origin).total_seconds() / delta_s
        if anchor < 0 or abs(anchor - round(anchor)) > 1e-6:
            return False, "INVALID_FORECAST_WINDOW", "UTC window is not aligned with measurement bins"
    if "rows" in forecast_data:
        rows = forecast_data["rows"]
        if rows and all("forecast_start" in row for row in rows):
            if tuple(expected_offsets) != tuple(range(round(anchor), round(anchor) + expected_bins)):
                return False, "INVALID_FORECAST_WINDOW", "Row bins differ from forecast window"

    if "tick_at_utc" in forecast_data and forecast_data["tick_at_utc"] is not None:
        dt_tick, err_tick = _parse_iso_utc(forecast_data["tick_at_utc"], "tick_at_utc")
        if dt_tick is None:
            return False, "INVALID_TIMESTAMP", err_tick

    deadline_raw = apply_deadline_utc or forecast_data.get("apply_deadline_utc")
    if not deadline_raw:
        return False, "MISSING_CONTEXT", "Thiếu apply_deadline_utc cho chu kỳ tối ưu online"

    dt_deadline, err_dl = _parse_iso_utc(deadline_raw, "apply_deadline_utc")
    if dt_deadline is None:
        return False, "INVALID_TIMESTAMP", err_dl

    if dt_window_end is not None and dt_deadline > dt_window_end:
        return False, "INVALID_DEADLINE", "apply_deadline_utc không được vượt quá forecast_end"

    if now_utc is None:
        return False, "MISSING_CONTEXT", "Thiếu now_utc của tick hiện tại"

    dt_now, err_now = _parse_iso_utc(now_utc, "now_utc")
    if dt_now is None:
        return False, "INVALID_TIMESTAMP", err_now

    remaining_ms = (dt_deadline - dt_now).total_seconds() * 1000.0
    if remaining_ms < (deadline_ms + apply_guard_ms):
        return False, "DATA_STALE", f"Quỹ thời gian còn lại ({remaining_ms:.1f}ms) nhỏ hơn {deadline_ms + apply_guard_ms}ms"

    return True, "OK", ""
