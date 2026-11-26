import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

try:
    from sdn.schemas.apply_models import compute_apply_payload_hash as _sdn_compute_hash
except ImportError:
    _sdn_compute_hash = None


def _to_rfc3339_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _parse_optional_utc(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    dt = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _resolve_forecast_batch_id(forecast_data: Mapping[str, Any]) -> str | None:
    """Ưu tiên forecast_batch_id (đề xuất của ML), fallback sang batch_id (tên cũ)."""
    for key in ("forecast_batch_id", "batch_id"):
        val = forecast_data.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return None


def _extract_decision_batch_bounds(
    forecast_data: Mapping[str, Any], now_utc: datetime
) -> tuple[int | str, int | str]:
    f_start = forecast_data.get("forecast_start")
    if f_start is None:
        f_start = forecast_data.get("forecast_start_utc")
    f_end = forecast_data.get("forecast_end")
    if f_end is None:
        f_end = forecast_data.get("forecast_end_utc")

    if (
        isinstance(f_start, int)
        and not isinstance(f_start, bool)
        and isinstance(f_end, int)
        and not isinstance(f_end, bool)
    ):
        return f_start, f_end

    s_str = str(f_start) if f_start is not None else _to_rfc3339_utc(now_utc)
    e_str = str(f_end) if f_end is not None else s_str
    return s_str, e_str


def _extract_apply_request_utc_bounds(
    forecast_data: Mapping[str, Any], now_utc: datetime
) -> tuple[str, str]:
    """Quy đổi integer bin offsets (vd: 100..105) sang chuỗi RFC3339 UTC khớp ApplyRequest.from_dict(strict=True)."""
    s_utc = forecast_data.get("forecast_start_utc")
    e_utc = forecast_data.get("forecast_end_utc")
    if isinstance(s_utc, str) and s_utc.strip() and isinstance(e_utc, str) and e_utc.strip():
        return s_utc.strip(), e_utc.strip()

    f_start = forecast_data.get("forecast_start")
    f_end = forecast_data.get("forecast_end")
    if isinstance(f_start, str) and f_start.strip() and isinstance(f_end, str) and f_end.strip():
        return f_start.strip(), f_end.strip()

    origin_refs = forecast_data.get("origin_refs")
    if not isinstance(origin_refs, Mapping):
        origin_refs = {}

    delta_s = float(forecast_data.get("delta_s") or origin_refs.get("delta_s") or 1.0)
    if delta_s <= 0:
        delta_s = 1.0

    if (
        isinstance(f_start, int)
        and not isinstance(f_start, bool)
        and isinstance(f_end, int)
        and not isinstance(f_end, bool)
    ):
        meas_start = _parse_optional_utc(
            forecast_data.get("measurement_started_at_utc")
            or origin_refs.get("measurement_started_at_utc")
        )
        if meas_start is not None:
            dt_s = meas_start + timedelta(seconds=f_start * delta_s)
            dt_e = meas_start + timedelta(seconds=f_end * delta_s)
            return _to_rfc3339_utc(dt_s), _to_rfc3339_utc(dt_e)

        span_bins = max(1, f_end - f_start)
        base_dt = _parse_optional_utc(forecast_data.get("snapshot_as_of_utc")) or now_utc
        return _to_rfc3339_utc(base_dt), _to_rfc3339_utc(base_dt + timedelta(seconds=span_bins * delta_s))

    return _to_rfc3339_utc(now_utc), _to_rfc3339_utc(now_utc + timedelta(seconds=1))


def resolve_decision_batch_id(
    provided_id: str | None,
    run_id: str,
    now_utc: datetime,
    tick_id: int | str | None = None,
) -> str:
    """Không sinh UUID ngẫu nhiên: ưu tiên ID từ Runner/Data và giữ định danh tất định theo run_id + tick_id."""
    if provided_id and str(provided_id).strip():
        return str(provided_id).strip()
    if tick_id is not None and str(tick_id).strip():
        return f"dec_{run_id}_tick_{tick_id}"
    ts = now_utc.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"dec_{run_id}_{ts}"


def resolve_apply_deadline_utc(
    forecast_data: Mapping[str, Any],
    config: Mapping[str, Any],
    now_utc: datetime,
    override_deadline: str | None = None,
) -> str:
    """apply_deadline_utc thuộc DecisionBatch/ApplyRequest, không bắt buộc có trên Snapshot."""
    if isinstance(override_deadline, str) and override_deadline.strip():
        return override_deadline.strip()
    raw = forecast_data.get("apply_deadline_utc")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    budget_ms = int(config.get("deadline_ms", 200)) + int(config.get("apply_guard_ms", 200)) + 2000
    return _to_rfc3339_utc(now_utc + timedelta(milliseconds=budget_ms))


def compute_apply_request_hash(payload: Mapping[str, Any]) -> str:
    """Tính SHA-256 trên đúng 11 trường canonical khớp compute_apply_payload_hash() của SDN v1.4."""
    if _sdn_compute_hash is not None:
        return _sdn_compute_hash(payload)

    canonical_for_hash = {
        "run_id": str(payload["run_id"]),
        "decision_batch_id": str(payload["decision_batch_id"]),
        "snapshot_id": str(payload["snapshot_id"]),
        "expected_state_version": int(payload["expected_state_version"]),
        "config_version": str(payload["config_version"]),
        "topology_version": payload.get("topology_version"),
        "topology_hash": payload.get("topology_hash"),
        "forecast_start": str(payload["forecast_start"]),
        "forecast_end": str(payload["forecast_end"]),
        "apply_deadline_utc": str(payload["apply_deadline_utc"]),
        "changes": [
            {
                "flow_group_id": str(c["flow_group_id"]),
                "old_path_id": str(c["old_path_id"]),
                "new_path_id": str(c["new_path_id"]),
            }
            for c in payload.get("changes", ())
        ],
    }
    canonical_bytes = json.dumps(canonical_for_hash, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical_bytes).hexdigest()


def bind_decision_batch_id(plan_result: dict[str, Any], decision_batch_id: str) -> dict[str, Any]:
    """
    Khi Runner POST DecisionBatch lên Data và nhận ACK kèm decision_batch_id chính thức,
    gọi hàm này để đồng bộ decision_batch_id sang ApplyRequest và cập nhật lại payload_hash trước khi gửi SDN.
    """
    clean_id = str(decision_batch_id).strip()
    if not clean_id:
        raise ValueError("decision_batch_id không được rỗng")

    if isinstance(plan_result.get("decision_batch"), dict):
        plan_result["decision_batch"]["decision_batch_id"] = clean_id

    if isinstance(plan_result.get("apply_request"), dict):
        req = plan_result["apply_request"]
        req["decision_batch_id"] = clean_id
        req["payload_hash"] = compute_apply_request_hash(req)

    return plan_result


def serialize_decision_batch(
    *,
    decision_batch_id: str,
    topology: Mapping[str, Any],
    routing_state: Mapping[str, Mapping[str, Any]],
    selected_paths: Mapping[str, str],
    locked_groups: set[str],
    b_f: Mapping[str, float],
    forecast_data: Mapping[str, Any],
    state_version: int,
    config: Mapping[str, Any],
    apply_deadline_utc: str,
    solver_metrics: Mapping[str, Any],
    reason: str = "REROUTED",
    tick_id: int | str | None = None,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Đóng gói đủ 48 DecisionRows cho Data (cả tick KEPT và B=0)."""
    ref_now = now_utc.astimezone(timezone.utc) if now_utc is not None else datetime.now(timezone.utc)
    decision_rows: list[dict[str, Any]] = []
    changed_count = 0

    for fg in topology["F"]:
        old_p = str(routing_state[fg].get("current_path") or routing_state[fg].get("actual_path_id"))
        new_p = str(selected_paths.get(fg, old_p))
        is_changed = new_p != old_p
        if is_changed:
            changed_count += 1

        raw_ver_status = routing_state[fg].get("verification_status")
        if raw_ver_status is None:
            raw_ver_status = routing_state[fg].get("status", "UNKNOWN")

        decision_rows.append(
            {
                "flow_group_id": fg,
                "old_path_id": old_p,
                "new_path_id": new_p,
                "proposed_path_id": new_p,
                "changed": is_changed,
                "decision_status": "REROUTED" if is_changed else "KEPT",
                "verification_status": str(raw_ver_status).upper(),
                "is_locked": fg in locked_groups,
                "predicted_mbps": round(float(b_f.get(fg, 0.0)), 6),
            }
        )

    batch_status = "APPLY_REQUESTED" if changed_count > 0 else "KEPT"
    f_start, f_end = _extract_decision_batch_bounds(forecast_data, ref_now)

    batch: dict[str, Any] = {
        "decision_batch_id": decision_batch_id,
        "run_id": str(forecast_data.get("run_id", "run_default")),
        "snapshot_id": str(forecast_data.get("snapshot_id", "snap_default")),
        "forecast_batch_id": _resolve_forecast_batch_id(forecast_data),
        "snapshot_as_of_utc": forecast_data.get("snapshot_as_of_utc"),
        "history_cutoff_utc": forecast_data.get("history_cutoff_utc"),
        "topology_version": str(topology["topology_version"]),
        "topology_hash": topology.get("topology_hash"),
        "config_version": str(config.get("config_version", "1.0")),
        "config_hash": config.get("config_hash"),
        "expected_state_version": int(state_version),
        "forecast_start": f_start,
        "forecast_end": f_end,
        "apply_deadline_utc": apply_deadline_utc,
        "decision_status": batch_status,
        "reason": reason,
        "total_flows": len(decision_rows),
        "changed_flows_count": changed_count,
        "solver_status": solver_metrics.get("status", "Optimal"),
        "max_utilization_before": solver_metrics.get("U_before"),
        "max_utilization_after": solver_metrics.get("U_after"),
        "objective_before": solver_metrics.get("J_before"),
        "objective_after": solver_metrics.get("J_after"),
        "solve_ms": solver_metrics.get("solve_ms"),
        "decision_rows": decision_rows,
    }
    if tick_id is not None:
        batch["tick_id"] = tick_id
    return batch


def serialize_apply_request(
    *,
    decision_batch_id: str,
    decision_rows: list[dict[str, Any]],
    forecast_data: Mapping[str, Any],
    state_version: int,
    config: Mapping[str, Any],
    apply_deadline_utc: str,
    topology: Mapping[str, Any] | None = None,
    now_utc: datetime | None = None,
) -> dict[str, Any] | None:
    """Chỉ xuất 1..B luồng đổi đường; trả về None nếu B=0 hoặc 0 thay đổi."""
    max_b = int(config.get("B", 4))
    if max_b <= 0:
        return None

    changes = [
        {
            "flow_group_id": row["flow_group_id"],
            "old_path_id": row["old_path_id"],
            "new_path_id": row["new_path_id"],
        }
        for row in decision_rows
        if row["changed"]
    ]
    if not changes:
        return None

    ref_now = now_utc.astimezone(timezone.utc) if now_utc is not None else datetime.now(timezone.utc)
    f_start_utc, f_end_utc = _extract_apply_request_utc_bounds(forecast_data, ref_now)

    topo_ver = (
        str(topology["topology_version"])
        if topology and topology.get("topology_version") is not None
        else (str(config["topology_version"]) if config.get("topology_version") is not None else None)
    )
    topo_hash = (
        topology.get("topology_hash")
        if topology and topology.get("topology_hash") is not None
        else config.get("topology_hash")
    )

    payload: dict[str, Any] = {
        "run_id": str(forecast_data.get("run_id", "run_default")),
        "decision_batch_id": decision_batch_id,
        "snapshot_id": str(forecast_data.get("snapshot_id", "snap_default")),
        "expected_state_version": int(state_version),
        "config_version": str(config.get("config_version", "1.0")),
        "forecast_start": f_start_utc,
        "forecast_end": f_end_utc,
        "apply_deadline_utc": apply_deadline_utc,
        "changes": changes[:max_b],
        "topology_version": topo_ver,
        "topology_hash": topo_hash,
        "config_hash": config.get("config_hash"),
        "snapshot_as_of_utc": forecast_data.get("snapshot_as_of_utc"),
        "history_cutoff_utc": forecast_data.get("history_cutoff_utc"),
    }
    payload["payload_hash"] = compute_apply_request_hash(payload)
    return payload