import hashlib
import json
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional
from urllib.parse import quote

from optimization.input_validator import normalize_routing_entries

try:
    from sdn.schemas.apply_models import ApplyRequest
except ImportError:
    ApplyRequest = None  # type: ignore[assignment]


def _normalize_rfc3339_utc(ts: str) -> str:
    dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"Timestamp thiếu múi giờ UTC: {ts!r}")
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _compute_payload_hash(payload: Mapping[str, Any]) -> str:
    """Tính SHA-256 đồng nhất với compute_apply_payload_hash() của SDN."""
    canonical_obj = {
        "run_id": str(payload["run_id"]),
        "decision_batch_id": str(payload["decision_batch_id"]),
        "snapshot_id": str(payload["snapshot_id"]),
        "expected_state_version": int(payload["expected_state_version"]),
        "config_version": str(payload["config_version"]),
        "topology_version": payload.get("topology_version"),
        "topology_hash": payload.get("topology_hash"),
        "forecast_start": _normalize_rfc3339_utc(payload["forecast_start"]),
        "forecast_end": _normalize_rfc3339_utc(payload["forecast_end"]),
        "apply_deadline_utc": _normalize_rfc3339_utc(payload["apply_deadline_utc"]),
        "changes": [
            {
                "flow_group_id": str(c["flow_group_id"]),
                "old_path_id": str(c["old_path_id"]),
                "new_path_id": str(c["new_path_id"]),
            }
            for c in payload["changes"]
        ],
    }
    encoded = json.dumps(canonical_obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_optimization_result(
    topology: Mapping[str, Any],
    forecast_data: Mapping[str, Any],
    routing_state: Mapping[str, Any],
    config: Mapping[str, Any],
    U_old: float,
    U_new: float,
    J_old: float,
    J_new: float,
    status: str,
    solve_ms: float,
    x_vals: Mapping[str, Mapping[str, int]],
    y_vals: Mapping[str, int],
) -> dict[str, Any]:
    """Tạo OptimizationResult lưu audit và chuyển đổi sang ApplyRequest."""
    proposed_changes = []
    routes = normalize_routing_entries(routing_state)

    for f in topology["F"]:
        if y_vals.get(f) == 1:
            old_p = routes[f].get("current_path") or routes[f].get("actual_path_id")
            new_p = [p for p in topology["P"][f] if x_vals[f].get(p) == 1][0]
            proposed_changes.append(
                {
                    "flow_group_id": str(f),
                    "old_path_id": str(old_p),
                    "new_path_id": str(new_p),
                }
            )

    state_ver = routing_state.get("state_version", 1)
    if isinstance(state_ver, bool) or not isinstance(state_ver, int) or state_ver < 1:
        raise ValueError(f"expected_state_version không hợp lệ: {state_ver!r}")

    cfg_ver = str(config.get("config_version") or config.get("version") or "1.0").lstrip("v")

    return {
        "run_id": str(forecast_data.get("run_id") or routing_state.get("run_id") or "s0_live_run"),
        "snapshot_id": str(forecast_data["snapshot_id"]),
        "forecast_batch_id": str(forecast_data.get("forecast_batch_id", "fc_default")),
        "expected_state_version": state_ver,
        "config_version": cfg_ver,
        "topology_version": str(config.get("topology_version", "1.0")).lstrip("v"),
        "topology_hash": forecast_data.get("topology_hash") or config.get("topology_hash"),
        "config_hash": forecast_data.get("config_hash") or config.get("config_hash"),
        "U_old": round(U_old, 4),
        "U_new": round(U_new, 4),
        "J_old": round(J_old, 4),
        "J_new": round(J_new, 4),
        "solver_status": status.upper(),
        "solve_ms": round(solve_ms, 2),
        "changed_flows_count": len(proposed_changes),
        "changes": proposed_changes,
    }


def serialize_apply_request(
    optimization_result: Mapping[str, Any],
    forecast_data: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    """Đóng gói ApplyRequest chuẩn 100% với sdn/schemas/apply_models.py (v1.4)."""
    now_utc = datetime.now(timezone.utc)
    f_start_raw = forecast_data.get("forecast_start") or forecast_data.get("forecast_start_utc")
    f_end_raw = forecast_data.get("forecast_end") or forecast_data.get("forecast_end_utc")

    f_start = _normalize_rfc3339_utc(f_start_raw) if f_start_raw else _normalize_rfc3339_utc(now_utc.isoformat())
    if f_end_raw:
        f_end = _normalize_rfc3339_utc(f_end_raw)
    else:
        dt_s = datetime.fromisoformat(f_start.replace("Z", "+00:00"))
        f_end = _normalize_rfc3339_utc((dt_s + timedelta(seconds=3)).isoformat())

    deadline_raw = forecast_data.get("apply_deadline_utc")
    if deadline_raw:
        deadline_utc = _normalize_rfc3339_utc(deadline_raw)
    else:
        guard_ms = int(config.get("apply_guard_ms", 200))
        dt_e = datetime.fromisoformat(f_end.replace("Z", "+00:00"))
        deadline_utc = _normalize_rfc3339_utc((dt_e - timedelta(milliseconds=guard_ms)).isoformat())

    payload: dict[str, Any] = {
        "run_id": optimization_result["run_id"],
        "decision_batch_id": f"batch_{optimization_result['run_id']}_{uuid.uuid4().hex[:10]}",
        "snapshot_id": optimization_result["snapshot_id"],
        "expected_state_version": optimization_result["expected_state_version"],
        "config_version": optimization_result["config_version"],
        "forecast_start": f_start,
        "forecast_end": f_end,
        "apply_deadline_utc": deadline_utc,
        "changes": optimization_result["changes"],
        "topology_version": optimization_result.get("topology_version", "1.0"),
        "topology_hash": optimization_result.get("topology_hash"),
        "config_hash": optimization_result.get("config_hash"),
        "snapshot_as_of_utc": (
            _normalize_rfc3339_utc(forecast_data["snapshot_as_of_utc"])
            if forecast_data.get("snapshot_as_of_utc")
            else None
        ),
        "history_cutoff_utc": (
            _normalize_rfc3339_utc(forecast_data["history_cutoff_utc"])
            if forecast_data.get("history_cutoff_utc")
            else None
        ),
    }
    payload["payload_hash"] = _compute_payload_hash(payload)

    if ApplyRequest is not None:
        sdn_req = ApplyRequest.from_dict(payload, strict=True)
        return sdn_req.to_dict()

    return payload


class SDNHTTPClient:
    """Adapter giao tiếp HTTP/JSON với SDNNorthboundHTTPApp (http_control_api.py)."""

    def __init__(self, base_url: str = "http://127.0.0.1:18080", auth_token: Optional[str] = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.auth_token = auth_token

    def _headers(self) -> dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self.auth_token:
            h["X-SDN-Token"] = self.auth_token
        return h

    def get_routing_state(self, run_id: str, timeout_s: float = 0.5) -> dict[str, Any]:
        url = f"{self.base_url}/routing-state?run_id={quote(run_id, safe='')}"
        req = urllib.request.Request(url, headers=self._headers(), method="GET")
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def post_apply(self, apply_request: Mapping[str, Any], wait_ms: int = 1200) -> dict[str, Any]:
        url = f"{self.base_url}/apply?wait_ms={int(wait_ms)}"
        raw = json.dumps(apply_request, sort_keys=True, separators=(",", ":")).encode("utf-8")
        req = urllib.request.Request(url, data=raw, headers=self._headers(), method="POST")
        try:
            with urllib.request.urlopen(req, timeout=(wait_ms / 1000.0) + 0.5) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return json.loads(exc.read().decode("utf-8"))

    def poll_apply_status(self, run_id: str, decision_batch_id: str, timeout_s: float = 0.5) -> dict[str, Any]:
        url = (
            f"{self.base_url}/apply-status?run_id={quote(run_id, safe='')}"
            f"&decision_batch_id={quote(decision_batch_id, safe='')}"
        )
        req = urllib.request.Request(url, headers=self._headers(), method="GET")
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            return json.loads(resp.read().decode("utf-8"))