"""
SDN -> Data API v1 Transport & Payload Mapping Adapter (`Phase 2 & 3`).

Prepared against `origin/integration/e2e` Data API v1 (`data/ingestion/online_api.py`,
`data/ingestion/online_models.py`, `data/ingestion/management_api.py`) while keeping
SDN Controller Core and v1.4 cross-role schemas completely untouched.

Supported Data API v1 endpoints:
- `POST /v1/sdn/routing`       (`online_models.Routing`)
- `POST /v1/sdn/snapshots`     (`online_models.Snapshot` + exact byte-level `X-Content-SHA256`)
- `POST /v1/sdn/apply-results` (`management_api.ApplyResult`)

Unsupported / `CONTRACT_BLOCKED` in current Data API v1:
- `RoutingHistory` (no `/v1/sdn/routing-history` endpoint)
- `VerificationEvidence` (no `/v1/sdn/verification-evidence` endpoint)
- Missing-demand `Snapshot` rows where `available_at_utc is None` (`online_models.HistoryRow`
  declares `available_at_utc: UTC` non-nullable; SDN never fabricates timestamps).
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from sdn.schemas.apply_models import ApplyResponse
from sdn.schemas.snapshot_models import Snapshot
from sdn.controller.state_manager import parse_utc_timestamp
from sdn.schemas.state_models import (
    FlowDecisionStatus,
    RoutingStateEntry,
    RoutingStateSnapshot,
    VerificationStatus,
)


def _format_utc(ts: str) -> str:
    dt = parse_utc_timestamp(ts).astimezone(timezone.utc)
    if dt.microsecond:
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

DATA_V1_ENDPOINT_ROUTING = "/v1/sdn/routing"
DATA_V1_ENDPOINT_SNAPSHOTS = "/v1/sdn/snapshots"
DATA_V1_ENDPOINT_APPLY_RESULTS = "/v1/sdn/apply-results"

_ALLOWED_ROUTE_VERIFICATION_STATUSES = frozenset({"VERIFIED", "UNKNOWN", "INCONSISTENT"})
_SNAPSHOT_QUALITY_MAP: dict[str, str] = {
    "VALID": "VALID",
    "INCOMPLETE": "INCOMPLETE",
    "INCOMPLETE_HISTORY": "INCOMPLETE",
    "INCOMPLETE_ROUTING": "INCOMPLETE",
    "STALE": "STALE",
    "INVALID": "INVALID",
}
_ALLOWED_APPLY_BATCH_STATUSES = frozenset(
    {"APPLIED", "REJECTED", "FAILED", "ROLLED_BACK", "PARTIAL", "INCONSISTENT"}
)
_ALLOWED_APPLY_ROW_STATUSES = frozenset(
    {"APPLIED", "KEPT", "FAILED", "ROLLED_BACK", "UNKNOWN"}
)


class DataV1MappingError(ValueError):
    """Raised when an SDN object cannot be mapped consistently to Data API v1."""


class DataV1ContractBlockedError(DataV1MappingError):
    """
    Raised when lossless mapping is impossible under the current Data API v1 schema
    (e.g., missing `available_at_utc` for `DATA_MISSING` rows, or missing endpoints for
    `RoutingHistory` / `VerificationEvidence`).
    """


def _utc_now_rfc3339() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def serialize_canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    """Serialize payload to deterministic UTF-8 JSON bytes (for exact byte-level SHA-256)."""
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def compute_bytes_sha256(raw_bytes: bytes) -> str:
    """Compute lowercase hex SHA-256 over the exact wire bytes."""
    return hashlib.sha256(raw_bytes).hexdigest()


def _map_route_entry(entry: RoutingStateEntry) -> dict[str, Any]:
    """
    Map one SDN `RoutingStateEntry` to Data `online_models.Route`:
    `{flow_group_id, path_id, confirmed_at_utc, verification_status}`.

    Enforces Data's `Route.check_confirmation` invariant:
    `verified == (path_id is not None) == (confirmed_at_utc is not None)`.
    """
    v_status = entry.verification_status.value
    if v_status not in _ALLOWED_ROUTE_VERIFICATION_STATUSES:
        raise DataV1MappingError(
            f"Unsupported verification_status {v_status!r} for flow '{entry.flow_group_id}' "
            f"in Data API v1 Route model."
        )
    verified = v_status == VerificationStatus.VERIFIED.value
    path_id = entry.path_id
    confirmed_at = entry.confirmed_at

    if verified:
        if path_id is None or confirmed_at is None:
            raise DataV1MappingError(
                f"Inconsistent VERIFIED route for flow '{entry.flow_group_id}': "
                f"path_id={path_id!r}, confirmed_at={confirmed_at!r}."
            )
        confirmed_at_utc: Optional[str] = _format_utc(confirmed_at)
    else:
        if path_id is not None or confirmed_at is not None:
            raise DataV1MappingError(
                f"Inconsistent {v_status} route for flow '{entry.flow_group_id}': "
                f"Data API v1 requires null path_id and null confirmed_at_utc when not VERIFIED."
            )
        confirmed_at_utc = None

    return {
        "flow_group_id": entry.flow_group_id,
        "path_id": path_id,
        "confirmed_at_utc": confirmed_at_utc,
        "verification_status": v_status,
    }


def map_routing_state_to_data_v1(
    routing_state: RoutingStateSnapshot,
    *,
    published_at_utc: Optional[str] = None,
) -> dict[str, Any]:
    """
    Phase 3A: Map SDN `RoutingStateSnapshot` -> Data API v1 `POST /v1/sdn/routing` (`online_models.Routing`).

    Exact schema (`extra="forbid"`):
    `{run_id, state_version, published_at_utc, rows: [Route, ...]}`.
    """
    entries = routing_state.routing_state
    if not entries:
        raise DataV1MappingError("Cannot map empty RoutingStateSnapshot to Data API v1 Routing.")
    if published_at_utc is not None:
        pub_ts = _format_utc(published_at_utc)
    else:
        confirmed_ts = [e.confirmed_at for e in entries if e.confirmed_at is not None]
        pub_ts = _format_utc(max(confirmed_ts)) if confirmed_ts else _utc_now_rfc3339()
    rows = [
        _map_route_entry(entry)
        for entry in sorted(entries, key=lambda e: int(e.flow_group_id, 16))
    ]
    return {
        "run_id": routing_state.run_id,
        "state_version": int(routing_state.state_version),
        "published_at_utc": pub_ts,
        "rows": rows,
    }


def compute_warmup_bin_offset(snapshot: Snapshot) -> int:
    """
    Explicitly validate traffic-origin vs measurement-origin from `snapshot.origin_refs`
    and return `warmup_bins = (measurement_started_at_utc - traffic_started_at_utc) / delta_s`.

    Raises `DataV1MappingError` if origins are misaligned or negative.
    """
    origins = snapshot.origin_refs
    traffic_dt = parse_utc_timestamp(origins.traffic_started_at_utc)
    meas_dt = parse_utc_timestamp(origins.measurement_started_at_utc)
    delta_s = int(origins.delta_s)
    if delta_s <= 0:
        raise DataV1MappingError(f"Invalid origin_refs.delta_s={delta_s!r}; expected positive integer.")
    diff = meas_dt - traffic_dt
    if diff.total_seconds() < 0:
        raise DataV1MappingError(
            f"measurement_started_at_utc ({origins.measurement_started_at_utc}) precedes "
            f"traffic_started_at_utc ({origins.traffic_started_at_utc})."
        )
    if diff.microseconds != 0:
        raise DataV1MappingError(
            f"Non-integer second offset between traffic_started_at_utc and measurement_started_at_utc: {diff}."
        )
    total_sec = int(diff.total_seconds())
    if total_sec % delta_s != 0:
        raise DataV1MappingError(
            f"Warmup duration {total_sec}s is not divisible by delta_s={delta_s}s."
        )
    return total_sec // delta_s


def map_snapshot_to_data_v1(
    snapshot: Snapshot,
    *,
    use_traffic_origin_bins: bool = True,
) -> tuple[dict[str, Any], bytes, str]:
    """
    Phase 3B: Map SDN `Snapshot` -> Data API v1 `POST /v1/sdn/snapshots` (`online_models.Snapshot`).

    Returns `(payload_dict, exact_utf8_bytes, content_sha256_hex)` where `content_sha256_hex`
    is computed directly over `exact_utf8_bytes` for the required `X-Content-SHA256` header.

    Strict causal & fail-closed rules:
    - Never converts missing `requested_mbps` (`None`) into `0.0`.
    - Never fabricates `available_at_utc` or `released_at_utc`.
    - Raises `DataV1ContractBlockedError` if any history item has `available_at_utc is None`
      because Data `online_models.HistoryRow.available_at_utc` is non-nullable (`UTC`).
    - When `use_traffic_origin_bins=True` (matching `Online_API_Team_Decisions.md` warmup=60 rule),
      validates `measurement_started_at_utc - traffic_started_at_utc` and shifts measurement-origin
      bins by `+warmup_bins` (e.g., measurement bin `0` -> traffic bin `60`).
    """
    raw_q = snapshot.quality_status.value
    q_status = _SNAPSHOT_QUALITY_MAP.get(raw_q)
    if q_status is None:
        raise DataV1MappingError(f"Unsupported Snapshot quality_status {raw_q!r} for Data API v1.")

    warmup_bins = compute_warmup_bin_offset(snapshot) if use_traffic_origin_bins else 0
    delta_s = int(snapshot.delta_s)
    if delta_s <= 0 or int(snapshot.cutoff_offset_s) % delta_s != 0:
        raise DataV1MappingError(
            f"Snapshot cutoff_offset_s={snapshot.cutoff_offset_s} is not aligned with delta_s={delta_s}."
        )
    cutoff_bin = (int(snapshot.cutoff_offset_s) // delta_s) + warmup_bins
    if cutoff_bin < 0:
        raise DataV1MappingError(f"Mapped cutoff_bin={cutoff_bin} must be >= 0 for Data API v1.")

    history_rows: list[dict[str, Any]] = []
    for item in snapshot.history:
        if item.available_at_utc is None:
            raise DataV1ContractBlockedError(
                f"CONTRACT_BLOCKED: Snapshot '{snapshot.snapshot_id}' contains history row "
                f"(flow={item.flow_group_id}, bin={item.bin_index}, valid={item.valid}, "
                f"reason={item.reason}) with available_at_utc=None; Data API v1 "
                f"HistoryRow.available_at_utc is non-nullable and SDN must never fabricate timestamps."
            )
        mapped_bin = int(item.bin_index) + warmup_bins
        history_rows.append(
            {
                "flow_group_id": item.flow_group_id,
                "bin_index": mapped_bin,
                "requested_mbps": float(item.requested_mbps) if item.requested_mbps is not None else None,
                "available_at_utc": _format_utc(item.available_at_utc),
            }
        )

    rs_entries = (
        snapshot.routing_state.routing_state
        if isinstance(snapshot.routing_state, RoutingStateSnapshot)
        else snapshot.routing_state
    )
    routing_rows = [
        _map_route_entry(entry)
        for entry in sorted(rs_entries, key=lambda e: int(e.flow_group_id, 16))
    ]
    if not routing_rows:
        raise DataV1MappingError("Snapshot routing_state cannot be empty for Data API v1.")

    payload: dict[str, Any] = {
        "run_id": snapshot.run_id,
        "snapshot_id": snapshot.snapshot_id,
        "history_cutoff_utc": _format_utc(snapshot.history_cutoff_utc),
        "cutoff_bin": cutoff_bin,
        "snapshot_as_of_utc": _format_utc(snapshot.snapshot_as_of_utc),
        "state_version": int(snapshot.state_version),
        "schema_version": str(getattr(snapshot, "schema_version", None) or "v1.4"),
        "quality_status": q_status,
        "history": history_rows,
        "routing_state": routing_rows,
    }
    raw_bytes = serialize_canonical_json_bytes(payload)
    digest = compute_bytes_sha256(raw_bytes)
    return payload, raw_bytes, digest


def map_apply_response_to_data_v1(
    apply_response: ApplyResponse,
    *,
    full_routing_state: Optional[RoutingStateSnapshot] = None,
    expand_unchanged_as_kept: bool = False,
) -> dict[str, Any]:
    """
    Phase 3C: Map SDN `ApplyResponse` -> Data API v1 `POST /v1/sdn/apply-results` (`management_api.ApplyResult`).

    Exact schema (`extra="forbid"`):
    `{run_id, decision_batch_id, response_status, response_state_version, completed_at_utc, rows: [ResultRow, ...]}`.

    Enforces Data's `ResultRow.verification` invariant:
    `(actual_path_id is None) == (verified_at_utc is None) == (status == "UNKNOWN")`.
    Never converts `UNKNOWN` into `APPLIED` and rejects inconsistent per-flow outcomes.
    """
    batch_status = apply_response.status.value
    if batch_status not in _ALLOWED_APPLY_BATCH_STATUSES:
        raise DataV1MappingError(
            f"Unsupported ApplyResponse status {batch_status!r} for Data API v1."
        )
    completed_at_utc = _format_utc(apply_response.completed_at)

    rows_by_fg: dict[str, dict[str, Any]] = {}
    for item in apply_response.results:
        r_status = item.status.value
        if r_status not in _ALLOWED_APPLY_ROW_STATUSES:
            raise DataV1MappingError(
                f"Unsupported flow result status {r_status!r} for flow '{item.flow_group_id}'."
            )
        is_unknown = r_status == FlowDecisionStatus.UNKNOWN.value
        if is_unknown:
            if item.actual_path_id is not None:
                raise DataV1MappingError(
                    f"Inconsistent UNKNOWN flow result for '{item.flow_group_id}': "
                    f"actual_path_id must be None, got {item.actual_path_id!r}."
                )
            rows_by_fg[item.flow_group_id] = {
                "flow_group_id": item.flow_group_id,
                "actual_path_id": None,
                "status": "UNKNOWN",
                "verified_at_utc": None,
                "reason": item.reason,
            }
        else:
            if item.actual_path_id is None:
                raise DataV1MappingError(
                    f"Inconsistent {r_status} flow result for '{item.flow_group_id}': "
                    f"actual_path_id cannot be None for non-UNKNOWN status."
                )
            if item.verification_status == VerificationStatus.UNKNOWN:
                raise DataV1MappingError(
                    f"Inconsistent flow result for '{item.flow_group_id}': "
                    f"status={r_status} cannot pair with verification_status=UNKNOWN."
                )
            verified_ts = (
                _format_utc(item.applied_at)
                if item.applied_at is not None
                else completed_at_utc
            )
            rows_by_fg[item.flow_group_id] = {
                "flow_group_id": item.flow_group_id,
                "actual_path_id": item.actual_path_id,
                "status": r_status,
                "verified_at_utc": verified_ts,
                "reason": item.reason,
            }

    if expand_unchanged_as_kept and full_routing_state is not None:
        for entry in full_routing_state.routing_state:
            fid = entry.flow_group_id
            if fid in rows_by_fg:
                continue
            if entry.verification_status == VerificationStatus.VERIFIED and entry.path_id and entry.confirmed_at:
                rows_by_fg[fid] = {
                    "flow_group_id": fid,
                    "actual_path_id": entry.path_id,
                    "status": "KEPT",
                    "verified_at_utc": _format_utc(entry.confirmed_at),
                    "reason": None,
                }
            else:
                rows_by_fg[fid] = {
                    "flow_group_id": fid,
                    "actual_path_id": None,
                    "status": "UNKNOWN",
                    "verified_at_utc": None,
                    "reason": "ROUTING_STATE_NOT_VERIFIED",
                }

    if not rows_by_fg:
        raise DataV1ContractBlockedError(
            f"CONTRACT_BLOCKED: ApplyResponse '{apply_response.decision_batch_id}' "
            f"(status={batch_status}) has 0 flow result rows; Data API v1 ApplyResult.rows "
            f"requires min_length=1."
        )

    ordered_rows = [
        rows_by_fg[fid] for fid in sorted(rows_by_fg.keys(), key=lambda x: int(x, 16))
    ]
    return {
        "run_id": apply_response.run_id,
        "decision_batch_id": apply_response.decision_batch_id,
        "response_status": batch_status,
        "response_state_version": int(apply_response.state_version),
        "completed_at_utc": completed_at_utc,
        "rows": ordered_rows,
    }


def build_data_v1_headers(
    *,
    api_key: Optional[str],
    dedup_key: str,
    event_type: str,
    content_sha256: Optional[str] = None,
) -> dict[str, str]:
    """Build Data API v1 HTTP headers (`X-API-Key`, `X-Content-SHA256`, `X-Idempotency-Key`)."""
    headers = {
        "Content-Type": "application/json",
        "X-Idempotency-Key": dedup_key,
        "X-SDN-Event-Type": event_type,
    }
    if api_key:
        headers["X-API-Key"] = api_key
    if content_sha256:
        headers["X-Content-SHA256"] = content_sha256
    return headers


def is_valid_data_v1_ack(status_code: int, body_text: str) -> bool:
    """
    Validate Data API v1 persistence ACK semantics:
    Must be HTTP 2xx AND valid JSON carrying `inserted`, `updated`, or `ack: true`.
    Never treats empty/non-persistence 2xx responses as DELIVERED.
    """
    if not (200 <= int(status_code) < 300):
        return False
    try:
        parsed = json.loads(body_text)
    except Exception:
        return False
    if not isinstance(parsed, dict):
        return False
    if isinstance(parsed.get("inserted"), int) and not isinstance(parsed.get("inserted"), bool):
        return True
    if isinstance(parsed.get("updated"), int) and not isinstance(parsed.get("updated"), bool):
        return True
    if parsed.get("ack") is True:
        return True
    return False
