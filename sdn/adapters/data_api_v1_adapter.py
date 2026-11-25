"""
SDN -> Data API v1 Transport & Payload Mapping Adapter (`Phase 2` Canonical v1.4).

Aligned with `origin/main` @ `8ee1782554f48296942595873aaa989e9b4ca637`:
- `docs/architecture/Data_Integration_Contracts.md`
- `data/ingestion/online_models.py` (`SDNSnapshotV14`, `Snapshot`, `Routing`, `Route`)
- `data/ingestion/online_api.py` (`POST /v1/sdn/snapshots`, `POST /v1/sdn/routing`, `GET /v1/runs/{run_id}/demand-history`)
- `data/ingestion/online_store.py` (`snapshot_content_hash`, `ingest_snapshot`, `ingest_routing`)
- `data/ingestion/management_api.py` (`ApplyResult`, `ResultRow`, `GET /v1/runs/{run_id}/decisions/{decision_batch_id}`)

Supported Data API v1 endpoints:
- `POST /v1/sdn/routing`       (`online_models.Routing`)
- `POST /v1/sdn/snapshots`     (`{"sdn": <SDNSnapshotV14>}` + canonical `content_hash` + exact byte `X-Content-SHA256`)
- `POST /v1/sdn/apply-results` (`management_api.ApplyResult`, 48-flow audit from registered DecisionBatch)
- `GET  /v1/runs/{run_id}/decisions/{decision_batch_id}` (read-only lookup of registered DecisionBatch rows)

Unsupported / `CONTRACT_BLOCKED` in Data API v1:
- Standalone `RoutingHistory` endpoint (Data derives `routing_history` from `POST /v1/sdn/routing` and verifies it inside `Snapshot.sdn.routing_history`)
- Standalone `VerificationEvidence` endpoint (no `/v1/sdn/verification-evidence` endpoint yet)
"""

from __future__ import annotations

import hashlib
import json
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional, Sequence

from sdn.controller.state_manager import parse_utc_timestamp
from sdn.schemas.apply_models import ApplyResponse
from sdn.schemas.snapshot_models import Snapshot
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
DATA_V1_ENDPOINT_DECISION_LOOKUP_TEMPLATE = "/v1/runs/{run_id}/decisions/{decision_batch_id}"

_ALLOWED_ROUTE_VERIFICATION_STATUSES = frozenset({"VERIFIED", "UNKNOWN", "INCONSISTENT"})
_SNAPSHOT_QUALITY_MAP: dict[str, str] = {
    "VALID": "VALID",
    "INCOMPLETE": "INCOMPLETE",
    "INCOMPLETE_HISTORY": "INCOMPLETE",
    "INCOMPLETE_ROUTING": "INCOMPLETE",
    "STALE": "STALE",
    "INVALID": "INVALID",
    "INVALID_ROUTING_STATE": "INVALID",
    "INVALID_DATA": "INVALID",
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
    (e.g., missing registered 48-flow DecisionBatch rows for ApplyResult, or missing
    standalone endpoints for RoutingHistory / VerificationEvidence).
    """


def _utc_now_rfc3339() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def serialize_canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    """
    Serialize payload to deterministic UTF-8 JSON bytes matching Data's `online_store.py`
    canonical encoding (`ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False`).
    """
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def compute_bytes_sha256(raw_bytes: bytes) -> str:
    """Compute lowercase hex SHA-256 over the exact wire bytes."""
    return hashlib.sha256(raw_bytes).hexdigest()


def compute_sdn_v14_content_hash(sdn_payload: Mapping[str, Any]) -> str:
    """
    Compute canonical SDN v1.4 `content_hash` matching `data/ingestion/online_store.py`:
    SHA-256 over canonical UTF-8 JSON of `sdn_payload` with `"content_hash"` omitted.
    """
    content = dict(sdn_payload)
    content.pop("content_hash", None)
    return compute_bytes_sha256(serialize_canonical_json_bytes(content))


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
    Map SDN `RoutingStateSnapshot` -> Data API v1 `POST /v1/sdn/routing` (`online_models.Routing`).

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
    use_traffic_origin_bins: bool = False,
) -> tuple[dict[str, Any], bytes, str]:
    """
    Task B: Map SDN `Snapshot` -> Canonical Data API v1 `POST /v1/sdn/snapshots`
    (`online_models.Snapshot` wrapping `SDNSnapshotV14` under top-level key `"sdn"`).

    Returns `(wire_payload_dict, exact_utf8_bytes, x_content_sha256_hex)` where:
    - `wire_payload_dict == {"sdn": <canonical SDNSnapshotV14 dict>}`
    - `sdn["content_hash"]` is the exact canonical SHA-256 over `sdn` with `"content_hash"` omitted
      (matching `data/ingestion/online_store.py:snapshot_content_hash`).
    - `x_content_sha256_hex` is the SHA-256 of `exact_utf8_bytes` for the `X-Content-SHA256` header.

    Strict v1.4 contract guarantees:
    - Preserves all 27 rich v1.4 fields (`origin_refs` incl. `warmup_s`, `flow_group_ids`,
      `history`, `routing_state`, `routing_history`, `link_sample_refs`, `missing_groups`,
      `routing_state_complete`, `unknown_flow_group_ids`, `provenance`, `expected_bin_count`,
      `valid_bin_count`, `config_version`, `config_hash`, `topology_version`, `topology_hash`).
    - Keeps measurement-origin `bin_index` (`0 .. measurement_s - 1`) natively (parameter
      `use_traffic_origin_bins` is retained for signature compatibility only; canonical v1.4
      always validates `origin_refs` alignment and preserves measurement-origin bins).
    - Preserves missing demand explicitly as `requested_mbps=None`, `valid=False`,
      `released_at_utc=None`, `available_at_utc=None`, `release_record_id=None`, `reason=None`
      (matching `online_store.py:365-370` without ever fabricating `0.0` or timestamps).
    """
    del use_traffic_origin_bins  # Canonical SDNSnapshotV14 always uses measurement-origin bins.
    raw_q = snapshot.quality_status.value
    q_status = _SNAPSHOT_QUALITY_MAP.get(raw_q)
    if q_status is None:
        raise DataV1MappingError(f"Unsupported Snapshot quality_status {raw_q!r} for Data API v1.")

    warmup_bins = compute_warmup_bin_offset(snapshot)
    delta_s = int(snapshot.delta_s)
    if delta_s <= 0 or int(snapshot.cutoff_offset_s) % delta_s != 0:
        raise DataV1MappingError(
            f"Snapshot cutoff_offset_s={snapshot.cutoff_offset_s} is not aligned with delta_s={delta_s}."
        )
    if int(snapshot.origin_refs.warmup_s) != warmup_bins * delta_s:
        raise DataV1MappingError(
            f"Snapshot origin_refs.warmup_s={snapshot.origin_refs.warmup_s} does not match "
            f"measurement_started_at_utc - traffic_started_at_utc ({warmup_bins * delta_s}s)."
        )

    history_rows: list[dict[str, Any]] = []
    for item in snapshot.history:
        is_missing_evidence = (
            not item.valid
            and item.requested_mbps is None
            and item.released_at_utc is None
            and item.available_at_utc is None
            and item.release_record_id is None
        )
        if item.valid:
            if item.requested_mbps is None:
                raise DataV1MappingError(
                    f"Inconsistent history row (flow={item.flow_group_id}, bin={item.bin_index}): "
                    f"valid=True requires non-null requested_mbps."
                )
            if not (item.released_at_utc and item.available_at_utc and item.release_record_id):
                raise DataV1MappingError(
                    f"Inconsistent valid history row (flow={item.flow_group_id}, bin={item.bin_index}): "
                    f"valid demand requires released_at_utc, available_at_utc, and release_record_id."
                )
        elif item.requested_mbps is not None:
            raise DataV1MappingError(
                f"Inconsistent history row (flow={item.flow_group_id}, bin={item.bin_index}): "
                f"valid=False cannot carry non-null requested_mbps."
            )

        history_rows.append(
            {
                "flow_group_id": item.flow_group_id,
                "bin_index": int(item.bin_index),
                "window_start_utc": _format_utc(item.window_start_utc),
                "window_end_utc": _format_utc(item.window_end_utc),
                "requested_mbps": float(item.requested_mbps) if item.requested_mbps is not None else None,
                "released_at_utc": _format_utc(item.released_at_utc) if item.released_at_utc is not None else None,
                "available_at_utc": _format_utc(item.available_at_utc) if item.available_at_utc is not None else None,
                "valid": bool(item.valid),
                # Per online_store.py:365-370, when Data has no row for (flow, bin), all
                # evidence fields including reason MUST be None; when Data has a row with
                # requested_mbps=None and a stored reason, preserve that stored reason.
                "reason": None if is_missing_evidence else item.reason,
                "release_record_id": item.release_record_id,
            }
        )

    rs_entries = (
        snapshot.routing_state.routing_state
        if isinstance(snapshot.routing_state, RoutingStateSnapshot)
        else snapshot.routing_state
    )
    if not rs_entries:
        raise DataV1MappingError("Snapshot routing_state cannot be empty for Data API v1.")

    routing_rows: list[dict[str, Any]] = []
    for entry in sorted(rs_entries, key=lambda e: int(e.flow_group_id, 16)):
        # Reuse Route invariant check (verified == (path_id is not None) == (confirmed_at is not None))
        _map_route_entry(entry)
        routing_rows.append(
            {
                "run_id": entry.run_id,
                "flow_group_id": entry.flow_group_id,
                "path_id": entry.path_id,
                "state_version": int(entry.state_version),
                "confirmed_at": _format_utc(entry.confirmed_at) if entry.confirmed_at is not None else None,
                "last_successful_change_at": (
                    _format_utc(entry.last_successful_change_at)
                    if entry.last_successful_change_at is not None
                    else None
                ),
                "verification_status": entry.verification_status.value,
                "config_version": entry.config_version,
                "evidence_mode": entry.evidence_mode.value,
                "last_optimization_change_at": (
                    _format_utc(entry.last_optimization_change_at)
                    if entry.last_optimization_change_at is not None
                    else None
                ),
            }
        )

    routing_history_rows: list[dict[str, Any]] = []
    for iv in snapshot.routing_history:
        routing_history_rows.append(
            {
                "run_id": iv.run_id,
                "flow_group_id": iv.flow_group_id,
                "path_id": iv.path_id,
                "valid_from": _format_utc(iv.valid_from),
                "valid_to": _format_utc(iv.valid_to) if iv.valid_to is not None else None,
                "status": iv.status.value,
                "batch_id": iv.batch_id,
                "evidence_mode": iv.evidence_mode.value,
            }
        )

    link_sample_refs = [ref.to_dict() for ref in snapshot.link_sample_refs]

    sdn_dict: dict[str, Any] = {
        "snapshot_id": snapshot.snapshot_id,
        "run_id": snapshot.run_id,
        "snapshot_as_of_utc": _format_utc(snapshot.snapshot_as_of_utc),
        "history_cutoff_utc": _format_utc(snapshot.history_cutoff_utc),
        "cutoff_offset_s": int(snapshot.cutoff_offset_s),
        "lookback_L": int(snapshot.lookback_L),
        "delta_s": delta_s,
        "state_version": int(snapshot.state_version),
        "config_version": snapshot.config_version,
        "config_hash": snapshot.config_hash,
        "topology_version": snapshot.topology_version,
        "topology_hash": snapshot.topology_hash,
        "origin_refs": {
            "run_started_at_utc": _format_utc(snapshot.origin_refs.run_started_at_utc),
            "traffic_started_at_utc": _format_utc(snapshot.origin_refs.traffic_started_at_utc),
            "measurement_started_at_utc": _format_utc(snapshot.origin_refs.measurement_started_at_utc),
            "warmup_s": int(snapshot.origin_refs.warmup_s),
            "delta_s": int(snapshot.origin_refs.delta_s),
        },
        "flow_group_ids": list(snapshot.flow_group_ids),
        "history": history_rows,
        "routing_state": routing_rows,
        "routing_history": routing_history_rows,
        "link_sample_refs": link_sample_refs,
        "quality_status": q_status,
        "missing_groups": list(snapshot.missing_groups),
        "routing_state_complete": bool(snapshot.routing_state_complete),
        "unknown_flow_group_ids": list(snapshot.unknown_flow_group_ids),
        "provenance": snapshot.provenance.to_dict(),
        "reason": snapshot.reason,
        "expected_bin_count": int(snapshot.expected_bin_count),
        "valid_bin_count": int(snapshot.valid_bin_count),
    }
    sdn_dict["content_hash"] = compute_sdn_v14_content_hash(sdn_dict)

    wire_payload: dict[str, Any] = {"sdn": sdn_dict}
    raw_bytes = serialize_canonical_json_bytes(wire_payload)
    digest = compute_bytes_sha256(raw_bytes)
    return wire_payload, raw_bytes, digest


def build_48_flow_apply_result_data_v1(
    apply_response: ApplyResponse,
    *,
    decision_rows: Optional[Sequence[Mapping[str, Any]]],
    routing_state: Optional[RoutingStateSnapshot],
) -> dict[str, Any]:
    """
    Task C: Build the complete 48-flow Data API v1 `POST /v1/sdn/apply-results` payload
    (`management_api.ApplyResult`) strictly from:
    1. Registered `DecisionBatch` rows (`flow_group_id`, `old_path_id`, `proposed_path_id`),
    2. Actual SDN `ApplyResponse` outcomes for executed changed flows (`<= B`), and
    3. Verified post-apply `RoutingStateSnapshot` at `apply_response.state_version`.

    Fail-closed contract rules:
    - NEVER fabricates 48 rows if `decision_rows` is missing/empty/incomplete (raises
      `DataV1ContractBlockedError`).
    - NEVER fabricates `KEPT` for unverified unchanged flows: sets `KEPT` ONLY when
      `routing_state` confirms `VERIFIED` path == registered `old_path_id`; otherwise sets
      `UNKNOWN` (with `actual_path_id=None`, `verified_at_utc=None`) or raises if inconsistent.
    - Guarantees every row's `(actual_path_id, verified_at_utc)` matches the published
      `RoutingState` version (`management_api.py:224-226`).
    """
    if not decision_rows:
        raise DataV1ContractBlockedError(
            f"CONTRACT_BLOCKED: Registered DecisionBatch rows are required to build 48-flow "
            f"ApplyResult for batch '{apply_response.decision_batch_id}'; SDN never fabricates 48 rows."
        )
    if routing_state is None or not routing_state.routing_state:
        raise DataV1ContractBlockedError(
            f"CONTRACT_BLOCKED: Post-apply RoutingStateSnapshot is required to build 48-flow "
            f"ApplyResult for batch '{apply_response.decision_batch_id}'."
        )
    if int(routing_state.state_version) != int(apply_response.state_version):
        raise DataV1MappingError(
            f"RoutingStateSnapshot state_version={routing_state.state_version} does not match "
            f"ApplyResponse state_version={apply_response.state_version} for batch "
            f"'{apply_response.decision_batch_id}'."
        )

    batch_status = apply_response.status.value
    if batch_status not in _ALLOWED_APPLY_BATCH_STATUSES:
        raise DataV1MappingError(
            f"Unsupported ApplyResponse status {batch_status!r} for Data API v1."
        )
    completed_at_utc = _format_utc(apply_response.completed_at)

    decision_by_fg: dict[str, Mapping[str, Any]] = {}
    for drow in decision_rows:
        fg = drow.get("flow_group_id")
        if not isinstance(fg, str) or not fg:
            raise DataV1MappingError("DecisionBatch row missing valid 'flow_group_id'.")
        if fg in decision_by_fg:
            raise DataV1MappingError(f"Duplicate flow_group_id '{fg}' in DecisionBatch rows.")
        if not drow.get("old_path_id") or not drow.get("proposed_path_id"):
            raise DataV1MappingError(
                f"DecisionBatch row for '{fg}' missing 'old_path_id' or 'proposed_path_id'."
            )
        decision_by_fg[fg] = drow

    route_by_fg: dict[str, RoutingStateEntry] = {
        entry.flow_group_id: entry for entry in routing_state.routing_state
    }
    if decision_by_fg.keys() != route_by_fg.keys():
        raise DataV1ContractBlockedError(
            f"CONTRACT_BLOCKED: DecisionBatch flow set ({len(decision_by_fg)}) does not match "
            f"RoutingStateSnapshot flow set ({len(route_by_fg)})."
        )

    apply_by_fg = {item.flow_group_id: item for item in apply_response.results}
    unknown_apply_fgs = set(apply_by_fg.keys()) - set(decision_by_fg.keys())
    if unknown_apply_fgs:
        raise DataV1MappingError(
            f"ApplyResponse contains flow_group_ids not present in DecisionBatch: {sorted(unknown_apply_fgs)}."
        )

    ordered_rows: list[dict[str, Any]] = []
    for fg in sorted(decision_by_fg.keys(), key=lambda x: int(x, 16)):
        drow = decision_by_fg[fg]
        route = route_by_fg[fg]
        item = apply_by_fg.get(fg)

        route_verified = route.verification_status == VerificationStatus.VERIFIED
        route_path = route.path_id if route_verified else None
        route_confirmed = _format_utc(route.confirmed_at) if (route_verified and route.confirmed_at) else None

        if item is not None:
            r_status = item.status.value
            if r_status == FlowDecisionStatus.REJECTED.value:
                if batch_status == "APPLIED":
                    raise DataV1MappingError(
                        f"APPLIED batch '{apply_response.decision_batch_id}' cannot contain "
                        f"REJECTED flow '{fg}'; preserve batch-level REJECTED status."
                    )
                # Data API v1 ResultRow.status does not include 'REJECTED'; a pre-write
                # rejected flow remained on its verified old_path_id -> 'KEPT' (or 'UNKNOWN'
                # if routing state is not verified), preserving item.reason.
                if route_verified and route_path is not None and route_confirmed is not None:
                    if route_path != drow["old_path_id"]:
                        raise DataV1MappingError(
                            f"Rejected flow '{fg}' has verified path {route_path!r} != registered "
                            f"old_path_id={drow['old_path_id']!r}; cannot claim KEPT."
                        )
                    row_dict = {
                        "flow_group_id": fg,
                        "actual_path_id": route_path,
                        "status": "KEPT",
                        "verified_at_utc": route_confirmed,
                        "reason": item.reason,
                    }
                elif not route_verified and route_path is None and route_confirmed is None:
                    row_dict = {
                        "flow_group_id": fg,
                        "actual_path_id": None,
                        "status": "UNKNOWN",
                        "verified_at_utc": None,
                        "reason": item.reason or "ROUTING_STATE_NOT_VERIFIED",
                    }
                else:
                    raise DataV1MappingError(
                        f"Inconsistent RoutingState entry for rejected flow '{fg}'."
                    )
            elif r_status not in _ALLOWED_APPLY_ROW_STATUSES:
                raise DataV1MappingError(
                    f"Unsupported flow result status {r_status!r} for flow '{fg}'."
                )
            elif r_status == FlowDecisionStatus.UNKNOWN.value:
                if item.actual_path_id is not None or route_path is not None:
                    raise DataV1MappingError(
                        f"Inconsistent UNKNOWN flow result for '{fg}': "
                        f"actual_path_id={item.actual_path_id!r}, route_path={route_path!r}."
                    )
                row_dict = {
                    "flow_group_id": fg,
                    "actual_path_id": None,
                    "status": "UNKNOWN",
                    "verified_at_utc": None,
                    "reason": item.reason,
                }
            else:
                if item.actual_path_id is None or item.verification_status == VerificationStatus.UNKNOWN:
                    raise DataV1MappingError(
                        f"Inconsistent {r_status} flow result for '{fg}': requires verified actual_path_id."
                    )
                if item.actual_path_id != route_path or route_confirmed is None:
                    raise DataV1MappingError(
                        f"Flow '{fg}' ApplyResponse actual_path_id={item.actual_path_id!r} differs from "
                        f"published RoutingState path_id={route_path!r} (v{routing_state.state_version})."
                    )
                if r_status == "APPLIED" and item.actual_path_id != drow["proposed_path_id"]:
                    raise DataV1MappingError(
                        f"Flow '{fg}' status=APPLIED requires actual_path_id={drow['proposed_path_id']!r}, "
                        f"got {item.actual_path_id!r}."
                    )
                if r_status in ("KEPT", "ROLLED_BACK") and item.actual_path_id != drow["old_path_id"]:
                    raise DataV1MappingError(
                        f"Flow '{fg}' status={r_status} requires actual_path_id={drow['old_path_id']!r}, "
                        f"got {item.actual_path_id!r}."
                    )
                row_dict = {
                    "flow_group_id": fg,
                    "actual_path_id": route_path,
                    "status": r_status,
                    "verified_at_utc": route_confirmed,
                    "reason": item.reason,
                }
        else:
            # Unchanged flow in this tick: KEPT ONLY if verified path matches registered old_path_id.
            if route_verified and route_path is not None and route_confirmed is not None:
                if route_path != drow["old_path_id"]:
                    raise DataV1MappingError(
                        f"Unchanged flow '{fg}' has verified path {route_path!r} != registered "
                        f"old_path_id={drow['old_path_id']!r}; cannot claim KEPT."
                    )
                row_dict = {
                    "flow_group_id": fg,
                    "actual_path_id": route_path,
                    "status": "KEPT",
                    "verified_at_utc": route_confirmed,
                    "reason": None,
                }
            elif not route_verified and route_path is None and route_confirmed is None:
                row_dict = {
                    "flow_group_id": fg,
                    "actual_path_id": None,
                    "status": "UNKNOWN",
                    "verified_at_utc": None,
                    "reason": "ROUTING_STATE_NOT_VERIFIED",
                }
            else:
                raise DataV1MappingError(
                    f"Inconsistent RoutingState entry for unchanged flow '{fg}'."
                )

        if batch_status == "APPLIED" and row_dict["status"] not in ("APPLIED", "KEPT"):
            raise DataV1MappingError(
                f"APPLIED batch '{apply_response.decision_batch_id}' cannot contain row "
                f"'{fg}' with status={row_dict['status']!r}."
            )
        ordered_rows.append(row_dict)

    return {
        "run_id": apply_response.run_id,
        "decision_batch_id": apply_response.decision_batch_id,
        "response_status": batch_status,
        "response_state_version": int(apply_response.state_version),
        "completed_at_utc": completed_at_utc,
        "rows": ordered_rows,
    }


def fetch_decision_batch_from_data_v1(
    endpoint_base_url: str,
    run_id: str,
    decision_batch_id: str,
    *,
    api_key: Optional[str] = None,
    timeout_s: float = 1.0,
    http_get_fn: Optional[Callable[[str, dict[str, str], float], tuple[int, str]]] = None,
) -> list[dict[str, Any]]:
    """
    Read registered 48-flow `DecisionBatch.request_payload.rows` from Data API v1:
        `GET /v1/runs/{run_id}/decisions/{decision_batch_id}` (role `SDN`).

    Raises `DataV1ContractBlockedError` if the decision batch does not exist or cannot be retrieved.
    """
    encoded_run = urllib.parse.quote(run_id, safe="")
    encoded_batch = urllib.parse.quote(decision_batch_id, safe="")
    url = (
        f"{endpoint_base_url.rstrip('/')}"
        f"{DATA_V1_ENDPOINT_DECISION_LOOKUP_TEMPLATE.format(run_id=encoded_run, decision_batch_id=encoded_batch)}"
    )
    headers: dict[str, str] = {"Accept": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key

    def _default_get(u: str, h: dict[str, str], t: float) -> tuple[int, str]:
        req = urllib.request.Request(u, headers=h, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=t) as resp:
                return int(getattr(resp, "status", 200)), resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            return int(exc.code), exc.read().decode("utf-8", errors="replace")

    getter = http_get_fn or _default_get
    try:
        status, body = getter(url, headers, timeout_s)
    except Exception as exc:
        raise DataV1ContractBlockedError(
            f"CONTRACT_BLOCKED: Failed to fetch registered DecisionBatch '{decision_batch_id}' "
            f"from Data API v1: {type(exc).__name__}: {exc}"
        ) from exc
    if status != 200:
        raise DataV1ContractBlockedError(
            f"CONTRACT_BLOCKED: DecisionBatch lookup '{decision_batch_id}' returned HTTP {status}: {body[:200]}"
        )
    try:
        parsed = json.loads(body)
        req_payload = parsed.get("request_payload") if isinstance(parsed, dict) else None
        rows = req_payload.get("rows") if isinstance(req_payload, dict) else None
    except Exception as exc:
        raise DataV1ContractBlockedError(
            f"CONTRACT_BLOCKED: Malformed DecisionBatch JSON for '{decision_batch_id}': {exc}"
        ) from exc
    if not isinstance(rows, list) or not rows:
        raise DataV1ContractBlockedError(
            f"CONTRACT_BLOCKED: Registered DecisionBatch '{decision_batch_id}' has no 'request_payload.rows'."
        )
    return rows


def map_apply_response_to_data_v1(
    apply_response: ApplyResponse,
    *,
    full_routing_state: Optional[RoutingStateSnapshot] = None,
    expand_unchanged_as_kept: bool = False,
    decision_rows: Optional[Sequence[Mapping[str, Any]]] = None,
) -> dict[str, Any]:
    """
    Map SDN `ApplyResponse` -> Data API v1 `POST /v1/sdn/apply-results` (`management_api.ApplyResult`).

    - When `decision_rows` is supplied, delegates to `build_48_flow_apply_result_data_v1(...)`
      to produce the strict 48-flow audit result verified against registered DecisionBatch rows
      and post-apply `full_routing_state`.
    - Otherwise preserves backward-compatible changed-flow (or optional `expand_unchanged_as_kept`)
      mapping with strict `ResultRow.verification` invariants.
    """
    if decision_rows is not None:
        return build_48_flow_apply_result_data_v1(
            apply_response,
            decision_rows=decision_rows,
            routing_state=full_routing_state,
        )

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
