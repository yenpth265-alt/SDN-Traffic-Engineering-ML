"""
Minimal state and verification schemas for SDN Controller Phase 2 (StateManager).

Aligned with:
- docs/architecture/SDN_Traffic_Engineering_Role_Integration_Guide_v1.4.md (Sections 3.2, 4.1, 5.5, 13)
- testing/docs/interfaces_v2.md (Sections 7, 10, 11, 12)
- testing/docs/acceptance_criteria_v2.md (Section 6: AC-S01..AC-S10)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import json
import math
import re
from typing import Any, Mapping, Optional, TypeVar

_EnumT = TypeVar("_EnumT", bound=Enum)

_FLOW_GROUP_ID_RE = re.compile(r"^0x([1-8])0([1-8])$")
_PATH_ID_RE = re.compile(r"^(0x[1-8]0[1-8])_P([12])$")
_LINK_ID_RE = re.compile(
    r"^(h[1-8]>e[1-4]|e[1-4]>h[1-8]|e[1-4]>c[12]|c[12]>e[1-4])$"
)
_RFC3339_FRAC_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})\.(\d{1,6})([+-]\d{2}:\d{2})$"
)
_RFC3339_INVALID_DOT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\.(?:\d{7,}|)[+-]\d{2}:\d{2}$"
)


def normalize_iso8601_fractional_seconds(iso_str: str) -> str:
    """
    Pad 1..6 digit fractional seconds in an ISO-8601 / RFC3339 timestamp with a timezone
    offset to 6 microsecond digits so Python 3.10 `datetime.fromisoformat()` accepts
    canonical RFC3339 strings like `.2+00:00` or `.05+00:00` without losing precision.
    Explicitly rejects empty fractional dot (`.+00:00` / `.Z`) and >6 fractional digits
    on both Python 3.10 and Python 3.11+ so non-standard or truncated timestamps are rejected.
    """
    if _RFC3339_INVALID_DOT_RE.match(iso_str) is not None:
        raise ValueError(
            f"Invalid RFC3339 fractional seconds (must be 1..6 digits after '.'): {iso_str!r}"
        )
    m = _RFC3339_FRAC_RE.match(iso_str)
    if m is not None:
        prefix, frac, tz_part = m.groups()
        return f"{prefix}.{frac.ljust(6, '0')}{tz_part}"
    return iso_str


class ContractSerializationError(ValueError):
    """Raised when a cross-role schema payload fails contract serialization or deserialization."""


def require_non_empty_str(value: Any, field_name: str) -> str:
    """Require a non-empty string for `field_name`."""
    if not isinstance(value, str) or not value.strip():
        raise ContractSerializationError(
            f"Field '{field_name}' must be a non-empty string, got {value!r}."
        )
    return value.strip()


def require_strict_bool(value: Any, field_name: str) -> bool:
    """Require a strict boolean (`True` or `False`) for `field_name`."""
    if not isinstance(value, bool):
        raise ContractSerializationError(
            f"Field '{field_name}' must be a boolean, got {type(value).__name__} ({value!r})."
        )
    return value


def require_strict_int(
    value: Any,
    field_name: str,
    *,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> int:
    """Require a strict integer (rejecting `bool`, `float`, `str`, `None`) for `field_name`."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractSerializationError(
            f"Field '{field_name}' must be a strict int, got {type(value).__name__} ({value!r})."
        )
    if min_value is not None and value < min_value:
        raise ContractSerializationError(
            f"Field '{field_name}' must be >= {min_value}, got {value}."
        )
    if max_value is not None and value > max_value:
        raise ContractSerializationError(
            f"Field '{field_name}' must be <= {max_value}, got {value}."
        )
    return value


def require_non_negative_float(value: Any, field_name: str) -> float:
    """Require a finite, non-negative numeric (`int` or `float`, rejecting `bool`) for `field_name`."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractSerializationError(
            f"Field '{field_name}' must be a finite non-negative float, got {type(value).__name__} ({value!r})."
        )
    fval = float(value)
    if math.isnan(fval) or math.isinf(fval) or fval < 0.0:
        raise ContractSerializationError(
            f"Field '{field_name}' must be a finite float >= 0.0, got {value!r}."
        )
    return fval


def parse_enum_strict(enum_cls: type[_EnumT], value: Any, field_name: str) -> _EnumT:
    """Parse an enum value strictly, rejecting unknown enum values."""
    if isinstance(value, enum_cls):
        return value
    if not isinstance(value, str):
        raise ContractSerializationError(
            f"Field '{field_name}' must be a valid {enum_cls.__name__} string, got {value!r}."
        )
    try:
        return enum_cls(value)
    except ValueError as exc:
        valid_values = ", ".join(e.value for e in enum_cls)
        raise ContractSerializationError(
            f"Invalid {enum_cls.__name__} value {value!r} for field '{field_name}'. "
            f"Allowed values: [{valid_values}]."
        ) from exc


def validate_rfc3339_utc_timestamp(
    value: Any,
    field_name: str = "timestamp",
    *,
    normalize: bool = False,
) -> str:
    """
    Validate that `value` is an RFC3339 UTC timestamp string (ending in `'Z'` or `'+00:00'`).
    Rejects naive timestamps, non-UTC offsets, or non-strings.
    """
    raw = require_non_empty_str(value, field_name)
    if raw.endswith("Z"):
        iso_str = raw[:-1] + "+00:00"
    elif raw.endswith("+00:00") or raw.endswith("-00:00"):
        iso_str = raw[:-6] + "+00:00"
    else:
        raise ContractSerializationError(
            f"Field '{field_name}' must be an explicit UTC RFC3339 timestamp ending in 'Z' or '+00:00', got {value!r}."
        )
    try:
        dt = datetime.fromisoformat(normalize_iso8601_fractional_seconds(iso_str))
    except ValueError as exc:
        raise ContractSerializationError(
            f"Field '{field_name}' is not a valid RFC3339 UTC timestamp: {value!r}."
        ) from exc
    if dt.tzinfo is None or dt.utcoffset() != timezone.utc.utcoffset(dt):
        raise ContractSerializationError(
            f"Field '{field_name}' must have UTC offset 0, got {value!r}."
        )
    if not normalize:
        return raw
    dt_utc = dt.astimezone(timezone.utc)
    if dt_utc.microsecond == 0:
        return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    frac = f"{dt_utc.microsecond:06d}".rstrip("0")
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S") + f".{frac}Z"


def validate_canonical_flow_group_id(
    value: Any, field_name: str = "flow_group_id"
) -> str:
    """
    Validate a canonical inter-edge `flow_group_id` (`0x103`..`0x806`, `REQ-01` / `C01`).
    Rejects legacy identifiers (`h1_h3`, `h1-h3`, `h1->h3`, `f13`), intra-edge pairs (`0x101`, `0x102`),
    or lowercase `0x...` hex suffixes if any.
    """
    raw = require_non_empty_str(value, field_name)
    m = _FLOW_GROUP_ID_RE.match(raw)
    if m is None:
        raise ContractSerializationError(
            f"Invalid canonical flow_group_id {value!r} in '{field_name}' "
            f"(expected '0x{{src}}{{dst}}' in 0x103..0x806; legacy IDs like 'h1_h3' are forbidden)."
        )
    src_h = int(m.group(1))
    dst_h = int(m.group(2))
    src_edge = (src_h - 1) // 2
    dst_edge = (dst_h - 1) // 2
    if src_h == dst_h or src_edge == dst_edge:
        raise ContractSerializationError(
            f"Invalid inter-edge flow_group_id {value!r} in '{field_name}': "
            f"hosts h{src_h} and h{dst_h} belong to the same edge switch e{src_edge + 1}."
        )
    return raw


def validate_canonical_path_id(
    value: Any,
    field_name: str = "path_id",
    *,
    expected_flow_group_id: Optional[str] = None,
) -> str:
    """
    Validate a canonical `path_id` (`<flow_group_id>_P1` or `<flow_group_id>_P2`, `REQ-01` / `C01`).
    Rejects bare `'P1'`/`'P2'`, `'h1_h3_c1'`, `'0x103_P3'`, or mismatched flow prefixes.
    """
    raw = require_non_empty_str(value, field_name)
    m = _PATH_ID_RE.match(raw)
    if m is None:
        raise ContractSerializationError(
            f"Invalid canonical path_id {value!r} in '{field_name}' "
            f"(expected '<flow_group_id>_P1' or '<flow_group_id>_P2'; bare 'P1'/'P2' is forbidden)."
        )
    fg_prefix = m.group(1)
    validate_canonical_flow_group_id(fg_prefix, field_name=f"{field_name}.prefix")
    if expected_flow_group_id is not None and fg_prefix != expected_flow_group_id:
        raise ContractSerializationError(
            f"path_id {value!r} in '{field_name}' does not belong to flow_group_id {expected_flow_group_id!r}."
        )
    return raw


def validate_canonical_link_id(value: Any, field_name: str = "link_id") -> str:
    """
    Validate a canonical directed `link_id` (`<from_node>><to_node>`, e.g. `'e1>c1'`, `'c1>e2'`, `'h1>e1'`).
    Rejects legacy `'e1_to_c1'`, `'e1-c1'`, `'e1->c1'`, `'e1_c1'`.
    """
    raw = require_non_empty_str(value, field_name)
    if _LINK_ID_RE.match(raw) is None:
        raise ContractSerializationError(
            f"Invalid canonical link_id {value!r} in '{field_name}' "
            f"(expected '<from_node>><to_node>' such as 'e1>c1'; legacy IDs like 'e1_to_c1' are forbidden)."
        )
    return raw


class EvidenceMode(str, Enum):
    """
    Distinguishes simulated/mock verification evidence on Windows from live
    OpenFlow/OVS data-plane verification on Linux (AC-S03, Validation Rule 2).
    """

    MOCK_SIMULATED = "MOCK_SIMULATED"
    LIVE_OVS = "LIVE_OVS"


class VerificationStatus(str, Enum):
    """Verification status of a flow route in SDN routing_state."""

    VERIFIED = "VERIFIED"
    UNKNOWN = "UNKNOWN"
    VERIFY_FAILED = "VERIFY_FAILED"
    VERIFY_TIMEOUT = "VERIFY_TIMEOUT"
    UNVERIFIED = "UNVERIFIED"


class BatchStatus(str, Enum):
    """Final execution status of a decision batch (ApplyResponse.status)."""

    APPLIED = "APPLIED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"
    ROLLED_BACK = "ROLLED_BACK"
    PARTIAL = "PARTIAL"
    INCONSISTENT = "INCONSISTENT"


class FlowDecisionStatus(str, Enum):
    """Final per-flow execution status inside ApplyResponse.results."""

    APPLIED = "APPLIED"
    KEPT = "KEPT"
    REJECTED = "REJECTED"
    FAILED = "FAILED"
    ROLLED_BACK = "ROLLED_BACK"
    UNKNOWN = "UNKNOWN"


class HistoryIntervalStatus(str, Enum):
    """Status label for verified intervals in routing_history (interfaces_v2.md §11)."""

    INITIAL = "INITIAL"
    APPLIED = "APPLIED"
    ROLLED_BACK = "ROLLED_BACK"
    RECONCILED = "RECONCILED"


class ReasonCode(str, Enum):
    """Standardized SDN / control tick reason codes (interfaces_v2.md §12)."""

    VERSION_MISMATCH = "VERSION_MISMATCH"
    TOPOLOGY_MISMATCH = "TOPOLOGY_MISMATCH"
    CONFIG_MISMATCH = "CONFIG_MISMATCH"
    INVALID_REQUEST = "INVALID_REQUEST"
    STALE = "STALE"
    DEADLINE_EXPIRED = "DEADLINE_EXPIRED"
    BUSY = "BUSY"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"
    BATCH_ID_CONFLICT = "BATCH_ID_CONFLICT"
    HOLDDOWN = "HOLDDOWN"
    FLOWMOD_ERROR = "FLOWMOD_ERROR"
    VERIFY_TIMEOUT = "VERIFY_TIMEOUT"
    VERIFY_FAILED = "VERIFY_FAILED"
    ROLLBACK_FAILED = "ROLLBACK_FAILED"
    DEADLINE_MISSED = "DEADLINE_MISSED"
    ROUTE_STATE_UNKNOWN = "ROUTE_STATE_UNKNOWN"
    DATA_MISSING = "DATA_MISSING"
    DATA_STALE = "DATA_STALE"
    DATA_INVALID = "DATA_INVALID"


@dataclass(frozen=True)
class VerificationEvidence:
    """
    Proof of route verification required before StateManager accepts VERIFIED status.
    Barrier reply alone (barrier_confirmed=True without rule_readback_ok and
    counter_or_probe_ok) is NEVER sufficient for VERIFIED (AC-S03).
    """

    flow_group_id: str
    verified_path_id: str
    verified_at_utc: str
    evidence_mode: EvidenceMode
    rule_readback_ok: bool
    counter_or_probe_ok: bool
    barrier_confirmed: bool = True
    run_id: Optional[str] = None
    xid_refs: tuple[int, ...] = ()
    probe_id: Optional[str] = None
    detail: Optional[str] = None

    @property
    def is_valid_for_verified(self) -> bool:
        """
        True only when both rule read-back (FlowStats) and forwarding evidence
        (counter or probe) succeeded in addition to Barrier confirmation (AC-S03).
        """
        return bool(
            self.barrier_confirmed
            and self.rule_readback_ok
            and self.counter_or_probe_ok
            and self.verified_path_id
            and self.verified_at_utc
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "flow_group_id": self.flow_group_id,
            "verified_path_id": self.verified_path_id,
            "verified_at_utc": self.verified_at_utc,
            "evidence_mode": self.evidence_mode.value,
            "rule_readback_ok": self.rule_readback_ok,
            "counter_or_probe_ok": self.counter_or_probe_ok,
            "barrier_confirmed": self.barrier_confirmed,
            "run_id": self.run_id,
            "xid_refs": list(self.xid_refs),
            "probe_id": self.probe_id,
            "detail": self.detail,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> VerificationEvidence:
        if not isinstance(data, Mapping):
            raise ContractSerializationError("VerificationEvidence payload must be a mapping.")
        fg_id = (
            validate_canonical_flow_group_id(data.get("flow_group_id"), "flow_group_id")
            if strict
            else str(data["flow_group_id"])
        )
        v_path_id = (
            validate_canonical_path_id(
                data.get("verified_path_id"),
                "verified_path_id",
                expected_flow_group_id=fg_id,
            )
            if strict
            else str(data["verified_path_id"])
        )
        v_at = (
            validate_rfc3339_utc_timestamp(
                data.get("verified_at_utc"), "verified_at_utc", normalize=True
            )
            if strict
            else str(data["verified_at_utc"])
        )
        ev_mode = parse_enum_strict(
            EvidenceMode, data.get("evidence_mode"), "evidence_mode"
        )
        readback_ok = (
            require_strict_bool(data.get("rule_readback_ok"), "rule_readback_ok")
            if strict
            else bool(data["rule_readback_ok"])
        )
        probe_ok = (
            require_strict_bool(data.get("counter_or_probe_ok"), "counter_or_probe_ok")
            if strict
            else bool(data["counter_or_probe_ok"])
        )
        barrier_ok = (
            require_strict_bool(
                data.get("barrier_confirmed", True), "barrier_confirmed"
            )
            if strict
            else bool(data.get("barrier_confirmed", True))
        )
        raw_xids = data.get("xid_refs", ())
        if not isinstance(raw_xids, (list, tuple)):
            raise ContractSerializationError("Field 'xid_refs' must be a list or tuple.")
        xids = tuple(
            require_strict_int(x, "xid_refs[]", min_value=0) if strict else int(x)
            for x in raw_xids
        )
        return cls(
            flow_group_id=fg_id,
            verified_path_id=v_path_id,
            verified_at_utc=v_at,
            evidence_mode=ev_mode,
            rule_readback_ok=readback_ok,
            counter_or_probe_ok=probe_ok,
            barrier_confirmed=barrier_ok,
            run_id=(
                require_non_empty_str(data["run_id"], "run_id")
                if data.get("run_id") is not None and strict
                else data.get("run_id")
            ),
            xid_refs=xids,
            probe_id=data.get("probe_id"),
            detail=data.get("detail"),
        )


@dataclass(frozen=True)
class FlowBatchOutcome:
    """
    Per-flow execution and verification outcome supplied to StateManager at batch finalization.
    """

    flow_group_id: str
    old_path_id: str
    proposed_path_id: str
    actual_path_id: Optional[str]
    status: FlowDecisionStatus
    verification_status: VerificationStatus
    rule_count: int = 0
    reason: Optional[str] = None
    applied_at_utc: Optional[str] = None
    evidence: Optional[VerificationEvidence] = None
    had_intermediate_physical_change: bool = False


@dataclass(frozen=True)
class RoutingStateEntry:
    """
    Authoritative per-flow routing state row (interfaces_v2.md §11: routing_state / (run, group)).
    """

    run_id: str
    flow_group_id: str
    path_id: Optional[str]
    state_version: int
    confirmed_at: Optional[str]
    last_successful_change_at: Optional[str]
    verification_status: VerificationStatus
    config_version: str
    evidence_mode: Optional[EvidenceMode] = None
    last_optimization_change_at: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "flow_group_id": self.flow_group_id,
            "path_id": self.path_id,
            "state_version": self.state_version,
            "confirmed_at": self.confirmed_at,
            "last_successful_change_at": self.last_successful_change_at,
            "verification_status": self.verification_status.value,
            "config_version": self.config_version,
            "evidence_mode": (
                self.evidence_mode.value if self.evidence_mode is not None else None
            ),
            "last_optimization_change_at": self.last_optimization_change_at,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> RoutingStateEntry:
        if not isinstance(data, Mapping):
            raise ContractSerializationError("RoutingStateEntry payload must be a mapping.")
        run_id = require_non_empty_str(data.get("run_id"), "run_id")
        fg_id = (
            validate_canonical_flow_group_id(data.get("flow_group_id"), "flow_group_id")
            if strict
            else str(data["flow_group_id"])
        )
        v_status = parse_enum_strict(
            VerificationStatus, data.get("verification_status"), "verification_status"
        )
        raw_path_id = data.get("path_id")
        if raw_path_id is not None:
            path_id: Optional[str] = (
                validate_canonical_path_id(
                    raw_path_id, "path_id", expected_flow_group_id=fg_id
                )
                if strict
                else str(raw_path_id)
            )
        else:
            path_id = None

        if strict:
            if v_status == VerificationStatus.UNKNOWN and path_id is not None:
                raise ContractSerializationError(
                    f"RoutingStateEntry for '{fg_id}' has verification_status=UNKNOWN "
                    f"but non-null path_id={path_id!r} (AC-S04, REQ-16)."
                )
            if v_status == VerificationStatus.VERIFIED and path_id is None:
                raise ContractSerializationError(
                    f"RoutingStateEntry for '{fg_id}' has verification_status=VERIFIED "
                    f"but path_id is None."
                )

        sv = (
            require_strict_int(data.get("state_version"), "state_version", min_value=0)
            if strict
            else int(data["state_version"])
        )
        conf_at = data.get("confirmed_at")
        if conf_at is not None and strict:
            conf_at = validate_rfc3339_utc_timestamp(
                conf_at, "confirmed_at", normalize=True
            )
        last_chg = data.get("last_successful_change_at")
        if last_chg is not None and strict:
            last_chg = validate_rfc3339_utc_timestamp(
                last_chg, "last_successful_change_at", normalize=True
            )
        last_opt = data.get("last_optimization_change_at")
        if last_opt is not None and strict:
            last_opt = validate_rfc3339_utc_timestamp(
                last_opt, "last_optimization_change_at", normalize=True
            )
        cfg_ver = require_non_empty_str(data.get("config_version"), "config_version")
        raw_ev_mode = data.get("evidence_mode")
        ev_mode = (
            parse_enum_strict(EvidenceMode, raw_ev_mode, "evidence_mode")
            if raw_ev_mode is not None
            else None
        )
        return cls(
            run_id=run_id,
            flow_group_id=fg_id,
            path_id=path_id,
            state_version=sv,
            confirmed_at=conf_at,
            last_successful_change_at=last_chg,
            verification_status=v_status,
            config_version=cfg_ver,
            evidence_mode=ev_mode,
            last_optimization_change_at=last_opt,
        )


@dataclass(frozen=True)
class RoutingHistoryInterval:
    """
    Non-overlapping verified route interval (interfaces_v2.md §11: routing_history / (run, group, valid_from)).
    path_id is strictly non-null; UNKNOWN intervals are stored separately as UncertaintyGapRecord.
    """

    run_id: str
    flow_group_id: str
    path_id: str
    valid_from: str
    valid_to: Optional[str]
    status: HistoryIntervalStatus
    batch_id: Optional[str]
    evidence_mode: EvidenceMode

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "flow_group_id": self.flow_group_id,
            "path_id": self.path_id,
            "valid_from": self.valid_from,
            "valid_to": self.valid_to,
            "status": self.status.value,
            "batch_id": self.batch_id,
            "evidence_mode": self.evidence_mode.value,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> RoutingHistoryInterval:
        if not isinstance(data, Mapping):
            raise ContractSerializationError(
                "RoutingHistoryInterval payload must be a mapping."
            )
        run_id = require_non_empty_str(data.get("run_id"), "run_id")
        fg_id = (
            validate_canonical_flow_group_id(data.get("flow_group_id"), "flow_group_id")
            if strict
            else str(data["flow_group_id"])
        )
        path_id = (
            validate_canonical_path_id(
                data.get("path_id"), "path_id", expected_flow_group_id=fg_id
            )
            if strict
            else str(data["path_id"])
        )
        valid_from = (
            validate_rfc3339_utc_timestamp(
                data.get("valid_from"), "valid_from", normalize=True
            )
            if strict
            else str(data["valid_from"])
        )
        valid_to = data.get("valid_to")
        if valid_to is not None and strict:
            valid_to = validate_rfc3339_utc_timestamp(
                valid_to, "valid_to", normalize=True
            )
        status = parse_enum_strict(
            HistoryIntervalStatus, data.get("status"), "status"
        )
        raw_ev_mode = data.get("evidence_mode", EvidenceMode.MOCK_SIMULATED.value)
        ev_mode = parse_enum_strict(EvidenceMode, raw_ev_mode, "evidence_mode")
        return cls(
            run_id=run_id,
            flow_group_id=fg_id,
            path_id=path_id,
            valid_from=valid_from,
            valid_to=valid_to,
            status=status,
            batch_id=data.get("batch_id"),
            evidence_mode=ev_mode,
        )


@dataclass(frozen=True)
class UncertaintyGapRecord:
    """
    Records an interval where a flow's actual route was UNKNOWN (REQ-16, interfaces_v2.md §11).
    Kept separate from routing_history so routing_history never contains null path_id.
    """

    run_id: str
    flow_group_id: str
    last_known_path_id: Optional[str]
    lost_confirmation_at_utc: str
    resolved_at_utc: Optional[str]
    resolved_path_id: Optional[str]
    batch_id: Optional[str]
    reason: Optional[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "flow_group_id": self.flow_group_id,
            "last_known_path_id": self.last_known_path_id,
            "lost_confirmation_at_utc": self.lost_confirmation_at_utc,
            "resolved_at_utc": self.resolved_at_utc,
            "resolved_path_id": self.resolved_path_id,
            "batch_id": self.batch_id,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> UncertaintyGapRecord:
        if not isinstance(data, Mapping):
            raise ContractSerializationError(
                "UncertaintyGapRecord payload must be a mapping."
            )
        run_id = require_non_empty_str(data.get("run_id"), "run_id")
        fg_id = (
            validate_canonical_flow_group_id(data.get("flow_group_id"), "flow_group_id")
            if strict
            else str(data["flow_group_id"])
        )
        last_known = data.get("last_known_path_id")
        if last_known is not None and strict:
            last_known = validate_canonical_path_id(
                last_known, "last_known_path_id", expected_flow_group_id=fg_id
            )
        lost_at = (
            validate_rfc3339_utc_timestamp(
                data.get("lost_confirmation_at_utc"),
                "lost_confirmation_at_utc",
                normalize=True,
            )
            if strict
            else str(data["lost_confirmation_at_utc"])
        )
        resolved_at = data.get("resolved_at_utc")
        if resolved_at is not None and strict:
            resolved_at = validate_rfc3339_utc_timestamp(
                resolved_at, "resolved_at_utc", normalize=True
            )
        resolved_path = data.get("resolved_path_id")
        if resolved_path is not None and strict:
            resolved_path = validate_canonical_path_id(
                resolved_path, "resolved_path_id", expected_flow_group_id=fg_id
            )
        return cls(
            run_id=run_id,
            flow_group_id=fg_id,
            last_known_path_id=last_known,
            lost_confirmation_at_utc=lost_at,
            resolved_at_utc=resolved_at,
            resolved_path_id=resolved_path,
            batch_id=data.get("batch_id"),
            reason=data.get("reason"),
        )


@dataclass(frozen=True)
class StateTransitionEvent:
    """Audit event emitted by StateManager on state_version or reconciliation transitions."""

    event_id: str
    run_id: str
    event_at_utc: str
    event_type: str
    state_version: int
    batch_id: Optional[str]
    flow_group_id: Optional[str]
    detail: dict = field(default_factory=dict)


@dataclass(frozen=True)
class RoutingStateSnapshot:
    """
    Read-only snapshot of a run's live routing state returned by get_routing_state(run_id)
    and embedded into SnapshotCreator outputs (IF05, AC-S07, AC-S10).
    """

    run_id: str
    state_version: int
    config_version: str
    topology_version: str
    routing_state_complete: bool
    reconcile_required: bool
    in_flight_batch_id: Optional[str]
    unknown_flow_group_ids: tuple[str, ...]
    routing_state: tuple[RoutingStateEntry, ...]
    evidence_mode: Optional[EvidenceMode] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "state_version": self.state_version,
            "config_version": self.config_version,
            "topology_version": self.topology_version,
            "routing_state_complete": self.routing_state_complete,
            "reconcile_required": self.reconcile_required,
            "in_flight_batch_id": self.in_flight_batch_id,
            "unknown_flow_group_ids": list(self.unknown_flow_group_ids),
            "routing_state": [r.to_dict() for r in self.routing_state],
            "evidence_mode": (
                self.evidence_mode.value if self.evidence_mode is not None else None
            ),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> RoutingStateSnapshot:
        if not isinstance(data, Mapping):
            raise ContractSerializationError(
                "RoutingStateSnapshot payload must be a mapping."
            )
        run_id = require_non_empty_str(data.get("run_id"), "run_id")
        sv = (
            require_strict_int(data.get("state_version"), "state_version", min_value=0)
            if strict
            else int(data["state_version"])
        )
        cfg_ver = require_non_empty_str(data.get("config_version"), "config_version")
        topo_ver = require_non_empty_str(
            data.get("topology_version"), "topology_version"
        )
        complete = (
            require_strict_bool(
                data.get("routing_state_complete"), "routing_state_complete"
            )
            if strict
            else bool(data["routing_state_complete"])
        )
        reconcile = (
            require_strict_bool(data.get("reconcile_required"), "reconcile_required")
            if strict
            else bool(data["reconcile_required"])
        )
        raw_unk = data.get("unknown_flow_group_ids", ())
        if not isinstance(raw_unk, (list, tuple)):
            raise ContractSerializationError(
                "Field 'unknown_flow_group_ids' must be a list or tuple."
            )
        unk_ids = tuple(
            validate_canonical_flow_group_id(fid, "unknown_flow_group_ids[]")
            if strict
            else str(fid)
            for fid in raw_unk
        )
        raw_rs = data.get("routing_state", ())
        if not isinstance(raw_rs, (list, tuple)):
            raise ContractSerializationError(
                "Field 'routing_state' must be a list or tuple."
            )
        rs_entries = tuple(
            r
            if isinstance(r, RoutingStateEntry)
            else RoutingStateEntry.from_dict(r, strict=strict)
            for r in raw_rs
        )
        raw_ev_mode = data.get("evidence_mode")
        ev_mode = (
            parse_enum_strict(EvidenceMode, raw_ev_mode, "evidence_mode")
            if raw_ev_mode is not None
            else None
        )
        return cls(
            run_id=run_id,
            state_version=sv,
            config_version=cfg_ver,
            topology_version=topo_ver,
            routing_state_complete=complete,
            reconcile_required=reconcile,
            in_flight_batch_id=data.get("in_flight_batch_id"),
            unknown_flow_group_ids=unk_ids,
            routing_state=rs_entries,
            evidence_mode=ev_mode,
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, raw_json: str, *, strict: bool = True) -> RoutingStateSnapshot:
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ContractSerializationError(
                f"Invalid JSON in RoutingStateSnapshot: {exc}"
            ) from exc
        return cls.from_dict(parsed, strict=strict)


@dataclass(frozen=True)
class StateVersionPolicy:
    """
    Configurable policy for [MUST-LOCK] state_version edge cases (REQ-16, interfaces_v2.md §11).

    IMPORTANT: This remains an unresolved [MUST-LOCK] team contract item.
    The default values below are provisional mock/pilot policies (marked via
    policy_status="MUST_LOCK_PROVISIONAL_DEFAULT") and MUST NOT be treated as a
    frozen team contract until SDN, Data, and Optimization sign off in test_plan_v2.md.

    - increment_on_same_path_verified_rollback: Whether a batch that physically touched rules
      and rolled back to the original path (updating confirmed/hold-down metadata) increments
      state_version by +1 to invalidate earlier snapshots.
    - update_hold_down_on_same_path_rollback: Whether a verified rollback that returns to the
      same starting path after an intermediate physical change updates last_successful_change_at.
    """

    increment_on_same_path_verified_rollback: bool = True
    update_hold_down_on_same_path_rollback: bool = True
    policy_status: str = "MUST_LOCK_PROVISIONAL_DEFAULT"


@dataclass(frozen=True)
class SDNStateConfig:
    """
    Configuration parameters for StateManager (pilot defaults from Guide v1.4 Section 3.6 / 5.4).
    - require_explicit_version_policy: When True, requires caller to pass an explicitly locked
      StateVersionPolicy rather than relying on the provisional MUST-LOCK default.
    """

    config_version: str = "1.0"
    hold_down_s: float = 10.0
    max_changed_flows_B: int = 4
    version_policy: StateVersionPolicy = field(default_factory=StateVersionPolicy)
    require_explicit_version_policy: bool = False
