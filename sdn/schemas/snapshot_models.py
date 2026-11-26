"""
Snapshot contract schemas for SDN Controller Phase 4 (IF05, REQ-03, AC-S10, AC-D05, AC-D10).

Aligned with:
- docs/architecture/SDN_Traffic_Engineering_Role_Integration_Guide_v1.4.md (§3.4, §4.1, §4.2, §5.2 D3, §13.2, §13.3 Step B)
- testing/docs/interfaces_v2.md (§2.2, §7)
- testing/docs/integration_requirements.md (REQ-03, REQ-06, REQ-16)
- testing/docs/acceptance_criteria_v2.md (AC-D02, AC-D05, AC-D10, AC-S10)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
from typing import Any, Mapping, Optional, Sequence

from sdn.schemas.state_models import (
    ContractSerializationError,
    RoutingHistoryInterval,
    RoutingStateEntry,
    parse_enum_strict,
    require_non_empty_str,
    require_non_negative_float,
    require_strict_bool,
    require_strict_int,
    validate_canonical_flow_group_id,
    validate_canonical_link_id,
    validate_rfc3339_utc_timestamp,
)


class SnapshotQualityStatus(str, Enum):
    """Quality and validity classification of an SDN-created Snapshot (IF05, AC-S10)."""

    VALID = "VALID"
    INCOMPLETE_HISTORY = "INCOMPLETE_HISTORY"
    STALE = "STALE"
    INVALID_ROUTING_STATE = "INVALID_ROUTING_STATE"
    INVALID_DATA = "INVALID_DATA"


class DemandSourceKind(str, Enum):
    """
    Identifies the source of demand observations supplied to SnapshotCreator (REQ-03, AC-S10).
    SDN MUST ONLY accept causal `RELEASED_DEMAND` (`requested_mbps`) and MUST reject
    goodput, port counters, or un-released future `demand_plan` rows.
    """

    RELEASED_DEMAND = "RELEASED_DEMAND"
    GOODPUT_MBPS = "GOODPUT_MBPS"
    PORT_TX_MBPS = "PORT_TX_MBPS"
    PORT_COUNTER = "PORT_COUNTER"
    UNRELEASED_DEMAND_PLAN = "UNRELEASED_DEMAND_PLAN"


FORBIDDEN_DEMAND_SOURCE_KINDS = frozenset(
    {
        DemandSourceKind.GOODPUT_MBPS.value,
        DemandSourceKind.PORT_TX_MBPS.value,
        DemandSourceKind.PORT_COUNTER.value,
        DemandSourceKind.UNRELEASED_DEMAND_PLAN.value,
        "goodput_mbps",
        "port_tx_mbps",
        "port_counter",
        "receiver_goodput",
        "unreleased_demand_plan",
    }
)


@dataclass(frozen=True)
class OriginRefs:
    """
    Run timeline origins declared in RunManifest (`interfaces_v2.md` §2.2, `REQ-06`, `AC-I03`).
    Never conflate `run_started_at_utc`, `traffic_started_at_utc`, and `measurement_started_at_utc`.
    """

    run_started_at_utc: str
    traffic_started_at_utc: str
    measurement_started_at_utc: str
    warmup_s: int = 60
    delta_s: int = 1

    def validate_contract(self) -> None:
        validate_rfc3339_utc_timestamp(
            self.run_started_at_utc, "run_started_at_utc", normalize=True
        )
        validate_rfc3339_utc_timestamp(
            self.traffic_started_at_utc, "traffic_started_at_utc", normalize=True
        )
        validate_rfc3339_utc_timestamp(
            self.measurement_started_at_utc,
            "measurement_started_at_utc",
            normalize=True,
        )
        require_strict_int(self.warmup_s, "warmup_s", min_value=0)
        require_strict_int(self.delta_s, "delta_s", min_value=1)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_started_at_utc": self.run_started_at_utc,
            "traffic_started_at_utc": self.traffic_started_at_utc,
            "measurement_started_at_utc": self.measurement_started_at_utc,
            "warmup_s": self.warmup_s,
            "delta_s": self.delta_s,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> OriginRefs:
        if not isinstance(data, Mapping):
            raise ContractSerializationError("OriginRefs payload must be a mapping.")
        for req_key in (
            "run_started_at_utc",
            "traffic_started_at_utc",
            "measurement_started_at_utc",
        ):
            if req_key not in data or data[req_key] is None:
                raise ContractSerializationError(
                    f"Missing required field '{req_key}' in OriginRefs."
                )
        run_ts = (
            validate_rfc3339_utc_timestamp(
                data["run_started_at_utc"], "run_started_at_utc", normalize=True
            )
            if strict
            else str(data["run_started_at_utc"])
        )
        traffic_ts = (
            validate_rfc3339_utc_timestamp(
                data["traffic_started_at_utc"], "traffic_started_at_utc", normalize=True
            )
            if strict
            else str(data["traffic_started_at_utc"])
        )
        meas_ts = (
            validate_rfc3339_utc_timestamp(
                data["measurement_started_at_utc"],
                "measurement_started_at_utc",
                normalize=True,
            )
            if strict
            else str(data["measurement_started_at_utc"])
        )
        warmup_s = (
            require_strict_int(data.get("warmup_s", 60), "warmup_s", min_value=0)
            if strict
            else int(data.get("warmup_s", 60))
        )
        delta_s = (
            require_strict_int(data.get("delta_s", 1), "delta_s", min_value=1)
            if strict
            else int(data.get("delta_s", 1))
        )
        obj = cls(
            run_started_at_utc=run_ts,
            traffic_started_at_utc=traffic_ts,
            measurement_started_at_utc=meas_ts,
            warmup_s=warmup_s,
            delta_s=delta_s,
        )
        if strict:
            obj.validate_contract()
        return obj


@dataclass(frozen=True)
class DemandObservationRecord:
    """
    Single historical demand observation delivered via the abstract demand history provider
    (`released_demand_to_sdn` — `[OPEN / MUST-LOCK]`, `REQ-03`).
    """

    run_id: str
    flow_group_id: str
    bin_index: int
    window_start_utc: str
    window_end_utc: str
    requested_mbps: Optional[float]
    released_at_utc: Optional[str]
    available_at_utc: Optional[str]
    release_record_id: Optional[str] = None
    source_kind: str = DemandSourceKind.RELEASED_DEMAND.value
    valid: bool = True
    reason: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "flow_group_id": self.flow_group_id,
            "bin_index": self.bin_index,
            "window_start_utc": self.window_start_utc,
            "window_end_utc": self.window_end_utc,
            "requested_mbps": self.requested_mbps,
            "released_at_utc": self.released_at_utc,
            "available_at_utc": self.available_at_utc,
            "release_record_id": self.release_record_id,
            "source_kind": self.source_kind,
            "valid": self.valid,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> DemandObservationRecord:
        if not isinstance(data, Mapping):
            raise ContractSerializationError(
                "DemandObservationRecord payload must be a mapping."
            )
        run_id = require_non_empty_str(data.get("run_id"), "run_id")
        fg_id = (
            validate_canonical_flow_group_id(data.get("flow_group_id"), "flow_group_id")
            if strict
            else str(data["flow_group_id"])
        )
        bin_idx = (
            require_strict_int(data.get("bin_index"), "bin_index", min_value=0)
            if strict
            else int(data["bin_index"])
        )
        w_start = (
            validate_rfc3339_utc_timestamp(
                data.get("window_start_utc"), "window_start_utc", normalize=True
            )
            if strict
            else str(data["window_start_utc"])
        )
        w_end = (
            validate_rfc3339_utc_timestamp(
                data.get("window_end_utc"), "window_end_utc", normalize=True
            )
            if strict
            else str(data["window_end_utc"])
        )
        raw_req = data.get("requested_mbps")
        req_mbps = (
            require_non_negative_float(raw_req, "requested_mbps")
            if raw_req is not None and strict
            else (float(raw_req) if raw_req is not None else None)
        )
        rel_at = data.get("released_at_utc")
        if rel_at is not None and strict:
            rel_at = validate_rfc3339_utc_timestamp(
                rel_at, "released_at_utc", normalize=True
            )
        avail_at = data.get("available_at_utc")
        if avail_at is not None and strict:
            avail_at = validate_rfc3339_utc_timestamp(
                avail_at, "available_at_utc", normalize=True
            )
        valid = (
            require_strict_bool(data.get("valid", True), "valid")
            if strict
            else bool(data.get("valid", True))
        )
        src_kind = require_non_empty_str(
            data.get("source_kind", DemandSourceKind.RELEASED_DEMAND.value),
            "source_kind",
        )
        if strict and src_kind in FORBIDDEN_DEMAND_SOURCE_KINDS:
            raise ContractSerializationError(
                f"Forbidden source_kind {src_kind!r} in DemandObservationRecord (REQ-03)."
            )
        return cls(
            run_id=run_id,
            flow_group_id=fg_id,
            bin_index=bin_idx,
            window_start_utc=w_start,
            window_end_utc=w_end,
            requested_mbps=req_mbps,
            released_at_utc=rel_at,
            available_at_utc=avail_at,
            release_record_id=data.get("release_record_id"),
            source_kind=src_kind,
            valid=valid,
            reason=data.get("reason"),
        )


@dataclass(frozen=True)
class SnapshotHistoryItem:
    """
    Immutable per-(flow_group_id, bin_index) history entry inside `Snapshot.history`
    (`interfaces_v2.md` §7, Guide v1.4 §13.3 Step B).

    Note (`AC-D02`, `AC-S10`):
    - `requested_mbps = 0.0` is a genuine zero demand (`valid = True`).
    - Missing or invalid data MUST have `requested_mbps = None`, `valid = False`, and a non-empty `reason`
      (never substituted with `0.0`).
    """

    flow_group_id: str
    bin_index: int
    window_start_utc: str
    window_end_utc: str
    requested_mbps: Optional[float]
    released_at_utc: Optional[str]
    available_at_utc: Optional[str]
    valid: bool
    reason: Optional[str] = None
    release_record_id: Optional[str] = None

    def validate_contract(self) -> None:
        validate_canonical_flow_group_id(self.flow_group_id, "flow_group_id")
        require_strict_int(self.bin_index, "bin_index", min_value=0)
        w_start = validate_rfc3339_utc_timestamp(
            self.window_start_utc, "window_start_utc", normalize=True
        )
        w_end = validate_rfc3339_utc_timestamp(
            self.window_end_utc, "window_end_utc", normalize=True
        )
        if w_end <= w_start:
            raise ContractSerializationError(
                f"SnapshotHistoryItem window_end_utc ({self.window_end_utc!r}) must be > window_start_utc ({self.window_start_utc!r})."
            )
        require_strict_bool(self.valid, "valid")
        if self.released_at_utc is not None:
            validate_rfc3339_utc_timestamp(
                self.released_at_utc, "released_at_utc", normalize=True
            )
        if self.available_at_utc is not None:
            validate_rfc3339_utc_timestamp(
                self.available_at_utc, "available_at_utc", normalize=True
            )
        if self.valid:
            if self.requested_mbps is None:
                raise ContractSerializationError(
                    f"SnapshotHistoryItem ({self.flow_group_id}, bin={self.bin_index}) has valid=True "
                    f"but requested_mbps is None."
                )
            require_non_negative_float(self.requested_mbps, "requested_mbps")
            if self.released_at_utc is None or self.available_at_utc is None:
                raise ContractSerializationError(
                    f"SnapshotHistoryItem ({self.flow_group_id}, bin={self.bin_index}) has valid=True "
                    f"but missing released_at_utc or available_at_utc."
                )
        else:
            if self.requested_mbps is not None:
                raise ContractSerializationError(
                    f"SnapshotHistoryItem ({self.flow_group_id}, bin={self.bin_index}) has valid=False "
                    f"but non-null requested_mbps={self.requested_mbps!r} "
                    f"(missing/invalid demand must never be filled with 0.0; AC-D02, AC-S10)."
                )
            require_non_empty_str(self.reason, "reason")

    def to_dict(self) -> dict[str, Any]:
        return {
            "flow_group_id": self.flow_group_id,
            "bin_index": self.bin_index,
            "window_start_utc": self.window_start_utc,
            "window_end_utc": self.window_end_utc,
            "requested_mbps": self.requested_mbps,
            "released_at_utc": self.released_at_utc,
            "available_at_utc": self.available_at_utc,
            "valid": self.valid,
            "reason": self.reason,
            "release_record_id": self.release_record_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> SnapshotHistoryItem:
        if not isinstance(data, Mapping):
            raise ContractSerializationError(
                "SnapshotHistoryItem payload must be a mapping."
            )
        fg_id = (
            validate_canonical_flow_group_id(data.get("flow_group_id"), "flow_group_id")
            if strict
            else str(data["flow_group_id"])
        )
        bin_idx = (
            require_strict_int(data.get("bin_index"), "bin_index", min_value=0)
            if strict
            else int(data["bin_index"])
        )
        w_start = (
            validate_rfc3339_utc_timestamp(
                data.get("window_start_utc"), "window_start_utc", normalize=True
            )
            if strict
            else str(data["window_start_utc"])
        )
        w_end = (
            validate_rfc3339_utc_timestamp(
                data.get("window_end_utc"), "window_end_utc", normalize=True
            )
            if strict
            else str(data["window_end_utc"])
        )
        valid = (
            require_strict_bool(data.get("valid"), "valid")
            if strict
            else bool(data["valid"])
        )
        raw_req = data.get("requested_mbps")
        req_mbps = (
            require_non_negative_float(raw_req, "requested_mbps")
            if raw_req is not None and strict
            else (float(raw_req) if raw_req is not None else None)
        )
        rel_at = data.get("released_at_utc")
        if rel_at is not None and strict:
            rel_at = validate_rfc3339_utc_timestamp(
                rel_at, "released_at_utc", normalize=True
            )
        avail_at = data.get("available_at_utc")
        if avail_at is not None and strict:
            avail_at = validate_rfc3339_utc_timestamp(
                avail_at, "available_at_utc", normalize=True
            )
        obj = cls(
            flow_group_id=fg_id,
            bin_index=bin_idx,
            window_start_utc=w_start,
            window_end_utc=w_end,
            requested_mbps=req_mbps,
            released_at_utc=rel_at,
            available_at_utc=avail_at,
            valid=valid,
            reason=data.get("reason"),
            release_record_id=data.get("release_record_id"),
        )
        if strict:
            obj.validate_contract()
        return obj


@dataclass(frozen=True)
class LinkSampleRef:
    """Reference to a causal directional link telemetry sample associated with a snapshot (`IF05`)."""

    sample_id: str
    link_id: str
    sample_at_utc: str
    available_at_utc: str
    valid: bool = True

    def validate_contract(self) -> None:
        require_non_empty_str(self.sample_id, "sample_id")
        validate_canonical_link_id(self.link_id, "link_id")
        validate_rfc3339_utc_timestamp(
            self.sample_at_utc, "sample_at_utc", normalize=True
        )
        validate_rfc3339_utc_timestamp(
            self.available_at_utc, "available_at_utc", normalize=True
        )
        require_strict_bool(self.valid, "valid")

    def to_dict(self) -> dict[str, Any]:
        return {
            "sample_id": self.sample_id,
            "link_id": self.link_id,
            "sample_at_utc": self.sample_at_utc,
            "available_at_utc": self.available_at_utc,
            "valid": self.valid,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> LinkSampleRef:
        if not isinstance(data, Mapping):
            raise ContractSerializationError("LinkSampleRef payload must be a mapping.")
        sample_id = require_non_empty_str(data.get("sample_id"), "sample_id")
        link_id = (
            validate_canonical_link_id(data.get("link_id"), "link_id")
            if strict
            else str(data["link_id"])
        )
        sample_at = (
            validate_rfc3339_utc_timestamp(
                data.get("sample_at_utc"), "sample_at_utc", normalize=True
            )
            if strict
            else str(data["sample_at_utc"])
        )
        avail_at = (
            validate_rfc3339_utc_timestamp(
                data.get("available_at_utc"), "available_at_utc", normalize=True
            )
            if strict
            else str(data["available_at_utc"])
        )
        valid = (
            require_strict_bool(data.get("valid", True), "valid")
            if strict
            else bool(data.get("valid", True))
        )
        obj = cls(
            sample_id=sample_id,
            link_id=link_id,
            sample_at_utc=sample_at,
            available_at_utc=avail_at,
            valid=valid,
        )
        if strict:
            obj.validate_contract()
        return obj


@dataclass(frozen=True)
class SnapshotProvenance:
    """
    Provenance metadata attached to every SDN `Snapshot` (`IF05`, `REQ-03`).
    Explicitly flags that `released_demand_to_sdn` is `[OPEN / MUST-LOCK]` when using mock providers.
    """

    producer: str = "sdn_controller"
    demand_channel_status: str = "MUST_LOCK_MOCK_PROVIDER"
    demand_provider_name: str = "MockReleasedDemandProvider"
    schema_version: str = "1.0"
    evidence_mode: str = "MOCK_SIMULATED"

    def validate_contract(self) -> None:
        require_non_empty_str(self.producer, "producer")
        require_non_empty_str(self.demand_channel_status, "demand_channel_status")
        require_non_empty_str(self.demand_provider_name, "demand_provider_name")
        require_non_empty_str(self.schema_version, "schema_version")
        require_non_empty_str(self.evidence_mode, "evidence_mode")

    def to_dict(self) -> dict[str, str]:
        return {
            "producer": self.producer,
            "demand_channel_status": self.demand_channel_status,
            "demand_provider_name": self.demand_provider_name,
            "schema_version": self.schema_version,
            "evidence_mode": self.evidence_mode,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> SnapshotProvenance:
        if not isinstance(data, Mapping):
            raise ContractSerializationError(
                "SnapshotProvenance payload must be a mapping."
            )
        obj = cls(
            producer=require_non_empty_str(
                data.get("producer", "sdn_controller"), "producer"
            ),
            demand_channel_status=require_non_empty_str(
                data.get("demand_channel_status", "MUST_LOCK_MOCK_PROVIDER"),
                "demand_channel_status",
            ),
            demand_provider_name=require_non_empty_str(
                data.get("demand_provider_name", "MockReleasedDemandProvider"),
                "demand_provider_name",
            ),
            schema_version=require_non_empty_str(
                data.get("schema_version", "1.0"), "schema_version"
            ),
            evidence_mode=require_non_empty_str(
                data.get("evidence_mode", "MOCK_SIMULATED"), "evidence_mode"
            ),
        )
        if strict:
            obj.validate_contract()
        return obj


@dataclass(frozen=True)
class Snapshot:
    """
    Immutable online Snapshot created and published by SDN Controller (`IF05`, `REQ-03`, `AC-S10`).
    Data only validates, stores, and serves this Snapshot without rebuilding or mutating it (`AC-D10`).
    """

    snapshot_id: str
    run_id: str
    snapshot_as_of_utc: str
    history_cutoff_utc: str
    cutoff_offset_s: int
    lookback_L: int
    delta_s: int
    state_version: int
    config_version: str
    config_hash: str
    topology_version: str
    topology_hash: str
    content_hash: str
    origin_refs: OriginRefs
    flow_group_ids: tuple[str, ...]
    history: tuple[SnapshotHistoryItem, ...]
    routing_state: tuple[RoutingStateEntry, ...]
    link_sample_refs: tuple[LinkSampleRef, ...]
    quality_status: SnapshotQualityStatus
    missing_groups: tuple[str, ...]
    routing_state_complete: bool
    unknown_flow_group_ids: tuple[str, ...]
    provenance: SnapshotProvenance
    reason: Optional[str] = None
    expected_bin_count: int = 0
    valid_bin_count: int = 0
    routing_history: tuple[RoutingHistoryInterval, ...] = ()

    def __post_init__(self) -> None:
        for attr in (
            "flow_group_ids",
            "history",
            "routing_state",
            "link_sample_refs",
            "missing_groups",
            "unknown_flow_group_ids",
            "routing_history",
        ):
            val = getattr(self, attr)
            if not isinstance(val, tuple):
                object.__setattr__(self, attr, tuple(val))

    @property
    def is_valid(self) -> bool:
        """
        True only when `quality_status == SnapshotQualityStatus.VALID`, `routing_state_complete`
        is True, `missing_groups` is empty, `unknown_flow_group_ids` is empty, and all
        `48 * L*` bins are valid.
        """
        return bool(
            self.quality_status == SnapshotQualityStatus.VALID
            and self.routing_state_complete
            and len(self.missing_groups) == 0
            and len(self.unknown_flow_group_ids) == 0
            and self.expected_bin_count > 0
            and self.valid_bin_count == self.expected_bin_count
        )

    def verify_content_hash(self) -> bool:
        """Verify that `content_hash` matches the immutable contents of this Snapshot (`AC-D10`)."""
        return self.content_hash == compute_snapshot_content_hash(self)

    def validate_contract(self, *, verify_hash: bool = True) -> None:
        require_non_empty_str(self.snapshot_id, "snapshot_id")
        require_non_empty_str(self.run_id, "run_id")
        validate_rfc3339_utc_timestamp(
            self.snapshot_as_of_utc, "snapshot_as_of_utc", normalize=True
        )
        validate_rfc3339_utc_timestamp(
            self.history_cutoff_utc, "history_cutoff_utc", normalize=True
        )
        require_strict_int(self.cutoff_offset_s, "cutoff_offset_s", min_value=0)
        require_strict_int(self.lookback_L, "lookback_L", min_value=1)
        require_strict_int(self.delta_s, "delta_s", min_value=1)
        require_strict_int(self.state_version, "state_version", min_value=0)
        require_non_empty_str(self.config_version, "config_version")
        require_non_empty_str(self.config_hash, "config_hash")
        require_non_empty_str(self.topology_version, "topology_version")
        require_non_empty_str(self.topology_hash, "topology_hash")
        require_non_empty_str(self.content_hash, "content_hash")
        parse_enum_strict(
            SnapshotQualityStatus, self.quality_status, "quality_status"
        )
        require_strict_bool(self.routing_state_complete, "routing_state_complete")
        require_strict_int(
            self.expected_bin_count, "expected_bin_count", min_value=0
        )
        require_strict_int(self.valid_bin_count, "valid_bin_count", min_value=0)
        self.origin_refs.validate_contract()
        self.provenance.validate_contract()
        for fid in self.flow_group_ids:
            validate_canonical_flow_group_id(fid, "flow_group_ids[]")
        for fid in self.missing_groups:
            validate_canonical_flow_group_id(fid, "missing_groups[]")
        for fid in self.unknown_flow_group_ids:
            validate_canonical_flow_group_id(fid, "unknown_flow_group_ids[]")
        for h in self.history:
            h.validate_contract()
        for s in self.link_sample_refs:
            s.validate_contract()
        if verify_hash and not self.verify_content_hash():
            raise ContractSerializationError(
                f"Snapshot '{self.snapshot_id}' failed content_hash verification "
                f"(expected {compute_snapshot_content_hash(self)!r}, got {self.content_hash!r}; AC-D10)."
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "run_id": self.run_id,
            "snapshot_as_of_utc": self.snapshot_as_of_utc,
            "history_cutoff_utc": self.history_cutoff_utc,
            "cutoff_offset_s": self.cutoff_offset_s,
            "lookback_L": self.lookback_L,
            "delta_s": self.delta_s,
            "state_version": self.state_version,
            "config_version": self.config_version,
            "config_hash": self.config_hash,
            "topology_version": self.topology_version,
            "topology_hash": self.topology_hash,
            "content_hash": self.content_hash,
            "origin_refs": self.origin_refs.to_dict(),
            "flow_group_ids": list(self.flow_group_ids),
            "history": [h.to_dict() for h in self.history],
            "routing_state": [r.to_dict() for r in self.routing_state],
            "routing_history": [iv.to_dict() for iv in self.routing_history],
            "link_sample_refs": [s.to_dict() for s in self.link_sample_refs],
            "quality_status": self.quality_status.value,
            "missing_groups": list(self.missing_groups),
            "routing_state_complete": self.routing_state_complete,
            "unknown_flow_group_ids": list(self.unknown_flow_group_ids),
            "provenance": self.provenance.to_dict(),
            "reason": self.reason,
            "expected_bin_count": self.expected_bin_count,
            "valid_bin_count": self.valid_bin_count,
        }

    def to_json(self) -> str:
        """Serialize `Snapshot` to deterministic transport-neutral JSON."""
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = True,
        verify_hash: bool = True,
    ) -> Snapshot:
        if not isinstance(data, Mapping):
            raise ContractSerializationError("Snapshot payload must be a mapping.")
        for req_key in (
            "snapshot_id",
            "run_id",
            "snapshot_as_of_utc",
            "history_cutoff_utc",
            "cutoff_offset_s",
            "lookback_L",
            "delta_s",
            "state_version",
            "config_version",
            "config_hash",
            "topology_version",
            "topology_hash",
            "content_hash",
            "origin_refs",
            "flow_group_ids",
            "history",
            "routing_state",
            "quality_status",
            "missing_groups",
            "routing_state_complete",
            "unknown_flow_group_ids",
            "provenance",
        ):
            if req_key not in data or data[req_key] is None:
                raise ContractSerializationError(
                    f"Missing required field '{req_key}' in Snapshot."
                )

        raw_origins = data["origin_refs"]
        origin_refs = (
            raw_origins
            if isinstance(raw_origins, OriginRefs)
            else OriginRefs.from_dict(raw_origins, strict=strict)
        )
        raw_prov = data["provenance"]
        provenance = (
            raw_prov
            if isinstance(raw_prov, SnapshotProvenance)
            else SnapshotProvenance.from_dict(raw_prov, strict=strict)
        )

        raw_fg_ids: Sequence[Any] = data["flow_group_ids"]
        if not isinstance(raw_fg_ids, (list, tuple)):
            raise ContractSerializationError(
                "Field 'flow_group_ids' in Snapshot must be a list or tuple."
            )
        flow_group_ids = tuple(
            validate_canonical_flow_group_id(f, "flow_group_ids[]")
            if strict
            else str(f)
            for f in raw_fg_ids
        )

        raw_hist: Sequence[Any] = data["history"]
        if not isinstance(raw_hist, (list, tuple)):
            raise ContractSerializationError(
                "Field 'history' in Snapshot must be a list or tuple."
            )
        history = tuple(
            h
            if isinstance(h, SnapshotHistoryItem)
            else SnapshotHistoryItem.from_dict(h, strict=strict)
            for h in raw_hist
        )

        raw_rs: Sequence[Any] = data["routing_state"]
        if not isinstance(raw_rs, (list, tuple)):
            raise ContractSerializationError(
                "Field 'routing_state' in Snapshot must be a list or tuple."
            )
        routing_state = tuple(
            r
            if isinstance(r, RoutingStateEntry)
            else RoutingStateEntry.from_dict(r, strict=strict)
            for r in raw_rs
        )

        raw_rh: Sequence[Any] = data.get("routing_history", ())
        if not isinstance(raw_rh, (list, tuple)):
            raise ContractSerializationError(
                "Field 'routing_history' in Snapshot must be a list or tuple."
            )
        routing_history = tuple(
            iv
            if isinstance(iv, RoutingHistoryInterval)
            else RoutingHistoryInterval.from_dict(iv, strict=strict)
            for iv in raw_rh
        )

        raw_links: Sequence[Any] = data.get("link_sample_refs", ())
        if not isinstance(raw_links, (list, tuple)):
            raise ContractSerializationError(
                "Field 'link_sample_refs' in Snapshot must be a list or tuple."
            )
        link_sample_refs = tuple(
            s
            if isinstance(s, LinkSampleRef)
            else LinkSampleRef.from_dict(s, strict=strict)
            for s in raw_links
        )

        raw_missing: Sequence[Any] = data["missing_groups"]
        if not isinstance(raw_missing, (list, tuple)):
            raise ContractSerializationError(
                "Field 'missing_groups' in Snapshot must be a list or tuple."
            )
        missing_groups = tuple(
            validate_canonical_flow_group_id(f, "missing_groups[]")
            if strict
            else str(f)
            for f in raw_missing
        )

        raw_unk: Sequence[Any] = data["unknown_flow_group_ids"]
        if not isinstance(raw_unk, (list, tuple)):
            raise ContractSerializationError(
                "Field 'unknown_flow_group_ids' in Snapshot must be a list or tuple."
            )
        unknown_flow_group_ids = tuple(
            validate_canonical_flow_group_id(f, "unknown_flow_group_ids[]")
            if strict
            else str(f)
            for f in raw_unk
        )

        q_status = parse_enum_strict(
            SnapshotQualityStatus, data["quality_status"], "quality_status"
        )
        cutoff_offset_s = (
            require_strict_int(
                data["cutoff_offset_s"], "cutoff_offset_s", min_value=0
            )
            if strict
            else int(data["cutoff_offset_s"])
        )
        lookback_L = (
            require_strict_int(data["lookback_L"], "lookback_L", min_value=1)
            if strict
            else int(data["lookback_L"])
        )
        delta_s = (
            require_strict_int(data["delta_s"], "delta_s", min_value=1)
            if strict
            else int(data["delta_s"])
        )
        state_version = (
            require_strict_int(data["state_version"], "state_version", min_value=0)
            if strict
            else int(data["state_version"])
        )
        routing_state_complete = (
            require_strict_bool(
                data["routing_state_complete"], "routing_state_complete"
            )
            if strict
            else bool(data["routing_state_complete"])
        )
        expected_bin_count = (
            require_strict_int(
                data.get("expected_bin_count", len(flow_group_ids) * lookback_L),
                "expected_bin_count",
                min_value=0,
            )
            if strict
            else int(data.get("expected_bin_count", 0))
        )
        valid_bin_count = (
            require_strict_int(
                data.get("valid_bin_count", sum(1 for h in history if h.valid)),
                "valid_bin_count",
                min_value=0,
            )
            if strict
            else int(data.get("valid_bin_count", 0))
        )
        snap_as_of = (
            validate_rfc3339_utc_timestamp(
                data["snapshot_as_of_utc"], "snapshot_as_of_utc", normalize=True
            )
            if strict
            else str(data["snapshot_as_of_utc"])
        )
        hist_cutoff = (
            validate_rfc3339_utc_timestamp(
                data["history_cutoff_utc"], "history_cutoff_utc", normalize=True
            )
            if strict
            else str(data["history_cutoff_utc"])
        )

        obj = cls(
            snapshot_id=require_non_empty_str(data["snapshot_id"], "snapshot_id"),
            run_id=require_non_empty_str(data["run_id"], "run_id"),
            snapshot_as_of_utc=snap_as_of,
            history_cutoff_utc=hist_cutoff,
            cutoff_offset_s=cutoff_offset_s,
            lookback_L=lookback_L,
            delta_s=delta_s,
            state_version=state_version,
            config_version=require_non_empty_str(
                data["config_version"], "config_version"
            ),
            config_hash=require_non_empty_str(data["config_hash"], "config_hash"),
            topology_version=require_non_empty_str(
                data["topology_version"], "topology_version"
            ),
            topology_hash=require_non_empty_str(
                data["topology_hash"], "topology_hash"
            ),
            content_hash=require_non_empty_str(data["content_hash"], "content_hash"),
            origin_refs=origin_refs,
            flow_group_ids=flow_group_ids,
            history=history,
            routing_state=routing_state,
            link_sample_refs=link_sample_refs,
            quality_status=q_status,
            missing_groups=missing_groups,
            routing_state_complete=routing_state_complete,
            unknown_flow_group_ids=unknown_flow_group_ids,
            provenance=provenance,
            reason=data.get("reason"),
            expected_bin_count=expected_bin_count,
            valid_bin_count=valid_bin_count,
            routing_history=routing_history,
        )
        if strict:
            obj.validate_contract(verify_hash=verify_hash)
        return obj

    @classmethod
    def from_json(
        cls,
        raw_json: str,
        *,
        strict: bool = True,
        verify_hash: bool = True,
    ) -> Snapshot:
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ContractSerializationError(
                f"Invalid JSON in Snapshot: {exc}"
            ) from exc
        return cls.from_dict(parsed, strict=strict, verify_hash=verify_hash)


def compute_snapshot_content_hash(
    snapshot_or_dict: Snapshot | Mapping[str, Any]
) -> str:
    """
    Compute deterministic SHA-256 content hash over all immutable fields of a Snapshot
    (excluding `content_hash` itself).
    """
    data = (
        snapshot_or_dict.to_dict()
        if isinstance(snapshot_or_dict, Snapshot)
        else dict(snapshot_or_dict)
    )
    data.pop("content_hash", None)
    encoded = json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
