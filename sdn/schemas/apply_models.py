"""
ApplyRequest and ApplyResponse contract schemas for SDN Controller Phase 4 (IF08, REQ-04).

Aligned with:
- docs/architecture/SDN_Traffic_Engineering_Role_Integration_Guide_v1.4.md (§4.1, §5.5, §13.3 Steps E & F)
- testing/docs/interfaces_v2.md (§2.3, §10)
- testing/docs/integration_requirements.md (REQ-04, REQ-13, REQ-16)
- testing/docs/acceptance_criteria_v2.md (AC-S02, AC-S03, AC-S04, AC-S06, AC-S07, AC-S08, AC-S09)

Strict separation:
- Confirmed core payload fields (`ApplyRequest`, `ApplyFlowChange`, `ApplyResponse`, `ApplyFlowResult`)
  follow the exact field names and enums in Guide v1.4 and interfaces_v2.md §10.
- Transport envelope fields (`TransportEnvelope`) are separated and marked
  `transport_status="MUST_LOCK_TRANSPORT_ADAPTER"` because the inter-process transport protocol
  (Python IPC vs. REST vs. queue) remains `[OPEN / MUST-LOCK]`.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Mapping, Optional, Sequence

from sdn.schemas.state_models import (
    BatchStatus,
    ContractSerializationError,
    FlowDecisionStatus,
    VerificationStatus,
    parse_enum_strict,
    require_non_empty_str,
    require_strict_bool,
    require_strict_int,
    validate_canonical_flow_group_id,
    validate_canonical_path_id,
    validate_rfc3339_utc_timestamp,
)


@dataclass(frozen=True)
class ApplyFlowChange:
    """
    Single flow route change entry inside `ApplyRequest.changes` (`interfaces_v2.md` §10).
    Must use canonical IDs (`flow_group_id='0x103'`, `old_path_id='0x103_P1'`, `new_path_id='0x103_P2'`).
    """

    flow_group_id: str
    old_path_id: str
    new_path_id: str

    def validate_contract(self) -> None:
        """Strictly validate canonical IDs and non-identical old/new paths."""
        fg = validate_canonical_flow_group_id(self.flow_group_id, "flow_group_id")
        old_p = validate_canonical_path_id(
            self.old_path_id, "old_path_id", expected_flow_group_id=fg
        )
        new_p = validate_canonical_path_id(
            self.new_path_id, "new_path_id", expected_flow_group_id=fg
        )
        if old_p == new_p:
            raise ContractSerializationError(
                f"ApplyFlowChange for '{fg}' must change paths; old_path_id == new_path_id == {old_p!r}."
            )

    def to_dict(self) -> dict[str, str]:
        return {
            "flow_group_id": self.flow_group_id,
            "old_path_id": self.old_path_id,
            "new_path_id": self.new_path_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> ApplyFlowChange:
        if not isinstance(data, Mapping):
            raise ContractSerializationError("ApplyFlowChange payload must be a mapping.")
        for req_key in ("flow_group_id", "old_path_id", "new_path_id"):
            if req_key not in data or data[req_key] is None:
                raise ContractSerializationError(
                    f"Missing required field '{req_key}' in ApplyFlowChange."
                )
        item = cls(
            flow_group_id=str(data["flow_group_id"]),
            old_path_id=str(data["old_path_id"]),
            new_path_id=str(data["new_path_id"]),
        )
        if strict:
            item.validate_contract()
        return item


@dataclass(frozen=True)
class TransportEnvelope:
    """
    Transport and provenance envelope (`interfaces_v2.md` §2.3, §10).
    Kept separate from core payload logic because inter-process transport (IPC/REST/queue)
    is `[OPEN / MUST-LOCK]`.
    """

    run_id: str
    producer: str = "runner_orchestrator"
    produced_at: Optional[str] = None
    api_version: str = "sdn_qa_api_v2"
    schema_version: str = "1.0"
    config_version: str = "1.0"
    config_hash: Optional[str] = None
    topology_version: Optional[str] = None
    topology_hash: Optional[str] = None
    payload_hash: Optional[str] = None
    transport_status: str = "MUST_LOCK_TRANSPORT_ADAPTER"

    def to_dict(self) -> dict[str, Any]:
        return {
            "api_version": self.api_version,
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "producer": self.producer,
            "produced_at": self.produced_at,
            "config_version": self.config_version,
            "config_hash": self.config_hash,
            "topology_version": self.topology_version,
            "topology_hash": self.topology_hash,
            "payload_hash": self.payload_hash,
            "transport_status": self.transport_status,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> TransportEnvelope:
        if not isinstance(data, Mapping):
            raise ContractSerializationError("TransportEnvelope payload must be a mapping.")
        run_id = require_non_empty_str(data.get("run_id"), "run_id")
        produced_at = data.get("produced_at")
        if produced_at is not None and strict:
            produced_at = validate_rfc3339_utc_timestamp(
                produced_at, "produced_at", normalize=True
            )
        return cls(
            run_id=run_id,
            producer=require_non_empty_str(
                data.get("producer", "runner_orchestrator"), "producer"
            ),
            produced_at=produced_at,
            api_version=require_non_empty_str(
                data.get("api_version", "sdn_qa_api_v2"), "api_version"
            ),
            schema_version=require_non_empty_str(
                data.get("schema_version", "1.0"), "schema_version"
            ),
            config_version=require_non_empty_str(
                data.get("config_version", "1.0"), "config_version"
            ),
            config_hash=data.get("config_hash"),
            topology_version=data.get("topology_version"),
            topology_hash=data.get("topology_hash"),
            payload_hash=data.get("payload_hash"),
            transport_status=require_non_empty_str(
                data.get("transport_status", "MUST_LOCK_TRANSPORT_ADAPTER"),
                "transport_status",
            ),
        )


@dataclass(frozen=True)
class ApplyRequest:
    """
    Canonical `ApplyRequest` contract (`interfaces_v2.md` §10, Guide v1.4 §13.3 Step E, `REQ-04`).

    Confirmed core fields:
    - `run_id`: Run identifier.
    - `decision_batch_id`: Canonical batch identifier (`Optimization.batch_id -> decision_batch_id`).
    - `snapshot_id`: ID of the causal SDN snapshot used for forecasting and optimization.
    - `expected_state_version`: Expected global `state_version` of `run_id`.
    - `config_version`: Frozen configuration version string.
    - `forecast_start`: Target forecast start timestamp (UTC RFC3339 in `ApplyRequest`).
    - `forecast_end`: Target forecast end timestamp (UTC RFC3339 in `ApplyRequest`).
    - `apply_deadline_utc`: Latest UTC timestamp at which rule installation may begin.
    - `changes`: Tuple of `ApplyFlowChange` items (`1..B`).
    """

    run_id: str
    decision_batch_id: str
    snapshot_id: str
    expected_state_version: int
    config_version: str
    forecast_start: str
    forecast_end: str
    apply_deadline_utc: str
    changes: tuple[ApplyFlowChange, ...]
    topology_version: Optional[str] = None
    topology_hash: Optional[str] = None
    config_hash: Optional[str] = None
    payload_hash: Optional[str] = None
    snapshot_as_of_utc: Optional[str] = None
    history_cutoff_utc: Optional[str] = None
    envelope: Optional[TransportEnvelope] = None

    def __post_init__(self) -> None:
        if not isinstance(self.changes, tuple):
            object.__setattr__(self, "changes", tuple(self.changes))

    @property
    def effective_topology_version(self) -> Optional[str]:
        if self.topology_version is not None:
            return self.topology_version
        if self.envelope is not None:
            return self.envelope.topology_version
        return None

    @property
    def effective_topology_hash(self) -> Optional[str]:
        if self.topology_hash is not None:
            return self.topology_hash
        if self.envelope is not None:
            return self.envelope.topology_hash
        return None

    @property
    def effective_config_hash(self) -> Optional[str]:
        if self.config_hash is not None:
            return self.config_hash
        if self.envelope is not None:
            return self.envelope.config_hash
        return None

    @property
    def effective_payload_hash(self) -> str:
        if self.payload_hash is not None and self.payload_hash.strip():
            return self.payload_hash.strip()
        if (
            self.envelope is not None
            and self.envelope.payload_hash is not None
            and self.envelope.payload_hash.strip()
        ):
            return self.envelope.payload_hash.strip()
        return compute_apply_payload_hash(self)

    def validate_contract(self) -> None:
        """Strictly validate all canonical fields, timestamps, types, and changes on ApplyRequest."""
        require_non_empty_str(self.run_id, "run_id")
        require_non_empty_str(self.decision_batch_id, "decision_batch_id")
        require_non_empty_str(self.snapshot_id, "snapshot_id")
        require_strict_int(
            self.expected_state_version, "expected_state_version", min_value=1
        )
        require_non_empty_str(self.config_version, "config_version")
        f_start = validate_rfc3339_utc_timestamp(
            self.forecast_start, "forecast_start", normalize=True
        )
        f_end = validate_rfc3339_utc_timestamp(
            self.forecast_end, "forecast_end", normalize=True
        )
        validate_rfc3339_utc_timestamp(
            self.apply_deadline_utc, "apply_deadline_utc", normalize=True
        )
        if f_end <= f_start:
            raise ContractSerializationError(
                f"ApplyRequest forecast_end ({self.forecast_end!r}) must be > forecast_start ({self.forecast_start!r})."
            )
        if self.snapshot_as_of_utc is not None:
            validate_rfc3339_utc_timestamp(
                self.snapshot_as_of_utc, "snapshot_as_of_utc", normalize=True
            )
        if self.history_cutoff_utc is not None:
            validate_rfc3339_utc_timestamp(
                self.history_cutoff_utc, "history_cutoff_utc", normalize=True
            )
        if len(self.changes) == 0:
            raise ContractSerializationError(
                "ApplyRequest.changes must contain at least 1 flow change."
            )
        seen_fids: set[str] = set()
        for chg in self.changes:
            if not isinstance(chg, ApplyFlowChange):
                raise ContractSerializationError(
                    "Each entry in ApplyRequest.changes must be an ApplyFlowChange."
                )
            chg.validate_contract()
            if chg.flow_group_id in seen_fids:
                raise ContractSerializationError(
                    f"Duplicate flow_group_id {chg.flow_group_id!r} in ApplyRequest.changes."
                )
            seen_fids.add(chg.flow_group_id)

    def to_core_dict(self) -> dict[str, Any]:
        """Serialize the confirmed core IF08 `ApplyRequest` payload."""
        return {
            "run_id": self.run_id,
            "decision_batch_id": self.decision_batch_id,
            "snapshot_id": self.snapshot_id,
            "expected_state_version": self.expected_state_version,
            "config_version": self.config_version,
            "forecast_start": self.forecast_start,
            "forecast_end": self.forecast_end,
            "apply_deadline_utc": self.apply_deadline_utc,
            "changes": [c.to_dict() for c in self.changes],
        }

    def to_dict(self) -> dict[str, Any]:
        """Serialize full `ApplyRequest` including optional version/hash metadata and envelope."""
        payload = self.to_core_dict()
        payload.update(
            {
                "topology_version": self.topology_version,
                "topology_hash": self.topology_hash,
                "config_hash": self.config_hash,
                "payload_hash": self.effective_payload_hash,
                "snapshot_as_of_utc": self.snapshot_as_of_utc,
                "history_cutoff_utc": self.history_cutoff_utc,
                "envelope": (
                    self.envelope.to_dict() if self.envelope is not None else None
                ),
            }
        )
        return payload

    def to_json(self) -> str:
        """Serialize `ApplyRequest` to deterministic transport-neutral JSON."""
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        envelope: Optional[TransportEnvelope] = None,
        strict: bool = True,
    ) -> ApplyRequest:
        if not isinstance(data, Mapping):
            raise ContractSerializationError("ApplyRequest payload must be a mapping.")
        for req_key in (
            "run_id",
            "decision_batch_id",
            "snapshot_id",
            "expected_state_version",
            "config_version",
            "forecast_start",
            "forecast_end",
            "apply_deadline_utc",
            "changes",
        ):
            if req_key not in data or data[req_key] is None:
                raise ContractSerializationError(
                    f"Missing required field '{req_key}' in ApplyRequest."
                )

        raw_changes: Sequence[Any] = data.get("changes", ())
        if not isinstance(raw_changes, (list, tuple)):
            raise ContractSerializationError(
                "Field 'changes' in ApplyRequest must be a list or tuple."
            )
        changes = tuple(
            c
            if isinstance(c, ApplyFlowChange)
            else ApplyFlowChange.from_dict(c, strict=strict)
            for c in raw_changes
        )
        expected_sv = (
            require_strict_int(
                data["expected_state_version"],
                "expected_state_version",
                min_value=1,
            )
            if strict
            else int(data["expected_state_version"])
        )
        eff_envelope = envelope
        if eff_envelope is None and data.get("envelope") is not None:
            raw_env = data["envelope"]
            eff_envelope = (
                raw_env
                if isinstance(raw_env, TransportEnvelope)
                else TransportEnvelope.from_dict(raw_env, strict=strict)
            )

        f_start = (
            validate_rfc3339_utc_timestamp(
                data["forecast_start"], "forecast_start", normalize=True
            )
            if strict
            else str(data["forecast_start"])
        )
        f_end = (
            validate_rfc3339_utc_timestamp(
                data["forecast_end"], "forecast_end", normalize=True
            )
            if strict
            else str(data["forecast_end"])
        )
        deadline_utc = (
            validate_rfc3339_utc_timestamp(
                data["apply_deadline_utc"], "apply_deadline_utc", normalize=True
            )
            if strict
            else str(data["apply_deadline_utc"])
        )
        snap_as_of = data.get("snapshot_as_of_utc")
        if snap_as_of is not None and strict:
            snap_as_of = validate_rfc3339_utc_timestamp(
                snap_as_of, "snapshot_as_of_utc", normalize=True
            )
        hist_cutoff = data.get("history_cutoff_utc")
        if hist_cutoff is not None and strict:
            hist_cutoff = validate_rfc3339_utc_timestamp(
                hist_cutoff, "history_cutoff_utc", normalize=True
            )

        obj = cls(
            run_id=require_non_empty_str(data["run_id"], "run_id")
            if strict
            else str(data["run_id"]),
            decision_batch_id=require_non_empty_str(
                data["decision_batch_id"], "decision_batch_id"
            )
            if strict
            else str(data["decision_batch_id"]),
            snapshot_id=require_non_empty_str(data["snapshot_id"], "snapshot_id")
            if strict
            else str(data["snapshot_id"]),
            expected_state_version=expected_sv,
            config_version=require_non_empty_str(
                data["config_version"], "config_version"
            )
            if strict
            else str(data["config_version"]),
            forecast_start=f_start,
            forecast_end=f_end,
            apply_deadline_utc=deadline_utc,
            changes=changes,
            topology_version=data.get("topology_version"),
            topology_hash=data.get("topology_hash"),
            config_hash=data.get("config_hash"),
            payload_hash=data.get("payload_hash"),
            snapshot_as_of_utc=snap_as_of,
            history_cutoff_utc=hist_cutoff,
            envelope=eff_envelope,
        )
        if strict:
            obj.validate_contract()
        return obj

    @classmethod
    def from_json(
        cls,
        raw_json: str,
        *,
        envelope: Optional[TransportEnvelope] = None,
        strict: bool = True,
    ) -> ApplyRequest:
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ContractSerializationError(
                f"Invalid JSON in ApplyRequest: {exc}"
            ) from exc
        return cls.from_dict(parsed, envelope=envelope, strict=strict)


def _normalize_ts_if_valid(val: Any) -> Any:
    if not isinstance(val, str):
        return val
    try:
        return validate_rfc3339_utc_timestamp(val, normalize=True)
    except ContractSerializationError:
        return val


def compute_apply_payload_hash(request_or_dict: ApplyRequest | Mapping[str, Any]) -> str:
    """
    Compute a deterministic SHA-256 hex digest over the canonical fields of `ApplyRequest`
    for idempotency and `BATCH_ID_CONFLICT` detection (`AC-S08`, `interfaces_v2.md` §10).
    Normalizes RFC3339 UTC timestamps and sorts dictionary keys so field order or `+00:00` vs `Z`
    never alters the digest.
    """
    if isinstance(request_or_dict, ApplyRequest):
        canonical_obj = {
            "run_id": request_or_dict.run_id,
            "decision_batch_id": request_or_dict.decision_batch_id,
            "snapshot_id": request_or_dict.snapshot_id,
            "expected_state_version": request_or_dict.expected_state_version,
            "config_version": request_or_dict.config_version,
            "topology_version": request_or_dict.effective_topology_version,
            "topology_hash": request_or_dict.effective_topology_hash,
            "forecast_start": _normalize_ts_if_valid(request_or_dict.forecast_start),
            "forecast_end": _normalize_ts_if_valid(request_or_dict.forecast_end),
            "apply_deadline_utc": _normalize_ts_if_valid(
                request_or_dict.apply_deadline_utc
            ),
            "changes": [c.to_dict() for c in request_or_dict.changes],
        }
    elif isinstance(request_or_dict, Mapping):
        raw_changes = request_or_dict.get("changes", ())
        norm_changes = [
            c.to_dict()
            if isinstance(c, ApplyFlowChange)
            else {
                "flow_group_id": str(c["flow_group_id"]),
                "old_path_id": str(c["old_path_id"]),
                "new_path_id": str(c["new_path_id"]),
            }
            for c in raw_changes
        ]
        env = request_or_dict.get("envelope")
        topo_ver = request_or_dict.get("topology_version")
        if topo_ver is None and isinstance(env, Mapping):
            topo_ver = env.get("topology_version")
        topo_hash = request_or_dict.get("topology_hash")
        if topo_hash is None and isinstance(env, Mapping):
            topo_hash = env.get("topology_hash")
        canonical_obj = {
            "run_id": str(request_or_dict["run_id"]),
            "decision_batch_id": str(request_or_dict["decision_batch_id"]),
            "snapshot_id": str(request_or_dict["snapshot_id"]),
            "expected_state_version": int(request_or_dict["expected_state_version"]),
            "config_version": str(request_or_dict["config_version"]),
            "topology_version": topo_ver,
            "topology_hash": topo_hash,
            "forecast_start": _normalize_ts_if_valid(request_or_dict["forecast_start"]),
            "forecast_end": _normalize_ts_if_valid(request_or_dict["forecast_end"]),
            "apply_deadline_utc": _normalize_ts_if_valid(
                request_or_dict["apply_deadline_utc"]
            ),
            "changes": norm_changes,
        }
    else:
        raise ContractSerializationError(
            "compute_apply_payload_hash requires an ApplyRequest or Mapping."
        )

    encoded = json.dumps(canonical_obj, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class ApplyFlowResult:
    """
    Per-flow execution and verification result inside `ApplyResponse.results`
    (`interfaces_v2.md` §10, Guide v1.4 §13.3 Step F).

    Critical invariants (`AC-S04`, `REQ-16`):
    - If `status == FlowDecisionStatus.UNKNOWN` (or `verification_status == VerificationStatus.UNKNOWN`),
      `actual_path_id` MUST be `None` (never fake `old_path_id` or `proposed_path_id`).
    """

    flow_group_id: str
    actual_path_id: Optional[str]
    status: FlowDecisionStatus
    verification_status: VerificationStatus
    reason: Optional[str]
    rule_count: int
    applied_at: Optional[str]
    old_path_id: Optional[str] = None
    proposed_path_id: Optional[str] = None

    def validate_contract(self) -> None:
        fg_id = validate_canonical_flow_group_id(self.flow_group_id, "flow_group_id")
        parse_enum_strict(FlowDecisionStatus, self.status, "status")
        parse_enum_strict(
            VerificationStatus, self.verification_status, "verification_status"
        )
        require_strict_int(self.rule_count, "rule_count", min_value=0)
        if self.applied_at is not None:
            validate_rfc3339_utc_timestamp(
                self.applied_at, "applied_at", normalize=True
            )
        if (
            self.status == FlowDecisionStatus.UNKNOWN
            or self.verification_status == VerificationStatus.UNKNOWN
        ):
            if self.actual_path_id is not None:
                raise ContractSerializationError(
                    f"ApplyFlowResult for '{fg_id}' has UNKNOWN status but non-null "
                    f"actual_path_id={self.actual_path_id!r} (AC-S04, REQ-16)."
                )
        elif self.actual_path_id is not None:
            validate_canonical_path_id(
                self.actual_path_id, "actual_path_id", expected_flow_group_id=fg_id
            )
        if self.old_path_id is not None:
            validate_canonical_path_id(
                self.old_path_id, "old_path_id", expected_flow_group_id=fg_id
            )
        if self.proposed_path_id is not None:
            validate_canonical_path_id(
                self.proposed_path_id, "proposed_path_id", expected_flow_group_id=fg_id
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "flow_group_id": self.flow_group_id,
            "actual_path_id": self.actual_path_id,
            "status": self.status.value,
            "verification_status": self.verification_status.value,
            "reason": self.reason,
            "rule_count": self.rule_count,
            "applied_at": self.applied_at,
            "old_path_id": self.old_path_id,
            "proposed_path_id": self.proposed_path_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> ApplyFlowResult:
        if not isinstance(data, Mapping):
            raise ContractSerializationError("ApplyFlowResult payload must be a mapping.")
        fg_id = (
            validate_canonical_flow_group_id(data.get("flow_group_id"), "flow_group_id")
            if strict
            else str(data["flow_group_id"])
        )
        status = parse_enum_strict(
            FlowDecisionStatus, data.get("status"), "status"
        )
        v_status = parse_enum_strict(
            VerificationStatus, data.get("verification_status"), "verification_status"
        )
        rule_count = (
            require_strict_int(data.get("rule_count"), "rule_count", min_value=0)
            if strict
            else int(data.get("rule_count", 0))
        )
        applied_at = data.get("applied_at")
        if applied_at is not None and strict:
            applied_at = validate_rfc3339_utc_timestamp(
                applied_at, "applied_at", normalize=True
            )
        obj = cls(
            flow_group_id=fg_id,
            actual_path_id=data.get("actual_path_id"),
            status=status,
            verification_status=v_status,
            reason=data.get("reason"),
            rule_count=rule_count,
            applied_at=applied_at,
            old_path_id=data.get("old_path_id"),
            proposed_path_id=data.get("proposed_path_id"),
        )
        if strict:
            obj.validate_contract()
        return obj


@dataclass(frozen=True)
class ApplyResponse:
    """
    Canonical `ApplyResponse` returned by SDN Controller (`interfaces_v2.md` §10, Guide v1.4 §13.3 Step F).

    Confirmed core fields:
    - `run_id`: Run identifier.
    - `decision_batch_id`: Batch identifier matching `ApplyRequest.decision_batch_id`.
    - `status`: `BatchStatus` (`APPLIED`, `REJECTED`, `FAILED`, `ROLLED_BACK`, `PARTIAL`, `INCONSISTENT`).
    - `state_version`: Authoritative global `state_version` of `run_id` after batch finalization
      (unchanged when `status == BatchStatus.REJECTED`).
    - `completed_at`: UTC RFC3339 completion timestamp.
    - `results`: Tuple of `ApplyFlowResult` entries.
    """

    run_id: str
    decision_batch_id: str
    status: BatchStatus
    state_version: int
    completed_at: str
    results: tuple[ApplyFlowResult, ...]
    reason: Optional[str] = None
    rule_count: int = 0
    deadline_missed: bool = False
    idempotent_replay: bool = False
    payload_hash: Optional[str] = None
    is_network_atomic: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.results, tuple):
            object.__setattr__(self, "results", tuple(self.results))

    def validate_contract(self) -> None:
        require_non_empty_str(self.run_id, "run_id")
        require_non_empty_str(self.decision_batch_id, "decision_batch_id")
        parse_enum_strict(BatchStatus, self.status, "status")
        require_strict_int(self.state_version, "state_version", min_value=0)
        validate_rfc3339_utc_timestamp(
            self.completed_at, "completed_at", normalize=True
        )
        require_strict_int(self.rule_count, "rule_count", min_value=0)
        require_strict_bool(self.deadline_missed, "deadline_missed")
        require_strict_bool(self.idempotent_replay, "idempotent_replay")
        require_strict_bool(self.is_network_atomic, "is_network_atomic")
        for res in self.results:
            if not isinstance(res, ApplyFlowResult):
                raise ContractSerializationError(
                    "Each entry in ApplyResponse.results must be an ApplyFlowResult."
                )
            res.validate_contract()

    def to_core_dict(self) -> dict[str, Any]:
        """Serialize the confirmed core IF08 `ApplyResponse` payload."""
        return {
            "run_id": self.run_id,
            "decision_batch_id": self.decision_batch_id,
            "status": self.status.value,
            "state_version": self.state_version,
            "completed_at": self.completed_at,
            "results": [r.to_dict() for r in self.results],
        }

    def to_dict(self) -> dict[str, Any]:
        """Serialize full `ApplyResponse` including audit/status metadata."""
        payload = self.to_core_dict()
        payload.update(
            {
                "reason": self.reason,
                "rule_count": self.rule_count,
                "deadline_missed": self.deadline_missed,
                "idempotent_replay": self.idempotent_replay,
                "payload_hash": self.payload_hash,
                "is_network_atomic": self.is_network_atomic,
            }
        )
        return payload

    def to_json(self) -> str:
        """Serialize `ApplyResponse` to deterministic transport-neutral JSON."""
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True
    ) -> ApplyResponse:
        if not isinstance(data, Mapping):
            raise ContractSerializationError("ApplyResponse payload must be a mapping.")
        for req_key in (
            "run_id",
            "decision_batch_id",
            "status",
            "state_version",
            "completed_at",
            "results",
        ):
            if req_key not in data or data[req_key] is None:
                raise ContractSerializationError(
                    f"Missing required field '{req_key}' in ApplyResponse."
                )
        raw_results = data.get("results", ())
        if not isinstance(raw_results, (list, tuple)):
            raise ContractSerializationError(
                "Field 'results' in ApplyResponse must be a list or tuple."
            )
        results = tuple(
            r
            if isinstance(r, ApplyFlowResult)
            else ApplyFlowResult.from_dict(r, strict=strict)
            for r in raw_results
        )
        status = parse_enum_strict(BatchStatus, data["status"], "status")
        sv = (
            require_strict_int(data["state_version"], "state_version", min_value=0)
            if strict
            else int(data["state_version"])
        )
        comp_at = (
            validate_rfc3339_utc_timestamp(
                data["completed_at"], "completed_at", normalize=True
            )
            if strict
            else str(data["completed_at"])
        )
        rule_count = (
            require_strict_int(
                data.get("rule_count", sum(r.rule_count for r in results)),
                "rule_count",
                min_value=0,
            )
            if strict
            else int(data.get("rule_count", 0))
        )
        deadline_missed = (
            require_strict_bool(
                data.get("deadline_missed", False), "deadline_missed"
            )
            if strict
            else bool(data.get("deadline_missed", False))
        )
        idempotent_replay = (
            require_strict_bool(
                data.get("idempotent_replay", False), "idempotent_replay"
            )
            if strict
            else bool(data.get("idempotent_replay", False))
        )
        is_network_atomic = (
            require_strict_bool(
                data.get("is_network_atomic", False), "is_network_atomic"
            )
            if strict
            else bool(data.get("is_network_atomic", False))
        )
        obj = cls(
            run_id=require_non_empty_str(data["run_id"], "run_id")
            if strict
            else str(data["run_id"]),
            decision_batch_id=require_non_empty_str(
                data["decision_batch_id"], "decision_batch_id"
            )
            if strict
            else str(data["decision_batch_id"]),
            status=status,
            state_version=sv,
            completed_at=comp_at,
            results=results,
            reason=data.get("reason"),
            rule_count=rule_count,
            deadline_missed=deadline_missed,
            idempotent_replay=idempotent_replay,
            payload_hash=data.get("payload_hash"),
            is_network_atomic=is_network_atomic,
        )
        if strict:
            obj.validate_contract()
        return obj

    @classmethod
    def from_json(cls, raw_json: str, *, strict: bool = True) -> ApplyResponse:
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise ContractSerializationError(
                f"Invalid JSON in ApplyResponse: {exc}"
            ) from exc
        return cls.from_dict(parsed, strict=strict)
