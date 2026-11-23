"""
Eventlet-Compatible Northbound HTTP Control API & Async Apply Coordinator (`Phase 12`, Tasks 2 & 3).

Exposes the SDN Controller over a strict, production-oriented WSGI HTTP interface
without altering SDN Core logic or v1.4 cross-role wire schemas:
- `GET  /health`
- `GET  /routing-state?run_id=...`
- `GET  /routing-history?run_id=...&flow_group_id=...`
- `GET  /apply-status?run_id=...&decision_batch_id=...`
- `POST /demand-releases`
- `POST /snapshots`
- `POST /apply` (supports sync-within-budget `200 OK` + safe async `202 Accepted` fallback)

Design guarantees:
- Binds to `127.0.0.1` by default; optional token authentication (`X-SDN-Token` or
  `Authorization: Bearer <token>`).
- Request-size limits (`max_request_bytes`) and controlled JSON error responses.
- Never bypasses `DecisionHandler` or `SnapshotCreator`; never returns `APPLIED` before
  real forwarding verification completes.
- Explicitly distinguishes HTTP job state (`IN_PROGRESS` / `FINALIZED`) in the HTTP
  envelope from the unaltered v1.4 `ApplyResponse` schema.
"""

from __future__ import annotations

import json
import math
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Mapping, Optional, Sequence
from urllib.parse import parse_qs, quote

from sdn.adapters.data_event_publisher import DataEventPublisher
from sdn.adapters.sqlite_demand_ledger import (
    DemandPersistenceError,
    PersistedDemandEntry,
    SQLiteDemandReleaseLedger,
    compute_demand_payload_fingerprint,
)
from sdn.controller.decision_handler import DecisionHandler
from sdn.controller.state_manager import (
    StateManager,
    StateManagerError,
    format_utc_timestamp,
    parse_utc_timestamp,
)
from sdn.monitoring.snapshot_creator import (
    ForbiddenDemandSourceError,
    SnapshotCreationError,
    SnapshotCreator,
)
from sdn.schemas.apply_models import ApplyRequest, ApplyResponse
from sdn.schemas.snapshot_models import (
    DemandObservationRecord,
    DemandSourceKind,
    OriginRefs,
)
from sdn.schemas.state_models import (
    ContractSerializationError,
    validate_canonical_flow_group_id,
    validate_rfc3339_utc_timestamp,
)


def _utc_now_rfc3339() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _cooperative_spawn(fn: Callable[[], None]) -> Any:
    """Spawn a non-blocking worker using `eventlet.spawn` if monkey-patched, else a daemon thread."""
    try:
        import eventlet  # type: ignore[import-not-found]
        import eventlet.patcher  # type: ignore[import-not-found]

        if eventlet.patcher.is_monkey_patched("socket") or eventlet.patcher.is_monkey_patched("time"):
            return eventlet.spawn(fn)
    except Exception:
        pass
    t = threading.Thread(target=fn, daemon=True)
    t.start()
    return t


def _cooperative_wait(event_obj: threading.Event, timeout_s: float) -> bool:
    """Wait on an event cooperatively under eventlet or standard threads."""
    if timeout_s <= 0.0:
        return event_obj.is_set()
    deadline = time.monotonic() + timeout_s
    try:
        import eventlet  # type: ignore[import-not-found]
        import eventlet.patcher  # type: ignore[import-not-found]

        if eventlet.patcher.is_monkey_patched("time"):
            while not event_obj.is_set():
                rem = deadline - time.monotonic()
                if rem <= 0.0:
                    return False
                eventlet.sleep(min(0.01, rem))
            return True
    except Exception:
        pass
    return event_obj.wait(timeout=timeout_s)


class DemandReleaseConflictError(ValueError):
    """Raised when a duplicate demand release carries a conflicting `requested_mbps` value."""


class HTTPDemandReleaseProvider:
    """
    Production HTTP-backed implementation of `DemandHistoryProviderProtocol` (`REQ-03`).

    Accepts `DemandRelease` JSON payloads emitted by `network/traffic_generator/sender.py`
    (`--release-url`), stamps `available_at_utc` at server receipt, validates canonical IDs
    and forbidden demand sources, and handles duplicate deliveries idempotently.
    """

    FORBIDDEN_SOURCES = frozenset(
        {
            DemandSourceKind.GOODPUT_MBPS.value,
            DemandSourceKind.PORT_TX_MBPS.value,
            DemandSourceKind.PORT_COUNTER.value,
            DemandSourceKind.UNRELEASED_DEMAND_PLAN.value,
        }
    )

    def __init__(
        self,
        *,
        origin_refs_lookup: Optional[Callable[[str], Optional[OriginRefs]]] = None,
        ledger: Optional[SQLiteDemandReleaseLedger] = None,
    ) -> None:
        self._lock = threading.RLock()
        self._records_by_run: dict[str, list[DemandObservationRecord]] = {}
        self._by_event_id: dict[tuple[str, str], DemandObservationRecord] = {}
        self._by_bin_key: dict[tuple[str, str, int], DemandObservationRecord] = {}
        self._fingerprints_by_event_id: dict[tuple[str, str], str] = {}
        self._origin_refs_lookup = origin_refs_lookup
        self._ledger = ledger
        if self._ledger is not None:
            self._restore_from_ledger()

    def _restore_from_ledger(self) -> None:
        assert self._ledger is not None
        entries = self._ledger.load_all()
        with self._lock:
            for entry in entries:
                rec = entry.record
                eid = rec.release_record_id or f"network:{rec.run_id}:demand:{rec.flow_group_id}:{rec.bin_index}"
                if (rec.run_id, eid) in self._by_event_id:
                    continue
                self._records_by_run.setdefault(rec.run_id, []).append(rec)
                self._by_event_id[(rec.run_id, eid)] = rec
                self._by_bin_key[(rec.run_id, rec.flow_group_id, rec.bin_index)] = rec
                self._fingerprints_by_event_id[(rec.run_id, eid)] = entry.payload_fingerprint

    @property
    def provider_name(self) -> str:
        return "HTTPDemandReleaseProvider"

    @property
    def is_durable(self) -> bool:
        return self._ledger is not None

    @property
    def demand_channel_status(self) -> str:
        if self._ledger is not None:
            return "NORTHBOUND_HTTP_RELEASED_DEMAND_DURABLE_SQLITE_V1"
        return "NORTHBOUND_HTTP_RELEASED_DEMAND_V1"

    def ledger_status(self) -> dict[str, Any]:
        if self._ledger is not None:
            return self._ledger.status_summary()
        with self._lock:
            total = sum(len(v) for v in self._records_by_run.values())
        return {
            "durable": False,
            "healthy": True,
            "db_path": None,
            "retention_bins": None,
            "persisted_count": total,
            "last_error": None,
        }

    def close(self) -> None:
        if self._ledger is not None:
            self._ledger.close()

    def get_observations(self, run_id: str) -> tuple[DemandObservationRecord, ...]:
        with self._lock:
            return tuple(self._records_by_run.get(run_id, ()))

    def _parse_and_validate_payload(
        self,
        payload: Mapping[str, Any],
        *,
        server_receipt_utc: Optional[str] = None,
    ) -> tuple[DemandObservationRecord, str]:
        if not isinstance(payload, Mapping):
            raise ContractSerializationError("DemandRelease payload must be a JSON object.")

        raw_run_id = payload.get("run_id")
        if not isinstance(raw_run_id, str) or not raw_run_id.strip():
            raise ContractSerializationError("Field 'run_id' must be a non-empty string.")
        run_id = raw_run_id.strip()

        flow_group_id = validate_canonical_flow_group_id(
            payload.get("flow_group_id"), "flow_group_id"
        )
        w_start = validate_rfc3339_utc_timestamp(
            payload.get("window_start_utc"), "window_start_utc", normalize=True
        )
        w_end = validate_rfc3339_utc_timestamp(
            payload.get("window_end_utc"), "window_end_utc", normalize=True
        )
        if parse_utc_timestamp(w_end) <= parse_utc_timestamp(w_start):
            raise ContractSerializationError(
                f"window_end_utc ({w_end}) must be strictly greater than window_start_utc ({w_start})."
            )
        rel_utc = validate_rfc3339_utc_timestamp(
            payload.get("released_at_utc"), "released_at_utc", normalize=True
        )

        # Server always stamps available_at_utc at receipt time (monotonic >= released_at_utc)
        receipt_utc = (
            validate_rfc3339_utc_timestamp(server_receipt_utc, "available_at_utc", normalize=True)
            if server_receipt_utc is not None
            else _utc_now_rfc3339()
        )
        if parse_utc_timestamp(receipt_utc) < parse_utc_timestamp(rel_utc):
            avail_utc = rel_utc
        else:
            avail_utc = format_utc_timestamp(parse_utc_timestamp(receipt_utc))

        src_kind = str(payload.get("source_kind", DemandSourceKind.RELEASED_DEMAND.value)).strip().upper()
        if src_kind in self.FORBIDDEN_SOURCES:
            raise ForbiddenDemandSourceError(
                f"Forbidden demand source_kind {src_kind!r} in POST /demand-releases (REQ-03)."
            )
        if src_kind != DemandSourceKind.RELEASED_DEMAND.value:
            raise ContractSerializationError(
                f"Invalid source_kind {src_kind!r}; expected {DemandSourceKind.RELEASED_DEMAND.value!r}."
            )

        raw_req = payload.get("requested_mbps")
        if isinstance(raw_req, bool) or not isinstance(raw_req, (int, float)):
            raise ContractSerializationError(
                f"Field 'requested_mbps' must be a non-negative finite number, got {raw_req!r}."
            )
        req_mbps = float(raw_req)
        if not math.isfinite(req_mbps) or req_mbps < 0.0:
            raise ContractSerializationError(
                f"Field 'requested_mbps' must be finite and >= 0.0, got {req_mbps!r}."
            )

        raw_bin = payload.get("bin_index")
        origin_refs = self._origin_refs_lookup(run_id) if self._origin_refs_lookup else None
        if raw_bin is not None:
            if isinstance(raw_bin, bool) or not isinstance(raw_bin, int):
                raise ContractSerializationError(
                    f"Field 'bin_index' must be an integer, got {raw_bin!r}."
                )
            bin_index = int(raw_bin)
        elif origin_refs is not None:
            meas_dt = parse_utc_timestamp(origin_refs.measurement_started_at_utc)
            delta_s = origin_refs.delta_s if origin_refs.delta_s > 0 else 1
            bin_index = int(round((parse_utc_timestamp(w_start) - meas_dt).total_seconds())) // delta_s
        else:
            raise ContractSerializationError("Field 'bin_index' is required when origin_refs are unavailable.")

        raw_eid = payload.get("event_id") or payload.get("release_record_id")
        if raw_eid is not None:
            if not isinstance(raw_eid, str) or not raw_eid.strip():
                raise ContractSerializationError("Field 'event_id' must be a non-empty string if provided.")
            event_id = raw_eid.strip()
        else:
            event_id = f"network:{run_id}:demand:{flow_group_id}:{bin_index}"

        rec = DemandObservationRecord(
            run_id=run_id,
            flow_group_id=flow_group_id,
            bin_index=bin_index,
            window_start_utc=w_start,
            window_end_utc=w_end,
            requested_mbps=req_mbps,
            released_at_utc=rel_utc,
            available_at_utc=avail_utc,
            release_record_id=event_id,
            source_kind=DemandSourceKind.RELEASED_DEMAND.value,
            valid=True,
            reason=None,
        )
        fp = compute_demand_payload_fingerprint(
            run_id=run_id,
            flow_group_id=flow_group_id,
            bin_index=bin_index,
            window_start_utc=w_start,
            window_end_utc=w_end,
            requested_mbps=req_mbps,
            released_at_utc=rel_utc,
        )
        return rec, fp

    def _matches_existing(
        self, existing: DemandObservationRecord, candidate: DemandObservationRecord
    ) -> bool:
        return (
            existing.requested_mbps is not None
            and candidate.requested_mbps is not None
            and abs(existing.requested_mbps - candidate.requested_mbps) <= 1e-9
            and existing.window_start_utc == candidate.window_start_utc
            and existing.window_end_utc == candidate.window_end_utc
            and existing.flow_group_id == candidate.flow_group_id
            and existing.bin_index == candidate.bin_index
        )

    def ingest_release_batch(
        self,
        payloads: Sequence[Mapping[str, Any]],
        *,
        server_receipt_utc: Optional[str] = None,
    ) -> list[tuple[DemandObservationRecord, bool]]:
        """
        Validate and durably persist a batch of `DemandRelease` JSON objects in a SINGLE
        SQLite transaction (avoiding write amplification under 48 events/s).

        Persists to SQLite BEFORE updating RAM or returning ACK; if persistence fails,
        raises `DemandPersistenceError` and leaves RAM unchanged.
        """
        parsed = [
            self._parse_and_validate_payload(item, server_receipt_utc=server_receipt_utc)
            for item in payloads
        ]
        with self._lock:
            results: list[tuple[DemandObservationRecord, bool]] = []
            to_persist: list[PersistedDemandEntry] = []
            staged_by_eid: dict[tuple[str, str], DemandObservationRecord] = {}
            staged_by_bin: dict[tuple[str, str, int], DemandObservationRecord] = {}

            for rec, fp in parsed:
                eid = rec.release_record_id or f"network:{rec.run_id}:demand:{rec.flow_group_id}:{rec.bin_index}"
                ekey = (rec.run_id, eid)
                bkey = (rec.run_id, rec.flow_group_id, rec.bin_index)

                existing = (
                    self._by_event_id.get(ekey)
                    or self._by_bin_key.get(bkey)
                    or staged_by_eid.get(ekey)
                    or staged_by_bin.get(bkey)
                )
                if existing is not None:
                    if not self._matches_existing(existing, rec):
                        raise DemandReleaseConflictError(
                            f"Conflicting duplicate DemandRelease for ({rec.run_id}, {rec.flow_group_id}, "
                            f"bin={rec.bin_index}, event_id={eid}): existing={existing.requested_mbps} "
                            f"vs new={rec.requested_mbps}."
                        )
                    results.append((existing, True))
                else:
                    staged_by_eid[ekey] = rec
                    staged_by_bin[bkey] = rec
                    to_persist.append(PersistedDemandEntry(record=rec, payload_fingerprint=fp))
                    results.append((rec, False))

            # Durable write MUST succeed before mutating RAM or acknowledging caller!
            if self._ledger is not None and to_persist:
                self._ledger.persist_batch(to_persist)

            for entry in to_persist:
                rec = entry.record
                eid = rec.release_record_id or f"network:{rec.run_id}:demand:{rec.flow_group_id}:{rec.bin_index}"
                self._records_by_run.setdefault(rec.run_id, []).append(rec)
                self._by_event_id[(rec.run_id, eid)] = rec
                self._by_bin_key[(rec.run_id, rec.flow_group_id, rec.bin_index)] = rec
                self._fingerprints_by_event_id[(rec.run_id, eid)] = entry.payload_fingerprint

            return results

    def ingest_release_payload(
        self,
        payload: Mapping[str, Any],
        *,
        server_receipt_utc: Optional[str] = None,
    ) -> tuple[DemandObservationRecord, bool]:
        """
        Validate and durably store one `DemandRelease` JSON object.

        Returns `(record, is_duplicate)`.
        Raises `ContractSerializationError` / `ForbiddenDemandSourceError` on invalid input,
        `DemandReleaseConflictError` on conflicting duplicate delivery, or
        `DemandPersistenceError` if durable storage fails before ACK.
        """
        return self.ingest_release_batch([payload], server_receipt_utc=server_receipt_utc)[0]


class ApplyJobState(str, Enum):
    """HTTP-level async job state (strictly separate from v1.4 `ApplyResponse.status`)."""

    IN_PROGRESS = "IN_PROGRESS"
    FINALIZED = "FINALIZED"


@dataclass
class ApplyJobRecord:
    """Tracks an asynchronous or synchronous `POST /apply` job."""

    run_id: str
    decision_batch_id: str
    payload_hash: str
    accepted_at_utc: str
    accepted_mono: float = 0.0
    http_accept_ms: Optional[float] = None
    queue_wait_ms: Optional[float] = None
    job_state: ApplyJobState = ApplyJobState.IN_PROGRESS
    completed_at_utc: Optional[str] = None
    apply_latency_ms: Optional[float] = None
    openflow_install_ms: Optional[float] = None
    verification_ms: Optional[float] = None
    flow_stats_request_count: Optional[int] = None
    apply_response: Optional[ApplyResponse] = None
    internal_error: Optional[str] = None
    done_event: threading.Event = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.done_event is None:
            self.done_event = threading.Event()


@dataclass(frozen=True)
class NorthboundAPIConfig:
    """Configuration for `SDNNorthboundHTTPApp`."""

    bind_host: str = "127.0.0.1"
    bind_port: int = 18080
    auth_token: Optional[str] = None
    max_request_bytes: int = 1_048_576
    default_sync_wait_ms: int = 300
    max_sync_wait_ms: int = 15000
    default_run_id: str = "s0_live_run"


class SDNNorthboundHTTPApp:
    """
    Eventlet-compatible WSGI application exposing SDN Northbound Control API (`Phase 12`).

    Wraps the verified SDN Core (`StateManager`, `DecisionHandler`, `SnapshotCreator`)
    plus `HTTPDemandReleaseProvider` and `DataEventPublisher` without modifying core logic.
    """

    def __init__(
        self,
        *,
        state_manager: StateManager,
        decision_handler: DecisionHandler,
        snapshot_creator: SnapshotCreator,
        demand_provider: HTTPDemandReleaseProvider,
        data_publisher: Optional[DataEventPublisher] = None,
        config: Optional[NorthboundAPIConfig] = None,
        health_provider: Optional[Callable[[], dict[str, Any]]] = None,
        spawn_fn: Optional[Callable[[Callable[[], None]], Any]] = None,
    ) -> None:
        self._state_manager = state_manager
        self._decision_handler = decision_handler
        self._snapshot_creator = snapshot_creator
        self._demand_provider = demand_provider
        self._data_publisher = data_publisher
        self._config = config or NorthboundAPIConfig()
        self._health_provider = health_provider
        self._spawn_fn = spawn_fn or _cooperative_spawn
        self._jobs_lock = threading.RLock()
        self._jobs: dict[tuple[str, str], ApplyJobRecord] = {}
        self._active_batch_by_run: dict[str, str] = {}

    @property
    def config(self) -> NorthboundAPIConfig:
        return self._config

    @property
    def demand_provider(self) -> HTTPDemandReleaseProvider:
        return self._demand_provider

    @property
    def data_publisher(self) -> Optional[DataEventPublisher]:
        return self._data_publisher

    # ------------------------------------------------------------------
    # WSGI Entrypoint
    # ------------------------------------------------------------------

    def __call__(
        self,
        environ: Mapping[str, Any],
        start_response: Callable[[str, list[tuple[str, str]]], Any],
    ) -> list[bytes]:
        method = str(environ.get("REQUEST_METHOD", "GET")).upper()
        path = str(environ.get("PATH_INFO", "/")).rstrip("/") or "/"
        query = parse_qs(str(environ.get("QUERY_STRING", "")), keep_blank_values=False)

        if not self._is_authorized(environ):
            return self._json_response(
                start_response,
                "401 Unauthorized",
                {"error": "UNAUTHORIZED", "message": "Missing or invalid SDN authentication token."},
            )

        try:
            if method == "GET" and path == "/health":
                return self._handle_health(start_response)
            if method == "GET" and path == "/routing-state":
                return self._handle_routing_state(query, start_response)
            if method == "GET" and path == "/routing-history":
                return self._handle_routing_history(query, start_response)
            if method == "GET" and path == "/apply-status":
                return self._handle_apply_status(query, start_response)
            if method == "POST" and path == "/demand-releases":
                body, err = self._read_json_body(environ, start_response)
                if err is not None:
                    return err
                return self._handle_demand_releases(body, start_response)
            if method == "POST" and path == "/snapshots":
                body, err = self._read_json_body(environ, start_response)
                if err is not None:
                    return err
                return self._handle_snapshots(body, start_response)
            if method == "POST" and path == "/apply":
                body, err = self._read_json_body(environ, start_response)
                if err is not None:
                    return err
                return self._handle_apply(body, query, start_response)

            return self._json_response(
                start_response,
                "404 Not Found",
                {"error": "NOT_FOUND", "method": method, "path": path},
            )
        except ContractSerializationError as exc:
            return self._json_response(
                start_response,
                "400 Bad Request",
                {"error": "CONTRACT_VALIDATION_ERROR", "message": str(exc)},
            )
        except ForbiddenDemandSourceError as exc:
            return self._json_response(
                start_response,
                "400 Bad Request",
                {"error": "FORBIDDEN_DEMAND_SOURCE", "message": str(exc)},
            )
        except DemandReleaseConflictError as exc:
            return self._json_response(
                start_response,
                "409 Conflict",
                {"error": "DEMAND_RELEASE_CONFLICT", "message": str(exc)},
            )
        except DemandPersistenceError as exc:
            return self._json_response(
                start_response,
                "503 Service Unavailable",
                {"ack": False, "error": "DEMAND_PERSISTENCE_FAILED", "message": str(exc)},
            )
        except (StateManagerError, KeyError) as exc:
            return self._json_response(
                start_response,
                "404 Not Found",
                {"error": "RUN_NOT_FOUND", "message": str(exc)},
            )
        except SnapshotCreationError as exc:
            return self._json_response(
                start_response,
                "400 Bad Request",
                {"error": "SNAPSHOT_CREATION_ERROR", "message": str(exc)},
            )
        except Exception as exc:
            return self._json_response(
                start_response,
                "500 Internal Server Error",
                {"error": "INTERNAL_ERROR", "message": f"{type(exc).__name__}: {exc}"},
            )

    # ------------------------------------------------------------------
    # Auth & Body Parsing Helpers
    # ------------------------------------------------------------------

    def _is_authorized(self, environ: Mapping[str, Any]) -> bool:
        expected = self._config.auth_token
        if not expected:
            return True
        tok = environ.get("HTTP_X_SDN_TOKEN")
        if isinstance(tok, str) and tok == expected:
            return True
        auth = environ.get("HTTP_AUTHORIZATION")
        if isinstance(auth, str) and auth == f"Bearer {expected}":
            return True
        return False

    def _read_json_body(
        self,
        environ: Mapping[str, Any],
        start_response: Callable[[str, list[tuple[str, str]]], Any],
    ) -> tuple[dict[str, Any], Optional[list[bytes]]]:
        raw_len = environ.get("CONTENT_LENGTH")
        try:
            content_length = int(raw_len) if raw_len else 0
        except ValueError:
            return {}, self._json_response(
                start_response,
                "400 Bad Request",
                {"error": "INVALID_CONTENT_LENGTH", "message": "Invalid Content-Length header."},
            )
        if content_length < 0:
            return {}, self._json_response(
                start_response,
                "400 Bad Request",
                {"error": "INVALID_CONTENT_LENGTH", "message": "Negative Content-Length header."},
            )
        if content_length > self._config.max_request_bytes:
            return {}, self._json_response(
                start_response,
                "413 Payload Too Large",
                {
                    "error": "PAYLOAD_TOO_LARGE",
                    "max_request_bytes": self._config.max_request_bytes,
                },
            )
        wsgi_input = environ.get("wsgi.input")
        if wsgi_input is None:
            return {}, self._json_response(
                start_response,
                "400 Bad Request",
                {"error": "EMPTY_BODY", "message": "Missing request body."},
            )
        raw_bytes = wsgi_input.read(self._config.max_request_bytes + 1)
        if len(raw_bytes) > self._config.max_request_bytes:
            return {}, self._json_response(
                start_response,
                "413 Payload Too Large",
                {
                    "error": "PAYLOAD_TOO_LARGE",
                    "max_request_bytes": self._config.max_request_bytes,
                },
            )
        try:
            parsed = json.loads(raw_bytes.decode("utf-8"))
        except Exception as exc:
            return {}, self._json_response(
                start_response,
                "400 Bad Request",
                {"error": "INVALID_JSON", "message": str(exc)},
            )
        if not isinstance(parsed, dict):
            return {}, self._json_response(
                start_response,
                "400 Bad Request",
                {"error": "INVALID_JSON", "message": "Request body must be a JSON object."},
            )
        return parsed, None

    def _json_response(
        self,
        start_response: Callable[[str, list[tuple[str, str]]], Any],
        status: str,
        payload: Mapping[str, Any],
    ) -> list[bytes]:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        headers = [
            ("Content-Type", "application/json; charset=utf-8"),
            ("Content-Length", str(len(raw))),
            ("Cache-Control", "no-store"),
        ]
        start_response(status, headers)
        return [raw]

    def _query_param(self, query: Mapping[str, list[str]], key: str, default: Optional[str] = None) -> Optional[str]:
        vals = query.get(key)
        if not vals:
            return default
        val = vals[0].strip()
        return val if val else default

    # ------------------------------------------------------------------
    # Endpoint Handlers
    # ------------------------------------------------------------------

    def _handle_health(self, start_response: Callable[[str, list[tuple[str, str]]], Any]) -> list[bytes]:
        dp_summary = self._data_publisher.status_summary() if self._data_publisher else None
        demand_ledger = self._demand_provider.ledger_status()
        idem_durable = bool(self._decision_handler.idempotency_store.is_durable)
        topo = getattr(self._state_manager, "_topology", None)
        topo_ver = getattr(topo, "topology_version", None) if topo is not None else None
        topo_hash = getattr(topo, "topology_hash", None) if topo is not None else None
        cfg_ver = getattr(self._decision_handler.config, "expected_config_version", None)

        base: dict[str, Any] = {
            "status": "ok",
            "run_id": self._config.default_run_id,
            "idempotency_store_durable": idem_durable,
            "idempotency_store_status": self._decision_handler.idempotency_store.store_status,
            "demand_provider": self._demand_provider.provider_name,
            "demand_channel_status": self._demand_provider.demand_channel_status,
            "demand_ledger": demand_ledger,
            "data_publisher": dp_summary,
            "topology_version": topo_ver,
            "topology_hash": topo_hash,
            "config_version": cfg_ver,
        }
        with self._jobs_lock:
            base["in_flight_jobs"] = dict(self._active_batch_by_run)
        if self._health_provider is not None:
            base.update(self._health_provider())
        else:
            try:
                snap = self._state_manager.get_routing_state(self._config.default_run_id)
                base["state_version"] = snap.state_version
                base["reconcile_required"] = snap.reconcile_required
                base["in_flight_batch_id"] = snap.in_flight_batch_id
            except (StateManagerError, KeyError):
                base["state_version"] = None
                base["reconcile_required"] = None
                base["in_flight_batch_id"] = None

        # Structured deployment readiness gates (Task C):
        # Strictly separate process liveness, SDN readiness, Data delivery readiness, and full-system readiness.
        connected_dpids = base.get("connected_dpids")
        expected_sw_count = 6
        if isinstance(connected_dpids, list):
            switches_ok = len(connected_dpids) >= expected_sw_count
        else:
            switches_ok = base.get("state_version") is not None

        readiness_state = base.get("readiness_state")
        verified_cached = base.get("verified_cached")
        reconcile_required = base.get("reconcile_required")
        all_48_verified = (
            (readiness_state == "VERIFIED" and (verified_cached is None or verified_cached >= 48))
            if readiness_state is not None
            else (base.get("state_version") is not None and reconcile_required is False)
        )

        sdn_blockers: list[str] = []
        if not switches_ok:
            sdn_blockers.append("SWITCHES_NOT_ALL_CONNECTED")
        if not all_48_verified:
            sdn_blockers.append("BOOTSTRAP_48_FLOWS_NOT_VERIFIED")
        if reconcile_required is True:
            sdn_blockers.append("RECONCILIATION_REQUIRED")
        if not demand_ledger.get("healthy", True):
            sdn_blockers.append("DEMAND_LEDGER_UNHEALTHY")

        sdn_ready = len(sdn_blockers) == 0

        data_configured = dp_summary is not None and bool(dp_summary.get("endpoint_base_url"))
        data_blockers: list[str] = []
        if not data_configured:
            data_blockers.append("DATA_ENDPOINT_NOT_CONFIGURED")
        else:
            assert dp_summary is not None
            if dp_summary.get("auto_flush") and not dp_summary.get("worker_running"):
                data_blockers.append("DATA_OUTBOX_WORKER_NOT_RUNNING")
            if int(dp_summary.get("pending_delivery_count") or 0) > 0 and dp_summary.get("last_error"):
                data_blockers.append(f"DATA_DELIVERY_BACKLOG:{dp_summary['last_error']}")
        data_ready = data_configured and len(data_blockers) == 0

        sdn_integration_blockers = list(sdn_blockers) + list(data_blockers)
        if not idem_durable:
            sdn_integration_blockers.append("IDEMPOTENCY_STORE_NOT_DURABLE")
        if not demand_ledger.get("durable", False):
            sdn_integration_blockers.append("DEMAND_LEDGER_NOT_DURABLE")
        sdn_integration_ready = len(sdn_integration_blockers) == 0

        # SDN /health can only observe SDN + local durable stores + Data delivery ACK.
        # It CANNOT verify Network sender, real Data DB, ML online worker, Optimization loop,
        # or closed-loop TC-I01, so full_system_readiness.ready MUST NOT falsely claim True.
        unverified_external_roles = [
            "network_sender",
            "data_db",
            "ml_online",
            "optimization_loop",
            "runner_tc_i01",
        ]
        full_system_blockers = list(sdn_integration_blockers) + [
            "EXTERNAL_ROLES_NOT_VERIFIED_BY_SDN_HEALTH"
        ]

        base["process_liveness"] = {
            "alive": True,
            "northbound_api_enabled": True,
        }
        base["sdn_readiness"] = {
            "ready": sdn_ready,
            "expected_switch_count": expected_sw_count,
            "connected_switch_count": len(connected_dpids) if isinstance(connected_dpids, list) else None,
            "all_switches_connected": switches_ok,
            "bootstrap_readiness_state": readiness_state,
            "verified_flow_count": verified_cached,
            "all_48_verified": all_48_verified,
            "reconcile_required": reconcile_required,
            "idempotency_store_durable": idem_durable,
            "demand_ledger_durable": bool(demand_ledger.get("durable", False)),
            "demand_ledger_healthy": bool(demand_ledger.get("healthy", True)),
            "topology_version": topo_ver,
            "topology_hash": topo_hash,
            "config_version": cfg_ver,
            "blockers": sdn_blockers,
        }
        base["data_delivery_readiness"] = {
            "configured": data_configured,
            "ready": data_ready,
            "worker_running": bool(dp_summary.get("worker_running")) if dp_summary else False,
            "pending_delivery_count": int(dp_summary.get("pending_delivery_count") or 0) if dp_summary else 0,
            "delivered_count": int(dp_summary.get("delivered_count") or 0) if dp_summary else 0,
            "last_successful_delivery_utc": dp_summary.get("last_successful_delivery_utc") if dp_summary else None,
            "last_error": dp_summary.get("last_error") if dp_summary else None,
            "blockers": data_blockers,
        }
        base["sdn_integration_readiness"] = {
            "ready": sdn_integration_ready,
            "sdn_ready": sdn_ready,
            "data_delivery_ready": data_ready,
            "idempotency_store_durable": idem_durable,
            "demand_ledger_durable": bool(demand_ledger.get("durable", False)),
            "blockers": sdn_integration_blockers,
        }
        base["full_system_readiness"] = {
            "ready": False,
            "scope": "SDN_LOCAL_OBSERVABILITY_ONLY",
            "sdn_ready": sdn_ready,
            "data_delivery_ready": data_ready,
            "sdn_integration_ready": sdn_integration_ready,
            "tc_i01_verified": False,
            "unverified_external_roles": unverified_external_roles,
            "blockers": full_system_blockers,
        }
        return self._json_response(start_response, "200 OK", base)

    def _handle_routing_state(
        self,
        query: Mapping[str, list[str]],
        start_response: Callable[[str, list[tuple[str, str]]], Any],
    ) -> list[bytes]:
        run_id = self._query_param(query, "run_id", self._config.default_run_id) or self._config.default_run_id
        snap = self._state_manager.get_routing_state(run_id)
        return self._json_response(start_response, "200 OK", snap.to_dict())

    def _handle_routing_history(
        self,
        query: Mapping[str, list[str]],
        start_response: Callable[[str, list[tuple[str, str]]], Any],
    ) -> list[bytes]:
        run_id = self._query_param(query, "run_id", self._config.default_run_id) or self._config.default_run_id
        fg = self._query_param(query, "flow_group_id")
        if fg is not None:
            fg = validate_canonical_flow_group_id(fg, "flow_group_id")
        intervals = self._state_manager.get_routing_history(run_id, flow_group_id=fg)
        return self._json_response(
            start_response,
            "200 OK",
            {
                "run_id": run_id,
                "flow_group_id": fg,
                "count": len(intervals),
                "intervals": [iv.to_dict() for iv in intervals],
            },
        )

    def _handle_demand_releases(
        self,
        body: dict[str, Any],
        start_response: Callable[[str, list[tuple[str, str]]], Any],
    ) -> list[bytes]:
        receipt_utc = _utc_now_rfc3339()
        if "events" in body:
            raw_events = body["events"]
            if not isinstance(raw_events, list) or not raw_events:
                raise ContractSerializationError("Field 'events' must be a non-empty JSON array.")
            ingested = self._demand_provider.ingest_release_batch(
                raw_events, server_receipt_utc=receipt_utc
            )
            acks = [
                {
                    "ack": True,
                    "event_id": rec.release_record_id,
                    "duplicate": dup,
                    "run_id": rec.run_id,
                    "flow_group_id": rec.flow_group_id,
                    "bin_index": rec.bin_index,
                    "released_at_utc": rec.released_at_utc,
                    "available_at_utc": rec.available_at_utc,
                }
                for rec, dup in ingested
            ]
            return self._json_response(
                start_response,
                "200 OK",
                {"ack": True, "count": len(acks), "results": acks},
            )

        rec, dup = self._demand_provider.ingest_release_payload(
            body, server_receipt_utc=receipt_utc
        )
        return self._json_response(
            start_response,
            "200 OK",
            {
                "ack": True,
                "event_id": rec.release_record_id,
                "duplicate": dup,
                "run_id": rec.run_id,
                "flow_group_id": rec.flow_group_id,
                "bin_index": rec.bin_index,
                "released_at_utc": rec.released_at_utc,
                "available_at_utc": rec.available_at_utc,
            },
        )

    def _handle_snapshots(
        self,
        body: dict[str, Any],
        start_response: Callable[[str, list[tuple[str, str]]], Any],
    ) -> list[bytes]:
        run_id = str(body.get("run_id") or self._config.default_run_id).strip()
        if not run_id:
            raise ContractSerializationError("Field 'run_id' must be a non-empty string.")
        cutoff = body.get("cutoff")
        lookback_L = body.get("lookback_L")
        if isinstance(cutoff, bool) or not isinstance(cutoff, int) or cutoff < 0:
            raise ContractSerializationError("Field 'cutoff' must be a non-negative integer.")
        if isinstance(lookback_L, bool) or not isinstance(lookback_L, int) or lookback_L <= 0:
            raise ContractSerializationError("Field 'lookback_L' must be a positive integer.")

        snap_as_of = body.get("snapshot_as_of_utc")
        if snap_as_of is not None:
            snap_as_of = validate_rfc3339_utc_timestamp(snap_as_of, "snapshot_as_of_utc", normalize=True)
        else:
            snap_as_of = _utc_now_rfc3339()

        raw_origins = body.get("origin_refs")
        if raw_origins is not None:
            if not isinstance(raw_origins, dict):
                raise ContractSerializationError("Field 'origin_refs' must be a JSON object if provided.")
            self._snapshot_creator.set_run_origin_refs(
                run_id,
                OriginRefs(
                    run_started_at_utc=validate_rfc3339_utc_timestamp(
                        raw_origins.get("run_started_at_utc"), "origin_refs.run_started_at_utc", normalize=True
                    ),
                    traffic_started_at_utc=validate_rfc3339_utc_timestamp(
                        raw_origins.get("traffic_started_at_utc"), "origin_refs.traffic_started_at_utc", normalize=True
                    ),
                    measurement_started_at_utc=validate_rfc3339_utc_timestamp(
                        raw_origins.get("measurement_started_at_utc"), "origin_refs.measurement_started_at_utc", normalize=True
                    ),
                    delta_s=int(raw_origins.get("delta_s", 1)),
                ),
            )

        snap = self._snapshot_creator.create_snapshot(
            run_id=run_id,
            cutoff=cutoff,
            lookback_L=lookback_L,
            snapshot_as_of_utc=snap_as_of,
        )
        self._decision_handler.register_snapshot(snap)
        if self._data_publisher is not None:
            self._data_publisher.publish_snapshot(snap, deliver_now=False)
        return self._json_response(start_response, "200 OK", snap.to_dict())

    def _handle_apply_status(
        self,
        query: Mapping[str, list[str]],
        start_response: Callable[[str, list[tuple[str, str]]], Any],
    ) -> list[bytes]:
        run_id = self._query_param(query, "run_id", self._config.default_run_id) or self._config.default_run_id
        batch_id = self._query_param(query, "decision_batch_id")
        if not batch_id:
            raise ContractSerializationError("Query parameter 'decision_batch_id' is required.")

        with self._jobs_lock:
            job = self._jobs.get((run_id, batch_id))

        if job is not None:
            if job.job_state == ApplyJobState.IN_PROGRESS:
                return self._json_response(
                    start_response,
                    "200 OK",
                    {
                        "http_status": "ACCEPTED",
                        "job_state": ApplyJobState.IN_PROGRESS.value,
                        "run_id": job.run_id,
                        "decision_batch_id": job.decision_batch_id,
                        "payload_hash": job.payload_hash,
                        "accepted_at_utc": job.accepted_at_utc,
                        "apply_response": None,
                    },
                )
            if job.apply_response is not None:
                return self._json_response(
                    start_response,
                    "200 OK",
                    {
                        "http_status": "FINALIZED",
                        "job_state": ApplyJobState.FINALIZED.value,
                        "run_id": job.run_id,
                        "decision_batch_id": job.decision_batch_id,
                        "payload_hash": job.payload_hash,
                        "accepted_at_utc": job.accepted_at_utc,
                        "completed_at_utc": job.completed_at_utc,
                        "http_accept_ms": job.http_accept_ms,
                        "queue_wait_ms": job.queue_wait_ms,
                        "openflow_install_ms": job.openflow_install_ms,
                        "verification_ms": job.verification_ms,
                        "flow_stats_request_count": job.flow_stats_request_count,
                        "apply_latency_ms": job.apply_latency_ms,
                        "apply_response": job.apply_response.to_dict(),
                    },
                )
            return self._json_response(
                start_response,
                "500 Internal Server Error",
                {
                    "http_status": "ERROR",
                    "job_state": ApplyJobState.FINALIZED.value,
                    "run_id": job.run_id,
                    "decision_batch_id": job.decision_batch_id,
                    "error": job.internal_error or "UNKNOWN_APPLY_WORKER_ERROR",
                },
            )

        # Fallback to durable idempotency store (e.g., after controller restart!)
        persisted = self._decision_handler.idempotency_store.get(run_id, batch_id)
        if persisted is not None:
            return self._json_response(
                start_response,
                "200 OK",
                {
                    "http_status": "FINALIZED",
                    "job_state": ApplyJobState.FINALIZED.value,
                    "run_id": run_id,
                    "decision_batch_id": batch_id,
                    "payload_hash": persisted.payload_hash,
                    "completed_at_utc": persisted.response.completed_at,
                    "persisted_replay": True,
                    "apply_response": persisted.response.to_dict(),
                },
            )

        return self._json_response(
            start_response,
            "404 Not Found",
            {
                "error": "BATCH_NOT_FOUND",
                "run_id": run_id,
                "decision_batch_id": batch_id,
            },
        )

    def _handle_apply(
        self,
        body: dict[str, Any],
        query: Mapping[str, list[str]],
        start_response: Callable[[str, list[tuple[str, str]]], Any],
    ) -> list[bytes]:
        t_http_start = time.monotonic()
        # Strict v1.4 ApplyRequest validation (never mutates schema)
        req = ApplyRequest.from_dict(body, strict=True)
        payload_hash = req.effective_payload_hash
        key = (req.run_id, req.decision_batch_id)

        async_flag = self._query_param(query, "async")
        wait_ms_raw = self._query_param(query, "wait_ms")
        if async_flag in ("1", "true", "True", "yes"):
            wait_ms = 0
        elif wait_ms_raw is not None:
            try:
                wait_ms = max(0, min(self._config.max_sync_wait_ms, int(wait_ms_raw)))
            except ValueError as exc:
                raise ContractSerializationError("Query parameter 'wait_ms' must be an integer.") from exc
        else:
            wait_ms = max(0, int(self._config.default_sync_wait_ms))

        poll_url = (
            f"/apply-status?run_id={quote(req.run_id, safe='')}"
            f"&decision_batch_id={quote(req.decision_batch_id, safe='')}"
        )

        # Check in-flight / completed job table atomically before spawning
        with self._jobs_lock:
            existing_job = self._jobs.get(key)
            if existing_job is not None:
                if existing_job.payload_hash != payload_hash:
                    # Conflicting payload for same (run_id, decision_batch_id)
                    if existing_job.job_state == ApplyJobState.IN_PROGRESS:
                        return self._json_response(
                            start_response,
                            "409 Conflict",
                            {
                                "error": "BATCH_ID_CONFLICT",
                                "run_id": req.run_id,
                                "decision_batch_id": req.decision_batch_id,
                                "existing_payload_hash": existing_job.payload_hash,
                                "received_payload_hash": payload_hash,
                            },
                        )
                    # Let DecisionHandler produce canonical v1.4 REJECTED(BATCH_ID_CONFLICT)
                    conflict_resp = self._decision_handler.handle_apply_request(
                        req, decision_time_utc=_utc_now_rfc3339()
                    )
                    return self._json_response(
                        start_response,
                        "200 OK",
                        {
                            "http_status": "FINALIZED",
                            "job_state": ApplyJobState.FINALIZED.value,
                            "apply_response": conflict_resp.to_dict(),
                        },
                    )

                if existing_job.job_state == ApplyJobState.IN_PROGRESS:
                    # Same batch_id + same payload_hash while still running -> safe 202
                    return self._json_response(
                        start_response,
                        "202 Accepted",
                        {
                            "http_status": "ACCEPTED",
                            "job_state": ApplyJobState.IN_PROGRESS.value,
                            "run_id": existing_job.run_id,
                            "decision_batch_id": existing_job.decision_batch_id,
                            "payload_hash": existing_job.payload_hash,
                            "accepted_at_utc": existing_job.accepted_at_utc,
                            "poll_url": poll_url,
                        },
                    )
                if existing_job.apply_response is not None:
                    # Already finalized -> let DecisionHandler return canonical idempotent_replay=True
                    replay_resp = self._decision_handler.handle_apply_request(
                        req, decision_time_utc=_utc_now_rfc3339()
                    )
                    return self._json_response(
                        start_response,
                        "200 OK",
                        {
                            "http_status": "FINALIZED",
                            "job_state": ApplyJobState.FINALIZED.value,
                            "apply_response": replay_resp.to_dict(),
                        },
                    )

            # Prevent overlapping conflicting apply jobs on the same run_id — even in the
            # sub-millisecond window before the active worker enters StateManager.begin_apply()!
            active_batch = self._active_batch_by_run.get(req.run_id)
            if active_batch is not None and active_batch != req.decision_batch_id:
                from sdn.schemas.state_models import ReasonCode

                busy_resp = self._decision_handler._reject_pre_write(
                    req,
                    reason=ReasonCode.BUSY,
                    rejected_at_utc=_utc_now_rfc3339(),
                    run_initialized=True,
                    cache_for_idempotency=False,
                )
                return self._json_response(
                    start_response,
                    "200 OK",
                    {
                        "http_status": "FINALIZED",
                        "job_state": ApplyJobState.FINALIZED.value,
                        "apply_response": busy_resp.to_dict(),
                    },
                )

            accepted_utc = _utc_now_rfc3339()
            accepted_mono = time.monotonic()
            job = ApplyJobRecord(
                run_id=req.run_id,
                decision_batch_id=req.decision_batch_id,
                payload_hash=payload_hash,
                accepted_at_utc=accepted_utc,
                accepted_mono=accepted_mono,
                http_accept_ms=round((accepted_mono - t_http_start) * 1000.0, 2),
            )
            self._jobs[key] = job
            self._active_batch_by_run[req.run_id] = req.decision_batch_id

        def _worker() -> None:
            t0 = time.monotonic()
            q_wait_ms = round((t0 - job.accepted_mono) * 1000.0, 2)
            try:
                decision_utc = _utc_now_rfc3339()
                resp = self._decision_handler.handle_apply_request(
                    req, decision_time_utc=decision_utc
                )
                elapsed_ms = round((time.monotonic() - t0) * 1000.0, 2)
                timing = getattr(self._decision_handler, "_last_batch_timing", None)
                if not isinstance(timing, dict) or timing.get("decision_batch_id") != req.decision_batch_id:
                    timing = {}
                with self._jobs_lock:
                    job.queue_wait_ms = q_wait_ms
                    job.apply_response = resp
                    job.completed_at_utc = resp.completed_at
                    job.apply_latency_ms = elapsed_ms
                    job.openflow_install_ms = timing.get("openflow_install_ms")
                    job.verification_ms = timing.get("verification_ms")
                    job.flow_stats_request_count = timing.get("flow_stats_request_count")
                    job.job_state = ApplyJobState.FINALIZED
                    if self._active_batch_by_run.get(req.run_id) == req.decision_batch_id:
                        self._active_batch_by_run.pop(req.run_id, None)
                self._publish_after_apply(req.run_id, resp)
            except Exception as exc:
                with self._jobs_lock:
                    job.queue_wait_ms = q_wait_ms
                    job.internal_error = f"{type(exc).__name__}: {exc}"
                    job.completed_at_utc = _utc_now_rfc3339()
                    job.job_state = ApplyJobState.FINALIZED
                    if self._active_batch_by_run.get(req.run_id) == req.decision_batch_id:
                        self._active_batch_by_run.pop(req.run_id, None)
            finally:
                job.done_event.set()

        self._spawn_fn(_worker)

        finished = _cooperative_wait(job.done_event, wait_ms / 1000.0)
        if finished:
            with self._jobs_lock:
                if job.apply_response is not None:
                    return self._json_response(
                        start_response,
                        "200 OK",
                        {
                            "http_status": "FINALIZED",
                            "job_state": ApplyJobState.FINALIZED.value,
                            "http_accept_ms": job.http_accept_ms,
                            "queue_wait_ms": job.queue_wait_ms,
                            "apply_latency_ms": job.apply_latency_ms,
                            "apply_response": job.apply_response.to_dict(),
                        },
                    )
                return self._json_response(
                    start_response,
                    "500 Internal Server Error",
                    {
                        "http_status": "ERROR",
                        "job_state": ApplyJobState.FINALIZED.value,
                        "error": job.internal_error or "UNKNOWN_APPLY_WORKER_ERROR",
                    },
                )

        # Still verifying on OVS -> return 202 Accepted before caller's HTTP timeout!
        return self._json_response(
            start_response,
            "202 Accepted",
            {
                "http_status": "ACCEPTED",
                "job_state": ApplyJobState.IN_PROGRESS.value,
                "run_id": job.run_id,
                "decision_batch_id": job.decision_batch_id,
                "payload_hash": job.payload_hash,
                "accepted_at_utc": job.accepted_at_utc,
                "poll_url": poll_url,
            },
        )

    def _publish_after_apply(self, run_id: str, resp: ApplyResponse) -> None:
        if self._data_publisher is None:
            return
        try:
            self._data_publisher.publish_apply_response(resp, deliver_now=False)
            snap = self._state_manager.get_routing_state(run_id)
            self._data_publisher.publish_routing_state(snap, deliver_now=False)
            history = self._state_manager.get_routing_history(run_id)
            self._data_publisher.publish_routing_history(
                run_id, snap.state_version, history, deliver_now=False
            )
        except Exception:
            # Never fail or crash finalized apply due to Data publisher error
            pass
