"""
SDN Data Event Publisher & Durable Outbox (`Phase 12`, Task 5).

Publishes SDN-produced v1.4 artifacts to Data's ingestion endpoint while keeping Data
as the sole primary database writer (`REQ-04`):
- `Snapshot`
- `RoutingStateSnapshot`
- `RoutingHistory` (`Sequence[RoutingHistoryInterval]`)
- `ApplyResponse`
- `VerificationEvidence`

Key safety properties:
- Preserves exact v1.4 schemas and canonical IDs (`0x103`, `0x103_P1`, `e1>c1`).
- Durable JSONL outbox (`outbox_events.jsonl` + `outbox_acked.jsonl`) survives process
  restarts and Data outages.
- Never claims `DELIVERED` without an explicit `2xx` ACK from the receiver; events whose
  delivery fails or whose endpoint is unconfigured/unreachable remain `PENDING_DELIVERY`.
- Bounded retry/backoff and deterministic deduplication keys (`dedup_key`).
- Eventlet-safe cooperative sleep (`eventlet.sleep` when monkey-patched) so background
  delivery never blocks the OpenFlow 1.3 event loop (`AC-S05`).
"""

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from sdn.adapters.data_api_v1_adapter import (
    DATA_V1_ENDPOINT_APPLY_RESULTS,
    DATA_V1_ENDPOINT_ROUTING,
    DATA_V1_ENDPOINT_SNAPSHOTS,
    DataV1ContractBlockedError,
    DataV1MappingError,
    build_48_flow_apply_result_data_v1,
    build_data_v1_headers,
    compute_bytes_sha256,
    fetch_decision_batch_from_data_v1,
    is_valid_data_v1_ack,
    map_apply_response_to_data_v1,
    map_routing_state_to_data_v1,
    map_snapshot_to_data_v1,
    serialize_canonical_json_bytes,
)
from sdn.schemas.apply_models import ApplyResponse
from sdn.schemas.snapshot_models import Snapshot
from sdn.schemas.state_models import (
    RoutingHistoryInterval,
    RoutingStateSnapshot,
    VerificationEvidence,
)


class DeliveryStatus(str, Enum):
    """Explicit delivery state of an outbox event (never claims delivered without ACK)."""

    PENDING_DELIVERY = "PENDING_DELIVERY"
    DELIVERED = "DELIVERED"


@dataclass(frozen=True)
class DataPublisherConfig:
    """Configuration for `DataEventPublisher`."""

    endpoint_base_url: Optional[str] = None
    outbox_dir: Optional[Path] = None
    timeout_s: float = 0.5
    max_retries: int = 3
    backoff_base_s: float = 0.05
    auth_token: Optional[str] = None
    auto_flush: bool = False
    flush_interval_s: float = 0.5
    api_mode: str = "legacy"  # "legacy" | "data_v1" (never default to data_v1 until team approval)
    api_key: Optional[str] = None  # Role SDN X-API-Key for data_v1 mode (never logged)
    snapshot_use_traffic_origin_bins: bool = True
    max_decision_lookup_404_retries: int = 3

    def __post_init__(self) -> None:
        if self.api_mode not in ("legacy", "data_v1"):
            raise ValueError(f"Unsupported api_mode {self.api_mode!r}; expected 'legacy' or 'data_v1'.")


@dataclass(frozen=True)
class OutboxEventRecord:
    """Single durable outbox record wrapping a v1.4 SDN payload."""

    dedup_key: str
    event_type: str
    endpoint_path: str
    run_id: str
    enqueued_at_utc: str
    payload: dict[str, Any]
    status: DeliveryStatus = DeliveryStatus.PENDING_DELIVERY
    attempts: int = 0
    last_error: Optional[str] = None
    acked_at_utc: Optional[str] = None
    ack_http_status: Optional[int] = None
    depends_on_dedup_key: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "dedup_key": self.dedup_key,
            "event_type": self.event_type,
            "endpoint_path": self.endpoint_path,
            "run_id": self.run_id,
            "enqueued_at_utc": self.enqueued_at_utc,
            "payload": self.payload,
            "status": self.status.value,
            "attempts": self.attempts,
            "last_error": self.last_error,
            "acked_at_utc": self.acked_at_utc,
            "ack_http_status": self.ack_http_status,
            "depends_on_dedup_key": self.depends_on_dedup_key,
        }


@dataclass(frozen=True)
class PublishResult:
    """Immediate result of enqueueing (and optionally attempting delivery of) an event."""

    dedup_key: str
    event_type: str
    status: DeliveryStatus
    duplicate: bool
    attempts: int
    last_error: Optional[str] = None


SenderFn = Callable[[str, dict[str, Any], dict[str, str], float], tuple[int, str]]


def _utc_now_rfc3339() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _cooperative_sleep(seconds: float) -> None:
    if seconds <= 0.0:
        return
    try:
        import eventlet  # type: ignore[import-not-found]
        import eventlet.patcher  # type: ignore[import-not-found]

        if eventlet.patcher.is_monkey_patched("time"):
            eventlet.sleep(seconds)
            return
    except Exception:
        pass
    time.sleep(seconds)


def _default_http_sender(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout_s: float,
) -> tuple[int, str]:
    body = serialize_canonical_json_bytes(payload)
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = int(getattr(resp, "status", 200))
            text = resp.read().decode("utf-8", errors="replace")
            return status, text
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")
        return int(exc.code), err_body


def _spawn_background_worker(fn: Callable[[], None]) -> Any:
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


class DataEventPublisher:
    """
    Durable outbox + HTTP publisher for SDN -> Data events (`Phase 12`, Task 5; hardened `Phase 12.4`).

    If `endpoint_base_url` is `None` or unreachable, events are durably persisted to
    `outbox_events.jsonl` with `status == DeliveryStatus.PENDING_DELIVERY` and can be
    replayed automatically by the supervised background outbox worker (`start_worker()` /
    `auto_flush=True`) or manually via `flush_pending()` once the Data receiver comes online.
    """

    def __init__(
        self,
        config: Optional[DataPublisherConfig] = None,
        *,
        sender_fn: Optional[SenderFn] = None,
        sleep_fn: Optional[Callable[[float], None]] = None,
        spawn_fn: Optional[Callable[[Callable[[], None]], Any]] = None,
        decision_lookup_fn: Optional[Callable[[str, str], list[dict[str, Any]]]] = None,
    ) -> None:
        self._config = config or DataPublisherConfig()
        self._sender_fn: SenderFn = sender_fn or _default_http_sender
        self._sleep_fn: Callable[[float], None] = sleep_fn or _cooperative_sleep
        self._spawn_fn: Callable[[Callable[[], None]], Any] = spawn_fn or _spawn_background_worker
        self._decision_lookup_fn = decision_lookup_fn
        self._lock = threading.RLock()
        self._records: dict[str, OutboxEventRecord] = {}
        self._order: list[str] = []
        self._in_flight_keys: set[str] = set()
        self._pending_audits: dict[str, dict[str, Any]] = {}
        self._durable_corruption_detected: bool = False
        self._durable_corruption_errors: list[str] = []
        self._outbox_file: Optional[Path] = None
        self._acked_file: Optional[Path] = None
        self._pending_audits_file: Optional[Path] = None
        self._pending_audits_resolved_file: Optional[Path] = None
        self._stop_event = threading.Event()
        self._worker_running: bool = False
        self._worker_expected: bool = bool(self._config.auto_flush)
        self._worker_handle: Any = None
        self._flush_cycles: int = 0
        self._last_successful_delivery_utc: Optional[str] = None
        self._last_error: Optional[str] = None

        if self._config.outbox_dir is not None:
            out_dir = Path(self._config.outbox_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            self._outbox_file = out_dir / "outbox_events.jsonl"
            self._acked_file = out_dir / "outbox_acked.jsonl"
            self._pending_audits_file = out_dir / "pending_apply_audits.jsonl"
            self._pending_audits_resolved_file = out_dir / "pending_apply_audits_resolved.jsonl"
            self._load_durable_outbox()

        if self._config.auto_flush:
            self.start_worker()

    @property
    def worker_running(self) -> bool:
        with self._lock:
            return self._worker_running

    def start_worker(
        self,
        *,
        spawn_fn: Optional[Callable[[Callable[[], None]], Any]] = None,
    ) -> bool:
        """
        Start the supervised, non-blocking background outbox dispatcher.
        Idempotent: returns `False` if the worker is already running.
        """
        with self._lock:
            if self._worker_running:
                return False
            self._stop_event.clear()
            self._worker_expected = True
            self._worker_running = True
            spawner = spawn_fn or self._spawn_fn
            self._worker_handle = spawner(self._run_worker_loop)
            return True

    def stop_worker(self, timeout_s: float = 2.0) -> None:
        """Gracefully stop the background outbox dispatcher and wait up to `timeout_s`."""
        with self._lock:
            self._stop_event.set()
            handle = self._worker_handle
        if handle is not None:
            join_fn = getattr(handle, "join", None)
            if callable(join_fn):
                try:
                    join_fn(timeout=timeout_s)
                except TypeError:
                    try:
                        join_fn()
                    except Exception:
                        pass
                except Exception:
                    pass
            wait_fn = getattr(handle, "wait", None)
            if callable(wait_fn) and not callable(join_fn):
                try:
                    wait_fn()
                except Exception:
                    pass
        with self._lock:
            self._worker_running = False
            self._worker_handle = None

    def _run_worker_loop(self) -> None:
        interval_s = max(0.02, float(self._config.flush_interval_s))
        try:
            while not self._stop_event.is_set():
                try:
                    self.flush_pending()
                except Exception as exc:
                    with self._lock:
                        self._last_error = f"WORKER_LOOP_ERROR: {type(exc).__name__}: {exc}"
                with self._lock:
                    self._flush_cycles += 1
                # Sleep in small cooperative slices so graceful shutdown is responsive
                slept = 0.0
                slice_s = min(0.05, interval_s)
                while slept < interval_s and not self._stop_event.is_set():
                    step = min(slice_s, interval_s - slept)
                    self._sleep_fn(step)
                    slept += step
        finally:
            with self._lock:
                self._worker_running = False

    @property
    def config(self) -> DataPublisherConfig:
        return self._config

    def _record_durable_corruption(self, filename: str, line_no: int, exc: Exception) -> None:
        msg = f"DURABLE_OUTBOX_CORRUPT:{filename}:L{line_no}:{type(exc).__name__}: {exc}"
        self._durable_corruption_detected = True
        self._durable_corruption_errors.append(msg)
        self._last_error = msg

    def _load_durable_outbox(self) -> None:
        acked_meta: dict[str, dict[str, Any]] = {}
        if self._acked_file is not None and self._acked_file.exists():
            for line_no, line in enumerate(self._acked_file.read_text(encoding="utf-8").splitlines(), start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    acked_meta[str(rec["dedup_key"])] = rec
                except Exception as exc:
                    self._record_durable_corruption(self._acked_file.name, line_no, exc)
                    continue

        if self._outbox_file is not None and self._outbox_file.exists():
            for line_no, line in enumerate(self._outbox_file.read_text(encoding="utf-8").splitlines(), start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                    dk = str(raw["dedup_key"])
                    if dk in self._records:
                        continue
                    ack = acked_meta.get(dk)
                    status = DeliveryStatus.DELIVERED if ack else DeliveryStatus.PENDING_DELIVERY
                    evt = OutboxEventRecord(
                        dedup_key=dk,
                        event_type=str(raw["event_type"]),
                        endpoint_path=str(raw["endpoint_path"]),
                        run_id=str(raw["run_id"]),
                        enqueued_at_utc=str(raw["enqueued_at_utc"]),
                        payload=dict(raw["payload"]),
                        status=status,
                        attempts=int(ack.get("attempts", 0)) if ack else 0,
                        last_error=None,
                        acked_at_utc=str(ack["acked_at_utc"]) if ack and ack.get("acked_at_utc") else None,
                        ack_http_status=int(ack["ack_http_status"]) if ack and ack.get("ack_http_status") is not None else None,
                        depends_on_dedup_key=(
                            str(raw["depends_on_dedup_key"])
                            if raw.get("depends_on_dedup_key") is not None
                            else None
                        ),
                    )
                    self._records[dk] = evt
                    self._order.append(dk)
                    if evt.acked_at_utc is not None:
                        if (
                            self._last_successful_delivery_utc is None
                            or evt.acked_at_utc > self._last_successful_delivery_utc
                        ):
                            self._last_successful_delivery_utc = evt.acked_at_utc
                except Exception as exc:
                    self._record_durable_corruption(self._outbox_file.name, line_no, exc)
                    continue

        # Reload durable pending 48-flow ApplyResult audits (Task B restart recovery)
        resolved_audits: set[str] = set()
        if self._pending_audits_resolved_file is not None and self._pending_audits_resolved_file.exists():
            for line_no, line in enumerate(
                self._pending_audits_resolved_file.read_text(encoding="utf-8").splitlines(), start=1
            ):
                line = line.strip()
                if not line:
                    continue
                try:
                    resolved_audits.add(str(json.loads(line)["dedup_key"]))
                except Exception as exc:
                    self._record_durable_corruption(self._pending_audits_resolved_file.name, line_no, exc)
                    continue
        if self._pending_audits_file is not None and self._pending_audits_file.exists():
            for line_no, line in enumerate(
                self._pending_audits_file.read_text(encoding="utf-8").splitlines(), start=1
            ):
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    dk = str(rec["dedup_key"])
                    _ = rec["run_id"]
                    _ = rec["decision_batch_id"]
                    _ = rec["apply_response"]
                    _ = rec["routing_state"]
                    if dk in resolved_audits or dk in self._records:
                        continue
                    self._pending_audits[dk] = rec
                except Exception as exc:
                    self._record_durable_corruption(self._pending_audits_file.name, line_no, exc)
                    continue

    def _append_jsonl(self, path: Optional[Path], obj: Mapping[str, Any]) -> None:
        if path is None:
            return
        line = json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n"
        with open(path, "a", encoding="utf-8") as fp:
            fp.write(line)
            fp.flush()

    def _enqueue(
        self,
        *,
        dedup_key: str,
        event_type: str,
        endpoint_path: str,
        run_id: str,
        payload: dict[str, Any],
        deliver_now: bool = False,
        depends_on_dedup_key: Optional[str] = None,
    ) -> PublishResult:
        with self._lock:
            existing = self._records.get(dedup_key)
            if existing is not None:
                return PublishResult(
                    dedup_key=dedup_key,
                    event_type=event_type,
                    status=existing.status,
                    duplicate=True,
                    attempts=existing.attempts,
                    last_error=existing.last_error,
                )
            rec = OutboxEventRecord(
                dedup_key=dedup_key,
                event_type=event_type,
                endpoint_path=endpoint_path,
                run_id=run_id,
                enqueued_at_utc=_utc_now_rfc3339(),
                payload=payload,
                status=DeliveryStatus.PENDING_DELIVERY,
                depends_on_dedup_key=depends_on_dedup_key,
            )
            self._records[dedup_key] = rec
            self._order.append(dedup_key)
            self._append_jsonl(
                self._outbox_file,
                {
                    "dedup_key": rec.dedup_key,
                    "event_type": rec.event_type,
                    "endpoint_path": rec.endpoint_path,
                    "run_id": rec.run_id,
                    "enqueued_at_utc": rec.enqueued_at_utc,
                    "payload": rec.payload,
                    "depends_on_dedup_key": rec.depends_on_dedup_key,
                },
            )

        if deliver_now or self._config.auto_flush:
            updated = self._deliver_single(dedup_key)
            return PublishResult(
                dedup_key=dedup_key,
                event_type=event_type,
                status=updated.status,
                duplicate=False,
                attempts=updated.attempts,
                last_error=updated.last_error,
            )
        return PublishResult(
            dedup_key=dedup_key,
            event_type=event_type,
            status=DeliveryStatus.PENDING_DELIVERY,
            duplicate=False,
            attempts=0,
            last_error=None,
        )

    # ------------------------------------------------------------------
    # Typed v1.4 Publish APIs
    # ------------------------------------------------------------------

    def publish_snapshot(
        self, snapshot: Snapshot, *, deliver_now: bool = False
    ) -> PublishResult:
        snapshot.validate_contract()
        if self._config.api_mode == "data_v1":
            try:
                mapped, _raw_bytes, _digest = map_snapshot_to_data_v1(
                    snapshot,
                    use_traffic_origin_bins=self._config.snapshot_use_traffic_origin_bins,
                )
                return self._enqueue(
                    dedup_key=f"snapshot:{snapshot.snapshot_id}",
                    event_type="Snapshot",
                    endpoint_path=DATA_V1_ENDPOINT_SNAPSHOTS,
                    run_id=snapshot.run_id,
                    payload=mapped,
                    deliver_now=deliver_now,
                )
            except DataV1ContractBlockedError as exc:
                return self._enqueue(
                    dedup_key=f"snapshot:{snapshot.snapshot_id}",
                    event_type="Snapshot",
                    endpoint_path=f"CONTRACT_BLOCKED:{exc}",
                    run_id=snapshot.run_id,
                    payload=snapshot.to_dict(),
                    deliver_now=False,
                )
        return self._enqueue(
            dedup_key=f"snapshot:{snapshot.snapshot_id}",
            event_type="Snapshot",
            endpoint_path="/v1/data/snapshots",
            run_id=snapshot.run_id,
            payload=snapshot.to_dict(),
            deliver_now=deliver_now,
        )

    def publish_routing_state(
        self, routing_state: RoutingStateSnapshot, *, deliver_now: bool = False
    ) -> PublishResult:
        if hasattr(routing_state, "validate_contract"):
            routing_state.validate_contract()  # type: ignore[attr-defined]
        if self._config.api_mode == "data_v1":
            mapped = map_routing_state_to_data_v1(routing_state)
            return self._enqueue(
                dedup_key=f"routing_state:{routing_state.run_id}:v{routing_state.state_version}",
                event_type="RoutingStateSnapshot",
                endpoint_path=DATA_V1_ENDPOINT_ROUTING,
                run_id=routing_state.run_id,
                payload=mapped,
                deliver_now=deliver_now,
            )
        return self._enqueue(
            dedup_key=f"routing_state:{routing_state.run_id}:v{routing_state.state_version}",
            event_type="RoutingStateSnapshot",
            endpoint_path="/v1/data/routing-states",
            run_id=routing_state.run_id,
            payload=routing_state.to_dict(),
            deliver_now=deliver_now,
        )

    def publish_routing_history(
        self,
        run_id: str,
        state_version: int,
        intervals: Sequence[RoutingHistoryInterval],
        *,
        deliver_now: bool = False,
    ) -> PublishResult:
        serialized = [iv.to_dict() for iv in intervals]
        endpoint = (
            "CONTRACT_BLOCKED:NO_DATA_V1_ROUTING_HISTORY_ENDPOINT"
            if self._config.api_mode == "data_v1"
            else "/v1/data/routing-history"
        )
        return self._enqueue(
            dedup_key=f"routing_history:{run_id}:v{state_version}:n{len(serialized)}",
            event_type="RoutingHistory",
            endpoint_path=endpoint,
            run_id=run_id,
            payload={
                "run_id": run_id,
                "state_version": int(state_version),
                "intervals": serialized,
            },
            deliver_now=False if self._config.api_mode == "data_v1" else deliver_now,
        )

    def publish_apply_response(
        self,
        response: ApplyResponse,
        *,
        deliver_now: bool = False,
        full_routing_state: Optional[RoutingStateSnapshot] = None,
        expand_unchanged_as_kept: bool = False,
        decision_rows: Optional[Sequence[Mapping[str, Any]]] = None,
        require_routing_state_ack: Optional[bool] = None,
    ) -> PublishResult:
        response.validate_contract()
        if self._config.api_mode == "data_v1":
            enforce_dep = True if require_routing_state_ack is None else bool(require_routing_state_ack)
            dep_key = (
                f"routing_state:{response.run_id}:v{response.state_version}"
                if enforce_dep
                else None
            )
            try:
                mapped = map_apply_response_to_data_v1(
                    response,
                    full_routing_state=full_routing_state,
                    expand_unchanged_as_kept=expand_unchanged_as_kept,
                    decision_rows=decision_rows,
                )
                return self._enqueue(
                    dedup_key=f"apply_response:{response.run_id}:{response.decision_batch_id}",
                    event_type="ApplyResponse",
                    endpoint_path=DATA_V1_ENDPOINT_APPLY_RESULTS,
                    run_id=response.run_id,
                    payload=mapped,
                    deliver_now=deliver_now,
                    depends_on_dedup_key=dep_key,
                )
            except DataV1ContractBlockedError as exc:
                return self._enqueue(
                    dedup_key=f"apply_response:{response.run_id}:{response.decision_batch_id}",
                    event_type="ApplyResponse",
                    endpoint_path=f"CONTRACT_BLOCKED:{exc}",
                    run_id=response.run_id,
                    payload=response.to_dict(),
                    deliver_now=False,
                    depends_on_dedup_key=dep_key,
                )
        return self._enqueue(
            dedup_key=f"apply_response:{response.run_id}:{response.decision_batch_id}",
            event_type="ApplyResponse",
            endpoint_path="/v1/data/apply-responses",
            run_id=response.run_id,
            payload=response.to_dict(),
            deliver_now=deliver_now,
        )

    def _lookup_decision_rows(self, run_id: str, decision_batch_id: str) -> list[dict[str, Any]]:
        if self._decision_lookup_fn is not None:
            return self._decision_lookup_fn(run_id, decision_batch_id)
        base_url = self._config.endpoint_base_url
        if not base_url:
            raise DataV1ContractBlockedError(
                "CONTRACT_BLOCKED: DATA_ENDPOINT_NOT_CONFIGURED for DecisionBatch lookup."
            )
        return fetch_decision_batch_from_data_v1(
            base_url,
            run_id,
            decision_batch_id,
            api_key=self._config.api_key or self._config.auth_token,
            timeout_s=float(self._config.timeout_s),
        )

    def _resolve_single_pending_audit(self, dedup_key: str) -> Optional[PublishResult]:
        with self._lock:
            pending = self._pending_audits.get(dedup_key)
            if pending is None:
                return None
            if dedup_key in self._records:
                self._pending_audits.pop(dedup_key, None)
                return None
            if pending.get("lookup_exhausted"):
                # Bounded 404 retry policy: keep audit durably pending without hammering Data API.
                return None
            run_id = str(pending["run_id"])
            batch_id = str(pending["decision_batch_id"])
            resp_dict = dict(pending["apply_response"])
            rs_dict = dict(pending["routing_state"])

        try:
            decision_rows = self._lookup_decision_rows(run_id, batch_id)
            resp = ApplyResponse.from_dict(resp_dict, strict=False)
            rs = RoutingStateSnapshot.from_dict(rs_dict, strict=False)
            mapped = build_48_flow_apply_result_data_v1(
                resp,
                decision_rows=decision_rows,
                routing_state=rs,
            )
        except (DataV1ContractBlockedError, DataV1MappingError, Exception) as exc:
            raw_err = str(exc)
            is_404 = ("HTTP 404" in raw_err) or ("404" in raw_err) or ("Unknown decision_batch_id" in raw_err)
            with self._lock:
                if dedup_key in self._pending_audits:
                    p_rec = self._pending_audits[dedup_key]
                    if is_404:
                        n404 = int(p_rec.get("lookup_404_attempts", 0)) + 1
                        p_rec["lookup_404_attempts"] = n404
                        max_404 = max(1, int(self._config.max_decision_lookup_404_retries))
                        if n404 >= max_404:
                            p_rec["lookup_exhausted"] = True
                            err_msg = f"DECISION_BATCH_404_RETRIES_EXHAUSTED({n404}/{max_404}): {raw_err}"
                        else:
                            err_msg = f"PENDING_DECISION_BATCH_404({n404}/{max_404}): {raw_err}"
                    else:
                        err_msg = f"PENDING_DECISION_BATCH_AUDIT:{type(exc).__name__}: {raw_err}"
                    p_rec["last_error"] = err_msg
                else:
                    err_msg = f"PENDING_DECISION_BATCH_AUDIT:{type(exc).__name__}: {raw_err}"
                self._last_error = err_msg
            return None

        dep_key = f"routing_state:{run_id}:v{int(resp.state_version)}"
        pub_res = self._enqueue(
            dedup_key=dedup_key,
            event_type="ApplyResponse",
            endpoint_path=DATA_V1_ENDPOINT_APPLY_RESULTS,
            run_id=run_id,
            payload=mapped,
            deliver_now=False,
            depends_on_dedup_key=dep_key,
        )
        with self._lock:
            self._pending_audits.pop(dedup_key, None)
            self._append_jsonl(
                self._pending_audits_resolved_file,
                {"dedup_key": dedup_key, "resolved_at_utc": _utc_now_rfc3339()},
            )
        return pub_res

    def _resolve_all_pending_audits(self) -> int:
        with self._lock:
            keys = list(self._pending_audits.keys())
        resolved = 0
        for dk in keys:
            if self._resolve_single_pending_audit(dk) is not None:
                resolved += 1
        return resolved

    def publish_apply_response_v1_runtime(
        self,
        response: ApplyResponse,
        *,
        post_apply_routing_state: RoutingStateSnapshot,
        deliver_now: bool = False,
    ) -> PublishResult:
        """
        Phase 8 Task B: Complete runtime 48-flow ApplyResult publication pipeline.

        - In `legacy` mode, delegates to `publish_apply_response(...)`.
        - In `data_v1` mode:
          1. Validates `post_apply_routing_state.state_version == response.state_version`.
          2. Retrieves the registered 48-flow `DecisionBatch` from Data API (`GET /v1/runs/{run_id}/decisions/{batch_id}`).
          3. Builds the exact 48-flow `ApplyResult` via `build_48_flow_apply_result_data_v1(...)`
             and enqueues it with `depends_on_dedup_key = routing_state:{run_id}:v{state_version}`.
          4. If Data is temporarily unreachable or `DecisionBatch` is not yet found, durably parks
             `(response, post_apply_routing_state)` in `pending_apply_audits.jsonl` (surviving
             controller restarts) so `flush_pending()` completes audit publication automatically
             once Data recovers — NEVER fabricating 48 rows and NEVER failing the finalized OVS apply.
        """
        response.validate_contract()
        if self._config.api_mode != "data_v1":
            return self.publish_apply_response(response, deliver_now=deliver_now)

        if int(post_apply_routing_state.state_version) != int(response.state_version):
            raise DataV1MappingError(
                f"Post-apply RoutingStateSnapshot state_version={post_apply_routing_state.state_version} "
                f"does not match ApplyResponse state_version={response.state_version} for batch "
                f"'{response.decision_batch_id}'."
            )

        dedup_key = f"apply_response:{response.run_id}:{response.decision_batch_id}"
        with self._lock:
            existing = self._records.get(dedup_key)
            if existing is not None:
                return PublishResult(
                    dedup_key=dedup_key,
                    event_type="ApplyResponse",
                    status=existing.status,
                    duplicate=True,
                    attempts=existing.attempts,
                    last_error=existing.last_error,
                )
            if dedup_key not in self._pending_audits:
                audit_rec = {
                    "dedup_key": dedup_key,
                    "run_id": response.run_id,
                    "decision_batch_id": response.decision_batch_id,
                    "state_version": int(response.state_version),
                    "enqueued_at_utc": _utc_now_rfc3339(),
                    "apply_response": response.to_dict(),
                    "routing_state": post_apply_routing_state.to_dict(),
                    "last_error": None,
                }
                self._pending_audits[dedup_key] = audit_rec
                self._append_jsonl(self._pending_audits_file, audit_rec)

        resolved = self._resolve_single_pending_audit(dedup_key)
        if resolved is not None:
            if deliver_now or self._config.auto_flush:
                updated = self._deliver_single(dedup_key)
                return PublishResult(
                    dedup_key=dedup_key,
                    event_type="ApplyResponse",
                    status=updated.status,
                    duplicate=False,
                    attempts=updated.attempts,
                    last_error=updated.last_error,
                )
            return resolved

        with self._lock:
            pending_err = self._pending_audits.get(dedup_key, {}).get("last_error")
        return PublishResult(
            dedup_key=dedup_key,
            event_type="ApplyResponse",
            status=DeliveryStatus.PENDING_DELIVERY,
            duplicate=False,
            attempts=0,
            last_error=pending_err,
        )

    def reconcile_pending_audit(
        self,
        run_id: str,
        decision_batch_id: str,
        *,
        current_state_version: Optional[int] = None,
        ovs_verified: bool = True,
        ovs_reconcile_required: bool = False,
    ) -> dict[str, Any]:
        """
        Controlled, targeted administrative reconciliation for a specific `(run_id, decision_batch_id)`
        after `DecisionBatch` 404 lookup retries were exhausted.

        Safety guarantees:
        - NEVER deletes `pending_apply_audits.jsonl` records.
        - NEVER resets unrelated pending batches.
        - NEVER fabricates a Data ACK.
        - NEVER opens `optimization_tick_eligible` unless both `RoutingState` and `ApplyResult`
          receive valid Data persistence ACKs.
        """
        dedup_key = f"apply_response:{run_id}:{decision_batch_id}"
        with self._lock:
            pending = self._pending_audits.get(dedup_key)
            existing = self._records.get(dedup_key)
            if pending is None and existing is None:
                return {
                    "found": False,
                    "run_id": run_id,
                    "decision_batch_id": decision_batch_id,
                    "reconciled": False,
                    "status": "NOT_FOUND",
                    "sync_gate": self.sync_gate_status(
                        run_id,
                        current_state_version=current_state_version,
                        ovs_verified=ovs_verified,
                        ovs_reconcile_required=ovs_reconcile_required,
                    ),
                }
            if pending is not None:
                # Reset retry counter ONLY for this specific (run_id, decision_batch_id)
                pending["lookup_exhausted"] = False
                pending["lookup_404_attempts"] = 0

        if pending is not None:
            self._resolve_single_pending_audit(dedup_key)
        self.flush_pending()

        with self._lock:
            rec_after = self._records.get(dedup_key)
            still_pending_audit = self._pending_audits.get(dedup_key)
            eff_sv = current_state_version
            if eff_sv is None and rec_after is not None:
                raw_sv = rec_after.payload.get("response_state_version")
                if isinstance(raw_sv, int) and not isinstance(raw_sv, bool):
                    eff_sv = raw_sv
            elif eff_sv is None and still_pending_audit is not None:
                raw_sv = still_pending_audit.get("state_version")
                if isinstance(raw_sv, int) and not isinstance(raw_sv, bool):
                    eff_sv = raw_sv

            delivered = rec_after is not None and rec_after.status == DeliveryStatus.DELIVERED
            last_err = (
                rec_after.last_error
                if rec_after is not None
                else (still_pending_audit.get("last_error") if still_pending_audit else None)
            )
            status_str = (
                rec_after.status.value
                if rec_after is not None
                else DeliveryStatus.PENDING_DELIVERY.value
            )

        gate = self.sync_gate_status(
            run_id,
            current_state_version=eff_sv,
            ovs_verified=ovs_verified,
            ovs_reconcile_required=ovs_reconcile_required,
        )
        return {
            "found": True,
            "run_id": run_id,
            "decision_batch_id": decision_batch_id,
            "reconciled": bool(delivered),
            "status": status_str,
            "last_error": last_err,
            "sync_gate": gate,
        }

    def publish_verification_evidence(
        self,
        run_id: str,
        evidence: VerificationEvidence,
        *,
        deliver_now: bool = False,
    ) -> PublishResult:
        ev_dict = evidence.to_dict()
        endpoint = (
            "CONTRACT_BLOCKED:NO_DATA_V1_VERIFICATION_EVIDENCE_ENDPOINT"
            if self._config.api_mode == "data_v1"
            else "/v1/data/verification-evidence"
        )
        return self._enqueue(
            dedup_key=f"verification_evidence:{run_id}:{evidence.flow_group_id}:{evidence.verified_at_utc}",
            event_type="VerificationEvidence",
            endpoint_path=endpoint,
            run_id=run_id,
            payload={"run_id": run_id, "evidence": ev_dict},
            deliver_now=False if self._config.api_mode == "data_v1" else deliver_now,
        )

    # ------------------------------------------------------------------
    # Delivery & Outbox Inspection
    # ------------------------------------------------------------------

    def _deliver_single(self, dedup_key: str) -> OutboxEventRecord:
        with self._lock:
            rec = self._records[dedup_key]
            if rec.status == DeliveryStatus.DELIVERED:
                return rec
            if rec.endpoint_path.startswith("CONTRACT_BLOCKED:"):
                updated = OutboxEventRecord(
                    dedup_key=rec.dedup_key,
                    event_type=rec.event_type,
                    endpoint_path=rec.endpoint_path,
                    run_id=rec.run_id,
                    enqueued_at_utc=rec.enqueued_at_utc,
                    payload=rec.payload,
                    status=DeliveryStatus.PENDING_DELIVERY,
                    attempts=rec.attempts,
                    last_error=rec.endpoint_path,
                    depends_on_dedup_key=rec.depends_on_dedup_key,
                )
                self._records[dedup_key] = updated
                return updated
            if rec.depends_on_dedup_key is not None:
                dep = self._records.get(rec.depends_on_dedup_key)
                if dep is None or dep.status != DeliveryStatus.DELIVERED:
                    dep_detail = (
                        f"WAITING_FOR_DEPENDENCY:{rec.depends_on_dedup_key} ({dep.last_error})"
                        if dep is not None and dep.last_error
                        else f"WAITING_FOR_DEPENDENCY:{rec.depends_on_dedup_key}"
                    )
                    updated = OutboxEventRecord(
                        dedup_key=rec.dedup_key,
                        event_type=rec.event_type,
                        endpoint_path=rec.endpoint_path,
                        run_id=rec.run_id,
                        enqueued_at_utc=rec.enqueued_at_utc,
                        payload=rec.payload,
                        status=DeliveryStatus.PENDING_DELIVERY,
                        attempts=rec.attempts,
                        last_error=dep_detail,
                        depends_on_dedup_key=rec.depends_on_dedup_key,
                    )
                    self._records[dedup_key] = updated
                    return updated
            if dedup_key in self._in_flight_keys:
                # Another worker/caller is already delivering this logical event; never double-send.
                return rec
            base_url = self._config.endpoint_base_url
            if not base_url:
                updated = OutboxEventRecord(
                    dedup_key=rec.dedup_key,
                    event_type=rec.event_type,
                    endpoint_path=rec.endpoint_path,
                    run_id=rec.run_id,
                    enqueued_at_utc=rec.enqueued_at_utc,
                    payload=rec.payload,
                    status=DeliveryStatus.PENDING_DELIVERY,
                    attempts=rec.attempts,
                    last_error="DATA_ENDPOINT_NOT_CONFIGURED",
                )
                self._records[dedup_key] = updated
                self._last_error = "DATA_ENDPOINT_NOT_CONFIGURED"
                return updated
            self._in_flight_keys.add(dedup_key)

        url = base_url.rstrip("/") + rec.endpoint_path
        is_v1 = self._config.api_mode == "data_v1"
        if is_v1:
            content_sha = (
                compute_bytes_sha256(serialize_canonical_json_bytes(rec.payload))
                if rec.endpoint_path == DATA_V1_ENDPOINT_SNAPSHOTS
                else None
            )
            headers = build_data_v1_headers(
                api_key=self._config.api_key or self._config.auth_token,
                dedup_key=rec.dedup_key,
                event_type=rec.event_type,
                content_sha256=content_sha,
            )
        else:
            headers = {
                "Content-Type": "application/json",
                "X-Idempotency-Key": rec.dedup_key,
                "X-SDN-Event-Type": rec.event_type,
            }
            if self._config.auth_token:
                headers["X-SDN-Token"] = self._config.auth_token

        attempts = rec.attempts
        last_err: Optional[str] = None
        max_tries = max(1, int(self._config.max_retries))
        try:
            for try_idx in range(max_tries):
                attempts += 1
                try:
                    status_code, body_text = self._sender_fn(
                        url, rec.payload, headers, self._config.timeout_s
                    )
                    ack_ok = (
                        is_valid_data_v1_ack(status_code, body_text)
                        if is_v1
                        else (200 <= status_code < 300)
                    )
                    if ack_ok:
                        acked_at = _utc_now_rfc3339()
                        delivered = OutboxEventRecord(
                            dedup_key=rec.dedup_key,
                            event_type=rec.event_type,
                            endpoint_path=rec.endpoint_path,
                            run_id=rec.run_id,
                            enqueued_at_utc=rec.enqueued_at_utc,
                            payload=rec.payload,
                            status=DeliveryStatus.DELIVERED,
                            attempts=attempts,
                            last_error=None,
                            acked_at_utc=acked_at,
                            ack_http_status=status_code,
                            depends_on_dedup_key=rec.depends_on_dedup_key,
                        )
                        with self._lock:
                            self._records[dedup_key] = delivered
                            self._last_successful_delivery_utc = acked_at
                            self._last_error = None
                            self._append_jsonl(
                                self._acked_file,
                                {
                                    "dedup_key": dedup_key,
                                    "acked_at_utc": acked_at,
                                    "ack_http_status": status_code,
                                    "attempts": attempts,
                                },
                            )
                        return delivered
                    if 200 <= status_code < 300:
                        last_err = f"INVALID_ACK_BODY_HTTP_{status_code}: {body_text[:160]}"
                    else:
                        last_err = f"HTTP_{status_code}: {body_text[:160]}"
                    # Do not retry deterministic client/auth/contract errors (401/403/409/422)
                    if status_code in (401, 403, 409, 422):
                        break
                except Exception as exc:
                    last_err = f"{type(exc).__name__}: {exc}"

                if try_idx + 1 < max_tries and not self._stop_event.is_set():
                    self._sleep_fn(self._config.backoff_base_s * (2**try_idx))

            still_pending = OutboxEventRecord(
                dedup_key=rec.dedup_key,
                event_type=rec.event_type,
                endpoint_path=rec.endpoint_path,
                run_id=rec.run_id,
                enqueued_at_utc=rec.enqueued_at_utc,
                payload=rec.payload,
                status=DeliveryStatus.PENDING_DELIVERY,
                attempts=attempts,
                last_error=last_err,
                depends_on_dedup_key=rec.depends_on_dedup_key,
            )
            with self._lock:
                self._records[dedup_key] = still_pending
                self._last_error = last_err
            return still_pending
        finally:
            with self._lock:
                self._in_flight_keys.discard(dedup_key)

    def flush_pending(self) -> dict[str, int]:
        """
        Resolve any pending durable 48-flow audits and attempt bounded delivery for all
        `PENDING_DELIVERY` outbox records, respecting causal `depends_on_dedup_key` ordering
        (`RoutingState` ACK strictly before `ApplyResult`).
        """
        if self._config.api_mode == "data_v1":
            self._resolve_all_pending_audits()

        total_delivered = 0
        with self._lock:
            max_passes = max(1, len(self._order) + 1)

        for _ in range(max_passes):
            with self._lock:
                pending_keys = [
                    k for k in self._order if self._records[k].status == DeliveryStatus.PENDING_DELIVERY
                ]
            if not pending_keys:
                break
            delivered_this_pass = 0
            for key in pending_keys:
                res = self._deliver_single(key)
                if res.status == DeliveryStatus.DELIVERED:
                    delivered_this_pass += 1
                    total_delivered += 1
            if delivered_this_pass == 0:
                break

        with self._lock:
            still_pending = sum(
                1 for k in self._order if self._records[k].status == DeliveryStatus.PENDING_DELIVERY
            ) + len(self._pending_audits)
        return {"delivered": total_delivered, "pending": still_pending}

    def get_event(self, dedup_key: str) -> Optional[OutboxEventRecord]:
        with self._lock:
            return self._records.get(dedup_key)

    def get_pending_events(self) -> tuple[OutboxEventRecord, ...]:
        with self._lock:
            return tuple(
                self._records[k]
                for k in self._order
                if self._records[k].status == DeliveryStatus.PENDING_DELIVERY
            )

    def get_delivered_events(self) -> tuple[OutboxEventRecord, ...]:
        with self._lock:
            return tuple(
                self._records[k]
                for k in self._order
                if self._records[k].status == DeliveryStatus.DELIVERED
            )

    def sync_gate_status(
        self,
        run_id: str,
        *,
        current_state_version: Optional[int],
        ovs_verified: bool = True,
        ovs_reconcile_required: bool = False,
    ) -> dict[str, Any]:
        """
        Phase 8 Task D: Durable Data synchronization & optimization tick eligibility gate.

        Computed strictly from persisted outbox records (`outbox_events.jsonl` + `outbox_acked.jsonl`)
        and persisted pending 48-flow audits (`pending_apply_audits.jsonl`), so controller restarts
        never produce false-positive eligibility.

        Strictly separates:
        - `ovs_verified` / `ovs_reconcile_required` (OVS data-plane truth)
        - `data_acknowledged_state_version` / `data_sync_pending` / `data_reconcile_required` (Data persistence truth)
        - `optimization_tick_eligible` / `optimization_tick_block_reason` (Runner gating decision)
        """
        with self._lock:
            acked_versions: list[int] = []
            routing_pending = False
            routing_conflict = False
            routing_conflict_error: Optional[str] = None
            apply_outbox_pending = False
            pending_apply_batches: list[str] = []

            for rec in self._records.values():
                if rec.run_id != run_id:
                    continue
                if rec.event_type == "RoutingStateSnapshot":
                    sv_raw = rec.payload.get("state_version")
                    sv_int = int(sv_raw) if isinstance(sv_raw, int) and not isinstance(sv_raw, bool) else None
                    if rec.status == DeliveryStatus.DELIVERED:
                        if sv_int is not None:
                            acked_versions.append(sv_int)
                    else:
                        routing_pending = True
                        err = rec.last_error or ""
                        if any(err.startswith(code) for code in ("HTTP_401", "HTTP_403", "HTTP_409", "HTTP_422")):
                            routing_conflict = True
                            routing_conflict_error = err
                elif rec.event_type == "ApplyResponse" and rec.status != DeliveryStatus.DELIVERED:
                    apply_outbox_pending = True
                    bid = rec.payload.get("decision_batch_id")
                    if isinstance(bid, str):
                        pending_apply_batches.append(bid)

            for audit in self._pending_audits.values():
                if audit.get("run_id") == run_id:
                    apply_outbox_pending = True
                    bid = audit.get("decision_batch_id")
                    if isinstance(bid, str) and bid not in pending_apply_batches:
                        pending_apply_batches.append(bid)

            data_acked_sv: Optional[int] = max(acked_versions) if acked_versions else None
            if current_state_version is not None:
                if data_acked_sv is None or data_acked_sv < int(current_state_version):
                    routing_pending = True

            data_sync_pending = bool(routing_pending or apply_outbox_pending)
            data_reconcile_required = bool(routing_conflict or self._durable_corruption_detected)
            reconcile_required = bool(ovs_reconcile_required) or data_reconcile_required

            block_reason: Optional[str] = None
            if self._durable_corruption_detected:
                block_reason = "DURABLE_OUTBOX_CORRUPTION_RECONCILIATION_REQUIRED"
            elif not ovs_verified:
                block_reason = "OVS_NOT_VERIFIED"
            elif ovs_reconcile_required:
                block_reason = "OVS_RECONCILIATION_REQUIRED"
            elif routing_conflict:
                block_reason = f"DATA_ROUTING_CONFLICT_RECONCILIATION_REQUIRED:{routing_conflict_error}"
            elif not self._config.endpoint_base_url:
                block_reason = "DATA_ENDPOINT_NOT_CONFIGURED"
            elif current_state_version is None:
                block_reason = "SDN_STATE_VERSION_UNAVAILABLE"
            elif routing_pending:
                block_reason = "DATA_ROUTING_SYNC_PENDING"
            elif apply_outbox_pending:
                block_reason = "DATA_APPLY_AUDIT_PENDING"

            return {
                "api_mode": self._config.api_mode,
                "run_id": run_id,
                "ovs_verified": bool(ovs_verified),
                "ovs_reconcile_required": bool(ovs_reconcile_required),
                "current_state_version": current_state_version,
                "data_acknowledged_state_version": data_acked_sv,
                "routing_sync_pending": bool(routing_pending),
                "apply_audit_pending": bool(apply_outbox_pending),
                "pending_apply_audit_batches": sorted(pending_apply_batches),
                "data_sync_pending": data_sync_pending,
                "durable_corruption_detected": bool(self._durable_corruption_detected),
                "routing_conflict_detected": bool(routing_conflict),
                "data_reconcile_required": data_reconcile_required,
                "reconcile_required": reconcile_required,
                "optimization_tick_eligible": block_reason is None,
                "optimization_tick_block_reason": block_reason,
            }

    def status_summary(self) -> dict[str, Any]:
        with self._lock:
            pending = sum(1 for r in self._records.values() if r.status == DeliveryStatus.PENDING_DELIVERY)
            delivered = sum(1 for r in self._records.values() if r.status == DeliveryStatus.DELIVERED)
            return {
                "api_mode": self._config.api_mode,
                "endpoint_base_url": self._config.endpoint_base_url,
                "outbox_dir": str(self._config.outbox_dir) if self._config.outbox_dir else None,
                "auto_flush": bool(self._config.auto_flush),
                "worker_expected": bool(self._worker_expected),
                "flush_interval_s": float(self._config.flush_interval_s),
                "worker_running": bool(self._worker_running),
                "flush_cycles": int(self._flush_cycles),
                "in_flight_count": len(self._in_flight_keys),
                "total_events": len(self._records),
                "delivered_count": delivered,
                "pending_delivery_count": pending,
                "pending_apply_audit_count": len(self._pending_audits),
                "last_successful_delivery_utc": self._last_successful_delivery_utc,
                "last_error": self._last_error,
            }
