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
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = int(getattr(resp, "status", 200))
            text = resp.read().decode("utf-8", errors="replace")
            return status, text
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")
        return int(exc.code), err_body


class DataEventPublisher:
    """
    Durable outbox + HTTP publisher for SDN -> Data events (`Phase 12`, Task 5).

    If `endpoint_base_url` is `None` or unreachable, events are durably persisted to
    `outbox_events.jsonl` with `status == DeliveryStatus.PENDING_DELIVERY` and can be
    replayed automatically via `flush_pending()` once the Data receiver comes online.
    """

    def __init__(
        self,
        config: Optional[DataPublisherConfig] = None,
        *,
        sender_fn: Optional[SenderFn] = None,
        sleep_fn: Optional[Callable[[float], None]] = None,
    ) -> None:
        self._config = config or DataPublisherConfig()
        self._sender_fn: SenderFn = sender_fn or _default_http_sender
        self._sleep_fn: Callable[[float], None] = sleep_fn or _cooperative_sleep
        self._lock = threading.RLock()
        self._records: dict[str, OutboxEventRecord] = {}
        self._order: list[str] = []
        self._outbox_file: Optional[Path] = None
        self._acked_file: Optional[Path] = None

        if self._config.outbox_dir is not None:
            out_dir = Path(self._config.outbox_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            self._outbox_file = out_dir / "outbox_events.jsonl"
            self._acked_file = out_dir / "outbox_acked.jsonl"
            self._load_durable_outbox()

    @property
    def config(self) -> DataPublisherConfig:
        return self._config

    def _load_durable_outbox(self) -> None:
        acked_meta: dict[str, dict[str, Any]] = {}
        if self._acked_file is not None and self._acked_file.exists():
            for line in self._acked_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    acked_meta[str(rec["dedup_key"])] = rec
                except Exception:
                    continue

        if self._outbox_file is not None and self._outbox_file.exists():
            for line in self._outbox_file.read_text(encoding="utf-8").splitlines():
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
                    )
                    self._records[dk] = evt
                    self._order.append(dk)
                except Exception:
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
        return self._enqueue(
            dedup_key=f"routing_history:{run_id}:v{state_version}:n{len(serialized)}",
            event_type="RoutingHistory",
            endpoint_path="/v1/data/routing-history",
            run_id=run_id,
            payload={
                "run_id": run_id,
                "state_version": int(state_version),
                "intervals": serialized,
            },
            deliver_now=deliver_now,
        )

    def publish_apply_response(
        self, response: ApplyResponse, *, deliver_now: bool = False
    ) -> PublishResult:
        response.validate_contract()
        return self._enqueue(
            dedup_key=f"apply_response:{response.run_id}:{response.decision_batch_id}",
            event_type="ApplyResponse",
            endpoint_path="/v1/data/apply-responses",
            run_id=response.run_id,
            payload=response.to_dict(),
            deliver_now=deliver_now,
        )

    def publish_verification_evidence(
        self,
        run_id: str,
        evidence: VerificationEvidence,
        *,
        deliver_now: bool = False,
    ) -> PublishResult:
        ev_dict = evidence.to_dict()
        return self._enqueue(
            dedup_key=f"verification_evidence:{run_id}:{evidence.flow_group_id}:{evidence.verified_at_utc}",
            event_type="VerificationEvidence",
            endpoint_path="/v1/data/verification-evidence",
            run_id=run_id,
            payload={"run_id": run_id, "evidence": ev_dict},
            deliver_now=deliver_now,
        )

    # ------------------------------------------------------------------
    # Delivery & Outbox Inspection
    # ------------------------------------------------------------------

    def _deliver_single(self, dedup_key: str) -> OutboxEventRecord:
        with self._lock:
            rec = self._records[dedup_key]
            if rec.status == DeliveryStatus.DELIVERED:
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
                return updated

        url = base_url.rstrip("/") + rec.endpoint_path
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
        for try_idx in range(max_tries):
            attempts += 1
            try:
                status_code, body_text = self._sender_fn(
                    url, rec.payload, headers, self._config.timeout_s
                )
                if 200 <= status_code < 300:
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
                    )
                    with self._lock:
                        self._records[dedup_key] = delivered
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
                last_err = f"HTTP_{status_code}: {body_text[:160]}"
            except Exception as exc:
                last_err = f"{type(exc).__name__}: {exc}"

            if try_idx + 1 < max_tries:
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
        )
        with self._lock:
            self._records[dedup_key] = still_pending
        return still_pending

    def flush_pending(self) -> dict[str, int]:
        """Attempt bounded delivery for all `PENDING_DELIVERY` outbox records."""
        with self._lock:
            pending_keys = [
                k for k in self._order if self._records[k].status == DeliveryStatus.PENDING_DELIVERY
            ]
        delivered = 0
        still_pending = 0
        for key in pending_keys:
            res = self._deliver_single(key)
            if res.status == DeliveryStatus.DELIVERED:
                delivered += 1
            else:
                still_pending += 1
        return {"delivered": delivered, "pending": still_pending}

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

    def status_summary(self) -> dict[str, Any]:
        with self._lock:
            pending = sum(1 for r in self._records.values() if r.status == DeliveryStatus.PENDING_DELIVERY)
            delivered = sum(1 for r in self._records.values() if r.status == DeliveryStatus.DELIVERED)
            return {
                "endpoint_base_url": self._config.endpoint_base_url,
                "outbox_dir": str(self._config.outbox_dir) if self._config.outbox_dir else None,
                "total_events": len(self._records),
                "delivered_count": delivered,
                "pending_delivery_count": pending,
            }
