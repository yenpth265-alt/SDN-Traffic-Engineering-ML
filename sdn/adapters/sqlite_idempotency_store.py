"""
Durable SQLite WAL Batch Idempotency Store (`Phase 12`, Task 4).

Implements `BatchIdempotencyStoreProtocol` (`sdn/controller/decision_handler.py`)
for crash-safe `(run_id, decision_batch_id)` replay and `BATCH_ID_CONFLICT` detection
across SDN controller restarts (`AC-S08`).

Scope boundary (`REQ-04`):
- This SQLite database is strictly an SDN-local idempotency cache for finalized
  `ApplyResponse` records. It is NOT a replacement for Data's primary experiment
  database; Data remains the sole primary database writer.
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from sdn.controller.decision_handler import CachedBatchEntry
from sdn.schemas.apply_models import ApplyResponse


class IdempotencyConflictError(ValueError):
    """Raised on atomic conflict when the same `(run_id, decision_batch_id)` has a different `payload_hash`."""


def _utc_now_rfc3339() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class SQLiteBatchIdempotencyStore:
    """
    Thread-safe, durable SQLite WAL implementation of `BatchIdempotencyStoreProtocol`.

    Properties:
    - `is_durable == True`
    - `store_status == "DURABLE_SQLITE_WAL"`
    - Atomic `BEGIN IMMEDIATE` write for `(run_id, decision_batch_id, payload_hash, response_json)`.
    - Preserves finalized committed entries across `invalidate_run_on_restart(run_id)` and
      process restarts so replayed requests return the exact persisted `ApplyResponse`
      (`idempotent_replay=True`) without re-sending `FlowMod`s, while conflicting payloads
      with the same batch ID are rejected with `BATCH_ID_CONFLICT`.
    """

    def __init__(self, db_path: str | Path, *, busy_timeout_ms: int = 5000) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._busy_timeout_ms = int(busy_timeout_ms)
        self._lock = threading.RLock()
        self._conn = self._open_connection()
        self._init_schema()

    def _open_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self._db_path),
            timeout=max(1.0, self._busy_timeout_ms / 1000.0),
            isolation_level=None,  # explicit BEGIN IMMEDIATE transactions
            check_same_thread=False,
        )
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=FULL;")
        conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms};")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                self._conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sdn_batch_idempotency (
                        run_id TEXT NOT NULL,
                        decision_batch_id TEXT NOT NULL,
                        payload_hash TEXT NOT NULL,
                        response_json TEXT NOT NULL,
                        finalized_at_utc TEXT NOT NULL,
                        PRIMARY KEY (run_id, decision_batch_id)
                    );
                    """
                )
                self._conn.execute("COMMIT;")
            except Exception:
                self._conn.execute("ROLLBACK;")
                raise

    @property
    def db_path(self) -> Path:
        return self._db_path

    @property
    def is_durable(self) -> bool:
        return True

    @property
    def store_status(self) -> str:
        return "DURABLE_SQLITE_WAL"

    def get(self, run_id: str, decision_batch_id: str) -> Optional[CachedBatchEntry]:
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT payload_hash, response_json
                FROM sdn_batch_idempotency
                WHERE run_id = ? AND decision_batch_id = ?
                """,
                (run_id, decision_batch_id),
            )
            row = cur.fetchone()
            if row is None:
                return None
            payload_hash, response_json = row
            resp = ApplyResponse.from_json(response_json, strict=True)
            return CachedBatchEntry(payload_hash=str(payload_hash), response=resp)

    def put(
        self,
        run_id: str,
        decision_batch_id: str,
        entry: CachedBatchEntry,
    ) -> None:
        """
        Atomically persist a finalized `(run_id, decision_batch_id)` entry.

        - If no record exists: inserts and commits durably in WAL mode.
        - If an identical `(run_id, decision_batch_id, payload_hash)` already exists:
          idempotent no-op (preserves original committed response).
        - If an entry exists with a DIFFERENT `payload_hash`: raises `IdempotencyConflictError`
          and never overwrites the committed record.
        """
        response_json = entry.response.to_json()
        now_utc = _utc_now_rfc3339()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                cur = self._conn.execute(
                    """
                    SELECT payload_hash
                    FROM sdn_batch_idempotency
                    WHERE run_id = ? AND decision_batch_id = ?
                    """,
                    (run_id, decision_batch_id),
                )
                existing = cur.fetchone()
                if existing is not None:
                    existing_hash = str(existing[0])
                    if existing_hash != entry.payload_hash:
                        self._conn.execute("ROLLBACK;")
                        raise IdempotencyConflictError(
                            f"Durable idempotency conflict for ({run_id!r}, {decision_batch_id!r}): "
                            f"existing payload_hash={existing_hash!r} != new {entry.payload_hash!r}."
                        )
                    self._conn.execute("COMMIT;")
                    return

                self._conn.execute(
                    """
                    INSERT INTO sdn_batch_idempotency (
                        run_id, decision_batch_id, payload_hash, response_json, finalized_at_utc
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (run_id, decision_batch_id, entry.payload_hash, response_json, now_utc),
                )
                self._conn.execute("COMMIT;")
            except IdempotencyConflictError:
                raise
            except Exception:
                try:
                    self._conn.execute("ROLLBACK;")
                except sqlite3.Error:
                    pass
                raise

    def invalidate_run_on_restart(self, run_id: str) -> None:
        """
        Durable store intentionally preserves finalized `(run_id, decision_batch_id)`
        entries across controller restarts so post-restart duplicate retries safely
        return the committed `ApplyResponse` (`idempotent_replay=True`) instead of
        re-executing `FlowMod`s, while `StateManager.mark_controller_restart()` sets
        `reconcile_required=True` to block any *new* batch until reconciliation completes.
        """
        return None

    def count_entries(self, run_id: Optional[str] = None) -> int:
        with self._lock:
            if run_id is None:
                row = self._conn.execute("SELECT COUNT(*) FROM sdn_batch_idempotency").fetchone()
            else:
                row = self._conn.execute(
                    "SELECT COUNT(*) FROM sdn_batch_idempotency WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
            return int(row[0]) if row else 0

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except sqlite3.Error:
                pass
