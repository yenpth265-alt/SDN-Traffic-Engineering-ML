"""
Durable SQLite WAL Ledger for SDN `DemandRelease` Ingestion (`REQ-03`, Final Hardening Task B).

Ensures that accepted `POST /demand-releases` events survive controller restarts without
losing causal `requested_mbps` history, while avoiding write amplification under 48
events/second by committing an entire batch in a single SQLite WAL transaction.

Guarantees:
- Persists before returning ACK; raises `DemandPersistenceError` on any storage failure.
- Preserves `event_id`, `run_id`, `flow_group_id`, `bin_index`, `window_start_utc`,
  `window_end_utc`, `requested_mbps`, `released_at_utc`, `available_at_utc`, and
  canonical `payload_fingerprint` (SHA-256).
- Never modifies previously recorded `available_at_utc` timestamps on restart or replay.
- Explicit retention policy (`retention_bins`) aligned with `Snapshot` lookback `L*`.
- Local SDN controller resilience ledger only; Data remains the sole primary DB writer (`REQ-04`).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

from sdn.schemas.snapshot_models import DemandObservationRecord, DemandSourceKind


class DemandPersistenceError(RuntimeError):
    """Raised when durable persistence of DemandRelease records fails before ACK."""


def compute_demand_payload_fingerprint(
    *,
    run_id: str,
    flow_group_id: str,
    bin_index: int,
    window_start_utc: str,
    window_end_utc: str,
    requested_mbps: float,
    released_at_utc: str,
) -> str:
    """Canonical SHA-256 fingerprint for idempotency / conflict detection."""
    canonical = {
        "run_id": run_id,
        "flow_group_id": flow_group_id,
        "bin_index": int(bin_index),
        "window_start_utc": window_start_utc,
        "window_end_utc": window_end_utc,
        "requested_mbps": round(float(requested_mbps), 9),
        "released_at_utc": released_at_utc,
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class PersistedDemandEntry:
    """Row representation loaded from or written to `SQLiteDemandReleaseLedger`."""

    record: DemandObservationRecord
    payload_fingerprint: str


class SQLiteDemandReleaseLedger:
    """
    Thread-safe, WAL-backed SQLite ledger for `HTTPDemandReleaseProvider`.
    """

    def __init__(
        self,
        db_path: Path | str,
        *,
        retention_bins: Optional[int] = 3600,
    ) -> None:
        self._db_path = Path(db_path)
        if retention_bins is not None and int(retention_bins) < 1:
            raise ValueError("retention_bins must be >= 1 or None.")
        self._retention_bins: Optional[int] = int(retention_bins) if retention_bins is not None else None
        self._lock = threading.RLock()
        self._last_error: Optional[str] = None
        self._closed: bool = False

        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self._db_path),
            timeout=5.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA synchronous=FULL;")
            cur.execute("PRAGMA busy_timeout=5000;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS demand_releases (
                    run_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    flow_group_id TEXT NOT NULL,
                    bin_index INTEGER NOT NULL,
                    window_start_utc TEXT NOT NULL,
                    window_end_utc TEXT NOT NULL,
                    requested_mbps REAL NOT NULL,
                    released_at_utc TEXT NOT NULL,
                    available_at_utc TEXT NOT NULL,
                    payload_fingerprint TEXT NOT NULL,
                    PRIMARY KEY (run_id, event_id),
                    UNIQUE (run_id, flow_group_id, bin_index)
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_demand_releases_run_bin
                ON demand_releases (run_id, bin_index);
                """
            )

    @property
    def db_path(self) -> Path:
        return self._db_path

    @property
    def retention_bins(self) -> Optional[int]:
        return self._retention_bins

    def persist_batch(self, entries: Sequence[PersistedDemandEntry]) -> int:
        """
        Persist a batch of new `PersistedDemandEntry` rows in a SINGLE SQLite WAL
        transaction (`BEGIN IMMEDIATE ... COMMIT`) to avoid write amplification at 48/s.

        Raises `DemandPersistenceError` if persistence fails (caller MUST NOT return ACK).
        """
        if not entries:
            return 0
        with self._lock:
            if self._closed:
                err = "SQLiteDemandReleaseLedger is closed."
                self._last_error = err
                raise DemandPersistenceError(err)
            cur = self._conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE;")
                rows = [
                    (
                        e.record.run_id,
                        e.record.release_record_id,
                        e.record.flow_group_id,
                        int(e.record.bin_index),
                        e.record.window_start_utc,
                        e.record.window_end_utc,
                        float(e.record.requested_mbps),
                        e.record.released_at_utc,
                        e.record.available_at_utc,
                        e.payload_fingerprint,
                    )
                    for e in entries
                ]
                cur.executemany(
                    """
                    INSERT INTO demand_releases (
                        run_id,
                        event_id,
                        flow_group_id,
                        bin_index,
                        window_start_utc,
                        window_end_utc,
                        requested_mbps,
                        released_at_utc,
                        available_at_utc,
                        payload_fingerprint
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    rows,
                )
                if self._retention_bins is not None:
                    # Prune strictly outside retention window per touched run_id
                    max_bin_by_run: dict[str, int] = {}
                    for e in entries:
                        prev = max_bin_by_run.get(e.record.run_id)
                        b = int(e.record.bin_index)
                        if prev is None or b > prev:
                            max_bin_by_run[e.record.run_id] = b
                    for rid, max_b in max_bin_by_run.items():
                        cutoff_bin = max_b - self._retention_bins
                        if cutoff_bin >= 0:
                            cur.execute(
                                "DELETE FROM demand_releases WHERE run_id = ? AND bin_index <= ?;",
                                (rid, cutoff_bin),
                            )
                cur.execute("COMMIT;")
                self._last_error = None
                return len(rows)
            except Exception as exc:
                try:
                    cur.execute("ROLLBACK;")
                except Exception:
                    pass
                self._last_error = f"{type(exc).__name__}: {exc}"
                raise DemandPersistenceError(
                    f"Failed to durably persist DemandRelease batch: {self._last_error}"
                ) from exc

    def load_all(self) -> tuple[PersistedDemandEntry, ...]:
        """Load all persisted demand release records ordered by `(run_id, bin_index, flow_group_id)`."""
        with self._lock:
            if self._closed:
                raise DemandPersistenceError("SQLiteDemandReleaseLedger is closed.")
            cur = self._conn.cursor()
            cur.execute(
                """
                SELECT
                    run_id,
                    event_id,
                    flow_group_id,
                    bin_index,
                    window_start_utc,
                    window_end_utc,
                    requested_mbps,
                    released_at_utc,
                    available_at_utc,
                    payload_fingerprint
                FROM demand_releases
                ORDER BY run_id ASC, bin_index ASC, flow_group_id ASC;
                """
            )
            out: list[PersistedDemandEntry] = []
            for row in cur.fetchall():
                rec = DemandObservationRecord(
                    run_id=str(row[0]),
                    flow_group_id=str(row[2]),
                    bin_index=int(row[3]),
                    window_start_utc=str(row[4]),
                    window_end_utc=str(row[5]),
                    requested_mbps=float(row[6]),
                    released_at_utc=str(row[7]),
                    available_at_utc=str(row[8]),
                    release_record_id=str(row[1]),
                    source_kind=DemandSourceKind.RELEASED_DEMAND.value,
                    valid=True,
                    reason=None,
                )
                out.append(PersistedDemandEntry(record=rec, payload_fingerprint=str(row[9])))
            return tuple(out)

    def count(self, run_id: Optional[str] = None) -> int:
        with self._lock:
            if self._closed:
                return 0
            cur = self._conn.cursor()
            if run_id is None:
                cur.execute("SELECT COUNT(*) FROM demand_releases;")
            else:
                cur.execute("SELECT COUNT(*) FROM demand_releases WHERE run_id = ?;", (run_id,))
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def is_healthy(self) -> bool:
        with self._lock:
            if self._closed:
                return False
            try:
                cur = self._conn.cursor()
                cur.execute("SELECT 1;")
                cur.fetchone()
                return True
            except Exception as exc:
                self._last_error = f"{type(exc).__name__}: {exc}"
                return False

    def status_summary(self) -> dict[str, Any]:
        with self._lock:
            healthy = self.is_healthy()
            return {
                "durable": True,
                "healthy": healthy,
                "db_path": str(self._db_path),
                "retention_bins": self._retention_bins,
                "persisted_count": self.count() if healthy else None,
                "last_error": self._last_error,
            }

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._closed = True
                try:
                    self._conn.close()
                except Exception:
                    pass
