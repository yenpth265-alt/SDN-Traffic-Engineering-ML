"""Persist immutable forecast/event messages before HTTP delivery.

This journal is ML-local replay state, not an experiment database or a tick
scheduler. Runner retries a tick with its original run/snapshot/batch IDs.
"""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3

from ml.common import ContractError, require


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


class ReplayJournal:
    def __init__(self, filename):
        self.filename = Path(filename)

    def _connect(self):
        self.filename.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(self.filename), timeout=5.0)
        try:
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    namespace TEXT NOT NULL, kind TEXT NOT NULL,
                    run_id TEXT NOT NULL, message_id TEXT NOT NULL,
                    binding TEXT NOT NULL, payload TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL, audit TEXT NOT NULL,
                    delivered INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (namespace, kind, run_id, message_id)
                )
            """)
            return connection
        except Exception:
            connection.close()
            raise

    def freeze(self, namespace, kind, run_id, message_id, binding, build):
        """Atomically reuse a bound message or build and commit it before POST.

        BEGIN IMMEDIATE serializes competing writers, including inference for
        the same tick. A second process must reuse the first committed payload.
        Only a caller-provided build function may create a new message.
        """
        key = (namespace, kind, run_id, message_id)
        binding_text = json.dumps(binding, sort_keys=True, separators=(",", ":"), allow_nan=False)
        try:
            with closing(self._connect()) as connection, connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT binding,payload,payload_sha256,audit FROM messages "
                    "WHERE namespace=? AND kind=? AND run_id=? AND message_id=?", key,
                ).fetchone()
                if row is not None:
                    require(row[0] == binding_text,
                            "Replay ID is already bound to a different snapshot, model or run context")
                    require(hashlib.sha256(row[1].encode("utf-8")).hexdigest() == row[2],
                            "Replay journal payload hash mismatch")
                    return json.loads(row[1]), json.loads(row[3]), True
                payload, audit = build()
                raw = encode(payload)
                connection.execute(
                    "INSERT INTO messages(namespace,kind,run_id,message_id,binding,payload,payload_sha256,audit) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (*key, binding_text, raw, hashlib.sha256(raw.encode("utf-8")).hexdigest(), encode(audit)),
                )
                return payload, audit, False
        except (sqlite3.Error, OSError, json.JSONDecodeError) as exc:
            raise ContractError(f"ML replay journal unavailable or corrupt: {exc}") from exc

    def mark_delivered(self, namespace, kind, run_id, message_id):
        try:
            with closing(self._connect()) as connection, connection:
                connection.execute(
                    "UPDATE messages SET delivered=1 "
                    "WHERE namespace=? AND kind=? AND run_id=? AND message_id=?",
                    (namespace, kind, run_id, message_id),
                )
        except (sqlite3.Error, OSError) as exc:
            raise ContractError(f"ML replay ACK checkpoint failed: {exc}") from exc
