"""
SDN Controller integration adapters (`Phase 12`).

Provides production-oriented adapters around the verified SDN Core without modifying
core control logic or v1.4 cross-role schemas:
- `SQLiteBatchIdempotencyStore`: Durable SQLite WAL implementation of
  `BatchIdempotencyStoreProtocol` for crash-safe `AC-S08` idempotency.
- `DataEventPublisher`: Non-blocking durable outbox / HTTP event publisher for
  `Snapshot`, `RoutingStateSnapshot`, `RoutingHistory`, `ApplyResponse`, and
  `VerificationEvidence` (keeping Data as the sole primary DB writer).
- `SDNNorthboundHTTPApp` & `HTTPDemandReleaseProvider`: Eventlet-compatible WSGI
  Northbound Control API with strict schema validation, idempotent demand ingestion,
  and safe async apply + status polling.
"""

from sdn.adapters.data_event_publisher import (
    DataEventPublisher,
    DataPublisherConfig,
    DeliveryStatus,
    OutboxEventRecord,
    PublishResult,
)
from sdn.adapters.http_control_api import (
    ApplyJobRecord,
    ApplyJobState,
    HTTPDemandReleaseProvider,
    NorthboundAPIConfig,
    SDNNorthboundHTTPApp,
)
from sdn.adapters.sqlite_idempotency_store import (
    IdempotencyConflictError,
    SQLiteBatchIdempotencyStore,
)

__all__ = [
    "ApplyJobRecord",
    "ApplyJobState",
    "DataEventPublisher",
    "DataPublisherConfig",
    "DeliveryStatus",
    "HTTPDemandReleaseProvider",
    "IdempotencyConflictError",
    "NorthboundAPIConfig",
    "OutboxEventRecord",
    "PublishResult",
    "SDNNorthboundHTTPApp",
    "SQLiteBatchIdempotencyStore",
]
