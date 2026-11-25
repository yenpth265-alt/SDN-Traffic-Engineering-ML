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

from sdn.adapters.data_api_v1_adapter import (
    DATA_V1_ENDPOINT_APPLY_RESULTS,
    DATA_V1_ENDPOINT_DECISION_LOOKUP_TEMPLATE,
    DATA_V1_ENDPOINT_ROUTING,
    DATA_V1_ENDPOINT_SNAPSHOTS,
    DataV1ContractBlockedError,
    DataV1MappingError,
    build_48_flow_apply_result_data_v1,
    build_data_v1_headers,
    compute_bytes_sha256,
    compute_sdn_v14_content_hash,
    compute_warmup_bin_offset,
    fetch_decision_batch_from_data_v1,
    is_valid_data_v1_ack,
    map_apply_response_to_data_v1,
    map_routing_state_to_data_v1,
    map_snapshot_to_data_v1,
    serialize_canonical_json_bytes,
)
from sdn.adapters.data_demand_history_provider import (
    DATA_V1_DEMAND_HISTORY_PATH_TEMPLATE,
    DataAPIDemandHistoryProvider,
    DataDemandHistoryConfig,
    DataDemandHistoryError,
)
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
from sdn.adapters.sqlite_demand_ledger import (
    DemandPersistenceError,
    PersistedDemandEntry,
    SQLiteDemandReleaseLedger,
    compute_demand_payload_fingerprint,
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
    "DemandPersistenceError",
    "HTTPDemandReleaseProvider",
    "IdempotencyConflictError",
    "NorthboundAPIConfig",
    "OutboxEventRecord",
    "PersistedDemandEntry",
    "PublishResult",
    "SDNNorthboundHTTPApp",
    "SQLiteBatchIdempotencyStore",
    "SQLiteDemandReleaseLedger",
    "compute_demand_payload_fingerprint",
]
