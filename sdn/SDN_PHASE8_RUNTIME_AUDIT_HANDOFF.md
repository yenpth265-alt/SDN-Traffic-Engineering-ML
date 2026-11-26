# SDN Phase 8 — Runtime 48-Flow Audit & Data Sync Gate Handoff

## 1. Exact Git SHAs Reviewed
- **SDN Working Branch / Pushed Baseline:** `feature/sdn-data-contract-v14` @ `8015f7d01c2eae9eee52282e4987807827ddfed6` (+ uncommitted Phase 7 & Phase 8 changes strictly inside `sdn/`)
- **Data / Network Baseline:** `origin/main` @ `8ee1782554f48296942595873aaa989e9b4ca637` (`e0db494`, `458df93`, `8ee1782`)
- **Optimization Reference (read-only):** `origin/feature/optimization` @ `f0aaa26cfb6febbf1361a3be39715764c0b097e9` (`optimization/result_serializer.py`)

## 2. Runtime Execution Diagram (Approved Contracts 1–3)
```mermaid
sequenceDiagram
    participant Opt as Optimization / Runner
    participant Data as Data API (PostgreSQL)
    participant SDN as SDN Northbound & Outbox
    participant OVS as Open vSwitch (OF1.3)

    Opt->>Data: POST /v1/optimization/decisions (48 rows, decision_batch_id)
    Data-->>Opt: 200 {"inserted": 1}
    Opt->>SDN: POST /apply (B changed flows, decision_batch_id)
    SDN->>OVS: Two-phase FlowMod + Barrier + FlowStats verify (B flows only)
    OVS-->>SDN: VERIFIED (state_version = v+1)
    SDN-->>Opt: 200/202 ApplyResponse (OVS truth decoupled from Data availability)
    SDN->>Data: POST /v1/sdn/routing (state_version = v+1)
    Data-->>SDN: 200 {"inserted": 1} (or {"inserted": 0} on timeout retry)
    SDN->>Data: GET /v1/runs/{run_id}/decisions/{decision_batch_id} (48 rows)
    Data-->>SDN: 200 DecisionBatch.request_payload.rows
    SDN->>Data: POST /v1/sdn/apply-results (48 grounded rows: APPLIED/KEPT/ROLLED_BACK/UNKNOWN)
    Data-->>SDN: 200 {"updated": 1}
    Opt->>SDN: GET /health or GET /routing-state (data_sync_gate.optimization_tick_eligible == true)
```

## 3. Ownership of `DecisionBatch` & Exact 48-Flow `ApplyResult` Path
- **Ownership:** Optimization/Runner owns registering the 48-flow `DecisionBatch` (`POST /v1/optimization/decisions`) **before** calling SDN `/apply`. SDN **never** creates, overwrites, or fabricates `DecisionBatch` rows.
- **Runtime Path (`sdn/adapters/http_control_api.py:_publish_after_apply` → `sdn/adapters/data_event_publisher.py:publish_apply_response_v1_runtime`):**
  1. Enqueues `RoutingStateSnapshot` (`POST /v1/sdn/routing`) **first**.
  2. Verifies `post_apply_routing_state.state_version == apply_response.state_version`.
  3. Fetches the registered 48 `DecisionRow`s via `GET /v1/runs/{run_id}/decisions/{decision_batch_id}`.
  4. Constructs the complete 48-flow `ApplyResult` via `build_48_flow_apply_result_data_v1(...)` and enqueues it with durable `depends_on_dedup_key = routing_state:{run_id}:v{state_version}`.
  5. If Data is temporarily unreachable (`503`/timeout) or `DecisionBatch` is missing (`404`), durably parks `(ApplyResponse, RoutingStateSnapshot)` in `pending_apply_audits.jsonl` without failing the finalized OVS apply and without fabricating 48 rows.

## 4. `B=0`, `REJECTED`, `KEPT`, `ROLLED_BACK` & `UNKNOWN` Behavior
- **`B=0` (No-Op Tick):** Core `ApplyRequest` schema remains unchanged (`len(changes) >= 1`; empty `ApplyRequest` returns `400`). For valid `B=0` ticks where Runner registered a 48-flow no-op `DecisionBatch`, use `build_noop_48_flow_apply_result_data_v1(...)` to emit 48 grounded `KEPT` rows (or `UNKNOWN` if unverified) without modifying `last_successful_change_at`.
- **`REJECTED`:** Preserves batch-level `response_status = "REJECTED"` and rejection `reason` (e.g., `DEADLINE_EXPIRED`, `VERSION_MISMATCH`); maps per-flow `REJECTED` → `"KEPT"` only when verified `path_id == old_path_id` (or `"UNKNOWN"` if unverified); explicitly rejects any `APPLIED` batch containing a `REJECTED` flow.
- **`UNKNOWN`:** `actual_path_id = null`, `verified_at_utc = null` (never fabricated).
- **`KEPT`:** Requires verified `path_id == old_path_id`; never overwrites `last_successful_change_at`.
- **`ROLLED_BACK`:** `actual_path_id == old_path_id`, preserves rollback reason.

## 5. Data Sync Gate & Restart Recovery (`GET /health` & `GET /routing-state`)
Computed strictly from persisted outbox state (`outbox_events.jsonl`, `outbox_acked.jsonl`, `pending_apply_audits.jsonl`, `pending_apply_audits_resolved.jsonl`):
- `ovs_verified` / `ovs_reconcile_required`: OVS data-plane truth (never conflated with Data ACK).
- `data_acknowledged_state_version`: highest `DELIVERED` `RoutingState` version in Data.
- `routing_sync_pending`, `apply_audit_pending`, `data_sync_pending`: pending Data sync flags.
- `routing_conflict_detected`, `data_reconcile_required`, `reconcile_required`: permanent `401/403/409/422` routing conflict sets `reconcile_required = true`.
- `optimization_tick_eligible` & `optimization_tick_block_reason`: `true` **only** when OVS is verified, no reconciliation is required, `data_acknowledged_state_version >= current_state_version`, and `apply_audit_pending == false`. Zero false-positive eligibility after controller restart.
- Legacy mode (`SDN_DATA_API_MODE=legacy`) remains default and 100% backward-compatible.

## 6. Retry, Idempotency & Hold-Down Rules
- `OutboxEventRecord` payloads and `dedup_key`s are immutable across retries and restarts.
- Routing POST timeout after PostgreSQL commit recovers cleanly via idempotent retry receiving `200 {"inserted": 0}`.
- Missing Data ACK never resets OVS hold-down (`T_hold`) or trigger counters, and never degrades OVS `VERIFIED` state.

## 7. PostgreSQL CI Requirements (Exact QA Updates for `.github/workflows/postgresql_interface_contracts.yml`)
1. Update `producer_src/sdn` `ref:` to the new committed `feature/sdn-data-contract-v14` SHA.
2. Add `-f data/schemas/004_decision_tick_order.sql` to `Initialize disposable database`.
3. Add `sdn.tests.integration_postgres_v14` to `Run PostgreSQL contract tests`:
   ```bash
   python -m unittest -v \
     data.ingestion.test_management_api \
     data.ingestion.test_online_ingestion \
     testing.scripts.test_postgresql_contracts \
     sdn.tests.integration_postgres_v14
   ```
- Isolated entrypoint: [integration_postgres_v14.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/tests/integration_postgres_v14.py) (status: **`NOT_EXECUTED` / `PENDING_REAL_POSTGRES`** on local Windows/Ubuntu due to absent PostgreSQL service).

## 8. Remaining Cross-Role Dependencies
- **Testing/QA:** Update `.github/workflows/postgresql_interface_contracts.yml` (above) to execute `PASS_REAL_POSTGRES` CI.
- **Data:** Finalize `M8` (standalone `RoutingHistory` / `VerificationEvidence` endpoints or event mapping).
- **Runner / Optimization:** Register 48-flow `DecisionBatch` before `/apply`, gate next tick on `data_sync_gate.optimization_tick_eligible == true`, and skip `/apply` when `B=0`.
