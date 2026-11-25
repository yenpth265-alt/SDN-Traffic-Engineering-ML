# SDN Cross-Role Integration Specification (v1.4 — Phase 2)

- **Repository:** `SDN-Traffic-Engineering-ML`
- **Working Branch:** `feature/sdn-data-contract-v14` (branched from `feature/sdn-controller` @ `ca405ecf6f6f595df979656d490e96e2fdce3cb8`)
- **Cross-Role Reference Baseline:** `origin/main` @ `8ee1782554f48296942595873aaa989e9b4ca637`
  - `e0db494`: Canonical SDN Snapshot v1.4 (`{"sdn": <SDNSnapshotV14>}`)
  - `458df93`: `DecisionBatch` / `ApplyResult` 48-flow validation & migration `004`
  - `8ee1782`: Network `DemandRelease` outbox/replay (`POST /v1/network/demand`)
  - `docs/architecture/Data_Integration_Contracts.md`
- **SDN Core Modified:** **NO** (0 lines changed in `sdn/controller/`, `sdn/flow_management/`, `sdn/route_verification/`, `sdn/monitoring/`, `sdn/schemas/`).
- **Verification Status:** **`SDN_V14_ENVIRONMENT_BLOCKED`** (`152/152` Windows tests `PASS_CONTRACT_MOCK` / `MOCK_ONLY` for Data Pydantic; Ubuntu VM SSH unreachable on `127.0.0.1:2222`; no local PostgreSQL/FastAPI/Pydantic; live OVS & `TC-I01` remain **PENDING**).

---

## 1. Architecture & Role Ownership Summary

- **Network** releases causal demand observations to Data (`POST /v1/network/demand`).
- **Data** is the sole primary PostgreSQL writer (`REQ-04`): it stamps `available_at_utc` via PostgreSQL `clock_timestamp()`, serves causal demand history to SDN (`GET /v1/runs/{run_id}/demand-history?format=sdn`), stores canonical SDN Snapshot v1.4 raw bytes (`POST /v1/sdn/snapshots`), mirrors verified routing versions (`POST /v1/sdn/routing`), and audits 48-flow `DecisionBatch` / `ApplyResult` pairs (`POST /v1/sdn/apply-results`).
- **SDN Controller** owns:
  1. Authoritative live routing state (`StateManager`, global `state_version`), OpenFlow 1.3 compilation/installation (`RouteCompiler`, `RuleInstaller`), and real data-plane verification (`RuleVerifier`).
  2. Creating immutable online `Snapshot` objects (`SnapshotCreator`), now backed by explicit provider selection (`http_sqlite` legacy default vs. `data_v1` pull from Data API).
  3. Delivering `RoutingState`, canonical `{"sdn": <SDNSnapshotV14>}`, and 48-flow `ApplyResult` to Data via a durable JSONL outbox with explicit causal dependency ordering.

---

## 2. Task A — Data-Backed `DemandHistoryProvider`

- **Module:** [data_demand_history_provider.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/adapters/data_demand_history_provider.py)
- **Endpoint Consumed:**
  ```
  GET /v1/runs/{run_id}/demand-history?cutoff=<RFC3339_UTC>&as_of=<RFC3339_UTC>&lookback=<int>&format=sdn
  Header: X-API-Key: <SDN_DATA_API_KEY>
  ```
- **Key Guarantees:**
  - Implements `DemandHistoryProviderProtocol` without modifying `SnapshotCreator`.
  - Preserves Data's original PostgreSQL-stamped `available_at_utc` and `released_at_utc` verbatim (eliminating the `M5` dual-timestamp conflict).
  - Never fabricates `0.0` for missing bins (`requested_mbps=None` stays `None`; absent rows stay absent so `SnapshotCreator` marks them `valid=False`).
  - Fails closed on HTTP `401/403/404/409/422/5xx`, timeouts, unreachable host, malformed JSON, forbidden `source_kind`, causal violations (`window_end_utc > cutoff` or `available_at_utc > as_of`), or duplicate conflicting `(flow_group_id, bin_index)` rows: clears cached observations for that run and raises `DataDemandHistoryError`.
  - Never logs or leaks `api_key` in error messages or `status_summary()`.
- **Explicit Provider Selection (Zero Mixing):**
  - `SDN_DEMAND_PROVIDER_MODE=http_sqlite` *(default, backward compatible)*: uses `HTTPDemandReleaseProvider` + optional `SQLiteDemandReleaseLedger`.
  - `SDN_DEMAND_PROVIDER_MODE=data_v1`: uses `DataAPIDemandHistoryProvider` ONLY; never opens SQLite demand ledger; `POST /demand-releases` returns `409 DEMAND_PROVIDER_IS_DATA_V1_READ_ONLY`; `POST /snapshots` fetches from Data first and returns `503 DATA_DEMAND_HISTORY_UNAVAILABLE` if Data fails.

---

## 3. Task B — Canonical SDN Snapshot v1.4 Wire Format

- **Module:** [data_api_v1_adapter.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/adapters/data_api_v1_adapter.py) (`map_snapshot_to_data_v1`, `compute_sdn_v14_content_hash`)
- **Endpoint:** `POST /v1/sdn/snapshots`
- **Wire Envelope:**
  ```json
  {
    "sdn": {
      "snapshot_id": "...",
      "run_id": "...",
      "origin_refs": { "run_started_at_utc": "...", "traffic_started_at_utc": "...", "measurement_started_at_utc": "...", "warmup_s": 60, "delta_s": 1 },
      "content_hash": "<sha256 of canonical sdn without content_hash>",
      "quality_status": "VALID | INCOMPLETE | STALE | INVALID",
      "history": [ ... ],
      "routing_state": [ ... ],
      "routing_history": [ ... ],
      ...
    }
  }
  ```
- **Integrity & Causality Rules:**
  - Preserves all 27 `online_models.SDNSnapshotV14` fields (resolving `M1`, `M2`, `M3`, `M4`).
  - Preserves measurement-origin `bin_index` (`0 .. measurement_s - 1`) natively and validates `origin_refs.warmup_s == (measurement_started_at_utc - traffic_started_at_utc)`.
  - Maps `INCOMPLETE_HISTORY` / `INCOMPLETE_ROUTING` → `"INCOMPLETE"` and `INVALID_ROUTING_STATE` / `INVALID_DATA` → `"INVALID"`.
  - For missing demand observations (`valid=False`, `requested_mbps=None`, `released_at_utc=None`, `available_at_utc=None`, `release_record_id=None`), emits `"reason": null` to match `data/ingestion/online_store.py:365-370` without ever synthesizing `0.0` or timestamps.
  - Recomputes `sdn.content_hash` using `ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False` (byte-identical to `online_store.py:snapshot_content_hash`), and sends `X-Content-SHA256` computed over the exact UTF-8 wire bytes of `{"sdn": {...}}`.

---

## 4. Task C — 48-Flow Audit Builder & Causal Outbox Delivery Ordering

- **48-Flow Audit Builder:** `build_48_flow_apply_result_data_v1(apply_response, *, decision_rows, routing_state)` + `fetch_decision_batch_from_data_v1(...)` in [data_api_v1_adapter.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/adapters/data_api_v1_adapter.py).
  - Requires registered 48-flow `DecisionBatch` rows (`flow_group_id`, `old_path_id`, `proposed_path_id`) and post-apply `RoutingStateSnapshot` at `apply_response.state_version`.
  - **Fail-closed:** raises `DataV1ContractBlockedError` if `decision_rows` is missing/incomplete — NEVER fabricates 48 rows.
  - Sets `APPLIED` / `FAILED` / `ROLLED_BACK` / `UNKNOWN` for executed changed flows (`<= B`), and sets `KEPT` for unchanged flows **ONLY** when `routing_state` confirms `VERIFIED` `path_id == old_path_id` (else `UNKNOWN` with null `actual_path_id` & `verified_at_utc`, or raises on inconsistency).
  - Guarantees every row's `(actual_path_id, verified_at_utc)` matches the published `RoutingState` version (`management_api.py:224-226`).
- **Durable Causal Outbox Ordering:** [data_event_publisher.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/adapters/data_event_publisher.py)
  - `OutboxEventRecord` now persists `depends_on_dedup_key: Optional[str]` in `outbox_events.jsonl` and restores it on restart.
  - In `data_v1` mode, `publish_apply_response(...)` automatically sets `depends_on_dedup_key = f"routing_state:{run_id}:v{state_version}"`.
  - `_deliver_single()` refuses to send any HTTP request for an event whose dependency is not yet `DELIVERED` (leaves it `PENDING_DELIVERY` with `WAITING_FOR_DEPENDENCY:<key>` and `attempts=0`).
  - `flush_pending()` runs bounded multi-pass resolution so even out-of-order enqueues deliver `POST /v1/sdn/routing` first, await valid persistence ACK, and ONLY THEN deliver `POST /v1/sdn/apply-results`.

---

## 5. Configuration & Environment Variables

| Variable | Default | Options | Notes |
|---|---|---|---|
| `SDN_DATA_API_MODE` | `legacy` | `legacy` \| `data_v1` | Opt-in Data API v1 mode (legacy default preserved) |
| `SDN_DEMAND_PROVIDER_MODE` | `http_sqlite` | `http_sqlite` \| `data_v1` | Explicit provider selection (never mixes Data & SQLite) |
| `SDN_DATA_ENDPOINT_URL` | *(none)* | e.g. `http://127.0.0.1:8000` | Base URL for Data FastAPI service |
| `SDN_DATA_API_KEY` | *(none)* | Role `SDN` secret (`SDN_SDN_KEY`) | Sent in `X-API-Key` header; never logged |
| `SDN_DATA_OUTBOX_DIR` | *(none)* | Directory path | Durable JSONL outbox (`outbox_events.jsonl` + `outbox_acked.jsonl`) |
| `SDN_DATA_TIMEOUT_S` | `0.5` | Float seconds | Bounded HTTP timeout for Data calls |

---

## 6. Verification Matrix & Phase 3 Audit Findings

| Check | Result | Scope / Evidence |
|---|---|---|
| `python -m compileall -q sdn/` | **PASS** | Windows `.venv` |
| `pytest -q -p no:cacheprovider sdn/tests/` | **`152/152 PASS` (`PASS_CONTRACT_MOCK`)** | 142 baseline + 6 Phase 2 + 4 Phase 3 audit suites |
| Data `SDNSnapshotV14` Pydantic direct check | **`MOCK_ONLY` (`ENVIRONMENT_BLOCKED`)** | Windows `.venv` has no `pydantic`/`fastapi`/`psycopg2` |
| Live PostgreSQL + real Data FastAPI (`online_api.py` + `management_api.py`) | **`ENVIRONMENT_BLOCKED`** | No local `psql`/`docker`/`psycopg2` |
| Ubuntu OS-Ken + Mininet + OVS regression / S0 smoke | **`ENVIRONMENT_BLOCKED` (`UBUNTU_REQUIRED`)** | `Ubuntu SDN` VM running (`GuestAdditionsRunLevel=1`), SSH `127.0.0.1:2222` returned `kex_exchange_identification: read: Unknown error` |
| Closed-loop `TC-I01` (Network + Data + SDN + ML + Optimization + Runner) | **`PENDING`** | Requires Runner orchestrator + real PostgreSQL + live services |

### Phase 3 Confirmed Bugs & Minimal Fixes (SDN-Owned Only)
1. **`FlowDecisionStatus.REJECTED` in `build_48_flow_apply_result_data_v1()`** ([data_api_v1_adapter.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/adapters/data_api_v1_adapter.py)): Pre-write rejected batches (`DEADLINE_EXPIRED` / `VERSION_MISMATCH`) raised `DataV1MappingError` because Data's `ResultRow.status` only allows `APPLIED|KEPT|FAILED|ROLLED_BACK|UNKNOWN`. Fixed by mapping pre-write `REJECTED` → `"KEPT"` when verified `path_id == old_path_id` (preserving `reason`) or `"UNKNOWN"` if unverified.
2. **Stale & non-1s bin validation in `DataAPIDemandHistoryProvider`** ([data_demand_history_provider.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/adapters/data_demand_history_provider.py)): Added fail-closed rejection for `window_start_utc < cutoff - lookback_L` (`DATA_STALE_WINDOW_VIOLATION`) and `window_end - window_start != 1s` (`DATA_INVALID_BIN_DURATION`), preserving `DATA_CAUSALITY_VIOLATION` priority for future rows.
3. **Rejected dependency diagnostics in `DataEventPublisher`** ([data_event_publisher.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/adapters/data_event_publisher.py)): Surfaces `dep.last_error` inside `WAITING_FOR_DEPENDENCY:<key> (<dep.last_error>)` while keeping `attempts=0` and enforcing `RoutingState` ACK before `ApplyResult`.

### Remaining Non-SDN / Open Items
1. **Standalone `RoutingHistory` / `VerificationEvidence` endpoints:** Still absent in Data API v1 (`CONTRACT_BLOCKED`, kept safely `PENDING_DELIVERY` in outbox; note `routing_history` is now carried inside `Snapshot.sdn.routing_history`).
2. **Runner / Optimization Orchestration:** Runner must register `RunCreate`, Optimization must register 48-flow `DecisionBatch` before SDN posts 48-flow `ApplyResult`, and live PostgreSQL + `TC-I01` must be executed in the shared integration environment.
