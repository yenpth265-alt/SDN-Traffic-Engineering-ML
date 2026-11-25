# SDN <-> Data API v1 Integration Contract Review & Adapter Handoff

- **Repository:** `https://github.com/yenpth265-alt/SDN-Traffic-Engineering-ML`
- **SDN Branch / Baseline:** `feature/sdn-data-contract-v14` (from `feature/sdn-controller` @ `ca405ecf6f6f595df979656d490e96e2fdce3cb8`)
- **Updated Data/Network Reference (Phase 2):** `origin/main` @ `8ee1782554f48296942595873aaa989e9b4ca637` (`e0db494` canonical `SDNSnapshotV14`, `458df93` 48-flow `DecisionBatch`/`ApplyResult` + migration `004`, `8ee1782` Network DemandRelease outbox, `docs/architecture/Data_Integration_Contracts.md`)
- **Historical Phase 1 Reference:** `origin/integration/e2e` @ `59df9ce9459e02ec1cfaf1c9b7431f5437871b9c`
- **Test Classification:** **`PASS_CONTRACT_MOCK`** (`148/148` PASS; no live PostgreSQL database; `TC-I01` remains **PENDING**)

> **Phase 2 Update (`origin/main` @ `8ee1782`):** See [SDN_CROSS_ROLE_INTEGRATION_V14.md](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/SDN_CROSS_ROLE_INTEGRATION_V14.md) for the updated canonical v1.4 adapter specification. In Phase 2, **M1, M2, M3, M4, M5, M6, and M7 are RESOLVED** on the SDN adapter side (`PASS_CONTRACT_MOCK`); **M8** (standalone `RoutingHistory` / `VerificationEvidence` endpoints) and live PostgreSQL / `TC-I01` validation remain pending.

---

## 1. Confirmed SDN-Owned Fixes & Adapter Implemented

| Item | Status | Summary of Implementation (`sdn/` only) |
|---|---|---|
| **Phase 2 & 3: Standalone Data API v1 Adapter & Mappers** | `IMPLEMENTED_AND_TESTED` / `IMPLEMENTED_NEEDS_REAL_POSTGRES` | Added [data_api_v1_adapter.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/adapters/data_api_v1_adapter.py) and opt-in `SDN_DATA_API_MODE=legacy\|data_v1` (default **`legacy`**) + `SDN_DATA_API_KEY` in [data_event_publisher.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/adapters/data_event_publisher.py) and [ryu_s0_app.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/apps/ryu_s0_app.py). |
| **Phase 4: Readiness Bug (Worker Stopped + 0 Backlog)** | `IMPLEMENTED_AND_TESTED` | Fixed `_handle_health()` in [http_control_api.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/adapters/http_control_api.py): when `endpoint_base_url` is configured and `worker_running == False`, `data_delivery_readiness.ready` is ALWAYS `False` (`DATA_OUTBOX_WORKER_NOT_RUNNING`), even with 0 pending backlog. |
| **Phase 5: `DemandRelease` Duplicate `released_at_utc` Check** | `IMPLEMENTED_AND_TESTED` | Fixed `_matches_existing()` in [http_control_api.py](file:///d:/PROGRAMMING%20FOLDER/SDN-Traffic-Engineering-ML/sdn/adapters/http_control_api.py) to compare `released_at_utc` in addition to `requested_mbps`, `window_start_utc`, `window_end_utc`, `flow_group_id`, `bin_index`. Same `event_id` with altered `released_at_utc` now returns `409 DEMAND_RELEASE_CONFLICT`. |

---

## 2. Exact Data API v1 Endpoints, Auth & Headers

- **Mode Switch:** `SDN_DATA_API_MODE=data_v1` (default remains `legacy` until team approves cutover).
- **Auth Header:** `X-API-Key: <SDN_DATA_API_KEY>` (matched against `SDN_SDN_KEY` on Data API; never logged).
- **Common Headers:** `Content-Type: application/json`, `X-Idempotency-Key: <dedup_key>`, `X-SDN-Event-Type: <event_type>`.
- **Snapshot Integrity Header:** `X-Content-SHA256: <sha256_hex>` computed over the **exact UTF-8 JSON bytes** sent to `POST /v1/sdn/snapshots`.
- **ACK & Error Semantics:**
  - `DELIVERED` requires `2xx` **AND** JSON persistence ACK (`{"inserted": int}`, `{"updated": int}`, or `{"ack": true}`).
  - Deterministic `401`, `403`, `409`, `422` stop retrying immediately (1 attempt) and stay `PENDING_DELIVERY` (never marked `DELIVERED`).
  - Transient `5xx` / network errors retry with bounded exponential backoff and stay `PENDING_DELIVERY` in durable JSONL outbox until Data recovers.

| SDN Artifact | Data API v1 Endpoint | Status |
|---|---|---|
| `RoutingStateSnapshot` | `POST /v1/sdn/routing` | `IMPLEMENTED_NEEDS_REAL_POSTGRES` |
| `Snapshot` (complete causal bins) | `POST /v1/sdn/snapshots` | `IMPLEMENTED_NEEDS_REAL_POSTGRES` (see `CONTRACT_BLOCKED` nuances below) |
| `ApplyResponse` | `POST /v1/sdn/apply-results` | `IMPLEMENTED_NEEDS_REAL_POSTGRES` (see `1..B` vs `48` mismatch below) |
| `RoutingHistory` | *(none in `online_api.py`/`management_api.py`)* | **`CONTRACT_BLOCKED`** (kept safely `PENDING_DELIVERY`) |
| `VerificationEvidence` | *(none in `online_api.py`/`management_api.py`)* | **`CONTRACT_BLOCKED`** (kept safely `PENDING_DELIVERY`) |

---

## 3. Field-by-Field Payload Mapping

### A. `RoutingStateSnapshot` → `POST /v1/sdn/routing` (`online_models.Routing`)
- `run_id` → `run_id`
- `state_version` → `state_version`
- `as_of_utc` (or explicit `published_at_utc`) → `published_at_utc`
- `entries[fid]` → `rows[]`: `{flow_group_id, path_id, confirmed_at_utc (from confirmed_at), verification_status}`
- Enforces `verified == (path_id is not None) == (confirmed_at_utc is not None)`.

### B. `Snapshot` → `POST /v1/sdn/snapshots` (`online_models.Snapshot`)
- `run_id`, `snapshot_id`, `history_cutoff_utc`, `snapshot_as_of_utc`, `state_version`, `schema_version`, `quality_status` preserved verbatim.
- `cutoff` + `warmup_bins` → `cutoff_bin` (traffic-origin when `use_traffic_origin_bins=True`).
- `history[]` → `{flow_group_id, bin_index (+ warmup_bins), requested_mbps (None preserved, never 0.0), available_at_utc}`.
- `routing_state.entries` → `routing_state[]` (`Route` list).

### C. `ApplyResponse` → `POST /v1/sdn/apply-results` (`management_api.ApplyResult`)
- `run_id`, `decision_batch_id`, `status` → `response_status`, `state_version` → `response_state_version`, `completed_at` → `completed_at_utc`.
- `results[]` → `rows[]`: `{flow_group_id, actual_path_id, status, verified_at_utc (applied_at or completed_at_utc; null iff UNKNOWN), reason}`.
- Enforces `(actual_path_id is None) == (verified_at_utc is None) == (status == "UNKNOWN")`.

---

## 4. Exact Field-Level Cross-Role Contract Analysis (`Phase 5`)

### 4.1 Environment Probe Result (`Phase 2`)
- **Windows Host:** `psql` not installed (`NO_LOCAL_PSQL`); `.venv` has no `fastapi`, `pydantic`, `uvicorn`, or `psycopg2`.
- **Ubuntu VM (`Ubuntu SDN`):** `.venv_ubuntu` contains only `sdn/requirements-ubuntu.txt` (`os-ken`, `eventlet`, `pytest`); read-only VM probe timed out on console bridge (`ENVIRONMENT_BLOCKED`, no packages installed without approval, no production DB touched).
- **Verdict for Real PostgreSQL Gate:** **`SDN_DATA_V1_ENVIRONMENT_BLOCKED`** (static & mock-contract verified via `142/142` `PASS_CONTRACT_MOCK`; live PostgreSQL tests require installing/starting PostgreSQL + `fastapi` + `psycopg2` + applying `data/schemas/*.sql`).

### 4.2 Field-Level Mismatch Matrix (Exact Source Files & Line Numbers)

| # | Interface | Producer File:Line & Field | Consumer File:Line & Field | Type / Semantics | Lossless? | Owner | Required Team Decision |
|---|---|---|---|---|---|---|---|
| **M1** | **SDN Snapshot → Data Snapshot → ML Inference** | `sdn/schemas/snapshot_models.py:681-710` (`config_version`, `topology_version`, `config_hash`, `topology_hash`, `content_hash`, `flow_group_ids`, `routing_state_complete`, `missing_groups`, `unknown_flow_group_ids`, `origin_refs`, `provenance`, `routing_history`) | `data/ingestion/online_models.py:25-26, 135-146` (`extra="forbid"`, omits all 12 fields) **vs.** `ml/inference/predict.py:16-40` (**REQUIRES** `config_version`, `topology_version`, `config_hash`, `topology_hash`, `content_hash`, `flow_group_ids`, `routing_state_complete`, `missing_groups`, `unknown_flow_group_ids`, `routing_state[].state_version`) | **CRITICAL BREAKING MISMATCH:** Data compact `Snapshot` strips 12+ fields that `ml/inference/predict.py:16-40` strictly requires (`validate_snapshot` raises `ContractError` immediately!). | **NO (`CONTRACT_BLOCKED`)** | **Data + ML** | Unify Snapshot schema: either Data accepts/stores SDN's full v1.4 Snapshot JSON or ML updates `validate_snapshot` to accept Data's compact schema. |
| **M2** | **Snapshot `HistoryRow` Missing / Causal Fields** | `sdn/schemas/snapshot_models.py` (`valid`, `window_start_utc`, `window_end_utc`, `released_at_utc`, `available_at_utc: Optional[str]`, `release_record_id`, `reason`) | `data/ingestion/online_models.py:128-132` (`flow_group_id`, `bin_index`, `requested_mbps: Rate \| None`, `available_at_utc: UTC` **non-nullable**) **vs.** `ml/inference/predict.py:61-75` (**REQUIRES** `valid`, `window_start_utc`, `window_end_utc`, `released_at_utc`, `available_at_utc`, `release_record_id`) | Missing-demand rows (`available_at_utc=None`) cannot be sent to Data without fabricating timestamps; ML also fails without `window_start/end`, `released_at_utc`, `release_record_id`. | **NO (`CONTRACT_BLOCKED`)** | **Data + ML** | Make `HistoryRow.available_at_utc` nullable (`UTC \| None`) + include `reason` (and ML-required causal fields) in Data `HistoryRow`. |
| **M3** | **Snapshot `quality_status` Enum** | `sdn/schemas/snapshot_models.py` (`INCOMPLETE_HISTORY`, `INCOMPLETE_ROUTING`) | `data/ingestion/online_models.py:143` (`Literal["VALID","INCOMPLETE","STALE","INVALID"]`) | SDN distinguishes missing history vs. incomplete routing; Data collapses both to `"INCOMPLETE"`. | Partial (mapped explicitly) | **SDN + Data** | Confirm `"INCOMPLETE_HISTORY"` / `"INCOMPLETE_ROUTING"` → `"INCOMPLETE"` mapping. |
| **M4** | **Snapshot Time-Origin (`warmup=60s`)** | `sdn/schemas/snapshot_models.py:687` (`cutoff_offset_s` from `measurement_started_at_utc`) | `data/ingestion/online_store.py:236` (`traffic_bin(run, history_cutoff_utc) == cutoff_bin` from `traffic_started_at_utc`) | With `warmup_s=60`, measurement bin `0` == traffic bin `60`. Adapter shifts by `+warmup_bins`. | **YES** (when `warmup_s % delta_s == 0`) | **All Roles** | Lock traffic-origin vs. measurement-origin convention across SDN, Data, ML. |
| **M5** | **DemandRelease SQLite vs. PostgreSQL `available_at_utc`** | `sdn/adapters/http_control_api.py:182-190` (SDN stamps `available_at_utc` on `POST /demand-releases`) | `data/ingestion/online_store.py:253-257` (`observed.get(key) != row` exact equality on `available_at_utc`) | **CRITICAL:** If Network dual-delivers to Data and SDN, the two `available_at_utc` timestamps differ by ms → Data rejects EVERY Snapshot with `409 Conflict`! | **NO (`CONTRACT_BLOCKED`)** | **Data + Network + SDN** | Lock single demand path: either SDN pulls from Data `GET /v1/runs/{run_id}/demand-history` or Data relaxes `available_at_utc` exact-match in `ingest_snapshot`. |
| **M6** | **RoutingState → Data Routing** | `sdn/schemas/state_models.py:750-767` (`RoutingStateSnapshot.routing_state[]`: `confirmed_at`) | `data/ingestion/online_models.py:107-125` (`Routing.rows[]`: `confirmed_at_utc`, `published_at_utc`) | Lossless for all 48 routes when `VERIFIED` has non-null `(path_id, confirmed_at)` and `UNKNOWN` has `(None, None)`. | **YES** (`IMPLEMENTED_NEEDS_REAL_POSTGRES`) | **SDN + Data** | Confirm `POST /v1/sdn/routing` must always be called **before** `POST /v1/sdn/apply-results` (`management_api.py:208`). |
| **M7** | **ApplyResponse → Data ApplyResult (`1..B` vs. `48` rows)** | `sdn/schemas/apply_models.py` (`ApplyResponse.results`: `1..B` changed flows) | `data/ingestion/management_api.py:95-101, 163, 204-205` (`ApplyResult.rows` must cover **all 48 flows** matching `Decision.rows`) | SDN v1.4 sends `1..B` changed flows; Data `record_result` rejects `< 48` rows with `422`. Adapter refuses to fabricate 48 rows from a partial batch. | **NO for `B < 48` (`CONTRACT_BLOCKED`)** | **Data + Optimization** | Lock whether `Decision` & `ApplyResult` contain `48` flows (including `KEPT`) or only `1..B` changed flows. |
| **M8** | **RoutingHistory & VerificationEvidence** | `sdn/schemas/state_models.py` (`RoutingHistoryInterval`, `UncertaintyGapRecord`, `VerificationEvidence`) | `data/ingestion/online_store.py:183-192` (auto-derives `routing_history` from `POST /v1/sdn/routing`; **no** `/v1/sdn/routing-history` or `/v1/sdn/verification-evidence` endpoints) | Data derives basic intervals from routing transitions, but cannot store `UncertaintyGapRecord` or OpenFlow `VerificationEvidence` (`xid_refs`, `rule_readback_ok`, `counter_or_probe_ok`). | **NO (`CONTRACT_BLOCKED`)** | **Data** | Confirm whether to add structured endpoints or map to `/v1/runs/{run_id}/events/sdn`. |

---

## 5. Remaining Contract Mismatches Summary (Legacy Reference)

1. **Missing-Demand `HistoryRow.available_at_utc` (`CONTRACT_BLOCKED`):**
   - Data `online_models.HistoryRow` declares `available_at_utc: UTC` as **non-nullable**.
   - When a demand bin is missing (`valid=False`, `requested_mbps=None`, `available_at_utc=None`), SDN **refuses to fabricate a timestamp** and raises `DataV1ContractBlockedError` (keeping the event `PENDING_DELIVERY`).
2. **Lossy Snapshot Fields (`CONTRACT_BLOCKED` for full ML/audit provenance):**
   - Data `online_models.Snapshot` (`extra="forbid"`) drops `released_at_utc`, `reason`, `window_start_utc`/`window_end_utc`, `origin_refs`, `provenance`, `routing_history`, and `content_hash`. Team must confirm whether ML/Audit needs these fields in Data's Snapshot schema.
3. **Time-Origin Policy (`warmup = 60s`):**
   - Per `Online_API_Team_Decisions.md`, Data's `demand`/`snapshot` counts bins from `traffic_started_at_utc` (`warmup=60s` → measurement bin `0` == traffic bin `60`), while SDN `Snapshot` counts from `measurement_started_at_utc`.
   - `map_snapshot_to_data_v1(..., use_traffic_origin_bins=True)` explicitly validates `(measurement_started_at_utc - traffic_started_at_utc) % delta_s == 0` and shifts bins by `+warmup_bins` (`60`).
4. **`ApplyResult` Row Count (`1..B` vs `48` flows — `CONTRACT_BLOCKED`):**
   - `management_api.py` (`record_decision` & `record_result`) requires **all 48 flows** in `Decision.rows` and `ApplyResult.rows` AND requires `POST /v1/sdn/routing` for `response_state_version` to be published **before** `POST /v1/sdn/apply-results`.
   - SDN `ApplyResponse` per v1.4 contains only `1..B` changed flows (optional `expand_unchanged_as_kept=True` helper is provided, but team must lock whether Optimization/Data expects `48` rows or `1..B` rows).
5. **Missing Data Endpoints (`CONTRACT_BLOCKED`):**
   - No `/v1/sdn/routing-history` and no `/v1/sdn/verification-evidence` endpoints exist in `online_api.py`/`management_api.py`.
6. **DemandRelease Dual-Delivery / Sync Decision (`EXTERNAL_DEPENDENCY`):**
   - Currently `online_api.py` has `POST /v1/network/demand` + `GET /v1/runs/{run_id}/demand-history` while SDN has `POST /demand-releases` backed by `SQLiteDemandReleaseLedger`. Team must lock whether Network sends to Data-only (SDN pulls via `GET /demand-history`) or dual-delivers to SDN `/demand-releases`.

---

## 5. Questions for Next Team Meeting (Data, Network, Runner)

1. **Data:** Will `HistoryRow.available_at_utc` become nullable (`UTC | None`) + optional `reason` so `INCOMPLETE` snapshots with missing bins can be stored without fabricating timestamps?
2. **Data + Optimization:** Will `Decision`/`ApplyResult` require all `48` flows (including `KEPT`) or only `1..B` changed flows? And will Data add structured endpoints for `RoutingHistory` / `VerificationEvidence` (or map them to `/v1/runs/{run_id}/events/sdn`)?
3. **Network + Data:** Confirm final `DemandRelease` path (Network → SDN `/demand-releases` vs. Network → Data → SDN `GET /v1/runs/{run_id}/demand-history`) and static ARP in `network/topology/topology.py`.
4. **Runner:** Who owns the `TC-I01` orchestrator (`POST /v1/runs`, passing `decision_batch_id` & `snapshot_id`, enforcing `POST /v1/sdn/routing` before `POST /v1/sdn/apply-results`)?

---

## 6. Files Changed & Verification Results

- **Modified (`4`):** `sdn/adapters/__init__.py`, `sdn/adapters/data_event_publisher.py`, `sdn/adapters/http_control_api.py`, `sdn/apps/ryu_s0_app.py`
- **Added (`3`):** `sdn/adapters/data_api_v1_adapter.py`, `sdn/tests/test_data_api_v1_contract.py`, `sdn/SDN_DATA_INTEGRATION_CONTRACT_REVIEW.md`
- **SDN Core Changed:** **NO** (0 edits to `sdn/controller/`, `sdn/flow_management/`, `sdn/route_verification/`, `sdn/monitoring/`, `sdn/schemas/`).
- **Tests:** `python -m compileall -q sdn/` + `pytest -q -p no:cacheprovider sdn/tests/` (`142` total = `136` existing + `6` new contract/regression tests; `PASS_CONTRACT_MOCK`, awaiting real PostgreSQL & `TC-I01`).
