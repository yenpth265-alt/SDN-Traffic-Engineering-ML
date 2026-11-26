# SDN v1.4 Cross-Role Integration Profile & QA G1/G2 Handoff Runbook

- **SDN Branch:** `feature/sdn-data-contract-v14` (Phase 13 baseline `2ec0a583d6482d1ebcb749167d4fafeb22a8ec5e` already merged into `main`)
- **Data / Main Baseline:** `origin/main` @ `b6ebec73f904e1d2418b7c5724afc86da193a4ee`
- **Scope:** Production/Integration `data_v1` profile for SDN Controller (legacy `http_sqlite` mode remains the backward-compatible default for standalone testing).

---

## 1. Required Integration Environment Variables (`data_v1` Profile)

Never hardcode secrets in code, fixtures, or committed config files; inject all secrets strictly via environment variables (or secret manager):

| Variable | Required Value | Purpose |
|---|---|---|
| `SDN_STRICT_INTEGRATION_PROFILE` | `1` | Enforces fail-closed startup validation (`S0AppConfig.validate_integration_profile()`) |
| `SDN_DATA_API_MODE` | `data_v1` | Enables canonical Data API v1 endpoints & headers |
| `SDN_DEMAND_PROVIDER_MODE` | `data_v1` | Uses `DataAPIDemandHistoryProvider` ONLY (never opens/mixes SQLite demand history) |
| `SDN_DATA_ENDPOINT_URL` | `http(s)://<data-host>:<port>` | Data FastAPI base URL |
| `SDN_DATA_API_KEY` | `<SDN_SDN_KEY>` *(env secret)* | Role `SDN` `X-API-Key` header (never logged) |
| `SDN_DATA_OUTBOX_DIR` | `/var/lib/sdn/outbox` | Durable JSONL outbox + `pending_apply_audits.jsonl` |
| `SDN_IDEMPOTENCY_DB_PATH` | `/var/lib/sdn/idempotency.sqlite3` | Durable SQLite WAL `AC-S08` batch idempotency store |
| `SDN_NORTHBOUND_ENABLE` | `1` | Enables HTTP Northbound API (`/health`, `/routing-state`, `/snapshots`, `/apply`, `/apply-status`, `/reconcile-audit`) |
| `SDN_NORTHBOUND_TOKEN` | `<SDN_NORTHBOUND_SECRET>` *(env secret)* | Required `X-SDN-Token` / `Bearer` auth for all Northbound calls (including `/reconcile-audit`) |
| `SDN_DATA_TIMEOUT_S` | `1.0` | Bounded per-request HTTP timeout to Data API |
| `SDN_NORTHBOUND_SYNC_WAIT_MS` | `300` | Bounded sync wait before returning `202 Accepted` + `poll_url` |
| `SDN_ENABLE_INTRA_EDGE_L3` | `1` | Enables intra-edge L3 helper rules for full `pingall` alongside 48 inter-edge TE flows |

### Fail-Closed Startup Rules
When `SDN_STRICT_INTEGRATION_PROFILE=1`, `S0ControllerOrchestrator` refuses to start (`ValueError`) if any of the following is invalid:
- `SDN_DATA_API_MODE != "data_v1"` or `SDN_DEMAND_PROVIDER_MODE != "data_v1"`
- `SDN_DATA_ENDPOINT_URL` missing or not `http(s)://`
- `SDN_DATA_API_KEY` missing/blank
- `SDN_NORTHBOUND_ENABLE=1` with `SDN_NORTHBOUND_TOKEN` missing/blank (and `POST /reconcile-audit` independently fails closed with `403 Forbidden` if `SDN_NORTHBOUND_TOKEN` is unset)
- `SDN_DATA_OUTBOX_DIR` or `SDN_IDEMPOTENCY_DB_PATH` missing
- `SDN_DATA_TIMEOUT_S <= 0`
- `configs/network/topology_bundle.json` hash/CSV cross-check fails (`TopologyRegistry.from_bundle_file(..., verify_hash=True)`)

---

## 2. Timeout Ownership & Readiness Checks for Runner

| Timeout / Gate | Owner | Semantics |
|---|---|---|
| Tick deadline (`apply_deadline_utc`, `forecast_end`) | **Runner / Optimization** | End-to-end tick budget; SDN rejects pre-write if expired (`DEADLINE_EXPIRED`) |
| Per-request HTTP timeout (`SDN_DATA_TIMEOUT_S`) | **SDN Adapter** | Bounds single Data API HTTP call; never blocks OpenFlow event loop |
| Async apply wait (`SDN_NORTHBOUND_SYNC_WAIT_MS`) | **SDN Northbound** | Returns `202 Accepted` + `/apply-status` poll URL if OVS verify exceeds wait window |
| `GET /health` -> `sdn_readiness.ready` | **SDN** | `6/6` switches connected, `48/48` `VERIFIED`, `reconcile_required == false` |
| `GET /health` -> `data_sync_gate.optimization_tick_eligible` | **SDN → Runner** | `true` ONLY when OVS is `VERIFIED`, `reconcile_required == false`, `data_acknowledged_state_version >= current_state_version`, and `apply_audit_pending == false` |
| `POST /reconcile-audit` (`X-SDN-Token` required) | **Runner / Operator** | Targeted retry for a specific `(run_id, decision_batch_id)` after `DecisionBatch 404` retries were exhausted; never deletes pending audit or opens sync gate before valid Data ACK |

---

## 3. QA G1/G2 Handoff Checklist (Data & Testing)

- [x] **SDN Adapter & Runtime 48-Flow Audit:** `ecc63e56aacf4894aa7a24dbfe6840d28bb66e69` (`156/156` Windows & Ubuntu PASS)
- [x] **Canonical Data v1.4 Fixtures (Real Pydantic v2.13.4 Verified):** `2ec0a583d6482d1ebcb749167d4fafeb22a8ec5e` (`sdn/fixtures/data_v14/`, 8/8 PASS against `online_models.py` & `management_api.py` from `main @ 8ee1782`)
- [x] **Isolated PostgreSQL CI Entrypoint Ready:** `sdn/tests/integration_postgres_v14.py` (raises on missing migration `004`, never silently skips when `SDN_DATABASE_URL` is configured)
- [ ] **QA CI Execution (`PENDING_REAL_POSTGRES`):** Update `.github/workflows/postgresql_interface_contracts.yml` on QA branch to checkout latest `feature/sdn-data-contract-v14` SHA, apply `data/schemas/004_decision_tick_order.sql`, run `sdn.tests.integration_postgres_v14` with **0 skipped**, and collect SQL read-back (`public.snapshots`, `public.routing_versions`, `public.routing_decisions`)
- [ ] **Data `M8` Decision (`CONTRACT_BLOCKED`):** Finalize standalone `RoutingHistory` / `VerificationEvidence` endpoints or event mapping
- [ ] **Closed-Loop `TC-I01` (`PENDING_TC_I01`):** Execute full 6-role integration run before claiming `PASS_SYSTEM`

---

## 4. Confirmed Cross-Role Contract Findings (External Owners — Do Not Patch in SDN)

Audited against `origin/main` (`b6ebec73f904e1d2418b7c5724afc86da193a4ee`), `origin/feat/optimization-g0` (`903cb0da09f8f5afa268141507816e9627032b07`), and `origin/feat/ml-offline-data-v14` (`c551911b4f95f00f7051fed03a3d715a05847193`):

1. **Optimization (`DecisionBatch` strict Data schema):** `data/ingestion/management_api.py` enforces strict `Decision` (10 top-level fields) and `DecisionRow` (3 fields: `flow_group_id`, `old_path_id`, `proposed_path_id`) with `extra="forbid"`. Current `feat/optimization-g0` fixtures (`decision_batch_48_flows.json`, `decision_batch_b0_kept.json`) emit 29 top-level keys and 9 per-row keys; extra metadata must be placed inside `optimizer_result` only.
2. **Optimization (`tick_id` type):** `Decision.tick_id` in Data API is a non-empty string (`ID`), whereas `feat/optimization-g0` fixtures emit integer `tick_id` (`100`, `101`). Optimization must serialize `tick_id` as a string.
3. **Optimization (`data_sync_gate.optimization_tick_eligible`):** `optimization/service_client.py` must explicitly inspect SDN `GET /health` or `GET /routing-state` -> `data_sync_gate.optimization_tick_eligible` and skip/defer optimization ticks whenever `false`.
4. **Optimization (no silent `> B` truncation):** Optimizer/serializer must never silently truncate more than `B` flow changes; infeasible or over-budget plans must be rejected/audited explicitly rather than sliced.
5. **Runner / Optimization (register `DecisionBatch` before SDN `/apply`):** `POST /v1/optimization/decisions` must receive persistence ACK (`200`) from Data **before** calling SDN `POST /apply` with that `decision_batch_id` so SDN's post-apply `ApplyResult` audit lookup never hits `404`.
6. **Optimization / Data / Runner (`B = 0` no-op audit policy):** When `changed_flows == 0` (`old_path_id == proposed_path_id` for all 48 flows), follow the approved no-op audit policy (Data auto-records 48 `KEPT` rows on `POST /v1/optimization/decisions` and SDN is not called with an empty `ApplyRequest`, or SDN uses `build_noop_48_flow_apply_result_data_v1` if explicitly audited).
7. **ML (`origin/feat/ml-offline-data-v14`):** `ml/inference/online.py` is aligned with SDN canonical `{"sdn": <SDNSnapshotV14>}` (`unwrap_snapshot`) and `X-Content-SHA256` verification via `hmac.compare_digest`.
