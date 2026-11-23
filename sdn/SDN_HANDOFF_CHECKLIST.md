# SDN Controller — Cross-Role Handoff Checklist (`Phase 11`)

**Role:** SDN Controller (`sdn/`)  
**Branch:** `feature/sdn-controller` (baseline `d6bf9ad4851383a981de0743c3501e015924f240`)  
**Status:** `PARTIALLY_READY_FOR_INTEGRATION` *(SDN standalone: **READY**; cross-role closed-loop: pending partner roles & `MUST-LOCK` decisions)*

---

## 1. SDN Standalone Verification Gates (Completed)

- [x] **Pure-Python Core Isolation:** Zero `ryu`/`os_ken`/`mininet` imports in `sdn/controller/`, `sdn/monitoring/`, `sdn/route_verification/`, `sdn/schemas/`, or core `sdn/flow_management/`.
- [x] **Canonical Identifiers (`REQ-01` / `C01`):** `0x103`, `0x103_P1`/`0x103_P2`, `e1>c1` enforced strictly; legacy IDs rejected at all schema boundaries.
- [x] **Windows Regression Suite (`PASS_MOCK`):** `115/115 passed`, `compileall rc=0`.
- [x] **Ubuntu Regression Suite (Python 3.10.12 + OS-Ken 2.8.1):** `115/115 passed`, `compileall rc=0`.
- [x] **S0 Live Bootstrap (`PASS_LIVE_OVS`):** `6/6` OVS 2.17.12 switches connected, `PortDesc` validated, `pingall` `56/56` (`0%` loss), `48/48` P1 flows `VERIFIED`.
- [x] **Switch Reconnect & Recovery (`PASS_LIVE_OVS`):** `e1` rule delete + disconnect/reconnect reconciled back to `48/48 VERIFIED` with `0` tracebacks.
- [x] **Dynamic Routing Live (`PASS_DYNAMIC_ROUTING_LIVE`):** `0x103: P1 -> P2 -> P1` verified via real `DecisionHandler` + `RyuOpenFlowDriver` + bounded `FlowStats` polling (`0%` loss, exact 3-hop counters on `e1/c1/c2/e2`).
- [x] **Negative & Fault-Injection Live (`PASS_DYNAMIC_ROUTING_LIVE`):** Duplicate replay, `BATCH_ID_CONFLICT`, `HOLDDOWN`, `STALE`, `VERSION_MISMATCH`, `DEADLINE_EXPIRED`, Barrier timeout rollback (`0x104`), missing rule + reconcile (`0x105`), switch disconnect during apply + reconcile (`0x106`).
- [x] **3-Cycle Live Stability (`PASS_LIVE_OVS`):** 3 consecutive `P1 -> P2 -> P1` cycles (6 live switches) under `ping` (20/50 pps) + `iperf3` TCP (`8.27/7.12 Mbps`, `0` retransmits) + `iperf3` UDP `5 Mbps` (`0%` loss), constant thread count (`1`), `0` exceptions, clean `mn -c` teardown.
- [x] **Evidence & Backup Preservation:** Git bundle + working-tree patch + all Phase 9/10/10.1/11 logs backed up outside `/tmp` (`/home/vboxuser/sdn_evidence_backup_phase11/` and Windows scratch backup).
- [x] **Git Hygiene:** Only files inside `sdn/` modified/created; no edits to `network/`, `configs/`, `data/`, `ml/`, `optimization/`, `testing/`; no unapproved `commit`/`push`/`merge`.

---

## 2. Cross-Role Handoff & Integration Gates (Pending Team Action)

- [ ] **Lock `MUST-LOCK #1` (`REQ-03`):** Finalize `released_demand_to_sdn` runtime channel (Network `sender.py --release-url` vs. JSONL tailer vs. Data relay) and confirm `available_at_utc` stamping + retry/ack policy.
- [ ] **Lock `MUST-LOCK #2` (`IF09`/`IF10`):** Finalize inter-process transport (eventlet WSGI/REST vs. IPC/Queue) and timeout budgets (`forecast_timeout_ms`, `apply_timeout_ms`, `apply_guard_ms`).
- [ ] **Lock `MUST-LOCK #3` (Durability):** Provide durable `BatchIdempotencyStoreProtocol` backing store (SQLite/Data DB) to replace volatile `InMemoryBatchIdempotencyStore` across controller restarts.
- [ ] **Align `configs/experiments/runner_manifest.json`:** Fix `topology_version` (`"v1.0"` -> `"1.0"` to match `topology_bundle.json`) and `warmup_seconds` (`70` -> `60` per v1.4).
- [ ] **Partner Role Implementations:** Merge/deliver `data/` (DB writer & snapshot/routing-state ingestion), `optimization/` (MILP + `ApplyRequest` caller), `ml/` (7-field `PredictionRows`), `integration/` (orchestrator), and `testing/scripts/` (`run_tests.py`, `validate_results.py`, `run_benchmarks.py`).
- [ ] **Implement 3 Minimal SDN Adapters (post-lock):**
  1. Northbound control API adapter (`GET /status`, `GET /routing-state`, `POST /snapshots`, `POST /apply`, `POST /demand-releases`).
  2. Durable `BatchIdempotencyStoreProtocol` adapter.
  3. Data sink publisher adapter (keeping Data as sole DB writer).
- [ ] **Closed-Loop End-to-End Test (`TC-I01`):** Execute full `Network -> SDN -> Data -> ML -> Data -> Optimization -> SDN -> OVS -> Data` closed-loop trace on Ubuntu with raw evidence.

---

## 3. Handoff Artifacts in `sdn/`

- [SDN_INTEGRATION_READINESS_REPORT.md](SDN_INTEGRATION_READINESS_REPORT.md) — Phase 11 stabilization, stability live metrics, cross-role matrix, blockers, and reproduction commands.
- [DYNAMIC_ROUTING_LIVE_VALIDATION_REPORT.md](DYNAMIC_ROUTING_LIVE_VALIDATION_REPORT.md) — Phase 10.1 live dynamic routing, negative tests, rollback, and reconciliation report.
- [S0_LIVE_VALIDATION_REPORT.md](S0_LIVE_VALIDATION_REPORT.md) — Phase 9 / 9.1 S0 bootstrap & reconnect recovery live report.
- [INTEGRATION_HANDOFF.md](INTEGRATION_HANDOFF.md) — Wire serialization contracts, protocol signatures, and dependency injection guide.
- [UBUNTU_READINESS.md](UBUNTU_READINESS.md) & [LIVE_TEST_CHECKLIST.md](LIVE_TEST_CHECKLIST.md) — Ubuntu runtime setup and live verification checklists.
