# SDN Controller Module (`sdn/`) — Architecture & Execution Guide

**Project:** `SDN-Traffic-Engineering-ML`  
**Role:** SDN Controller (`sdn/`)  
**Contract Baseline:** `SDN_Traffic_Engineering_Role_Integration_Guide_v1.4.md` (`ALIGNED_V1_4`) & Testing v2 (`interfaces_v2.md`, `acceptance_criteria_v2.md`, `integration_requirements.md`)

---

## 1. Architectural Overview

The `sdn/` package implements the **SDN Control Plane, State Manager, Two-Phase Rule Installer, Route Verifier, Decision Handler, and Causal Snapshot Creator** for the leaf-spine traffic engineering closed loop (`8` hosts `h1`–`h8`, `4` edge switches `e1`–`e4` `DPID 1..4`, `2` core switches `c1`–`c2` `DPID 5..6`, `32` directed links, `16` fabric links, `48` canonical inter-edge flow groups `0x103..0x806`, `96` candidate paths `P1`/`P2`).

To support full development and deterministic contract testing on **Windows (Pure Python)** while remaining ready for **Ubuntu Linux (Mininet + Open vSwitch + Ryu OpenFlow 1.3)** without modifying any core control logic, `sdn/` strictly separates:
1. **Pure-Python Control Plane Core (Zero Linux/Ryu/Mininet import dependencies):**
   - `TopologyRegistry`: Loads and validates `configs/network/topology_bundle.json` and canonical CSV catalogs (`hosts.csv`, `switches.csv`, `links.csv`, `flow_groups.csv`, `candidate_paths.csv`, `initial_paths.csv`), enforcing SHA-256 `topology_hash` and rejecting legacy identifiers.
   - `FlowMatcher` & `RouteCompiler`: Compiles canonical `path_id` (`0x103_P1`, `0x103_P2`) into 3-hop OpenFlow 1.3 rules (`e_src`, `c1`/`c2`, `e_dst`) with configurable L3/L4 matching (`FlowMatcherConfig`, `FlowMatchPolicy`) and cookie (`flow_cookie == flow_group_id`).
   - `StateManager`: Authoritative owner of per-run `state_version`, `routing_state`, `routing_history`, `uncertainty_gaps`, single-batch in-flight locks (`BUSY`), reconciliation locks (`RECONCILIATION_REQUIRED`), and hold-down evaluation (`T_hold`).
   - `RuleInstaller` & `RuleVerifier`: Executes two-phase downstream-first OpenFlow 1.3 installation (`c1`/`c2` + `e_dst` + `Barrier` $\rightarrow$ `e_src` + `Barrier`), multi-signal route verification (`Barrier` + `FlowStats` read-back + counter/probe confirmation), automatic rollback on failure, and `UNKNOWN` containment when verification is inconclusive.
   - `DecisionHandler`: Validates `ApplyRequest` pre-conditions (`AC-S02`), enforces idempotency (`AC-S08`), orchestrates `RuleInstaller` and `StateManager`, and guarantees crash consistency (never returning a false `REJECTED` after switch writes begin).
   - `SnapshotCreator`: Produces immutable online `Snapshot` objects (`create_snapshot(run_id, cutoff, lookback_L)`) enforcing all three causality inequalities (`window_end_utc <= history_cutoff_utc`, `released_at_utc <= snapshot_as_of_utc`, `available_at_utc <= snapshot_as_of_utc`) and preventing zero-substitution for missing demand.
2. **Pluggable Runtime & External Boundary Adapters (`Protocol` interfaces):**
   - `SwitchDriverProtocol` (`sdn/flow_management/switch_driver.py`): Abstracts OpenFlow 1.3 wire operations (`send_flow_mod`, `send_barrier`, `read_flow_stats`, `verify_forwarding_counter_or_probe`, `get_operation_log`). Implemented by `InMemorySwitchDriver` on Windows (`PASS_MOCK`) and designed for `RyuOpenFlowDriver` on Ubuntu (`PASS_LIVE_OVS`).
   - `DemandHistoryProviderProtocol` & `LinkTelemetryProviderProtocol` (`sdn/monitoring/snapshot_creator.py`): Abstracts causal released-demand delivery (`REQ-03` / `IF02`) and link sample references.
   - `BatchIdempotencyStoreProtocol` (`sdn/controller/decision_handler.py`): Abstracts volatile vs. durable idempotency storage across controller restarts.
   - `TransportEnvelope` (`sdn/schemas/apply_models.py`): Wraps `ApplyRequest` / `ApplyResponse` across IPC/REST/Queue transport boundaries.

---

## 2. Package Directory Structure

```text
sdn/
├── __init__.py                         # Unified public API exports (103 symbols)
├── schemas/
│   ├── __init__.py
│   ├── state_models.py                 # RoutingStateEntry, RoutingHistoryInterval, VerificationEvidence, enums & strict validators
│   ├── apply_models.py                 # ApplyRequest, ApplyResponse, ApplyFlowChange, ApplyFlowResult, TransportEnvelope
│   └── snapshot_models.py              # Snapshot, SnapshotHistoryItem, SnapshotProvenance, DemandObservationRecord, LinkSampleRef
├── controller/
│   ├── __init__.py
│   ├── topology_registry.py            # Canonical Network catalog loader & validator (IF01, REQ-01)
│   ├── state_manager.py                # Authoritative live routing_state, state_version, routing_history & hold-down (IF08)
│   └── decision_handler.py             # Pre-write validation, idempotency, crash-safe apply orchestration (IF09, IF10)
├── apps/
│   ├── __init__.py
│   └── ryu_s0_app.py                   # S0 Ryu / OS-Ken app & pure-Python S0ControllerOrchestrator
├── flow_management/
│   ├── __init__.py
│   ├── flow_matcher.py                 # Configurable L2/L3/L4 OpenFlow 1.3 match builder
│   ├── route_compiler.py               # Compiles path_id -> 3-hop OpenFlowRuleSpec (e_src, core, e_dst)
│   ├── switch_driver.py                # SwitchDriverProtocol & InMemorySwitchDriver (with fault injection F09-F12)
│   ├── ryu_openflow_driver.py          # Live OpenFlow 1.3 Ryu / OS-Ken driver (connection epochs, PortDesc, multipart FlowStats)
│   └── rule_installer.py               # Downstream-first 2-phase installer, bootstrap, rollback & rule_count tracking
├── route_verification/
│   ├── __init__.py
│   └── rule_verifier.py                # Multi-signal route verifier (Barrier + FlowStats + Counter/Probe) & path discovery
├── monitoring/
│   ├── __init__.py
│   └── snapshot_creator.py             # Causal online Snapshot builder & provenance verifier (IF05, REQ-03, AC-S10)
├── tests/
│   ├── test_topology_registry.py                   # Phase 1: 33 tests (catalog, canonical IDs, negative fixtures)
│   ├── test_state_manager.py                       # Phase 2: 16 tests (bootstrap, state_version, hold-down, invariants)
│   ├── test_flow_management_and_verification.py    # Phase 3: 19 tests (OF1.3 semantics, 2-phase install, rollback, restart)
│   ├── test_phase4_decision_and_snapshot.py        # Phase 4: 24 tests (ApplyRequest/Response, fail-closed snapshot, idempotency, causal Snapshot)
│   ├── test_phase5_e2e_integration_and_handoff.py  # Phase 5:  6 comprehensive suites (public APIs, adapter hot-swap, 12-step E2E, 13 faults, JSON serialization, crash recovery)
│   └── test_ryu_openflow_driver_and_s0.py          # Phase 6-7: 11 adapter/S0 suites (PASS_MOCK_ADAPTER: OF1.3 encoding, bootstrap fault injection, incremental 20+28 verification, epoch safety, PortDesc)
├── requirements-ubuntu.txt             # Ubuntu Ryu / OS-Ken OpenFlow 1.3 runtime dependencies
├── README_SDN.md                       # This file
├── INTEGRATION_HANDOFF.md              # Cross-role integration contracts, schemas, and MUST-LOCK handoff guide
├── UBUNTU_READINESS.md                 # Ubuntu/OVS/Mininet/Ryu live deployment & 15-step PASS_LIVE_OVS runbook
├── PRE_UBUNTU_VALIDATION_REPORT.md     # Phase 7 pre-Ubuntu code hardening & cross-role validation report
└── LIVE_TEST_CHECKLIST.md              # Ubuntu/Mininet/OVS live verification checklist (PENDING_UBUNTU_VALIDATION)
```

---

## 3. Running Tests & Verification on Windows (`PASS_MOCK`)

All unit, contract, serialization, and end-to-end mock integration tests run in pure Python on Windows using the repository's `.venv`:

### 3.1 Run the Full Test Suite (`136` Tests)
```powershell
.\.venv\bin\python.exe -m pytest -v -p no:cacheprovider sdn/tests/
```

### 3.2 Run Individual Phase Suites
```powershell
# Phase 1: TopologyRegistry & Canonical IDs (33 tests)
.\.venv\bin\python.exe -m pytest -v -p no:cacheprovider sdn/tests/test_topology_registry.py

# Phase 2: StateManager, Hold-Down & History Invariants (16 tests)
.\.venv\bin\python.exe -m pytest -v -p no:cacheprovider sdn/tests/test_state_manager.py

# Phase 3: OpenFlow 1.3 Semantics, Two-Phase Install & Verification (19 tests)
.\.venv\bin\python.exe -m pytest -v -p no:cacheprovider sdn/tests/test_flow_management_and_verification.py

# Phase 4: DecisionHandler, Apply Contracts & Causal SnapshotCreator (23 tests)
.\.venv\bin\python.exe -m pytest -v -p no:cacheprovider sdn/tests/test_phase4_decision_and_snapshot.py

# Phase 5: E2E Mock Integration, Adapter Hot-Swap & Schema Serialization (6 suites)
.\.venv\bin\python.exe -m pytest -v -p no:cacheprovider sdn/tests/test_phase5_e2e_integration_and_handoff.py
```

### 3.3 Run Syntax & Import Verification
```powershell
.\.venv\bin\python.exe -m py_compile sdn/__init__.py sdn/schemas/*.py sdn/controller/*.py sdn/flow_management/*.py sdn/route_verification/*.py sdn/monitoring/*.py sdn/tests/*.py
```

---

## 4. Quick-Start Programmatic Usage

```python
from pathlib import Path
from sdn import (
    TopologyRegistry,
    InMemorySwitchDriver,
    FlowMatcher,
    RouteCompiler,
    RuleVerifier,
    RuleInstaller,
    StateManager,
    DecisionHandler,
    SnapshotCreator,
    MockReleasedDemandProvider,
    ApplyRequest,
    ApplyFlowChange,
)

# 1. Load canonical Network topology bundle
bundle_path = Path("configs/network/topology_bundle.json")
topology = TopologyRegistry.from_bundle_file(bundle_path, csv_config_dir=bundle_path.parent)

# 2. Wire SDN control plane components
driver = InMemorySwitchDriver(topology)
compiler = RouteCompiler(topology, FlowMatcher(topology))
verifier = RuleVerifier(compiler, driver)
installer = RuleInstaller(compiler, driver, verifier)
state_mgr = StateManager(topology)

# 3. Bootstrap all 48 flows on P1 and verify actual switch state
run_id = "run_demo_01"
boot_evidences = installer.bootstrap_initial_paths(run_id, "2026-11-12T09:00:00Z")
state_mgr.initialize_run(run_id, "2026-11-12T09:00:00Z", verification_evidences=boot_evidences)

# 4. Create causal Snapshot and handle ApplyRequest
demand_provider = MockReleasedDemandProvider()
snapshot_creator = SnapshotCreator(topology, state_mgr, demand_provider)
decision_handler = DecisionHandler(
    topology,
    state_mgr,
    installer,
    snapshot_lookup=snapshot_creator.get_closed_snapshot,
)
```


---

## Integration Adapters, Northbound HTTP API & Phase 12.2/12.3 Production Config

The SDN Controller ships with three opt-in production integration adapters inside `sdn/adapters/`:
1. **`sdn/adapters/http_control_api.py`** (`SDNNorthboundHTTPApp`, `HTTPDemandReleaseProvider`): Eventlet-compatible WSGI HTTP server (`127.0.0.1:18080` default, optional `X-SDN-Token` / `Authorization: Bearer` auth, `1 MiB` max body size). Supports **non-blocking async apply (`POST /apply?async=1` / `wait_ms=0`)** returning `202 Accepted` in `< 11 ms` (live P50 `6.13 ms`, max `10.71 ms`).
2. **`sdn/adapters/sqlite_idempotency_store.py`** (`SQLiteBatchIdempotencyStore`): Durable SQLite WAL (`PRAGMA synchronous=FULL`) implementation of `BatchIdempotencyStoreProtocol` for crash-safe `AC-S08` replay across controller restarts (not a replacement for Data's primary DB).
3. **`sdn/adapters/data_event_publisher.py`** (`DataEventPublisher`): Non-blocking durable JSONL outbox + HTTP publisher (`X-Idempotency-Key`, bounded retry/backoff, `PENDING_DELIVERY` until explicit `2xx` ACK).

### Ubuntu Startup Command (Recommended Phase 12.2/12.3 Production Settings)

```bash
cd /home/vboxuser/SDN-Traffic-Engineering-ML
SDN_RUN_ID=s0_live_run \
SDN_VERIFY_PORT_DESC=1 \
SDN_ENABLE_INTRA_EDGE_L3=1 \
SDN_ENABLE_ARP_EDGE_LOCAL_FLOOD=0 \
SDN_PREINSTALL_BOTH_DOWNSTREAM=1 \
SDN_COUNTER_VERIFY_TIMEOUT_S=1.2 \
SDN_COUNTER_VERIFY_POLL_INTERVAL_S=0.04 \
SDN_NORTHBOUND_ENABLE=1 \
SDN_NORTHBOUND_HOST=127.0.0.1 \
SDN_NORTHBOUND_PORT=18080 \
SDN_NORTHBOUND_TOKEN=sdn-secret-token \
SDN_NORTHBOUND_SYNC_WAIT_MS=0 \
SDN_IDEMPOTENCY_DB_PATH=/var/tmp/sdn_idempotency.sqlite3 \
SDN_DEMAND_DB_PATH=/var/tmp/sdn_demand_ledger.sqlite3 \
SDN_DEMAND_RETENTION_BINS=3600 \
SDN_DATA_OUTBOX_DIR=/var/tmp/sdn_data_outbox \
SDN_DATA_ENDPOINT_URL=http://127.0.0.1:19090 \
SDN_DATA_AUTO_FLUSH=1 \
SDN_DATA_FLUSH_INTERVAL_S=0.5 \
PYTHONPATH=. .venv_ubuntu/bin/osken-manager \
  --ofp-tcp-listen-port 6633 sdn/apps/ryu_s0_app.py
```
*(Set `SDN_ENABLE_ARP_EDGE_LOCAL_FLOOD=1` temporarily only if running against `network/topology/topology.py` before Network adds static `h.setARP(...)` entries.)*

### Environment Variables Reference

| Variable | Default | Purpose |
|---|---|---|
| `SDN_RUN_ID` | `s0_live_run` | Active experiment run ID. |
| `SDN_VERIFY_PORT_DESC` | `1` | Enforce OpenFlow 1.3 `OFPMP_PORT_DESC` topology port verification at bootstrap. |
| `SDN_ENABLE_INTRA_EDGE_L3` | `1` | Install intra-edge L3 rules (`h1<->h2`, etc.). |
| `SDN_ENABLE_ARP_EDGE_LOCAL_FLOOD` | `0` | Edge-local ARP flood (keep `0` when Network sets static ARP). |
| `SDN_PREINSTALL_BOTH_DOWNSTREAM` | `1` | Pre-install both `P1` and `P2` downstream rules at bootstrap (`AC-S01`). |
| `SDN_COUNTER_VERIFY_TIMEOUT_S` | `1.2` | Finite bounded timeout (seconds) for counter verification & rollback verification. |
| `SDN_COUNTER_VERIFY_POLL_INTERVAL_S` | `0.04` | Cooperative `FlowStats` poll interval (`40 ms`, Phase 12.2 default). |
| `SDN_NORTHBOUND_ENABLE` | `0` | Enable Northbound WSGI HTTP server (`1` to enable). |
| `SDN_NORTHBOUND_HOST` / `PORT` | `127.0.0.1` / `18080` | Bind address and port for HTTP Control API. |
| `SDN_NORTHBOUND_TOKEN` | *(empty)* | Optional shared bearer/header token (`X-SDN-Token` or `Authorization: Bearer`). |
| `SDN_NORTHBOUND_SYNC_WAIT_MS` | `300` | Default sync wait window; **set `0` or pass `?async=1`** for `< 11 ms` `202 Accepted`. |
| `SDN_IDEMPOTENCY_DB_PATH` | *(empty)* | Path to SQLite WAL file for durable `AC-S08` idempotency across restarts. |
| `SDN_DEMAND_DB_PATH` | *(empty)* | Path to SQLite WAL `DemandRelease` ledger (persists before ACK; restores causal history & original `available_at_utc` across restarts; returns `503 DEMAND_PERSISTENCE_FAILED` on DB failure). |
| `SDN_DEMAND_RETENTION_BINS` | `3600` | Retention window in bins for `SQLiteDemandReleaseLedger` (always `>= lookback_L`). |
| `SDN_DATA_OUTBOX_DIR` | *(empty)* | Durable JSONL outbox directory for `DataEventPublisher`. |
| `SDN_DATA_ENDPOINT_URL` | *(empty)* | Base URL of Data's HTTP ingestion service (optional; buffers durably if unreachable). |
| `SDN_DATA_AUTO_FLUSH` | `1` | Start supervised eventlet-safe background outbox worker automatically in `RyuS0App`. |
| `SDN_DATA_FLUSH_INTERVAL_S` | `0.5` | Background outbox flush polling interval (seconds). |
| `SDN_DATA_TIMEOUT_S` / `MAX_RETRIES` | `0.5` / `3` | Bounded HTTP timeout & retry count per outbox event delivery attempt. |

### Deployment Readiness Gates (`GET /health`)
`GET /health` explicitly separates five independent gates:
- **`process_liveness`**: HTTP process alive & Northbound API enabled.
- **`sdn_readiness`**: `6/6` switches connected, `48/48` P1 flows `VERIFIED`, `reconcile_required == False`, durable DemandRelease ledger healthy (does **not** require a live Data receiver so S0/SDN Core can be tested standalone).
- **`data_delivery_readiness`**: Data endpoint configured, supervised outbox worker running, zero delivery-error backlog, `delivered_count`, `pending_delivery_count`, `last_successful_delivery_utc`, `last_error`.
- **`sdn_integration_readiness`**: True when `sdn_readiness`, `data_delivery_readiness`, durable SQLite idempotency, and durable SQLite DemandRelease ledger are all healthy (`blockers == []`).
- **`full_system_readiness`**: Always `ready == False` (`tc_i01_verified == False`, blocker `EXTERNAL_ROLES_NOT_VERIFIED_BY_SDN_HEALTH`) from SDN `/health` alone, because SDN cannot observe Network sender, real Data DB, ML online worker, Optimization loop, or closed-loop `TC-I01`.

### Exact `curl` & JSON Examples (Async Apply & Polling)

```bash
TOKEN="sdn-secret-token"
BASE="http://127.0.0.1:18080"

# 1. GET /health
curl -s -H "X-SDN-Token: $TOKEN" "$BASE/health"

# 2. GET /routing-state
curl -s -H "X-SDN-Token: $TOKEN" "$BASE/routing-state?run_id=s0_live_run"

# 3. GET /routing-history
curl -s -H "X-SDN-Token: $TOKEN" "$BASE/routing-history?run_id=s0_live_run&flow_group_id=0x103"

# 4. POST /demand-releases (single event or {"events": [...]}; server stamps available_at_utc)
curl -s -X POST -H "X-SDN-Token: $TOKEN" -H "Content-Type: application/json" \
  "$BASE/demand-releases" \
  -d '{
    "event_id": "network:s0_live_run:demand:0x103:0",
    "run_id": "s0_live_run",
    "flow_group_id": "0x103",
    "bin_index": 0,
    "window_start_utc": "2026-11-21T17:30:38Z",
    "window_end_utc": "2026-11-21T17:30:39Z",
    "requested_mbps": 2.5,
    "released_at_utc": "2026-11-21T17:30:39Z"
  }'

# 5. POST /snapshots (explicit origin_refs; immutable causal Snapshot)
curl -s -X POST -H "X-SDN-Token: $TOKEN" -H "Content-Type: application/json" \
  "$BASE/snapshots" \
  -d '{
    "run_id": "s0_live_run",
    "cutoff": 1,
    "lookback_L": 1,
    "origin_refs": {
      "run_started_at_utc": "2026-11-21T17:29:38Z",
      "traffic_started_at_utc": "2026-11-21T17:29:38Z",
      "measurement_started_at_utc": "2026-11-21T17:30:38Z",
      "delta_s": 1
    }
  }'

# 6. POST /apply?async=1 (RECOMMENDED: returns 202 ACCEPTED in < 11 ms; never blocks caller 450 ms timeout)
curl -i -s -X POST -H "X-SDN-Token: $TOKEN" -H "Content-Type: application/json" \
  "$BASE/apply?async=1" \
  -d '{
    "run_id": "s0_live_run",
    "decision_batch_id": "batch_001",
    "snapshot_id": "snap:s0_live_run:t1:L1:v2",
    "expected_state_version": 2,
    "config_version": "1.0",
    "forecast_start": "2026-11-21T17:30:39Z",
    "forecast_end": "2026-11-21T17:30:41Z",
    "apply_deadline_utc": "2026-11-21T17:30:40Z",
    "changes": [{"flow_group_id": "0x103", "old_path_id": "0x103_P1", "new_path_id": "0x103_P2"}],
    "topology_version": "1.0",
    "topology_hash": "2fa43f873e8530d934661d455fad946cd28d4d77b8f0f87b0b0cb4aa4594d0dc",
    "snapshot_as_of_utc": "2026-11-21T17:30:39.500000Z",
    "history_cutoff_utc": "2026-11-21T17:30:39Z"
  }'

# 7. GET /apply-status (poll every 40 ms until job_state == "FINALIZED"; includes timing breakdown)
curl -s -H "X-SDN-Token: $TOKEN" "$BASE/apply-status?run_id=s0_live_run&decision_batch_id=batch_001"
```

### Performance Budgets, Limitations & Error Handling (`Phase 12.2/12.3`)

- **Measured Live Performance (Ubuntu 22.04 + OS-Ken 2.8.1 + OVS 2.17.12, `B=1, 2, 4`):**
  - `POST /apply?async=1` HTTP acceptance: **P50 = `6.13 ms`**, **Max = `10.71 ms`** (`100%` within `450 ms` caller HTTP timeout).
  - OpenFlow 2-phase install (`B=2..4`): **`21.9–66.6 ms`**; `B=4` `FlowStats` count: **`5`** (1 per unique switch).
  - End-to-end apply + real 3-hop counter verification: **P50 = `267.87 ms`**, **P95 = `683.37 ms`**, **Max = `721.09 ms`** (`100% <= 1000 ms`, `61.5% <= 400 ms`).
- **Documented Technical Limitations:**
  1. **OVS Kernel Revalidator Floor (~500 ms):** Open vSwitch flushes kernel megaflow counters to userspace on a ~500 ms revalidator cycle. Recommended caller budgets: **`<= 50 ms` HTTP accept timeout** (`async=1`) and **`800–1000 ms` `apply_deadline_utc`** for real counter verification.
  2. **Silent-Flow Fail-Closed Policy (`AC-S03` / `AC-S04`):** Because no synthetic probe host/IP is allocated in `configs/network/topology_bundle.json`, rerouting a flow with **zero active traffic** will NOT fake `APPLIED` and will fail-closed (`INCONSISTENT` / `UNKNOWN` + `reconcile_required=True`). Optimization should only reroute flows with active demand (`b_f > 0`).
  3. **Finite Rollback Timeout:** Rollback verification clears any expired primary deadline and runs with its own finite bounded timeout (`SDN_COUNTER_VERIFY_TIMEOUT_S = 1.2 s`), never hanging indefinitely.
- **Error Handling Summary:**
  - Pre-write rejects (`rule_count=0`, `state_version` unchanged): `VERSION_MISMATCH`, `HOLD_DOWN_ACTIVE`, `BUSY`, `BATCH_ID_CONFLICT`, `DEADLINE_EXPIRED`, `RECONCILIATION_REQUIRED`, `STALE_SNAPSHOT`, `INVALID_REQUEST`.
  - Post-write failures: `ROLLED_BACK` (verified return to `old_path_id`, `reconcile_required=False`), `PARTIAL` / `INCONSISTENT` (`reconcile_required=True` until switch reconciliation completes), `deadline_missed=True` when verification finishes after `forecast_end`/`apply_deadline_utc`.
