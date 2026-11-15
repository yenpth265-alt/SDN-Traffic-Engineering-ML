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
   - `FlowMatcher` & `RouteCompiler`: Compiles canonical `path_id` (`0x103_P1`, `0x103_P2`) into 3-hop OpenFlow 1.3 rules (`e_src`, `c1`/`c2`, `e_dst`) with configurable L2/L3/L4 matching (`FlowMatchPolicyConfig`) and cookie (`flow_cookie == flow_group_id`).
   - `StateManager`: Authoritative owner of per-run `state_version`, `routing_state`, `routing_history`, `uncertainty_gaps`, single-batch in-flight locks (`BUSY`), reconciliation locks (`RECONCILIATION_REQUIRED`), and hold-down evaluation (`T_hold`).
   - `RuleInstaller` & `RuleVerifier`: Executes two-phase downstream-first OpenFlow 1.3 installation (`c1`/`c2` + `e_dst` + `Barrier` $\rightarrow$ `e_src` + `Barrier`), multi-signal route verification (`Barrier` + `FlowStats` read-back + counter/probe confirmation), automatic rollback on failure, and `UNKNOWN` containment when verification is inconclusive.
   - `DecisionHandler`: Validates `ApplyRequest` pre-conditions (`AC-S02`), enforces idempotency (`AC-S08`), orchestrates `RuleInstaller` and `StateManager`, and guarantees crash consistency (never returning a false `REJECTED` after switch writes begin).
   - `SnapshotCreator`: Produces immutable online `Snapshot` objects (`create_snapshot(run_id, cutoff, lookback_L)`) enforcing all three causality inequalities (`window_end_utc <= history_cutoff_utc`, `released_at_utc <= snapshot_as_of_utc`, `available_at_utc <= snapshot_as_of_utc`) and preventing zero-substitution for missing demand.
2. **Pluggable Runtime & External Boundary Adapters (`Protocol` interfaces):**
   - `SwitchDriverProtocol` (`sdn/flow_management/switch_driver.py`): Abstracts OpenFlow 1.3 wire operations (`install_flow_mod`, `send_barrier`, `read_flow_stats`, `verify_forwarding_counter_or_probe`). Implemented by `InMemorySwitchDriver` on Windows (`PASS_MOCK`) and designed for `RyuOpenFlowDriver` on Ubuntu (`PASS_LIVE_OVS`).
   - `DemandHistoryProviderProtocol` & `LinkTelemetryProviderProtocol` (`sdn/monitoring/snapshot_creator.py`): Abstracts causal released-demand delivery (`REQ-03` / `IF02`) and link sample references.
   - `BatchIdempotencyStoreProtocol` (`sdn/controller/decision_handler.py`): Abstracts volatile vs. durable idempotency storage across controller restarts.
   - `TransportEnvelope` (`sdn/schemas/apply_models.py`): Wraps `ApplyRequest` / `ApplyResponse` across IPC/REST/Queue transport boundaries.

---

## 2. Package Directory Structure

```text
sdn/
├── __init__.py                         # Unified public API exports (102 symbols)
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
├── flow_management/
│   ├── __init__.py
│   ├── flow_matcher.py                 # Configurable L2/L3/L4 OpenFlow 1.3 match builder
│   ├── route_compiler.py               # Compiles path_id -> 3-hop HopFlowRule (e_src, core, e_dst)
│   ├── switch_driver.py                # SwitchDriverProtocol & InMemorySwitchDriver (with fault injection F09-F12)
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
│   ├── test_phase4_decision_and_snapshot.py        # Phase 4: 22 tests (ApplyRequest/Response, idempotency, causal Snapshot)
│   └── test_phase5_e2e_integration_and_handoff.py  # Phase 5:  6 comprehensive suites (public APIs, adapter hot-swap, 12-step E2E, 13 faults, JSON serialization, crash recovery)
├── README_SDN.md                       # This file
├── INTEGRATION_HANDOFF.md              # Cross-role integration contracts, schemas, and MUST-LOCK handoff guide
└── UBUNTU_READINESS.md                 # Ubuntu/OVS/Mininet/Ryu live deployment & PASS_LIVE_OVS verification plan
```

---

## 3. Running Tests & Verification on Windows (`PASS_MOCK`)

All unit, contract, serialization, and end-to-end mock integration tests run in pure Python on Windows using the repository's `.venv`:

### 3.1 Run the Full Test Suite (`96` Tests)
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

# Phase 4: DecisionHandler, Apply Contracts & Causal SnapshotCreator (22 tests)
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
