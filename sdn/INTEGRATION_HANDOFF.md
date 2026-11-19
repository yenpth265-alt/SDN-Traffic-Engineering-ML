# SDN Controller — Cross-Role Integration Handoff Guide (`ALIGNED_V1_4`)

**Project:** `SDN-Traffic-Engineering-ML`  
**Owner Role:** SDN Controller (`sdn/`)  
**Contract Baseline:** `SDN_Traffic_Engineering_Role_Integration_Guide_v1.4.md`, `testing/docs/interfaces_v2.md`, `testing/docs/acceptance_criteria_v2.md`, `testing/docs/integration_requirements.md`

---

## 1. Role Ownership & Separation of Responsibilities

Per `SDN_Traffic_Engineering_Role_Integration_Guide_v1.4.md` (Section 2 & Section 3), the SDN Controller strictly enforces the following boundaries:

| Role | What SDN Provides to Role | What SDN Consumes from Role | Strict Non-Overlaps (What SDN Never Does) |
| :--- | :--- | :--- | :--- |
| **Network** | Compiled OpenFlow 1.3 `FlowMod` (`OFPFC_ADD`, `OFPFC_MODIFY_STRICT`, `OFPFC_DELETE_STRICT`), `Barrier`, `FlowStatsRequest`, and active/counter verification probes via `SwitchDriverProtocol`. | Canonical static topology (`configs/network/topology_bundle.json` + CSV catalogs) and causal released demand observations (`DemandObservationRecord`) + link sample refs (`LinkSampleRef`). | SDN never hard-codes DPIDs, ports, IPs, MACs, or link capacities; SDN never infers `requested_mbps` from `goodput_mbps` or `port_tx_mbps`. |
| **Data** | Immutable online `Snapshot` (`Snapshot.to_json()`), verified `RoutingStateSnapshot` (`get_routing_state(run_id).to_json()`), causal `routing_history`, and finalized `ApplyResponse` (`ApplyResponse.to_json()`). | Optional durable idempotency store (`BatchIdempotencyStoreProtocol`) and historical released-demand query feed (`DemandHistoryProviderProtocol`) if routed via Data/Runner. | SDN never writes directly to SQLite/PostgreSQL/Parquet tables; **Data is the sole database writer** (`REQ-04`). |
| **ML** | Indirectly provides causal lookback window `X = [t - L*, t)` inside `Snapshot.history` (stored & served by Data). | None directly (ML outputs 7-field `PredictionRows` to Data; Optimization reads `READY` predictions from Data). | SDN never runs ML inference, never selects `L*`/`H*`/`epsilon*`, and never modifies `predicted_mbps`. |
| **Optimization / Runner** | Authoritative `state_version`, `routing_state`, hold-down lock status (`last_successful_change_at`), and synchronous `ApplyResponse` (`APPLIED`, `REJECTED`, `ROLLED_BACK`, `FAILED`, `PARTIAL`, `INCONSISTENT`). | `ApplyRequest` (or `TransportEnvelope` wrapping `ApplyRequest`) and `create_snapshot(run_id, cutoff, lookback_L)` trigger calls. | SDN never computes `b_f = mean(H* predicted_mbps)`, never evaluates congestion triggers (`TH`, `N`), and never solves the MILP (`AC-S05`). Optimization never sends `FlowMod` or raw port numbers. |
| **Testing / QA** | Deterministic fault injection hooks (`F09`–`F12`) in `InMemorySwitchDriver`, explicit `ReasonCode`s, `rule_count` breakdowns, `UncertaintyGapRecord`s, and strict JSON schema validators (`validate_contract()`, `from_json(..., strict=True)`). | Acceptance criteria (`AC-S01`–`AC-S10`), fault scenarios (`F01`–`F12`), and canonical test fixtures. | SDN never modifies files in `testing/` or `configs/network/` and strictly distinguishes `PASS_MOCK` from `PASS_LIVE_OVS`. |

---

## 2. Complete Cross-Role Interface Contract Table

| Interface ID | Producer | Consumer | Input | Output | Schema Class (`sdn.schemas`) | Owner | Status | `MUST-LOCK` Items |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`IF01`** (`topology_bundle`) | Network | SDN (`TopologyRegistry`) | `configs/network/topology_bundle.json` + 6 CSV catalogs | Validated `TopologyRegistry` (`8` hosts, `6` switches, `32` links, `16` fabric links, `48` flows, `96` paths) | `HostInfo`, `SwitchInfo`, `DirectedLink`, `FlowGroup`, `CandidatePath`, `InitialPathEntry` | Network (schema) / SDN (loader) | `IMPLEMENTED` (`PASS_MOCK`) | None (`ALIGNED_V1_4` locked) |
| **`IF02` / `REQ-03`** (`released_demand_to_sdn`) | Network / Runner / Data | SDN (`SnapshotCreator` via `DemandHistoryProviderProtocol`) | `(run_id, flow_group_ids, start_bin, end_bin_exclusive)` | `Sequence[DemandObservationRecord]` with `window_end_utc`, `released_at_utc`, `available_at_utc`, `requested_mbps` | `DemandObservationRecord`, `OriginRefs` | Network / Runner / SDN | `IMPLEMENTED` via Protocol (`PASS_MOCK`) | **`MUST-LOCK #1`**: Concrete runtime delivery channel (shared memory / IPC / DB view) for causal `released_demand` to SDN |
| **`IF03`** (`link_telemetry_refs`) | Network / Monitoring | SDN (`SnapshotCreator` via `LinkTelemetryProviderProtocol`) | `run_id` | `Sequence[LinkSampleRef]` with `sample_id`, `link_id`, `sample_at_utc`, `available_at_utc`, `valid` | `LinkSampleRef` | Network / SDN | `IMPLEMENTED` via Protocol (`PASS_MOCK`) | Calibrated link overhead factor `k_e` (`1.0` `UNCALIBRATED` until S0 lock) |
| **`IF05`** (`create_snapshot`) | SDN (`SnapshotCreator`) | Data / Runner / ML | `(run_id, cutoff, lookback_L, snapshot_as_of_utc)` | Immutable `Snapshot` with `history` (`48 * L*` items), `routing_state` (`48` entries), `routing_history`, `provenance`, `quality_status`, `content_hash` | `Snapshot`, `SnapshotHistoryItem`, `SnapshotProvenance`, `SnapshotQualityStatus` | **SDN** (sole creator) / **Data** (sole DB writer) | `IMPLEMENTED` (`PASS_MOCK`) | None (`IF05` causality rules locked in v1.4) |
| **`IF08`** (`routing_state` & `routing_history`) | SDN (`StateManager`) | Data / Optimization / Runner | `(run_id)` or `(run_id, as_of_utc)` | `RoutingStateSnapshot` (`state_version`, `routing_state_complete`, `reconcile_required`, `48` `RoutingStateEntry`), `tuple[RoutingHistoryInterval, ...]` | `RoutingStateSnapshot`, `RoutingStateEntry`, `RoutingHistoryInterval`, `UncertaintyGapRecord` | **SDN** (authoritative owner) | `IMPLEMENTED` (`PASS_MOCK`) | None (`ALIGNED_V1_4` locked) |
| **`IF09`** (`ApplyRequest`) | Optimization / Runner | SDN (`DecisionHandler`) | `ApplyRequest` (or `TransportEnvelope` + JSON string) with `run_id`, `decision_batch_id`, `snapshot_id`, `expected_state_version`, `changes` (`1..B`) | Pre-write validation + two-phase OpenFlow install + verification execution | `ApplyRequest`, `ApplyFlowChange`, `TransportEnvelope` | Optimization (caller) / SDN (validator & executor) | `IMPLEMENTED` (`PASS_MOCK`) | **`MUST-LOCK #2`**: Transport mechanism (Python IPC vs. REST vs. Queue) & timeout budgets (`apply_timeout_ms`, `apply_guard_ms`) |
| **`IF10`** (`ApplyResponse`) | SDN (`DecisionHandler`) | Optimization / Runner / Data | Executed or rejected `ApplyRequest` | `ApplyResponse` with `status`, `state_version`, `completed_at`, `results` (`ApplyFlowResult`), `rule_count`, `payload_hash`, `idempotent_replay` | `ApplyResponse`, `ApplyFlowResult`, `BatchStatus`, `FlowDecisionStatus`, `ReasonCode` | **SDN** (authoritative producer) | `IMPLEMENTED` (`PASS_MOCK`) | **`MUST-LOCK #3`**: Durable outbox / DB persistence of finalized `(run_id, decision_batch_id) -> ApplyResponse` across process restarts |
| **`SOUTHBOUND`** (`SwitchDriverProtocol`) | SDN (`RuleInstaller`, `RuleVerifier`) | Switch Adapter (`InMemorySwitchDriver` / `RyuOpenFlowDriver`) | `OpenFlowRuleSpec`, `FlowModCommand`, `Barrier`, `FlowStats`, `CompiledPathPlan` | `FlowModReceipt`, `BarrierReceipt`, `tuple[SwitchFlowStatsEntry, ...]`, `ForwardingCheckResult` | `OpenFlowRuleSpec`, `CompiledPathPlan`, `VerificationEvidence` | **SDN** | `IMPLEMENTED` (`PASS_MOCK` on Windows) | **`MUST-LOCK #4`**: L3/L4 match policy (`FlowMatchPolicy`) & active probe vs. counter window policy (`ProbeVerificationPolicy`) on live OVS (`UBUNTU_REQUIRED`) |

---

## 3. Cross-Role Wire Serialization & Validation Contract

Every cross-role schema in `sdn.schemas` provides deterministic serialization and strict validation methods:
- `.to_dict() -> dict[str, Any]` (JSON-ready dictionary with enum `.value` strings and normalized fields)
- `.to_json(*, indent: int | None = None, sort_keys: bool = True) -> str`
- `.from_dict(data: Mapping[str, Any], *, strict: bool = True) -> Self`
- `.from_json(raw_json: str | bytes, *, strict: bool = True) -> Self`
- `.validate_contract() -> None` (raises `ContractSerializationError` on any contract violation)

### 3.1 Enforced Validation Rules (`ContractSerializationError`)
1. **RFC3339 UTC Timestamps (`validate_rfc3339_utc_timestamp`):** Must include explicit UTC timezone (`Z` or `+00:00`). Naive timestamps (`2026-11-12T09:00:10`) or non-UTC offsets (`+07:00`) are rejected.
2. **Canonical Network Identifiers:**
   - `flow_group_id` (`validate_canonical_flow_group_id`): Must match `0x[1-8]0[1-8]` where `src_edge != dst_edge` (`0x103..0x806`). Legacy IDs (`h1_h3`, `h1->h3`, `f13`) are rejected.
   - `path_id` (`validate_canonical_path_id`): Must match `<flow_group_id>_P1` or `<flow_group_id>_P2` (e.g., `0x103_P1`). Bare `P1`/`P2` or `h1_h3_c1` are rejected.
   - `link_id` (`validate_canonical_link_id`): Must match `<node>><node>` (e.g., `e1>c1`, `h1>e1`). Legacy `e1_to_c1` or `e1->c1` are rejected.
3. **Strict Numeric & Boolean Types:** Python `bool` passed where `int` or `float` is expected (`expected_state_version=True`, `requested_mbps=False`) is rejected. `NaN`, `Inf`, and negative `requested_mbps` or `rule_count` are rejected.
4. **Nullability & Semantic Invariants:**
   - In `ApplyFlowResult`: When `status == UNKNOWN` or `verification_status == UNKNOWN`, `actual_path_id` **MUST** be `null` (`None`). Supplying a non-null `actual_path_id` with `UNKNOWN` raises `ContractSerializationError`.
   - In `SnapshotHistoryItem`: When `valid == False` (`DATA_MISSING`, `UNRELEASED_DEMAND`, `LATE_AVAILABLE`), `requested_mbps` **MUST** be `null` (`None`) — never `0.0`. Conversely, when `valid == True`, `requested_mbps` must be a finite float $\ge 0.0$ (where `0.0` represents genuine zero traffic demand).
5. **Key-Order-Invariant Content Hashes:**
   - `ApplyRequest.effective_payload_hash` (`compute_apply_payload_hash`): Canonical SHA-256 over `run_id`, `decision_batch_id`, `snapshot_id`, `expected_state_version`, `config_version`, `topology_version`, `topology_hash`, normalized UTC timestamps, and `changes` sorted by `flow_group_id`. Dictionary key order and `+00:00` vs. `Z` formatting produce identical hashes.
   - `Snapshot.content_hash`: Canonical SHA-256 over all snapshot fields (excluding `content_hash` itself), verified automatically on `Snapshot.from_json(..., verify_hash=True)`.

---

## 4. Adapter Boundaries & Dependency Injection Guide

### 4.1 `SwitchDriverProtocol` (`sdn/flow_management/switch_driver.py`)
Used by `RuleInstaller` and `RuleVerifier`. To swap `InMemorySwitchDriver` for `RyuOpenFlowDriver` on Ubuntu:
```python
@runtime_checkable
class SwitchDriverProtocol(Protocol):
    @property
    def evidence_mode(self) -> EvidenceMode: ...
    def send_flow_mod(
        self,
        rule: OpenFlowRuleSpec,
        command: FlowModCommand,
        *,
        phase: SwitchPhaseTag,
    ) -> FlowModReceipt: ...
    def send_barrier(
        self,
        dpid: int,
        *,
        phase: SwitchPhaseTag,
    ) -> BarrierReceipt: ...
    def read_flow_stats(
        self,
        dpid: int,
        *,
        flow_group_id: Optional[str] = None,
        cookie_int: Optional[int] = None,
    ) -> tuple[SwitchFlowStatsEntry, ...]: ...
    def verify_forwarding_counter_or_probe(
        self,
        compiled_plan: CompiledPathPlan,
        *,
        probe_id: Optional[str] = None,
    ) -> ForwardingCheckResult: ...
    def get_operation_log(self) -> tuple[SwitchOperationLogEntry, ...]: ...
```
No changes to `DecisionHandler`, `StateManager`, `RuleInstaller`, `RuleVerifier`, or `SnapshotCreator` are required when injecting `RyuOpenFlowDriver`.

### 4.2 `DemandHistoryProviderProtocol` & `LinkTelemetryProviderProtocol` (`sdn/monitoring/snapshot_creator.py`)
Used by `SnapshotCreator.create_snapshot(run_id, cutoff, lookback_L)`:
- **Data / Runner / Network Integration:** Implement `DemandHistoryProviderProtocol`:
  - `@property def provider_name(self) -> str`
  - `@property def demand_channel_status(self) -> str`
  - `def get_observations(self, run_id: str) -> Sequence[DemandObservationRecord]` to feed causal released demand records for `bins in [cutoff - lookback_L, cutoff)`.
  - `MockReleasedDemandProvider.ingest_network_release_event(event, origin_refs=..., available_at_utc=...)` directly converts `DemandRelease` JSONL/HTTP events emitted by `network/traffic_generator/sender.py` into canonical `DemandObservationRecord` items.
- Every `DemandObservationRecord` must carry `window_start_utc`, `window_end_utc`, `released_at_utc`, `available_at_utc`, `requested_mbps`, and `source_kind="released_demand"`.
- Optional `LinkTelemetryProviderProtocol`: implement `def get_link_sample_refs(self, run_id: str) -> Sequence[LinkSampleRef]` (or use `MockLinkTelemetryProvider.ingest_network_telemetry_batch(batch_event)` for `network/telemetry/telemetry_collector.py` events).

### 4.3 `BatchIdempotencyStoreProtocol` (`sdn/controller/decision_handler.py`)
Used by `DecisionHandler`:
- Default: `InMemoryBatchIdempotencyStore()` (`is_durable == False`, `store_status == "VOLATILE_IN_MEMORY_MUST_LOCK_DURABLE_STORE"`) — clears volatile cache on `notify_controller_restart(run_id)` via `invalidate_run_on_restart(run_id)` so SDN never claims durable replay without persistent backing.
- Production / Data Integration: Inject a SQLite/Redis/Data-backed implementation with `is_durable == True` so duplicate `(run_id, decision_batch_id)` requests re-submitted after a controller process restart return the persisted `ApplyResponse` (`idempotent_replay=True`) or reject conflicting payloads (`BATCH_ID_CONFLICT`).
- **Snapshot Validation Policy (`DecisionHandlerConfig`):** Production/integration callers should use `DecisionHandlerConfig()` or `DecisionHandlerConfig.for_integration()` (`SnapshotValidationPolicy.FAIL_CLOSED_INTEGRATION`, `require_registered_snapshot=True`). `DecisionHandlerConfig.for_mock_unit_tests()` (`SnapshotValidationPolicy.TEST_ONLY_ALLOW_UNREGISTERED`) is reserved strictly for isolated unit-test fixtures.

---

## 5. Crash Recovery & Durability Classification (`PHẦN 5`)

| Category | Scope | Items & Guarantees | Verification Status |
| :--- | :--- | :--- | :--- |
| **Category A** | **Implemented & Tested in Pure Python (`sdn/`)** | 1. Pre-write exception safety (`rule_count == 0`, `state_version` unchanged, `in_flight_batch_id` cleared).<br>2. Mid-batch switch write failure (`FlowMod` / `Barrier` timeout) $\rightarrow$ automatic rollback to `old_path_id` + verification (`ROLLED_BACK`) or `UNKNOWN` (`INCONSISTENT`) if rollback cannot be verified.<br>3. Post-switch-write / pre-state-commit crash (`commit_batch_outcome` exception) $\rightarrow$ `abort_in_flight_batch_as_unknown` marks affected flows `UNKNOWN` (`actual_path_id=None`), opens `UncertaintyGapRecord`, increments `state_version`, sets `reconcile_required=True`, and returns `INCONSISTENT` (never false `REJECTED`).<br>4. Controller restart (`notify_controller_restart`) + switch table read-back recovery (`recover_and_reconcile_run`). | `PASS_MOCK` (`test_phase4_decision_and_snapshot.py` & `test_phase5_e2e_integration_and_handoff.py`) |
| **Category B** | **Completed in Phase 5 on Windows** | 1. Switch power-cycle / wiped flow-table recovery (`recover_and_reconcile_run(..., reinstall_missing_from_paths=...)`) vs. retained flow-table recovery.<br>2. Strict wire-format JSON serialization/deserialization (`to_json`/`from_json`) and SHA-256 content integrity verification across all schemas.<br>3. `TransportEnvelope` wrapper and hot-swappable `SwitchDriverProtocol` + `BatchIdempotencyStoreProtocol` verification. | `PASS_MOCK` (`test_phase5_e2e_integration_and_handoff.py`) |
| **Category C** | **Requires Cross-Role Contract Lock (`Data` / `Runner` / `Network`)** | 1. **`MUST-LOCK #1`**: Runtime delivery channel for `released_demand_to_sdn` (`REQ-03`).<br>2. **`MUST-LOCK #2`**: Inter-process transport (`IPC` vs. `REST` vs. `Queue`) and exact millisecond budgets (`forecast_timeout_ms`, `apply_timeout_ms`, `apply_guard_ms`).<br>3. **`MUST-LOCK #3`**: Durable persistence of `BatchIdempotencyStoreProtocol` and `EventOutbox` in Data's database across OS/process crashes. | Interface ready; awaiting team config lock |
| **Category D** | **Requires Ubuntu / Mininet / OVS / Ryu Live Execution (`UBUNTU_REQUIRED`)** | 1. Live OpenFlow 1.3 socket connection, `OFPFC_ADD`/`OFPFC_MODIFY_STRICT`/`OFPFC_DELETE_STRICT`, `OFPMP_FLOW` (`FlowStats`), and `OFPPT_BARRIER` wire timing.<br>2. Physical OVS port TX/RX counter deltas and live `ping`/`iperf3` data-plane verification (`S0`–`S7`, `F09`–`F12`).<br>3. Calibration of link overhead factor `k_e` during `S0`. | Pending Ubuntu VM execution (`UBUNTU_READINESS.md`) |
