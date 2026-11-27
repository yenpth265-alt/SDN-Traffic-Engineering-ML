"""
Phase 5 End-to-End Mock Integration, Adapter Boundary, Schema Serialization,
and Crash Recovery Test Suite on Windows (PASS_MOCK).

Aligned with:
- docs/architecture/SDN_Traffic_Engineering_Role_Integration_Guide_v1.4.md
- testing/docs/interfaces_v2.md (IF01, IF03, IF05, IF08, IF09, IF10)
- testing/docs/integration_requirements.md (REQ-01..REQ-16)
- testing/docs/acceptance_criteria_v2.md (AC-S01..AC-S10, AC-D02, AC-D05, AC-D10)
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
import subprocess
import sys
from typing import Any, Optional, Sequence
import pytest

import sdn
from sdn import (
    ApplyFlowChange,
    ApplyFlowResult,
    ApplyRequest,
    ApplyResponse,
    BarrierReceipt,
    BatchIdempotencyStoreProtocol,
    BatchStatus,
    CachedBatchEntry,
    CompiledPathPlan,
    ContractSerializationError,
    DecisionHandler,
    DecisionHandlerConfig,
    DemandHistoryProviderProtocol,
    DemandObservationRecord,
    DemandSourceKind,
    EvidenceMode,
    FlowDecisionStatus,
    FlowMatcher,
    FlowModCommand,
    FlowModReceipt,
    ForwardingCheckResult,
    HistoryIntervalStatus,
    InMemoryBatchIdempotencyStore,
    InMemorySwitchDriver,
    LinkSampleRef,
    LinkTelemetryProviderProtocol,
    MockLinkTelemetryProvider,
    MockReleasedDemandProvider,
    OpenFlowRuleSpec,
    OriginRefs,
    ReasonCode,
    RouteCompiler,
    RoutingHistoryInterval,
    RoutingStateEntry,
    RoutingStateSnapshot,
    RuleInstaller,
    RuleVerifier,
    Snapshot,
    SnapshotCreator,
    SnapshotCreatorConfig,
    SnapshotHistoryItem,
    SnapshotProvenance,
    SnapshotQualityStatus,
    StateManager,
    StateValidationError,
    SwitchDriverError,
    SwitchDriverProtocol,
    SwitchFlowStatsEntry,
    SwitchOperationLogEntry,
    SwitchPhaseTag,
    TopologyRegistry,
    TransportEnvelope,
    UncertaintyGapRecord,
    VerificationEvidence,
    VerificationStatus,
    compute_apply_payload_hash,
    compute_snapshot_content_hash,
    validate_canonical_flow_group_id,
    validate_canonical_link_id,
    validate_canonical_path_id,
    validate_rfc3339_utc_timestamp,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TOPOLOGY_BUNDLE_PATH = REPO_ROOT / "configs" / "network" / "topology_bundle.json"
CATALOG_DIR = REPO_ROOT / "configs" / "network"

DEFAULT_ORIGINS = OriginRefs(
    run_started_at_utc="2026-11-12T08:59:00Z",
    traffic_started_at_utc="2026-11-12T08:59:00Z",
    measurement_started_at_utc="2026-11-12T09:00:00Z",
    warmup_s=60,
    delta_s=1,
)


@pytest.fixture()
def topology() -> TopologyRegistry:
    return TopologyRegistry.from_bundle_file(
        TOPOLOGY_BUNDLE_PATH,
        csv_config_dir=CATALOG_DIR,
        verify_hash=True,
    )


# =============================================================================
# Minimal Contract-Compliant Role Mocks (Network, Data, Runner/Optimization)
# =============================================================================


class MockNetworkTelemetryProducer:
    """
    Minimal producer representing Network / Traffic Generator (IF02 / REQ-03):
    - Emits causal released demand records (`DemandObservationRecord`) for 48 canonical flows
      with explicit `released_at_utc` and `available_at_utc` timestamps.
    - Emits directional link telemetry sample references (`LinkSampleRef`).
    """

    def __init__(
        self,
        topology: TopologyRegistry,
        demand_provider: MockReleasedDemandProvider,
        link_provider: MockLinkTelemetryProvider,
    ) -> None:
        self._topology = topology
        self._demand_provider = demand_provider
        self._link_provider = link_provider

    def release_causal_demand_window(
        self,
        run_id: str,
        start_bin: int,
        end_bin_exclusive: int,
        *,
        base_mbps: float = 20.0,
        zero_demand_flows: Sequence[str] = (),
        release_delay_s: float = 0.0,
        available_delay_s: float = 0.05,
    ) -> None:
        all_flow_ids = [fg.flow_group_id for fg in self._topology.get_all_flow_groups()]
        zero_set = frozenset(zero_demand_flows)
        non_zero_ids = [fid for fid in all_flow_ids if fid not in zero_set]
        if non_zero_ids:
            self._demand_provider.populate_uniform_window(
                run_id=run_id,
                flow_group_ids=non_zero_ids,
                origin_refs=DEFAULT_ORIGINS,
                start_bin=start_bin,
                end_bin_exclusive=end_bin_exclusive,
                base_mbps=base_mbps,
                release_delay_s=release_delay_s,
                available_delay_s=available_delay_s,
            )
        if zero_set:
            self._demand_provider.populate_uniform_window(
                run_id=run_id,
                flow_group_ids=list(zero_set),
                origin_refs=DEFAULT_ORIGINS,
                start_bin=start_bin,
                end_bin_exclusive=end_bin_exclusive,
                base_mbps=0.0,
                release_delay_s=release_delay_s,
                available_delay_s=available_delay_s,
            )

    def publish_link_samples(
        self,
        run_id: str,
        sample_at_utc: str,
        available_at_utc: str,
    ) -> None:
        samples = [
            LinkSampleRef(
                sample_id=f"sample:{run_id}:{lk.link_id}:{sample_at_utc}",
                link_id=lk.link_id,
                sample_at_utc=sample_at_utc,
                available_at_utc=available_at_utc,
                valid=True,
            )
            for lk in self._topology.get_fabric_links()
        ]
        self._link_provider.add_samples(run_id, samples)


class MockDataStoreConsumer:
    """
    Minimal consumer representing Data role (IF05, IF08, AC-D05, AC-D10):
    - Sole database writer: receives serialized `Snapshot` JSON from SDN, deserializes and
      verifies `content_hash` without rebuilding or mutating the snapshot (`AC-D10`).
    - Serves stored `Snapshot` to ML and Runner/Optimization.
    - Mirrors SDN's verified `ApplyResponse`, `RoutingStateSnapshot`, and `RoutingHistoryInterval`.
    """

    def __init__(self) -> None:
        self._snapshots_by_key: dict[tuple[str, str], str] = {}
        self._apply_responses: dict[tuple[str, str], str] = {}
        self._mirrored_routing_states: dict[str, str] = {}

    def ingest_sdn_snapshot_json(self, raw_snapshot_json: str) -> Snapshot:
        snap = Snapshot.from_json(raw_snapshot_json, strict=True, verify_hash=True)
        key = (snap.run_id, snap.snapshot_id)
        if key in self._snapshots_by_key:
            # Verify immutability: re-ingesting must match the exact content_hash
            existing = Snapshot.from_json(self._snapshots_by_key[key])
            if existing.content_hash != snap.content_hash:
                raise ContractSerializationError(
                    f"Data detected mutated content_hash for closed snapshot '{snap.snapshot_id}'!"
                )
            return existing
        self._snapshots_by_key[key] = raw_snapshot_json
        return snap

    def serve_snapshot(self, run_id: str, snapshot_id: str) -> Snapshot:
        raw = self._snapshots_by_key.get((run_id, snapshot_id))
        if raw is None:
            raise KeyError(f"Snapshot ({run_id}, {snapshot_id}) not found in Data store.")
        return Snapshot.from_json(raw, strict=True, verify_hash=True)

    def record_apply_response_and_state(
        self,
        raw_response_json: str,
        raw_routing_state_json: str,
    ) -> tuple[ApplyResponse, RoutingStateSnapshot]:
        resp = ApplyResponse.from_json(raw_response_json, strict=True)
        rs = RoutingStateSnapshot.from_json(raw_routing_state_json, strict=True)
        self._apply_responses[(resp.run_id, resp.decision_batch_id)] = raw_response_json
        self._mirrored_routing_states[rs.run_id] = raw_routing_state_json
        return resp, rs


class MockRunnerOptimizationClient:
    """
    Minimal client representing Runner / Optimization (IF07 / IF08):
    - Reads a `VALID` Snapshot from Data.
    - Emits a canonical `ApplyRequest` JSON payload (never sends FlowMod or raw port numbers).
    """

    def __init__(self, topology: TopologyRegistry) -> None:
        self._topology = topology

    def build_apply_request_json(
        self,
        snapshot: Snapshot,
        *,
        decision_batch_id: str,
        forecast_start_utc: str,
        forecast_end_utc: str,
        apply_deadline_utc: str,
        target_flows_to_flip: Sequence[str],
    ) -> str:
        if not snapshot.is_valid:
            raise ValueError(
                f"Optimization refuses to build ApplyRequest from invalid snapshot "
                f"(quality_status={snapshot.quality_status.value})."
            )
        state_by_flow = {r.flow_group_id: r for r in snapshot.routing_state}
        changes: list[ApplyFlowChange] = []
        for fid in target_flows_to_flip:
            cur_entry = state_by_flow[fid]
            assert cur_entry.path_id is not None
            old_path = cur_entry.path_id
            new_path = f"{fid}_P2" if old_path.endswith("_P1") else f"{fid}_P1"
            changes.append(
                ApplyFlowChange(
                    flow_group_id=fid,
                    old_path_id=old_path,
                    new_path_id=new_path,
                )
            )

        envelope = TransportEnvelope(
            run_id=snapshot.run_id,
            producer="runner_orchestrator",
            produced_at=forecast_start_utc,
            config_version=snapshot.config_version,
            config_hash=snapshot.config_hash,
            topology_version=self._topology.topology_version,
            topology_hash=self._topology.topology_hash,
        )
        req = ApplyRequest(
            run_id=snapshot.run_id,
            decision_batch_id=decision_batch_id,
            snapshot_id=snapshot.snapshot_id,
            expected_state_version=snapshot.state_version,
            config_version=snapshot.config_version,
            forecast_start=forecast_start_utc,
            forecast_end=forecast_end_utc,
            apply_deadline_utc=apply_deadline_utc,
            changes=tuple(changes),
            topology_version=self._topology.topology_version,
            topology_hash=self._topology.topology_hash,
            config_hash=snapshot.config_hash,
            snapshot_as_of_utc=snapshot.snapshot_as_of_utc,
            history_cutoff_utc=snapshot.history_cutoff_utc,
            envelope=envelope,
        )
        return req.to_json()


# =============================================================================
# PART 1: Architecture, Public API, Circular Import & Linux-Independence Audit
# =============================================================================


def test_p5_01_public_api_imports_and_no_linux_dependencies(
    topology: TopologyRegistry,
) -> None:
    submodules = [
        "sdn",
        "sdn.schemas",
        "sdn.schemas.state_models",
        "sdn.schemas.apply_models",
        "sdn.schemas.snapshot_models",
        "sdn.controller",
        "sdn.controller.topology_registry",
        "sdn.controller.state_manager",
        "sdn.controller.decision_handler",
        "sdn.flow_management",
        "sdn.flow_management.flow_matcher",
        "sdn.flow_management.route_compiler",
        "sdn.flow_management.switch_driver",
        "sdn.flow_management.rule_installer",
        "sdn.route_verification",
        "sdn.route_verification.rule_verifier",
        "sdn.monitoring",
        "sdn.monitoring.snapshot_creator",
    ]
    for mod_name in submodules:
        mod = importlib.import_module(mod_name)
        assert mod is not None
        # Confirm no core SDN module source file imports Linux-only or external SDN runtime modules
        mod_file = getattr(mod, "__file__", None)
        if mod_file:
            src_txt = Path(mod_file).read_text(encoding="utf-8")
            for forbidden_tok in ("import ryu", "from ryu", "import os_ken", "from os_ken", "import mininet", "from mininet", "import fcntl", "from fcntl"):
                assert forbidden_tok not in src_txt, f"Forbidden import {forbidden_tok!r} in {mod_name}"

    # Confirm every symbol in sdn.__all__ resolves cleanly
    for sym in sdn.__all__:
        assert hasattr(sdn, sym), f"Missing exported symbol {sym!r} in sdn.__init__"

    # Confirm no external SDN runtime modules (ryu, mininet, os_ken) are imported when loading
    # all pure-Python core SDN modules in a fresh Python interpreter (unaffected by adapter tests
    # in the same pytest session that explicitly import sdn.apps.ryu_s0_app).
    subproc_check = (
        "import importlib, sys; "
        f"mods = {submodules!r}; "
        "[importlib.import_module(m) for m in mods]; "
        "forbidden = ('ryu', 'mininet', 'os_ken', 'fcntl'); "
        "loaded = [f for f in forbidden if f in sys.modules]; "
        "assert not loaded, f'Core SDN modules imported forbidden modules: {loaded}'"
    )
    res = subprocess.run(
        [sys.executable, "-c", subproc_check],
        capture_output=True,
        text=True,
        check=False,
    )
    assert res.returncode == 0, f"Fresh-interpreter core isolation check failed: {res.stderr}"

    # Confirm canonical topology invariants from Network catalog
    assert len(topology.get_all_hosts()) == 8
    assert len(topology.get_all_switches()) == 6
    assert len(topology.get_all_links()) == 32
    assert len(topology.get_fabric_links()) == 16
    assert len(topology.get_all_flow_groups()) == 48
    assert (
        sum(
            len(topology.get_candidate_paths_for_flow(fg.flow_group_id))
            for fg in topology.get_all_flow_groups()
        )
        == 96
    )


# =============================================================================
# PART 2: Adapter Boundary Hot-Swap Verification (SwitchDriver & IdempotencyStore)
# =============================================================================


class _AlternativeAdapterSwitchDriver(SwitchDriverProtocol):
    """
    Custom SwitchDriverProtocol implementation that wraps InMemorySwitchDriver
    and exposes EvidenceMode.LIVE_OVS to verify that RuleInstaller, RuleVerifier,
    StateManager, DecisionHandler, and SnapshotCreator depend strictly on
    SwitchDriverProtocol without hard-coding InMemorySwitchDriver.
    """

    def __init__(self, topology: TopologyRegistry) -> None:
        self._inner = InMemorySwitchDriver(topology)

    @property
    def evidence_mode(self) -> EvidenceMode:
        return EvidenceMode.LIVE_OVS

    def send_flow_mod(
        self,
        rule: OpenFlowRuleSpec,
        command: FlowModCommand,
        *,
        phase: SwitchPhaseTag,
    ) -> FlowModReceipt:
        return self._inner.send_flow_mod(rule, command, phase=phase)

    def send_barrier(
        self,
        dpid: int,
        *,
        phase: SwitchPhaseTag,
    ) -> BarrierReceipt:
        return self._inner.send_barrier(dpid, phase=phase)

    def read_flow_stats(
        self,
        dpid: int,
        *,
        flow_group_id: Optional[str] = None,
        cookie_int: Optional[int] = None,
    ) -> tuple[SwitchFlowStatsEntry, ...]:
        return self._inner.read_flow_stats(
            dpid, flow_group_id=flow_group_id, cookie_int=cookie_int
        )

    def verify_forwarding_counter_or_probe(
        self,
        compiled_plan: CompiledPathPlan,
        *,
        probe_id: Optional[str] = None,
    ) -> ForwardingCheckResult:
        return self._inner.verify_forwarding_counter_or_probe(
            compiled_plan, probe_id=probe_id
        )

    def get_operation_log(self) -> tuple[SwitchOperationLogEntry, ...]:
        return self._inner.get_operation_log()


class _MockDurableIdempotencyStore(BatchIdempotencyStoreProtocol):
    """
    Simulated durable idempotency store (`is_durable = True`) to verify that
    DecisionHandler respects BatchIdempotencyStoreProtocol across restarts when
    an external durable store is injected by the integration harness.
    """

    def __init__(self) -> None:
        self._persisted: dict[tuple[str, str], CachedBatchEntry] = {}

    @property
    def is_durable(self) -> bool:
        return True

    @property
    def store_status(self) -> str:
        return "MOCK_DURABLE_ADAPTER_FOR_CONTRACT_TEST"

    def get(self, run_id: str, decision_batch_id: str) -> Optional[CachedBatchEntry]:
        return self._persisted.get((run_id, decision_batch_id))

    def put(
        self, run_id: str, decision_batch_id: str, entry: CachedBatchEntry
    ) -> None:
        self._persisted[(run_id, decision_batch_id)] = entry

    def invalidate_run_on_restart(self, run_id: str) -> None:
        # Durable store retains committed batch records across process restarts
        pass


def test_p5_02_adapter_boundary_hotswap_without_business_logic_changes(
    topology: TopologyRegistry,
) -> None:
    alt_driver = _AlternativeAdapterSwitchDriver(topology)
    assert isinstance(alt_driver, SwitchDriverProtocol)

    matcher = FlowMatcher(topology)
    compiler = RouteCompiler(topology, matcher)
    verifier = RuleVerifier(compiler, alt_driver)
    installer = RuleInstaller(compiler, alt_driver, verifier)
    state_mgr = StateManager(topology)

    evidences = installer.bootstrap_initial_paths(
        run_id="run_alt_adapter",
        verified_at_utc="2026-11-12T09:00:00Z",
    )
    state_mgr.initialize_run(
        run_id="run_alt_adapter",
        bootstrapped_at_utc="2026-11-12T09:00:00Z",
        verification_evidences=evidences,
    )

    durable_store = _MockDurableIdempotencyStore()
    assert isinstance(durable_store, BatchIdempotencyStoreProtocol)
    demand_provider = MockReleasedDemandProvider()
    link_provider = MockLinkTelemetryProvider()
    assert isinstance(demand_provider, DemandHistoryProviderProtocol)
    assert isinstance(link_provider, LinkTelemetryProviderProtocol)

    snap_creator = SnapshotCreator(
        topology,
        state_mgr,
        demand_provider,
        link_telemetry_provider=link_provider,
    )
    snap_creator.set_run_origin_refs("run_alt_adapter", DEFAULT_ORIGINS)

    handler = DecisionHandler(
        topology,
        state_mgr,
        installer,
        snapshot_lookup=snap_creator.get_closed_snapshot,
        idempotency_store=durable_store,
    )

    # Populate demand and create Snapshot
    all_fids = [fg.flow_group_id for fg in topology.get_all_flow_groups()]
    demand_provider.populate_uniform_window(
        run_id="run_alt_adapter",
        flow_group_ids=all_fids,
        origin_refs=DEFAULT_ORIGINS,
        start_bin=5,
        end_bin_exclusive=10,
        base_mbps=14.0,
    )
    snap = snap_creator.create_snapshot(
        run_id="run_alt_adapter",
        cutoff=10,
        lookback_L=5,
    )
    assert snap.is_valid is True
    assert snap.provenance.evidence_mode == EvidenceMode.LIVE_OVS.value

    # Execute ApplyRequest via swapped driver & durable idempotency store
    req = ApplyRequest(
        run_id="run_alt_adapter",
        decision_batch_id="batch_alt_01",
        snapshot_id=snap.snapshot_id,
        expected_state_version=1,
        config_version="1.0",
        forecast_start="2026-11-12T09:00:10Z",
        forecast_end="2026-11-12T09:00:20Z",
        apply_deadline_utc="2026-11-12T09:00:12Z",
        changes=(ApplyFlowChange("0x103", "0x103_P1", "0x103_P2"),),
        topology_version=topology.topology_version,
        topology_hash=topology.topology_hash,
    )
    resp = handler.handle_apply_request(
        req,
        decision_time_utc="2026-11-12T09:00:10.200Z",
        applied_at_utc="2026-11-12T09:00:10.300Z",
    )
    assert resp.status == BatchStatus.APPLIED
    assert resp.state_version == 2
    assert (
        state_mgr.get_flow_state("run_alt_adapter", "0x103").evidence_mode
        == EvidenceMode.LIVE_OVS
    )


# =============================================================================
# PART 3: Full 12-Step End-to-End Mock Integration Lifecycle + 13 Fault Scenarios
# =============================================================================


def test_p5_03_end_to_end_12_step_lifecycle_across_multiple_ticks(
    topology: TopologyRegistry,
) -> None:
    run_id = "run_e2e_lifecycle"

    # Step 1: Load official topology from Network catalog (supports historical 1.0 & canonical v1.0)
    assert topology.topology_version in ("1.0", "v1.0")
    assert len(topology.get_all_flow_groups()) == 48

    # Step 2 & Step 3: Initialize run & bootstrap 48 flows on P1 with simulated verification evidence
    driver = InMemorySwitchDriver(topology)
    matcher = FlowMatcher(topology)
    compiler = RouteCompiler(topology, matcher)
    verifier = RuleVerifier(compiler, driver)
    installer = RuleInstaller(compiler, driver, verifier)
    state_mgr = StateManager(topology)

    boot_evidences = installer.bootstrap_initial_paths(
        run_id=run_id,
        verified_at_utc="2026-11-12T09:00:00Z",
        preinstall_both_downstream_branches=True,
    )
    assert len(boot_evidences) == 48
    init_snap = state_mgr.initialize_run(
        run_id=run_id,
        bootstrapped_at_utc="2026-11-12T09:00:00Z",
        verification_evidences=boot_evidences,
    )
    assert init_snap.state_version == 1
    assert init_snap.routing_state_complete is True

    demand_provider = MockReleasedDemandProvider()
    link_provider = MockLinkTelemetryProvider()
    snap_creator = SnapshotCreator(
        topology,
        state_mgr,
        demand_provider,
        link_telemetry_provider=link_provider,
    )
    snap_creator.set_run_origin_refs(run_id, DEFAULT_ORIGINS)

    handler = DecisionHandler(
        topology,
        state_mgr,
        installer,
        config=DecisionHandlerConfig(require_registered_snapshot=True),
        snapshot_lookup=snap_creator.get_closed_snapshot,
    )

    net_producer = MockNetworkTelemetryProducer(topology, demand_provider, link_provider)
    data_store = MockDataStoreConsumer()
    opt_client = MockRunnerOptimizationClient(topology)

    # Step 4: Provide mock released demand for [0s, 10s) with genuine 0.0 demand on 0x806
    net_producer.release_causal_demand_window(
        run_id=run_id,
        start_bin=0,
        end_bin_exclusive=10,
        base_mbps=25.0,
        zero_demand_flows=("0x806",),
        release_delay_s=0.0,
        available_delay_s=0.05,
    )
    net_producer.publish_link_samples(
        run_id=run_id,
        sample_at_utc="2026-11-12T09:00:09Z",
        available_at_utc="2026-11-12T09:00:09.050Z",
    )

    # Step 5: Create valid Snapshot #1 at cutoff=10s (window [5s, 10s)) and store in Data
    snap1 = snap_creator.create_snapshot(
        run_id=run_id,
        cutoff=10,
        lookback_L=5,
        snapshot_as_of_utc="2026-11-12T09:00:10.100Z",
    )
    assert snap1.is_valid is True
    assert snap1.state_version == 1
    stored_snap1 = data_store.ingest_sdn_snapshot_json(snap1.to_json())
    assert stored_snap1 == snap1

    # Verify genuine 0.0 demand on 0x806 was preserved as 0.0 (valid=True)
    zero_bins = [h for h in stored_snap1.history if h.flow_group_id == "0x806"]
    assert len(zero_bins) == 5
    for zb in zero_bins:
        assert zb.requested_mbps == 0.0
        assert zb.valid is True

    # Step 6: Mock Runner/Optimization reads Snapshot #1 from Data and builds ApplyRequest JSON
    served_snap1 = data_store.serve_snapshot(run_id, snap1.snapshot_id)
    req1_json = opt_client.build_apply_request_json(
        served_snap1,
        decision_batch_id="batch_tick_10",
        forecast_start_utc="2026-11-12T09:00:10Z",
        forecast_end_utc="2026-11-12T09:00:20Z",
        apply_deadline_utc="2026-11-12T09:00:12Z",
        target_flows_to_flip=("0x103", "0x205"),
    )
    req1 = ApplyRequest.from_json(req1_json, strict=True)

    # Steps 7–11: DecisionHandler validates, RuleInstaller installs 2-phase via InMemorySwitchDriver,
    # RuleVerifier verifies, StateManager commits state_version=2 & routing_history, DecisionHandler returns ApplyResponse
    resp1 = handler.handle_apply_request(
        req1,
        decision_time_utc="2026-11-12T09:00:10.200Z",
        applied_at_utc="2026-11-12T09:00:10.300Z",
        completed_at_utc="2026-11-12T09:00:10.350Z",
    )
    assert resp1.status == BatchStatus.APPLIED
    assert resp1.state_version == 2
    assert resp1.rule_count == 6  # 2 flows * 3 hops
    assert len(resp1.results) == 2
    assert resp1.results[0].actual_path_id == "0x103_P2"
    assert resp1.results[1].actual_path_id == "0x205_P2"

    # Data mirrors ApplyResponse and live RoutingStateSnapshot
    mirrored_resp1, mirrored_rs1 = data_store.record_apply_response_and_state(
        resp1.to_json(),
        state_mgr.get_routing_state(run_id).to_json(),
    )
    assert mirrored_resp1 == resp1
    assert mirrored_rs1.state_version == 2

    # Step 12: SnapshotCreator creates Snapshot #2 at cutoff=15s (window [10s, 15s))
    # based on verified routing_state and causal routing_history
    net_producer.release_causal_demand_window(
        run_id=run_id,
        start_bin=10,
        end_bin_exclusive=15,
        base_mbps=30.0,
        release_delay_s=0.0,
        available_delay_s=0.05,
    )
    snap2 = snap_creator.create_snapshot(
        run_id=run_id,
        cutoff=15,
        lookback_L=5,
        snapshot_as_of_utc="2026-11-12T09:00:15.100Z",
    )
    stored_snap2 = data_store.ingest_sdn_snapshot_json(snap2.to_json())
    assert stored_snap2.is_valid is True
    assert stored_snap2.state_version == 2

    # Verify Snapshot #2 reflects the new paths (0x103_P2, 0x205_P2) and causal routing_history in [10s, 15s)
    rs2_map = {r.flow_group_id: r for r in stored_snap2.routing_state}
    assert rs2_map["0x103"].path_id == "0x103_P2"
    assert rs2_map["0x205"].path_id == "0x205_P2"
    assert rs2_map["0x104"].path_id == "0x104_P1"

    hist2_0x103 = [
        iv for iv in stored_snap2.routing_history if iv.flow_group_id == "0x103"
    ]
    assert len(hist2_0x103) == 2
    assert hist2_0x103[0].path_id == "0x103_P1"
    assert hist2_0x103[0].valid_to == "2026-11-12T09:00:10.3Z"
    assert hist2_0x103[1].path_id == "0x103_P2"
    assert hist2_0x103[1].valid_from == "2026-11-12T09:00:10.3Z"
    assert hist2_0x103[1].valid_to is None


def test_p5_04_end_to_end_all_13_fault_scenarios(
    topology: TopologyRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id = "run_e2e_faults"
    driver = InMemorySwitchDriver(topology)
    matcher = FlowMatcher(topology)
    compiler = RouteCompiler(topology, matcher)
    verifier = RuleVerifier(compiler, driver)
    installer = RuleInstaller(compiler, driver, verifier)
    state_mgr = StateManager(topology)

    boot_ev = installer.bootstrap_initial_paths(run_id, "2026-11-12T09:00:00Z")
    state_mgr.initialize_run(
        run_id,
        "2026-11-12T09:00:00Z",
        verification_evidences=boot_ev,
    )

    demand_provider = MockReleasedDemandProvider()
    snap_creator = SnapshotCreator(topology, state_mgr, demand_provider)
    snap_creator.set_run_origin_refs(run_id, DEFAULT_ORIGINS)
    handler = DecisionHandler(
        topology,
        state_mgr,
        installer,
        snapshot_lookup=snap_creator.get_closed_snapshot,
    )

    all_fids = [fg.flow_group_id for fg in topology.get_all_flow_groups()]
    demand_provider.populate_uniform_window(
        run_id=run_id,
        flow_group_ids=all_fids,
        origin_refs=DEFAULT_ORIGINS,
        start_bin=0,
        end_bin_exclusive=30,
        base_mbps=15.0,
    )
    snap10 = snap_creator.create_snapshot(
        run_id=run_id,
        cutoff=10,
        lookback_L=5,
        snapshot_as_of_utc="2026-11-12T09:00:10.100Z",
    )

    # Fault 1: Invalid request (batch size > B=4) -> REJECTED (INVALID_REQUEST, rule_count=0)
    req_too_many = ApplyRequest(
        run_id=run_id,
        decision_batch_id="f01_too_many",
        snapshot_id=snap10.snapshot_id,
        expected_state_version=1,
        config_version="1.0",
        forecast_start="2026-11-12T09:00:10Z",
        forecast_end="2026-11-12T09:00:20Z",
        apply_deadline_utc="2026-11-12T09:00:12Z",
        changes=tuple(
            ApplyFlowChange(fid, f"{fid}_P1", f"{fid}_P2") for fid in all_fids[:5]
        ),
        topology_version=topology.topology_version,
        topology_hash=topology.topology_hash,
    )
    r_f1 = handler.handle_apply_request(
        req_too_many, decision_time_utc="2026-11-12T09:00:10.200Z"
    )
    assert r_f1.status == BatchStatus.REJECTED
    assert r_f1.reason == ReasonCode.INVALID_REQUEST.value
    assert r_f1.rule_count == 0

    # Fault 2: Stale snapshot (decision_time 12.5s - snapshot_as_of 10.1s = 2.4s > T_stale 2.0s)
    req_stale = ApplyRequest(
        run_id=run_id,
        decision_batch_id="f02_stale",
        snapshot_id=snap10.snapshot_id,
        expected_state_version=1,
        config_version="1.0",
        forecast_start="2026-11-12T09:00:10Z",
        forecast_end="2026-11-12T09:00:20Z",
        apply_deadline_utc="2026-11-12T09:00:14Z",
        changes=(ApplyFlowChange("0x103", "0x103_P1", "0x103_P2"),),
        topology_version=topology.topology_version,
        topology_hash=topology.topology_hash,
    )
    r_f2 = handler.handle_apply_request(
        req_stale, decision_time_utc="2026-11-12T09:00:12.500Z"
    )
    assert r_f2.status == BatchStatus.REJECTED
    assert r_f2.reason == ReasonCode.STALE.value
    assert r_f2.rule_count == 0

    # Fault 3: Version mismatch (expected_state_version=99 != 1)
    req_ver = ApplyRequest(
        run_id=run_id,
        decision_batch_id="f03_ver_mismatch",
        snapshot_id=snap10.snapshot_id,
        expected_state_version=99,
        config_version="1.0",
        forecast_start="2026-11-12T09:00:10Z",
        forecast_end="2026-11-12T09:00:20Z",
        apply_deadline_utc="2026-11-12T09:00:12Z",
        changes=(ApplyFlowChange("0x103", "0x103_P1", "0x103_P2"),),
        topology_version=topology.topology_version,
        topology_hash=topology.topology_hash,
    )
    r_f3 = handler.handle_apply_request(
        req_ver, decision_time_utc="2026-11-12T09:00:10.200Z"
    )
    assert r_f3.status == BatchStatus.REJECTED
    assert r_f3.reason == ReasonCode.VERSION_MISMATCH.value
    assert r_f3.rule_count == 0

    # Execute a valid batch on 0x103 -> APPLIED (state_version=2)
    req_ok = ApplyRequest(
        run_id=run_id,
        decision_batch_id="f04_idem_batch",
        snapshot_id=snap10.snapshot_id,
        expected_state_version=1,
        config_version="1.0",
        forecast_start="2026-11-12T09:00:10Z",
        forecast_end="2026-11-12T09:00:20Z",
        apply_deadline_utc="2026-11-12T09:00:12Z",
        changes=(ApplyFlowChange("0x103", "0x103_P1", "0x103_P2"),),
        topology_version=topology.topology_version,
        topology_hash=topology.topology_hash,
    )
    r_ok = handler.handle_apply_request(
        req_ok,
        decision_time_utc="2026-11-12T09:00:10.200Z",
        applied_at_utc="2026-11-12T09:00:10.300Z",
    )
    assert r_ok.status == BatchStatus.APPLIED
    assert r_ok.state_version == 2

    # Fault 4: Duplicate decision_batch_id with same payload -> idempotent replay
    r_f4 = handler.handle_apply_request(
        req_ok, decision_time_utc="2026-11-12T09:00:10.400Z"
    )
    assert r_f4.status == BatchStatus.APPLIED
    assert r_f4.idempotent_replay is True
    assert r_f4.state_version == 2

    # Fault 5: Duplicate decision_batch_id with different payload -> BATCH_ID_CONFLICT
    req_conflict = ApplyRequest(
        run_id=run_id,
        decision_batch_id="f04_idem_batch",
        snapshot_id=snap10.snapshot_id,
        expected_state_version=1,
        config_version="1.0",
        forecast_start="2026-11-12T09:00:10Z",
        forecast_end="2026-11-12T09:00:20Z",
        apply_deadline_utc="2026-11-12T09:00:12Z",
        changes=(ApplyFlowChange("0x104", "0x104_P1", "0x104_P2"),),
        topology_version=topology.topology_version,
        topology_hash=topology.topology_hash,
    )
    r_f5 = handler.handle_apply_request(
        req_conflict, decision_time_utc="2026-11-12T09:00:10.500Z"
    )
    assert r_f5.status == BatchStatus.REJECTED
    assert r_f5.reason == ReasonCode.BATCH_ID_CONFLICT.value
    assert r_f5.rule_count == 0

    # Fault 6: Concurrent / in-flight request -> BUSY
    state_mgr.begin_batch_execution(
        run_id,
        "in_flight_other",
        started_at_utc="2026-11-12T09:00:10.600Z",
        expected_state_version=2,
        payload_hash="dummy_hash",
    )
    req_busy = ApplyRequest(
        run_id=run_id,
        decision_batch_id="f06_while_busy",
        snapshot_id=snap10.snapshot_id,
        expected_state_version=2,
        config_version="1.0",
        forecast_start="2026-11-12T09:00:10Z",
        forecast_end="2026-11-12T09:00:20Z",
        apply_deadline_utc="2026-11-12T09:00:12Z",
        changes=(ApplyFlowChange("0x104", "0x104_P1", "0x104_P2"),),
        topology_version=topology.topology_version,
        topology_hash=topology.topology_hash,
    )
    r_f6 = handler.handle_apply_request(
        req_busy, decision_time_utc="2026-11-12T09:00:10.700Z"
    )
    assert r_f6.status == BatchStatus.REJECTED
    assert r_f6.reason == ReasonCode.BUSY.value
    assert r_f6.rule_count == 0
    handler.recover_and_reconcile_run(run_id, "2026-11-12T09:00:10.750Z")

    # Fault 7 & Fault 9: FlowMod timeout on primary source edge -> Rollback verified (ROLLED_BACK)
    snap_f7 = snap_creator.create_snapshot(
        run_id=run_id,
        cutoff=10,
        lookback_L=5,
        snapshot_as_of_utc="2026-11-12T09:00:10.760Z",
    )
    driver.inject_flowmod_failure(
        dpid=1,
        flow_group_id="0x104",
        phase=SwitchPhaseTag.PRIMARY_SOURCE_EDGE,
        reason="FLOWMOD_TIMEOUT",
        timed_out=True,
    )
    req_f7 = ApplyRequest(
        run_id=run_id,
        decision_batch_id="f07_flowmod_timeout_rb",
        snapshot_id=snap_f7.snapshot_id,
        expected_state_version=state_mgr.get_state_version(run_id),
        config_version="1.0",
        forecast_start="2026-11-12T09:00:10Z",
        forecast_end="2026-11-12T09:00:20Z",
        apply_deadline_utc="2026-11-12T09:00:12Z",
        changes=(ApplyFlowChange("0x104", "0x104_P1", "0x104_P2"),),
        topology_version=topology.topology_version,
        topology_hash=topology.topology_hash,
    )
    r_f7 = handler.handle_apply_request(
        req_f7,
        decision_time_utc="2026-11-12T09:00:10.800Z",
        applied_at_utc="2026-11-12T09:00:10.900Z",
    )
    driver.clear_faults()
    assert r_f7.status == BatchStatus.ROLLED_BACK
    assert r_f7.reason == ReasonCode.VERIFY_TIMEOUT.value
    assert r_f7.results[0].status == FlowDecisionStatus.ROLLED_BACK
    assert r_f7.results[0].actual_path_id == "0x104_P1"
    assert r_f7.results[0].verification_status == VerificationStatus.VERIFIED

    # Fault 8: Verification failure (active probe failure on 0x105_P2) -> Rollback to 0x105_P1 verified
    snap_f8 = snap_creator.create_snapshot(
        run_id=run_id,
        cutoff=11,
        lookback_L=5,
        snapshot_as_of_utc="2026-11-12T09:00:11.060Z",
    )
    driver.inject_forwarding_probe_failure(
        flow_group_id="0x105",
        path_id="0x105_P2",
        reason="PROBE_LOSS_ON_P2",
        timed_out=False,
    )
    req_f8 = ApplyRequest(
        run_id=run_id,
        decision_batch_id="f08_probe_fail_rb",
        snapshot_id=snap_f8.snapshot_id,
        expected_state_version=state_mgr.get_state_version(run_id),
        config_version="1.0",
        forecast_start="2026-11-12T09:00:11Z",
        forecast_end="2026-11-12T09:00:21Z",
        apply_deadline_utc="2026-11-12T09:00:13Z",
        changes=(ApplyFlowChange("0x105", "0x105_P1", "0x105_P2"),),
        topology_version=topology.topology_version,
        topology_hash=topology.topology_hash,
    )
    r_f8 = handler.handle_apply_request(
        req_f8,
        decision_time_utc="2026-11-12T09:00:11.100Z",
        applied_at_utc="2026-11-12T09:00:11.200Z",
    )
    driver.clear_faults()
    assert r_f8.status == BatchStatus.ROLLED_BACK
    assert r_f8.reason == ReasonCode.VERIFY_FAILED.value
    assert r_f8.results[0].actual_path_id == "0x105_P1"

    # Fault 10 & Fault 11: UNKNOWN (FlowStats timeout on both primary & rollback) + Reconcile + Controller Restart
    snap_f10 = snap_creator.create_snapshot(
        run_id=run_id,
        cutoff=11,
        lookback_L=5,
        snapshot_as_of_utc="2026-11-12T09:00:11.350Z",
    )
    driver.inject_flow_stats_fault(dpid=1, flow_group_id="0x106", timed_out=True)
    req_f10 = ApplyRequest(
        run_id=run_id,
        decision_batch_id="f10_unknown",
        snapshot_id=snap_f10.snapshot_id,
        expected_state_version=state_mgr.get_state_version(run_id),
        config_version="1.0",
        forecast_start="2026-11-12T09:00:11Z",
        forecast_end="2026-11-12T09:00:21Z",
        apply_deadline_utc="2026-11-12T09:00:13Z",
        changes=(ApplyFlowChange("0x106", "0x106_P1", "0x106_P2"),),
        topology_version=topology.topology_version,
        topology_hash=topology.topology_hash,
    )
    r_f10 = handler.handle_apply_request(
        req_f10,
        decision_time_utc="2026-11-12T09:00:11.400Z",
        applied_at_utc="2026-11-12T09:00:11.500Z",
    )
    driver.clear_faults()
    assert r_f10.status == BatchStatus.INCONSISTENT
    assert r_f10.results[0].status == FlowDecisionStatus.UNKNOWN
    assert r_f10.results[0].actual_path_id is None
    assert state_mgr.is_reconcile_required(run_id) is True

    # Simulate controller restart and recover/reconcile from switch tables
    # (Because primary verification timed out, RuleInstaller rolled back switch rules to 0x106_P1 before
    # rollback verification also timed out; thus switch read-back reconciles 0x106 to 0x106_P1!)
    handler.notify_controller_restart(run_id, "2026-11-12T09:00:11.700Z")
    reconciled = handler.recover_and_reconcile_run(
        run_id, "2026-11-12T09:00:11.800Z"
    )
    assert reconciled.reconcile_required is False
    assert reconciled.routing_state_complete is True
    assert state_mgr.get_flow_state(run_id, "0x106").path_id == "0x106_P1"

    # Fault 12: Missing / late demand in [30s, 35s) -> INCOMPLETE_HISTORY, requested_mbps is None (never 0.0)
    snap_missing = snap_creator.create_snapshot(
        run_id=run_id,
        cutoff=35,
        lookback_L=5,
        snapshot_as_of_utc="2026-11-12T09:00:35.100Z",
    )
    assert snap_missing.quality_status == SnapshotQualityStatus.INCOMPLETE_HISTORY
    assert snap_missing.is_valid is False
    for h in snap_missing.history:
        assert h.requested_mbps is None
        assert h.valid is False

    # Fault 13: Exception after network modification before state commit -> INCONSISTENT (never fake REJECTED)
    orig_commit = state_mgr.commit_batch_outcome

    def _boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("CRASH_BEFORE_STATE_COMMIT")

    monkeypatch.setattr(state_mgr, "commit_batch_outcome", _boom)
    snap20 = snap_creator.create_snapshot(
        run_id=run_id,
        cutoff=20,
        lookback_L=5,
        snapshot_as_of_utc="2026-11-12T09:00:20.100Z",
    )
    req_f13 = ApplyRequest(
        run_id=run_id,
        decision_batch_id="f13_post_switch_crash",
        snapshot_id=snap20.snapshot_id,
        expected_state_version=state_mgr.get_state_version(run_id),
        config_version="1.0",
        forecast_start="2026-11-12T09:00:20Z",
        forecast_end="2026-11-12T09:00:30Z",
        apply_deadline_utc="2026-11-12T09:00:22Z",
        changes=(ApplyFlowChange("0x203", "0x203_P1", "0x203_P2"),),
        topology_version=topology.topology_version,
        topology_hash=topology.topology_hash,
    )
    r_f13 = handler.handle_apply_request(
        req_f13,
        decision_time_utc="2026-11-12T09:00:20.200Z",
        applied_at_utc="2026-11-12T09:00:20.300Z",
    )
    monkeypatch.setattr(state_mgr, "commit_batch_outcome", orig_commit)
    assert r_f13.status == BatchStatus.INCONSISTENT
    assert r_f13.status != BatchStatus.REJECTED
    assert r_f13.results[0].status == FlowDecisionStatus.UNKNOWN
    assert r_f13.results[0].actual_path_id is None

    # Reconciling after Fault 13 discovers that switch actually switched 0x203 to 0x203_P2!
    reconciled_f13 = handler.recover_and_reconcile_run(
        run_id, "2026-11-12T09:00:20.500Z"
    )
    assert reconciled_f13.reconcile_required is False
    assert state_mgr.get_flow_state(run_id, "0x203").path_id == "0x203_P2"


# =============================================================================
# PART 4: Schema Serialization, Deserialization & Strict Contract Validation
# =============================================================================


def test_p5_05_schema_serialization_roundtrip_and_strict_validation(
    topology: TopologyRegistry,
) -> None:
    # 1. Timestamp validation & normalization (+00:00 -> Z, reject local/naive)
    assert (
        validate_rfc3339_utc_timestamp("2026-11-12T09:00:10+00:00", normalize=True)
        == "2026-11-12T09:00:10Z"
    )
    assert (
        validate_rfc3339_utc_timestamp(
            "2026-11-12T09:00:10.250000+00:00", normalize=True
        )
        == "2026-11-12T09:00:10.25Z"
    )
    for bad_ts in (
        "2026-11-12T09:00:10",  # naive (no timezone)
        "2026-11-12T16:00:10+07:00",  # non-UTC offset
        "not-a-timestamp",
        "",
    ):
        with pytest.raises(ContractSerializationError):
            validate_rfc3339_utc_timestamp(bad_ts)

    # 2. Canonical ID validation (reject legacy flow_group_id, path_id, link_id)
    assert validate_canonical_flow_group_id("0x103") == "0x103"
    assert validate_canonical_path_id("0x103_P2", expected_flow_group_id="0x103") == "0x103_P2"
    assert validate_canonical_link_id("e1>c1") == "e1>c1"

    for bad_fg in ("h1_h3", "h1->h3", "f13", "0x101", "0x102", "0x999"):
        with pytest.raises(ContractSerializationError):
            validate_canonical_flow_group_id(bad_fg)

    for bad_path in ("P1", "P2", "h1_h3_c1", "0x103_P3", "0x102_P1"):
        with pytest.raises(ContractSerializationError):
            validate_canonical_path_id(bad_path)

    for bad_link in ("e1_to_c1", "e1-c1", "e1->c1", "e1_c1"):
        with pytest.raises(ContractSerializationError):
            validate_canonical_link_id(bad_link)

    # 3. ApplyRequest & ApplyResponse JSON round-trip and key-order hash invariance
    req = ApplyRequest(
        run_id="run_ser_01",
        decision_batch_id="batch_ser_01",
        snapshot_id="snap:run_ser_01:t10:L5:v1",
        expected_state_version=1,
        config_version="1.0",
        forecast_start="2026-11-12T09:00:10Z",
        forecast_end="2026-11-12T09:00:20Z",
        apply_deadline_utc="2026-11-12T09:00:12Z",
        changes=(ApplyFlowChange("0x103", "0x103_P1", "0x103_P2"),),
        topology_version=topology.topology_version,
        topology_hash=topology.topology_hash,
    )
    req_roundtrip = ApplyRequest.from_json(req.to_json(), strict=True)
    assert req_roundtrip.to_dict() == req.to_dict()
    assert req_roundtrip.effective_payload_hash == req.effective_payload_hash

    # Reversed dictionary key order and +00:00 timestamp format must produce identical payload_hash
    reversed_dict = dict(reversed(list(req.to_dict().items())))
    reversed_dict["forecast_start"] = "2026-11-12T09:00:10+00:00"
    assert compute_apply_payload_hash(reversed_dict) == req.effective_payload_hash

    resp = ApplyResponse(
        run_id="run_ser_01",
        decision_batch_id="batch_ser_01",
        status=BatchStatus.APPLIED,
        state_version=2,
        completed_at="2026-11-12T09:00:10.4Z",
        results=(
            ApplyFlowResult(
                flow_group_id="0x103",
                actual_path_id="0x103_P2",
                status=FlowDecisionStatus.APPLIED,
                verification_status=VerificationStatus.VERIFIED,
                reason=None,
                rule_count=3,
                applied_at="2026-11-12T09:00:10.3Z",
                old_path_id="0x103_P1",
                proposed_path_id="0x103_P2",
            ),
        ),
        rule_count=3,
        payload_hash=req.effective_payload_hash,
        is_network_atomic=True,
    )
    resp_roundtrip = ApplyResponse.from_json(resp.to_json(), strict=True)
    assert resp_roundtrip == resp

    # 4. Reject invalid numeric types (bool as int/float, NaN, negative) and nullability violations
    bad_req_bool_sv = req.to_dict()
    bad_req_bool_sv["expected_state_version"] = True
    with pytest.raises(ContractSerializationError):
        ApplyRequest.from_dict(bad_req_bool_sv, strict=True)

    # Reject UNKNOWN ApplyFlowResult with faked actual_path_id
    with pytest.raises(ContractSerializationError):
        ApplyFlowResult.from_dict(
            {
                "flow_group_id": "0x103",
                "actual_path_id": "0x103_P1",  # FORBIDDEN when status is UNKNOWN!
                "status": "UNKNOWN",
                "verification_status": "UNKNOWN",
                "reason": "VERIFY_TIMEOUT",
                "rule_count": 3,
                "applied_at": "2026-11-12T09:00:10.3Z",
            },
            strict=True,
        )

    # Reject SnapshotHistoryItem where valid=False but requested_mbps=0.0 (zero-substitution forbidden!)
    with pytest.raises(ContractSerializationError):
        SnapshotHistoryItem.from_dict(
            {
                "flow_group_id": "0x103",
                "bin_index": 5,
                "window_start_utc": "2026-11-12T09:00:05Z",
                "window_end_utc": "2026-11-12T09:00:06Z",
                "requested_mbps": 0.0,
                "released_at_utc": None,
                "available_at_utc": None,
                "valid": False,
                "reason": "DATA_MISSING",
            },
            strict=True,
        )

    # Reject Snapshot with tampered content_hash
    driver = InMemorySwitchDriver(topology)
    compiler = RouteCompiler(topology, FlowMatcher(topology))
    verifier = RuleVerifier(compiler, driver)
    installer = RuleInstaller(compiler, driver, verifier)
    state_mgr = StateManager(topology)
    ev = installer.bootstrap_initial_paths("run_ser_01", "2026-11-12T09:00:00Z")
    state_mgr.initialize_run(
        "run_ser_01",
        "2026-11-12T09:00:00Z",
        verification_evidences=ev,
    )
    demand_provider = MockReleasedDemandProvider()
    demand_provider.populate_uniform_window(
        run_id="run_ser_01",
        flow_group_ids=[fg.flow_group_id for fg in topology.get_all_flow_groups()],
        origin_refs=DEFAULT_ORIGINS,
        start_bin=5,
        end_bin_exclusive=10,
        base_mbps=12.5,
    )
    snap = SnapshotCreator(topology, state_mgr, demand_provider).create_snapshot(
        "run_ser_01", cutoff=10, lookback_L=5
    )
    snap_roundtrip = Snapshot.from_json(snap.to_json(), strict=True, verify_hash=True)
    assert snap_roundtrip == snap

    tampered = snap.to_dict()
    tampered["history"][0]["requested_mbps"] = 999.0
    with pytest.raises(ContractSerializationError):
        Snapshot.from_dict(tampered, strict=True, verify_hash=True)


# =============================================================================
# PART 5: Crash Recovery & Durability Review (Switch Table Retained vs. Wiped)
# =============================================================================


def test_p5_06_crash_recovery_when_switch_tables_wiped_vs_retained(
    topology: TopologyRegistry,
) -> None:
    run_id = "run_crash_wipe"
    driver = InMemorySwitchDriver(topology)
    matcher = FlowMatcher(topology)
    compiler = RouteCompiler(topology, matcher)
    verifier = RuleVerifier(compiler, driver)
    installer = RuleInstaller(compiler, driver, verifier)
    state_mgr = StateManager(topology)

    boot_ev = installer.bootstrap_initial_paths(run_id, "2026-11-12T09:00:00Z")
    state_mgr.initialize_run(
        run_id,
        "2026-11-12T09:00:00Z",
        verification_evidences=boot_ev,
    )
    handler = DecisionHandler(topology, state_mgr, installer)

    # Simulate a switch + controller power cycle where OpenFlow tables on all switches were wiped
    driver.clear_switch_flow_table()
    handler.notify_controller_restart(run_id, "2026-11-12T09:00:05Z")
    assert state_mgr.is_reconcile_required(run_id) is True

    # 1. Attempting recover_and_reconcile_run WITHOUT reinstall_missing_from_paths must fail
    # because switch tables are empty and cannot verify any active path!
    with pytest.raises(StateValidationError):
        handler.recover_and_reconcile_run(
            run_id,
            "2026-11-12T09:00:06Z",
            reinstall_missing_from_paths=None,
        )
    assert state_mgr.is_reconcile_required(run_id) is True

    # 2. Recovering WITH reinstall_missing_from_paths reinstalls initial P1 paths via two-phase ADD,
    # verifies all 48 flows, and unlocks reconciliation!
    init_paths = {
        fg.flow_group_id: topology.get_initial_candidate_path(fg.flow_group_id).path_id
        for fg in topology.get_all_flow_groups()
    }
    recovered = handler.recover_and_reconcile_run(
        run_id,
        "2026-11-12T09:00:07Z",
        reinstall_missing_from_paths=init_paths,
    )
    assert recovered.reconcile_required is False
    assert recovered.routing_state_complete is True
    assert len(recovered.unknown_flow_group_ids) == 0
    for r in recovered.routing_state:
        assert r.verification_status == VerificationStatus.VERIFIED
        assert r.path_id == f"{r.flow_group_id}_P1"
