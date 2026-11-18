"""
Unit, contract, and fault-injection tests for SDN Phase 3:
FlowMatcher, RouteCompiler, InMemorySwitchDriver, RuleVerifier, and RuleInstaller
(Windows Pure-Python / Simulated Switch Driver — PASS_MOCK).

Mapped 1-to-1 with the 12 mandatory Phase 3 test requirements:
1. test_01_compile_p1_and_p2_and_match_policies: Biên dịch P1 và P2.
2. test_02_switch_0x103_p1_to_p2: Chuyển P1 -> P2.
3. test_03_switch_0x103_p2_to_p1: Chuyển P2 -> P1.
4. test_04_barrier_failure_handling: Barrier thất bại.
5. test_05_flow_stats_mismatch_f12: FlowStats không khớp (F12).
6. test_06_counter_or_probe_unconfirmed: Counter/probe không xác nhận (AC-S03).
7. test_07_rollback_succeeded_and_verified: Rollback thành công và được verify.
8. test_08_rollback_unverified_returns_unknown: Rollback không xác minh được -> UNKNOWN.
9. test_09_switch_unresponsive_or_timeout_f09: Switch không phản hồi hoặc timeout (F09).
10. test_10_source_edge_never_updated_before_downstream_ready: Không xảy ra cập nhật source trước khi downstream sẵn sàng.
11. test_11_never_verified_from_incomplete_evidence: Không cho kết quả VERIFIED từ dữ liệu thiếu bằng chứng.
12. test_12_multi_flow_batch_non_atomicity_and_state_separation: Thay đổi nhiều flow, không tự tuyên bố atomicity toàn mạng, không tự sửa state_version.
"""

from __future__ import annotations

from pathlib import Path
import pytest

from sdn.controller.state_manager import StateManager
from sdn.controller.topology_registry import TopologyLookupError, TopologyRegistry
from sdn.flow_management import (
    ETH_TYPE_IPV4,
    FlowChangeSpec,
    FlowMatcher,
    FlowMatchPolicy,
    FlowModCommand,
    HopStage,
    IP_PROTO_UDP,
    InMemorySwitchDriver,
    RouteCompiler,
    RuleInstaller,
    SwitchPhaseTag,
)
from sdn.route_verification import RuleVerifier
from sdn.schemas.state_models import (
    BatchStatus,
    EvidenceMode,
    FlowDecisionStatus,
    ReasonCode,
    VerificationStatus,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
TOPOLOGY_BUNDLE_PATH = REPO_ROOT / "configs" / "network" / "topology_bundle.json"
CATALOG_DIR = REPO_ROOT / "configs" / "network"


@pytest.fixture(scope="module")
def topology_registry() -> TopologyRegistry:
    return TopologyRegistry.from_bundle_file(
        TOPOLOGY_BUNDLE_PATH,
        csv_config_dir=CATALOG_DIR,
        verify_hash=True,
    )


@pytest.fixture()
def phase3_stack(
    topology_registry: TopologyRegistry,
) -> tuple[RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager]:
    compiler = RouteCompiler(topology_registry)
    driver = InMemorySwitchDriver(topology_registry)
    verifier = RuleVerifier(compiler, driver)
    installer = RuleInstaller(compiler, driver, verifier)
    state_manager = StateManager(topology_registry)
    return compiler, driver, verifier, installer, state_manager


# -----------------------------------------------------------------------------
# 1. Biên dịch P1 và P2
# -----------------------------------------------------------------------------


def test_01_compile_p1_and_p2_and_match_policies(
    topology_registry: TopologyRegistry,
) -> None:
    matcher = FlowMatcher(topology_registry)
    compiler = RouteCompiler(topology_registry, matcher)

    p1_plan = compiler.compile_path("0x103_P1")
    p2_plan = compiler.compile_path("0x103_P2")

    # P1: h1 -> e1 (in=1, out=3) -> c1 (in=1, out=2) -> e2 (in=3, out=1) -> h3
    assert p1_plan.flow_group_id == "0x103"
    assert p1_plan.path_id == "0x103_P1"
    assert p1_plan.path_label == "P1"
    assert p1_plan.core_switch == "c1"
    assert p1_plan.ordered_link_ids == ("h1>e1", "e1>c1", "c1>e2", "e2>h3")
    assert p1_plan.fabric_link_ids == ("e1>c1", "c1>e2")
    assert len(p1_plan.downstream_rules) == 2
    assert p1_plan.downstream_rules[0].switch_id == "c1"
    assert p1_plan.downstream_rules[0].dpid == 5
    assert p1_plan.downstream_rules[0].hop_stage == HopStage.CORE
    assert p1_plan.downstream_rules[0].match.in_port == 1
    assert p1_plan.downstream_rules[0].out_port == 2
    assert p1_plan.downstream_rules[1].switch_id == "e2"
    assert p1_plan.downstream_rules[1].dpid == 2
    assert p1_plan.downstream_rules[1].hop_stage == HopStage.DEST_EDGE
    assert p1_plan.downstream_rules[1].match.in_port == 3
    assert p1_plan.downstream_rules[1].out_port == 1
    assert p1_plan.source_edge_rule.switch_id == "e1"
    assert p1_plan.source_edge_rule.dpid == 1
    assert p1_plan.source_edge_rule.hop_stage == HopStage.SOURCE_EDGE
    assert p1_plan.source_edge_rule.match.in_port == 1
    assert p1_plan.source_edge_rule.out_port == 3
    assert p1_plan.source_edge_rule.cookie_hex == "0x103"
    assert p1_plan.source_edge_rule.cookie_int == 0x103

    # P2: h1 -> e1 (in=1, out=4) -> c2 (in=1, out=2) -> e2 (in=4, out=1) -> h3
    assert p2_plan.flow_group_id == "0x103"
    assert p2_plan.path_id == "0x103_P2"
    assert p2_plan.path_label == "P2"
    assert p2_plan.core_switch == "c2"
    assert p2_plan.ordered_link_ids == ("h1>e1", "e1>c2", "c2>e2", "e2>h3")
    assert p2_plan.downstream_rules[0].switch_id == "c2"
    assert p2_plan.downstream_rules[0].dpid == 6
    assert p2_plan.downstream_rules[0].match.in_port == 1
    assert p2_plan.downstream_rules[0].out_port == 2
    assert p2_plan.downstream_rules[1].switch_id == "e2"
    assert p2_plan.downstream_rules[1].match.in_port == 4
    assert p2_plan.downstream_rules[1].out_port == 1
    assert p2_plan.source_edge_rule.switch_id == "e1"
    assert p2_plan.source_edge_rule.match.in_port == 1
    assert p2_plan.source_edge_rule.out_port == 4

    # Configurable L4_UDP_DPORT match policy ([MUST-LOCK] abstraction)
    fg_103 = topology_registry.get_flow_group("0x103")
    p1_l4 = compiler.compile_path("0x103_P1", policy_override=FlowMatchPolicy.L4_UDP_DPORT)
    m_l4 = p1_l4.source_edge_rule.match
    assert m_l4.eth_type == ETH_TYPE_IPV4
    assert m_l4.ip_proto == IP_PROTO_UDP
    assert m_l4.udp_dst == fg_103.udp_port == 5001
    assert m_l4.to_openflow_match_dict()["udp_dst"] == fg_103.udp_port

    with pytest.raises(TopologyLookupError):
        compiler.compile_path("P1")
    with pytest.raises(TopologyLookupError):
        compiler.compile_path("h1_h3_c1")


# -----------------------------------------------------------------------------
# 2. Chuyển P1 -> P2
# -----------------------------------------------------------------------------


def test_02_switch_0x103_p1_to_p2(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack
    boot_evidences = installer.bootstrap_initial_paths(
        "run_p1_p2", "2026-04-06T00:00:00Z"
    )
    state_manager.initialize_run(
        "run_p1_p2", "2026-04-06T00:00:00Z", verification_evidences=boot_evidences
    )

    outcome_p2 = installer.install_flow_change(
        run_id="run_p1_p2",
        flow_group_id="0x103",
        old_path_id="0x103_P1",
        new_path_id="0x103_P2",
        applied_at_utc="2026-04-06T00:01:00Z",
    )

    assert outcome_p2.status == FlowDecisionStatus.APPLIED
    assert outcome_p2.verification_status == VerificationStatus.VERIFIED
    assert outcome_p2.actual_path_id == "0x103_P2"
    assert outcome_p2.rule_count == 3
    assert outcome_p2.evidence is not None
    assert outcome_p2.evidence.evidence_mode == EvidenceMode.MOCK_SIMULATED
    assert outcome_p2.evidence.is_valid_for_verified is True

    b_status, ver = state_manager.commit_batch_outcome(
        run_id="run_p1_p2",
        decision_batch_id="batch_p1_to_p2",
        outcomes=[outcome_p2],
        completed_at_utc="2026-04-06T00:01:00Z",
    )
    assert b_status == BatchStatus.APPLIED
    assert ver == 2
    assert state_manager.get_flow_state("run_p1_p2", "0x103").path_id == "0x103_P2"


# -----------------------------------------------------------------------------
# 3. Chuyển P2 -> P1
# -----------------------------------------------------------------------------


def test_03_switch_0x103_p2_to_p1(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack
    boot_evidences = installer.bootstrap_initial_paths(
        "run_p2_p1", "2026-04-06T00:00:00Z"
    )
    state_manager.initialize_run(
        "run_p2_p1", "2026-04-06T00:00:00Z", verification_evidences=boot_evidences
    )

    # First move 0x103 to P2
    out_to_p2 = installer.install_flow_change(
        "run_p2_p1", "0x103", "0x103_P1", "0x103_P2", "2026-04-06T00:01:00Z"
    )
    state_manager.commit_batch_outcome(
        "run_p2_p1", "b_to_p2", [out_to_p2], "2026-04-06T00:01:00Z"
    )

    # Now switch P2 -> P1
    out_to_p1 = installer.install_flow_change(
        "run_p2_p1", "0x103", "0x103_P2", "0x103_P1", "2026-04-06T00:02:00Z"
    )
    assert out_to_p1.status == FlowDecisionStatus.APPLIED
    assert out_to_p1.verification_status == VerificationStatus.VERIFIED
    assert out_to_p1.actual_path_id == "0x103_P1"
    assert out_to_p1.rule_count == 3

    b_status, ver = state_manager.commit_batch_outcome(
        "run_p2_p1", "b_to_p1", [out_to_p1], "2026-04-06T00:02:00Z"
    )
    assert b_status == BatchStatus.APPLIED
    assert ver == 3
    assert state_manager.get_flow_state("run_p2_p1", "0x103").path_id == "0x103_P1"


# -----------------------------------------------------------------------------
# 4. Barrier thất bại
# -----------------------------------------------------------------------------


def test_04_barrier_failure_handling(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack
    boot_ev = installer.bootstrap_initial_paths("run_bar", "2026-04-06T00:00:00Z")
    state_manager.initialize_run(
        "run_bar", "2026-04-06T00:00:00Z", verification_evidences=boot_ev
    )

    # Case A: Downstream Barrier fails on c2 (dpid=6)
    driver.inject_barrier_failure(
        dpid=6, phase=SwitchPhaseTag.PRIMARY_DOWNSTREAM, reason="CORE_BARRIER_FAILED"
    )
    out_ds = installer.install_flow_change(
        "run_bar", "0x103", "0x103_P1", "0x103_P2", "2026-04-06T00:01:00Z"
    )
    assert out_ds.status == FlowDecisionStatus.ROLLED_BACK
    assert out_ds.actual_path_id == "0x103_P1"
    assert out_ds.reason == ReasonCode.VERIFY_FAILED.value

    # Case B: Source-Edge Barrier fails on e1 (dpid=1)
    driver.clear_faults()
    driver.inject_barrier_failure(
        dpid=1, phase=SwitchPhaseTag.PRIMARY_SOURCE_EDGE, reason="SRC_BARRIER_FAILED"
    )
    out_src = installer.install_flow_change(
        "run_bar", "0x103", "0x103_P1", "0x103_P2", "2026-04-06T00:01:10Z"
    )
    assert out_src.status == FlowDecisionStatus.ROLLED_BACK
    assert out_src.actual_path_id == "0x103_P1"
    assert out_src.had_intermediate_physical_change is True


# -----------------------------------------------------------------------------
# 5. FlowStats không khớp (F12)
# -----------------------------------------------------------------------------


def test_05_flow_stats_mismatch_f12(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack
    boot_ev = installer.bootstrap_initial_paths("run_f12", "2026-04-06T00:00:00Z")
    state_manager.initialize_run(
        "run_f12", "2026-04-06T00:00:00Z", verification_evidences=boot_ev
    )

    # F12: c2 (dpid=6) reports wrong out_port=99 in FlowStats
    driver.inject_flow_stats_fault(dpid=6, flow_group_id="0x103", wrong_out_port=99)

    outcome = installer.install_flow_change(
        "run_f12", "0x103", "0x103_P1", "0x103_P2", "2026-04-06T00:01:00Z"
    )
    assert outcome.status == FlowDecisionStatus.ROLLED_BACK
    assert outcome.verification_status == VerificationStatus.VERIFIED
    assert outcome.actual_path_id == "0x103_P1"
    assert outcome.reason == ReasonCode.VERIFY_FAILED.value


# -----------------------------------------------------------------------------
# 6. Counter/probe không xác nhận (AC-S03)
# -----------------------------------------------------------------------------


def test_06_counter_or_probe_unconfirmed(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack
    boot_ev = installer.bootstrap_initial_paths("run_probe", "2026-04-06T00:00:00Z")
    state_manager.initialize_run(
        "run_probe", "2026-04-06T00:00:00Z", verification_evidences=boot_ev
    )

    # FlowMods, Barriers, and FlowStats succeed on 0x103_P2, but counter/probe check fails
    driver.inject_forwarding_probe_failure(
        flow_group_id="0x103",
        path_id="0x103_P2",
        reason="ZERO_COUNTER_AND_PROBE_LOSS",
    )

    outcome = installer.install_flow_change(
        "run_probe", "0x103", "0x103_P1", "0x103_P2", "2026-04-06T00:01:00Z"
    )
    assert outcome.status == FlowDecisionStatus.ROLLED_BACK
    assert outcome.verification_status == VerificationStatus.VERIFIED
    assert outcome.actual_path_id == "0x103_P1"
    assert outcome.reason == ReasonCode.VERIFY_FAILED.value


# -----------------------------------------------------------------------------
# 7. Rollback thành công và được verify
# -----------------------------------------------------------------------------


def test_07_rollback_succeeded_and_verified(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack
    boot_ev = installer.bootstrap_initial_paths("run_rb_ok", "2026-04-06T00:00:00Z")
    state_manager.initialize_run(
        "run_rb_ok", "2026-04-06T00:00:00Z", verification_evidences=boot_ev
    )

    # Inject FlowMod failure on source edge e1 (dpid=1) during PRIMARY_SOURCE_EDGE
    driver.inject_flowmod_failure(
        dpid=1,
        flow_group_id="0x103",
        phase=SwitchPhaseTag.PRIMARY_SOURCE_EDGE,
        reason="E1_FLOWMOD_REJECTED",
    )

    outcome = installer.install_flow_change(
        "run_rb_ok", "0x103", "0x103_P1", "0x103_P2", "2026-04-06T00:01:00Z"
    )
    assert outcome.status == FlowDecisionStatus.ROLLED_BACK
    assert outcome.verification_status == VerificationStatus.VERIFIED
    assert outcome.actual_path_id == "0x103_P1"
    assert outcome.evidence is not None
    assert outcome.evidence.verified_path_id == "0x103_P1"
    assert outcome.evidence.is_valid_for_verified is True
    # 3 primary FlowMods attempted (2 downstream + 1 failed source) + 3 rollback FlowMods = 6
    assert outcome.rule_count == 6

    b_status, ver = state_manager.commit_batch_outcome(
        "run_rb_ok", "batch_rb_ok", [outcome], "2026-04-06T00:01:00Z"
    )
    assert b_status == BatchStatus.ROLLED_BACK
    assert ver == 2
    assert state_manager.get_flow_state("run_rb_ok", "0x103").path_id == "0x103_P1"


# -----------------------------------------------------------------------------
# 8. Rollback không xác minh được -> UNKNOWN / INCONSISTENT
# -----------------------------------------------------------------------------


def test_08_rollback_unverified_returns_unknown(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack
    boot_ev = installer.bootstrap_initial_paths("run_rb_unk", "2026-04-06T00:00:00Z")
    state_manager.initialize_run(
        "run_rb_unk", "2026-04-06T00:00:00Z", verification_evidences=boot_ev
    )

    # Primary verification fails on P2 AND rollback FlowMod on e1 also fails
    driver.inject_forwarding_probe_failure("0x103", path_id="0x103_P2")
    driver.inject_flowmod_failure(
        dpid=1,
        flow_group_id="0x103",
        phase=SwitchPhaseTag.ROLLBACK_SOURCE_EDGE,
        reason="ROLLBACK_FLOWMOD_REJECTED",
    )

    outcome = installer.install_flow_change(
        "run_rb_unk", "0x103", "0x103_P1", "0x103_P2", "2026-04-06T00:01:00Z"
    )
    assert outcome.status == FlowDecisionStatus.UNKNOWN
    assert outcome.verification_status == VerificationStatus.UNKNOWN
    assert outcome.actual_path_id is None
    assert outcome.evidence is None
    assert outcome.reason == ReasonCode.ROLLBACK_FAILED.value

    b_status, ver = state_manager.commit_batch_outcome(
        "run_rb_unk", "batch_rb_unk", [outcome], "2026-04-06T00:01:00Z"
    )
    assert b_status == BatchStatus.INCONSISTENT
    assert ver == 2
    assert state_manager.is_reconcile_required("run_rb_unk") is True
    assert state_manager.get_flow_state("run_rb_unk", "0x103").path_id is None


# -----------------------------------------------------------------------------
# 9. Switch không phản hồi hoặc timeout (F09)
# -----------------------------------------------------------------------------


def test_09_switch_unresponsive_or_timeout_f09(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack
    boot_ev = installer.bootstrap_initial_paths("run_f09", "2026-04-06T00:00:00Z")
    state_manager.initialize_run(
        "run_f09", "2026-04-06T00:00:00Z", verification_evidences=boot_ev
    )

    # Switch e2 (dpid=2, destination edge for 0x103) becomes unresponsive
    driver.set_switch_unresponsive(dpid=2, unresponsive=True)

    outcome = installer.install_flow_change(
        "run_f09", "0x103", "0x103_P1", "0x103_P2", "2026-04-06T00:01:00Z"
    )
    assert outcome.status == FlowDecisionStatus.UNKNOWN
    assert outcome.verification_status == VerificationStatus.UNKNOWN
    assert outcome.actual_path_id is None
    assert outcome.reason == ReasonCode.VERIFY_TIMEOUT.value


# -----------------------------------------------------------------------------
# 10. Không xảy ra cập nhật source trước khi downstream sẵn sàng
# -----------------------------------------------------------------------------


def test_10_source_edge_never_updated_before_downstream_ready(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack
    installer.bootstrap_initial_paths("run_order", "2026-04-06T00:00:00Z")
    driver.clear_operation_log()

    # 1) On successful P1 -> P2: all PRIMARY_DOWNSTREAM FlowMods & Barriers precede PRIMARY_SOURCE_EDGE
    installer.install_flow_change(
        "run_order", "0x103", "0x103_P1", "0x103_P2", "2026-04-06T00:01:00Z"
    )
    ops_ok = driver.get_operation_log()
    ds_mods = [o for o in ops_ok if o.op_type == "FLOW_MOD" and o.phase == SwitchPhaseTag.PRIMARY_DOWNSTREAM]
    ds_bars = [o for o in ops_ok if o.op_type == "BARRIER" and o.phase == SwitchPhaseTag.PRIMARY_DOWNSTREAM]
    src_mods = [o for o in ops_ok if o.op_type == "FLOW_MOD" and o.phase == SwitchPhaseTag.PRIMARY_SOURCE_EDGE]
    assert max(o.seq for o in ds_mods) < min(o.seq for o in ds_bars)
    assert max(o.seq for o in ds_bars) < min(o.seq for o in src_mods)

    # 2) When downstream FlowMod on e2 (dpid=2) fails: zero PRIMARY_SOURCE_EDGE operations occur!
    driver.clear_operation_log()
    driver.inject_flowmod_failure(
        dpid=2,
        flow_group_id="0x103",
        phase=SwitchPhaseTag.PRIMARY_DOWNSTREAM,
        reason="DOWNSTREAM_E2_REJECTED",
    )
    installer.install_flow_change(
        "run_order", "0x103", "0x103_P2", "0x103_P1", "2026-04-06T00:02:00Z"
    )
    ops_fail = driver.get_operation_log()
    assert [o for o in ops_fail if o.phase == SwitchPhaseTag.PRIMARY_SOURCE_EDGE] == []


# -----------------------------------------------------------------------------
# 11. Không cho kết quả VERIFIED từ dữ liệu thiếu bằng chứng
# -----------------------------------------------------------------------------


def test_11_never_verified_from_incomplete_evidence(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack
    installer.bootstrap_initial_paths("run_inc_ev", "2026-04-06T00:00:00Z")

    # 1) barrier_confirmed=False -> never VERIFIED
    res_no_bar = verifier.verify_flow_path(
        "run_inc_ev", "0x103", "0x103_P1", "2026-04-06T00:00:10Z", barrier_confirmed=False
    )
    assert res_no_bar.is_verified is False
    assert res_no_bar.verification_status == VerificationStatus.VERIFY_FAILED
    assert res_no_bar.evidence is None

    # 2) Missing rule in FlowStats -> never VERIFIED
    driver.inject_flow_stats_fault(dpid=5, flow_group_id="0x103", omit_rule=True)
    res_no_stats = verifier.verify_flow_path(
        "run_inc_ev", "0x103", "0x103_P1", "2026-04-06T00:00:10Z", barrier_confirmed=True
    )
    assert res_no_stats.is_verified is False
    assert res_no_stats.rule_readback_ok is False
    assert res_no_stats.evidence is None

    # 3) FlowStats ok, but counter/probe fails -> never VERIFIED
    driver.clear_faults()
    driver.inject_forwarding_probe_failure("0x103", path_id="0x103_P1")
    res_no_probe = verifier.verify_flow_path(
        "run_inc_ev", "0x103", "0x103_P1", "2026-04-06T00:00:10Z", barrier_confirmed=True
    )
    assert res_no_probe.is_verified is False
    assert res_no_probe.rule_readback_ok is True
    assert res_no_probe.counter_or_probe_ok is False
    assert res_no_probe.evidence is None


# -----------------------------------------------------------------------------
# 12. Thay đổi nhiều flow, không tự tuyên bố atomicity toàn mạng & tách biệt StateManager
# -----------------------------------------------------------------------------


def test_12_multi_flow_batch_non_atomicity_and_state_separation(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack
    boot_ev = installer.bootstrap_initial_paths("run_batch_m", "2026-04-06T00:00:00Z")
    state_manager.initialize_run(
        "run_batch_m", "2026-04-06T00:00:00Z", verification_evidences=boot_ev
    )
    driver.clear_operation_log()

    # Inject probe failure ONLY for 0x104 on P2; 0x103 on P2 succeeds
    driver.inject_forwarding_probe_failure("0x104", path_id="0x104_P2")

    batch_res = installer.install_batch(
        run_id="run_batch_m",
        decision_batch_id="batch_multi_01",
        changes=[
            FlowChangeSpec("0x103", "0x103_P1", "0x103_P2"),
            FlowChangeSpec("0x104", "0x104_P1", "0x104_P2"),
        ],
        applied_at_utc="2026-04-06T00:01:00Z",
    )

    # RuleInstaller does NOT claim network-wide atomicity and does NOT mutate StateManager directly
    assert batch_res.is_network_atomic is False
    assert state_manager.get_state_version("run_batch_m") == 1
    assert batch_res.suggested_batch_status == BatchStatus.PARTIAL
    assert batch_res.total_rule_count == 9  # 3 for 0x103 + 6 for 0x104

    # Untouched flows outside {"0x103", "0x104"} had zero FlowMods (AC-S06)
    touched_flows = {
        op.flow_group_id
        for op in driver.get_operation_log()
        if op.op_type == "FLOW_MOD"
    }
    assert touched_flows == {"0x103", "0x104"}

    # Handoff to StateManager commits PARTIAL and locks reconcile_required
    b_status, ver = state_manager.commit_batch_outcome(
        run_id="run_batch_m",
        decision_batch_id="batch_multi_01",
        outcomes=batch_res.outcomes,
        completed_at_utc=batch_res.completed_at_utc,
    )
    assert b_status == BatchStatus.PARTIAL
    assert ver == 2
    assert state_manager.is_reconcile_required("run_batch_m") is True


# -----------------------------------------------------------------------------
# 13. Audit Phase 3: OFPFC_MODIFY_STRICT khi rule chưa tồn tại trên switch
# -----------------------------------------------------------------------------


def test_13_modify_strict_on_nonexistent_rule_fails_and_does_not_create_rule(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack
    p1_plan = compiler.compile_path("0x103_P1")
    src_rule = p1_plan.source_edge_rule  # e1 (dpid=1)

    assert driver.get_installed_rule_count(dpid=1) == 0

    # Attempting MODIFY_STRICT on an empty switch table MUST NOT create a rule!
    rec = driver.send_flow_mod(
        src_rule,
        FlowModCommand.MODIFY_STRICT,
        phase=SwitchPhaseTag.PRIMARY_SOURCE_EDGE,
    )
    assert rec.accepted is False
    assert rec.error_reason == "OFPFMFC_UNKNOWN_STRICT_ENTRY_NOT_FOUND"
    assert driver.get_installed_rule_count(dpid=1) == 0
    assert driver.read_flow_stats(dpid=1, flow_group_id="0x103") == ()


# -----------------------------------------------------------------------------
# 14. Audit Phase 3: OFPFC_ADD, OFPFC_MODIFY_STRICT, OFPFC_DELETE_STRICT khi
#     rule chưa tồn tại và khi rule đã tồn tại với cùng (table_id, priority, match)
# -----------------------------------------------------------------------------


def test_14_add_when_rule_missing_and_replace_when_same_match_priority_exists(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack
    p1_plan = compiler.compile_path("0x103_P1")
    p2_plan = compiler.compile_path("0x103_P2")

    # 1) Install all 3 hops of P1 via OFPFC_ADD when rules do not exist
    for hop_rule in p1_plan.ordered_hop_rules:
        rec_add = driver.send_flow_mod(
            hop_rule, FlowModCommand.ADD, phase=SwitchPhaseTag.BOOTSTRAP
        )
        assert rec_add.accepted is True
        assert driver.has_pending_barrier(hop_rule.dpid) is True
        bar_rec = driver.send_barrier(hop_rule.dpid, phase=SwitchPhaseTag.BOOTSTRAP)
        assert bar_rec.confirmed is True
        assert driver.has_pending_barrier(hop_rule.dpid) is False

    assert driver.get_installed_rule_count(flow_group_id="0x103") == 3
    e1_stats_initial = driver.read_flow_stats(dpid=1, flow_group_id="0x103")
    assert len(e1_stats_initial) == 1
    assert e1_stats_initial[0].out_port == 3
    assert e1_stats_initial[0].packet_count == 0

    # Simulate forwarding packet on P1 so packet_count increments to 1
    fwd = driver.verify_forwarding_counter_or_probe(p1_plan)
    assert fwd.counter_or_probe_ok is True
    assert driver.read_flow_stats(dpid=1, flow_group_id="0x103")[0].packet_count == 1

    # 2) Rule already exists on e1 with identical (table_id=0, priority=100, match):
    #    OFPFC_MODIFY_STRICT updates out_port (3 -> 4) in-place and PRESERVES counters!
    assert p1_plan.source_edge_rule.table_entry_key == p2_plan.source_edge_rule.table_entry_key
    rec_mod = driver.send_flow_mod(
        p2_plan.source_edge_rule,
        FlowModCommand.MODIFY_STRICT,
        phase=SwitchPhaseTag.PRIMARY_SOURCE_EDGE,
    )
    assert rec_mod.accepted is True
    assert driver.get_installed_rule_count(dpid=1, flow_group_id="0x103") == 1
    e1_stats_after_mod = driver.read_flow_stats(dpid=1, flow_group_id="0x103")
    assert len(e1_stats_after_mod) == 1
    assert e1_stats_after_mod[0].out_port == 4
    assert e1_stats_after_mod[0].packet_count == 1

    # 3) OFPFC_ADD when rule already exists with identical (table_id, priority, match):
    #    Replaces instructions (out_port 4 -> 3) and resets counters to 0 without duplicating entry
    rec_replace = driver.send_flow_mod(
        p1_plan.source_edge_rule,
        FlowModCommand.ADD,
        phase=SwitchPhaseTag.BOOTSTRAP,
    )
    assert rec_replace.accepted is True
    assert driver.get_installed_rule_count(dpid=1, flow_group_id="0x103") == 1
    e1_stats_after_add = driver.read_flow_stats(dpid=1, flow_group_id="0x103")
    assert len(e1_stats_after_add) == 1
    assert e1_stats_after_add[0].out_port == 3
    assert e1_stats_after_add[0].packet_count == 0

    # 4) OFPFC_DELETE_STRICT with non-matching out_port (4 != 3) leaves rule untouched;
    #    OFPFC_DELETE_STRICT with matching out_port (3 == 3) removes the rule.
    driver.send_flow_mod(
        p2_plan.source_edge_rule,
        FlowModCommand.DELETE_STRICT,
        phase=SwitchPhaseTag.CLEANUP,
    )
    assert driver.get_installed_rule_count(dpid=1, flow_group_id="0x103") == 1

    driver.send_flow_mod(
        p1_plan.source_edge_rule,
        FlowModCommand.DELETE_STRICT,
        phase=SwitchPhaseTag.CLEANUP,
    )
    assert driver.get_installed_rule_count(dpid=1, flow_group_id="0x103") == 0


# -----------------------------------------------------------------------------
# 15. Audit Phase 3: Chuyển P1 -> P2 từ bảng flow hợp lệ vs. khi thiếu rule
#     downstream vs. khi thiếu rule source-edge
# -----------------------------------------------------------------------------


def test_15_switch_p1_to_p2_from_valid_table_vs_missing_downstream_table(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack

    # Case A: Bootstrap with ONLY P1 installed (`preinstall_both_downstream_branches=False`)
    #         -> 48 * 3 = 144 rules total; c2 (dpid=6) has 0 rules initially!
    boot_ev = installer.bootstrap_initial_paths(
        "run_no_pre_p2",
        "2026-04-06T00:00:00Z",
        preinstall_both_downstream_branches=False,
    )
    state_manager.initialize_run(
        "run_no_pre_p2", "2026-04-06T00:00:00Z", verification_evidences=boot_ev
    )
    assert driver.get_installed_rule_count() == 144
    assert driver.get_installed_rule_count(dpid=6) == 0
    assert driver.get_installed_rule_count(flow_group_id="0x103") == 3

    driver.clear_operation_log()
    # Switch 0x103: P1 -> P2 when P2 downstream rules do NOT exist on c2 (dpid=6) or e2 (dpid=2, in_port=4)
    out_p2 = installer.install_flow_change(
        run_id="run_no_pre_p2",
        flow_group_id="0x103",
        old_path_id="0x103_P1",
        new_path_id="0x103_P2",
        applied_at_utc="2026-04-06T00:01:00Z",
    )
    assert out_p2.status == FlowDecisionStatus.APPLIED
    assert out_p2.verification_status == VerificationStatus.VERIFIED
    assert out_p2.actual_path_id == "0x103_P2"
    assert out_p2.rule_count == 3

    # Verify exact OpenFlow command sequence:
    # 1) OFPFC_ADD on downstream switches (c2, e2)
    # 2) BARRIER on downstream switches
    # 3) OFPFC_MODIFY_STRICT on source-edge switch (e1)
    # 4) BARRIER on source-edge switch
    ops = driver.get_operation_log()
    flow_mods = [o for o in ops if o.op_type == "FLOW_MOD"]
    assert len(flow_mods) == 3
    assert flow_mods[0].phase == SwitchPhaseTag.PRIMARY_DOWNSTREAM
    assert flow_mods[0].command == FlowModCommand.ADD
    assert flow_mods[1].phase == SwitchPhaseTag.PRIMARY_DOWNSTREAM
    assert flow_mods[1].command == FlowModCommand.ADD
    assert flow_mods[2].phase == SwitchPhaseTag.PRIMARY_SOURCE_EDGE
    assert flow_mods[2].command == FlowModCommand.MODIFY_STRICT
    assert driver.get_installed_rule_count(dpid=6, flow_group_id="0x103") == 1
    assert driver.get_installed_rule_count(flow_group_id="0x103") == 5

    # Case B: Missing source-edge rule (e1 table wiped) -> MODIFY_STRICT on e1 fails
    #         and rollback on e1 also fails -> UNKNOWN
    driver.clear_switch_flow_table(dpid=1)
    out_missing_src = installer.install_flow_change(
        run_id="run_no_pre_p2",
        flow_group_id="0x103",
        old_path_id="0x103_P2",
        new_path_id="0x103_P1",
        applied_at_utc="2026-04-06T00:02:00Z",
    )
    assert out_missing_src.status == FlowDecisionStatus.UNKNOWN
    assert out_missing_src.verification_status == VerificationStatus.UNKNOWN
    assert out_missing_src.actual_path_id is None


# -----------------------------------------------------------------------------
# 16. Audit Phase 3: Đổi đường cho 1 flow không ghi đè rule của flow khác
# -----------------------------------------------------------------------------


def test_16_route_switch_does_not_overwrite_other_flows_on_shared_switches(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack
    boot_ev = installer.bootstrap_initial_paths("run_iso", "2026-04-06T00:00:00Z")
    state_manager.initialize_run(
        "run_iso", "2026-04-06T00:00:00Z", verification_evidences=boot_ev
    )

    # Switch 0x103 (h1->h3) to P2 AND clean up its old P1 downstream rules
    out = installer.install_flow_change(
        run_id="run_iso",
        flow_group_id="0x103",
        old_path_id="0x103_P1",
        new_path_id="0x103_P2",
        applied_at_utc="2026-04-06T00:01:00Z",
        cleanup_old_downstream=True,
    )
    assert out.status == FlowDecisionStatus.APPLIED

    # Verify all 47 other flows (including 0x104 h1->h4 and 0x203 h2->h3 on e1/c1/e2)
    # are still 100% intact and VERIFIED on their initial P1 paths!
    for fg in compiler.topology.get_all_flow_groups():
        fid = fg.flow_group_id
        expected_pid = "0x103_P2" if fid == "0x103" else f"{fid}_P1"
        v_check = verifier.verify_flow_path(
            "run_iso", fid, expected_pid, "2026-04-06T00:01:05Z"
        )
        assert v_check.is_verified is True, f"Flow {fid} on {expected_pid} was disrupted!"


# -----------------------------------------------------------------------------
# 17. Audit Phase 3: Kiểm tra chi tiết Rule Count Breakdown
#     (attempted, succeeded, failed, rollback, cleanup, active_switch_rule_count)
# -----------------------------------------------------------------------------


def test_17_rule_count_breakdown_attempted_succeeded_rollback_and_active_rules(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack
    installer.bootstrap_initial_paths(
        "run_cnt",
        "2026-04-06T00:00:00Z",
        preinstall_both_downstream_branches=False,
    )
    # Initially: 48 flows * 3 rules (P1 only) = 144 active switch rules; 3 rules for 0x103
    assert installer.get_active_switch_rule_count() == 144
    assert installer.get_active_switch_rule_count(flow_group_id="0x103") == 3

    # 1) Switch 0x103: P1 -> P2 without cleanup (adds 2 P2 downstream rules, modifies 1 e1 rule)
    out_1 = installer.install_flow_change(
        "run_cnt",
        "0x103",
        "0x103_P1",
        "0x103_P2",
        "2026-04-06T00:01:00Z",
        cleanup_old_downstream=False,
    )
    bd_1 = installer.get_last_flow_count_breakdown("0x103")
    assert bd_1 is not None
    assert out_1.rule_count == bd_1.attempted_flow_mod_count == 3
    assert bd_1.succeeded_flow_mod_count == 3
    assert bd_1.failed_flow_mod_count == 0
    assert bd_1.primary_flow_mod_count == 3
    assert bd_1.rollback_flow_mod_count == 0
    assert bd_1.cleanup_flow_mod_count == 0
    assert bd_1.active_switch_rule_count == 5  # 1 on e1 + 2 on P1 downstream + 2 on P2 downstream
    assert installer.get_active_switch_rule_count() == 146

    # 2) Switch 0x103: P2 -> P1 WITH cleanup_old_downstream=True
    #    (2 downstream ADD + 1 source MODIFY_STRICT + 2 cleanup DELETE_STRICT = 5 FlowMods)
    out_2 = installer.install_flow_change(
        "run_cnt",
        "0x103",
        "0x103_P2",
        "0x103_P1",
        "2026-04-06T00:02:00Z",
        cleanup_old_downstream=True,
    )
    bd_2 = installer.get_last_flow_count_breakdown("0x103")
    assert bd_2 is not None
    assert out_2.rule_count == bd_2.attempted_flow_mod_count == 5
    assert bd_2.succeeded_flow_mod_count == 5
    assert bd_2.failed_flow_mod_count == 0
    assert bd_2.primary_flow_mod_count == 3
    assert bd_2.rollback_flow_mod_count == 0
    assert bd_2.cleanup_flow_mod_count == 2
    assert bd_2.active_switch_rule_count == 3  # P2 downstream rules were deleted!
    assert installer.get_active_switch_rule_count() == 144

    # 3) Failed primary source-edge FlowMod on e1 + successful rollback:
    #    Primary: 2 downstream succeeded + 1 source-edge failed = 3 primary (2 succeeded, 1 failed)
    #    Rollback: 2 downstream succeeded + 1 source-edge succeeded = 3 rollback (3 succeeded)
    #    Total attempted = 6, succeeded = 5, failed = 1, rollback = 3
    driver.inject_flowmod_failure(
        dpid=1,
        flow_group_id="0x103",
        phase=SwitchPhaseTag.PRIMARY_SOURCE_EDGE,
        reason="E1_REJECTED",
    )
    out_3 = installer.install_flow_change(
        "run_cnt",
        "0x103",
        "0x103_P1",
        "0x103_P2",
        "2026-04-06T00:03:00Z",
    )
    bd_3 = installer.get_last_flow_count_breakdown("0x103")
    assert out_3.status == FlowDecisionStatus.ROLLED_BACK
    assert bd_3 is not None
    assert out_3.rule_count == bd_3.attempted_flow_mod_count == 6
    assert bd_3.succeeded_flow_mod_count == 5
    assert bd_3.failed_flow_mod_count == 1
    assert bd_3.primary_flow_mod_count == 3
    assert bd_3.rollback_flow_mod_count == 3
    assert bd_3.cleanup_flow_mod_count == 0
    assert bd_3.active_switch_rule_count == 5


# -----------------------------------------------------------------------------
# 18. Audit Phase 3: Bootstrap khi bảng flow trống hoặc thiếu rule
# -----------------------------------------------------------------------------


def test_18_bootstrap_fails_when_switch_table_empty_or_missing_rule(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack

    # 1) Empty switch flow tables: topology catalog alone NEVER makes bootstrap VERIFIED!
    assert driver.get_installed_rule_count() == 0
    initial_check = verifier.verify_all_initial_paths(
        "run_empty_boot", "2026-04-06T00:00:00Z"
    )
    assert len(initial_check) == 48
    assert all(not res.is_verified for res in initial_check.values())

    state_manager.initialize_run(
        "run_empty_boot", "2026-04-06T00:00:00Z", verification_evidences=None
    )
    assert state_manager.is_reconcile_required("run_empty_boot") is True
    assert (
        state_manager.get_flow_state("run_empty_boot", "0x103").verification_status
        == VerificationStatus.UNVERIFIED
    )

    # 2) Missing rule during bootstrap (e.g. FlowStats omits 0x103 on c1 dpid=5) -> raises RuntimeError
    driver.inject_flow_stats_fault(dpid=5, flow_group_id="0x103", omit_rule=True)
    with pytest.raises(RuntimeError, match="Bootstrap verification failed for flow '0x103'"):
        installer.bootstrap_initial_paths("run_missing_boot", "2026-04-06T00:00:05Z")


# -----------------------------------------------------------------------------
# 19. Audit Phase 3: Switch / Controller restart, FlowStats read-back & reconcile
# -----------------------------------------------------------------------------


def test_19_controller_and_switch_restart_readback_and_reconcile(
    phase3_stack: tuple[
        RouteCompiler, InMemorySwitchDriver, RuleVerifier, RuleInstaller, StateManager
    ],
) -> None:
    compiler, driver, verifier, installer, state_manager = phase3_stack

    # Bootstrap all 48 flows on P1, then move 0x103 to P2
    boot_ev = installer.bootstrap_initial_paths(
        "run_restart", "2026-04-06T00:00:00Z"
    )
    state_manager.initialize_run(
        "run_restart", "2026-04-06T00:00:00Z", verification_evidences=boot_ev
    )
    out_p2 = installer.install_flow_change(
        "run_restart", "0x103", "0x103_P1", "0x103_P2", "2026-04-06T00:01:00Z"
    )
    state_manager.commit_batch_outcome(
        "run_restart", "b_restart_01", [out_p2], "2026-04-06T00:01:00Z"
    )
    assert state_manager.get_state_version("run_restart") == 2

    # Scenario A: Controller restarts while OVS switches keep their flow tables intact
    state_manager.mark_controller_restart(
        "run_restart", "2026-04-06T00:02:00Z"
    )
    assert state_manager.is_reconcile_required("run_restart") is True
    assert state_manager.get_state_version("run_restart") == 3
    assert state_manager.get_flow_state("run_restart", "0x103").path_id is None

    # Read back active paths from switches via RuleVerifier.discover_all_active_paths
    discovered = verifier.discover_all_active_paths(
        "run_restart", "2026-04-06T00:02:05Z"
    )
    assert all(r.is_verified and r.evidence is not None for r in discovered.values())
    assert discovered["0x103"].actual_path_id == "0x103_P2"
    assert discovered["0x104"].actual_path_id == "0x104_P1"

    state_manager.reconcile_run_state(
        run_id="run_restart",
        verified_evidences={fid: r.evidence for fid, r in discovered.items() if r.evidence},
        reconciled_at_utc="2026-04-06T00:02:05Z",
    )
    assert state_manager.is_reconcile_required("run_restart") is False
    assert state_manager.get_state_version("run_restart") == 4
    assert state_manager.get_flow_state("run_restart", "0x103").path_id == "0x103_P2"

    # Scenario B: Switch e1 (dpid=1) restarts and wipes its flow table
    driver.clear_switch_flow_table(dpid=1)
    state_manager.mark_controller_restart(
        "run_restart", "2026-04-06T00:03:00Z"
    )
    assert state_manager.is_reconcile_required("run_restart") is True

    discovered_after_wipe = verifier.discover_all_active_paths(
        "run_restart", "2026-04-06T00:03:02Z"
    )
    # Every flow touching e1 (h1/h2 as src or dst -> 24 flows) fails readback;
    # the 24 flows between e2, e3, e4 remain verified!
    unverified_fids = [
        fid for fid, r in discovered_after_wipe.items() if not r.is_verified
    ]
    verified_fids = [
        fid for fid, r in discovered_after_wipe.items() if r.is_verified
    ]
    assert len(unverified_fids) == 24
    assert len(verified_fids) == 24
    assert "0x103" in unverified_fids

    # Re-install desired paths and verify all 48 flows to complete reconciliation
    desired_paths = {
        fg.flow_group_id: (
            "0x103_P2" if fg.flow_group_id == "0x103" else f"{fg.flow_group_id}_P1"
        )
        for fg in compiler.topology.get_all_flow_groups()
    }
    recovered_evidences = installer.reinstall_and_verify_paths(
        "run_restart", desired_paths, "2026-04-06T00:03:05Z"
    )
    state_manager.reconcile_run_state(
        run_id="run_restart",
        verified_evidences=recovered_evidences,
        reconciled_at_utc="2026-04-06T00:03:05Z",
    )
    assert state_manager.is_reconcile_required("run_restart") is False
    assert state_manager.get_state_version("run_restart") == 6
    assert state_manager.get_flow_state("run_restart", "0x103").path_id == "0x103_P2"

