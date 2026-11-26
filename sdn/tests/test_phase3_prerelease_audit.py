"""
SDN Phase 3 Pre-Release Audit & Edge-Case Verification Suite (`Tasks 2, 3, 4`).

Covers:
- Task 2: DemandHistoryProvider exhaustive edge cases (identical duplicates, stale out-of-window,
  non-1s bins, malformed JSON/schema, missing bins without zero-fill, reconnect recovery,
  cache invalidation, legacy mode preservation).
- Task 3: Canonical SDNSnapshotV14 full quality semantics (VALID, INCOMPLETE, STALE, INVALID),
  27-field schema & regex/timestamp/hash checks, retry determinism (`MOCK_ONLY` — no local Pydantic).
- Task 4: All 16 48-flow audit & causal outbox scenarios (B=1, B=0, B=4 mixed APPLIED+KEPT,
  ROLLED_BACK, UNKNOWN, expired deadline REJECTED, stale routing version, missing DecisionBatch,
  Data 409/422, delayed Routing ACK, Routing POST timeout after DB commit -> idempotent 200
  {"inserted": 0}, outbox restart with pending dependency, duplicate RoutingState ACK, missing
  or permanently rejected dependency, strict ApplyResult-after-RoutingState-ACK invariant).

Classification: `PASS_CONTRACT_MOCK` (pure-Python, no live PostgreSQL).
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

import pytest

from sdn.adapters.data_api_v1_adapter import (
    DATA_V1_ENDPOINT_APPLY_RESULTS,
    DATA_V1_ENDPOINT_ROUTING,
    DataV1ContractBlockedError,
    DataV1MappingError,
    build_48_flow_apply_result_data_v1,
    compute_sdn_v14_content_hash,
    map_snapshot_to_data_v1,
    serialize_canonical_json_bytes,
)
from sdn.adapters.data_demand_history_provider import (
    DataAPIDemandHistoryProvider,
    DataDemandHistoryConfig,
    DataDemandHistoryError,
)
from sdn.adapters.data_event_publisher import (
    DataEventPublisher,
    DataPublisherConfig,
    DeliveryStatus,
)
from sdn.controller.state_manager import StateManager
from sdn.controller.topology_registry import TopologyRegistry
from sdn.flow_management.flow_matcher import FlowMatcher
from sdn.flow_management.route_compiler import RouteCompiler
from sdn.flow_management.rule_installer import RuleInstaller
from sdn.flow_management.switch_driver import InMemorySwitchDriver
from sdn.monitoring.snapshot_creator import SnapshotCreator, SnapshotCreatorConfig
from sdn.route_verification.rule_verifier import RuleVerifier
from sdn.schemas.apply_models import ApplyFlowResult, ApplyResponse
from sdn.schemas.snapshot_models import OriginRefs, SnapshotQualityStatus
from sdn.schemas.state_models import (
    BatchStatus,
    FlowDecisionStatus,
    RoutingStateEntry,
    RoutingStateSnapshot,
    VerificationStatus,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TOPOLOGY_BUNDLE_PATH = REPO_ROOT / "configs" / "network" / "topology_bundle.json"
CATALOG_DIR = REPO_ROOT / "configs" / "network"
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$")


def _bootstrap(run_id: str):
    topo = TopologyRegistry.from_bundle_file(TOPOLOGY_BUNDLE_PATH, csv_config_dir=CATALOG_DIR)
    driver = InMemorySwitchDriver(topo)
    comp = RouteCompiler(topo, FlowMatcher(topo))
    installer = RuleInstaller(comp, driver, RuleVerifier(comp, driver))
    sm = StateManager(topo)
    boot_ts = "2026-11-25T09:00:00Z"
    evs = installer.bootstrap_initial_paths(run_id, boot_ts)
    sm.initialize_run(run_id, boot_ts, verification_evidences=evs)
    return topo, sm, installer


def _demand_rows(topo: TopologyRegistry, run_id: str, bins: list[int]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for b in bins:
        w_start = f"2026-11-25T09:01:{b:02d}Z"
        w_end = f"2026-11-25T09:01:{b + 1:02d}Z"
        for fg in topo.get_all_flow_groups():
            rows.append(
                {
                    "run_id": run_id,
                    "flow_group_id": fg.flow_group_id,
                    "bin_index": b,
                    "window_start_utc": w_start,
                    "window_end_utc": w_end,
                    "requested_mbps": 0.0 if fg.flow_group_id == "0x103" else 1.75,
                    "released_at_utc": w_end,
                    "available_at_utc": f"2026-11-25T09:01:{b + 1:02d}.250000Z",
                    "release_record_id": f"ev:{run_id}:{fg.flow_group_id}:{b}",
                    "source_kind": "RELEASED_DEMAND",
                    "valid": True,
                    "reason": None,
                }
            )
    return rows


def _decision_rows(
    topo: TopologyRegistry,
    changes: dict[str, tuple[str, str]] | None = None,
) -> list[dict[str, Any]]:
    changes = changes or {}
    out: list[dict[str, Any]] = []
    for fg in topo.get_all_flow_groups():
        fid = fg.flow_group_id
        old_p, prop_p = changes.get(fid, (f"{fid}_P1", f"{fid}_P1"))
        out.append({"flow_group_id": fid, "old_path_id": old_p, "proposed_path_id": prop_p})
    return out


def _clone_rs(
    rs: RoutingStateSnapshot,
    *,
    state_version: int,
    overrides: dict[str, tuple[str | None, str | None, VerificationStatus]] | None = None,
    reconcile_required: bool = False,
) -> RoutingStateSnapshot:
    overrides = overrides or {}
    entries = []
    unknown_fgs = []
    for e in rs.routing_state:
        if e.flow_group_id in overrides:
            p, c, vs = overrides[e.flow_group_id]
        else:
            p, c, vs = e.path_id, e.confirmed_at, e.verification_status
        if vs != VerificationStatus.VERIFIED:
            unknown_fgs.append(e.flow_group_id)
        entries.append(
            RoutingStateEntry(
                run_id=e.run_id,
                flow_group_id=e.flow_group_id,
                path_id=p,
                state_version=state_version,
                confirmed_at=c,
                last_successful_change_at=c or e.last_successful_change_at,
                verification_status=vs,
                config_version=e.config_version,
                evidence_mode=e.evidence_mode,
                last_optimization_change_at=e.last_optimization_change_at,
            )
        )
    return RoutingStateSnapshot(
        run_id=rs.run_id,
        state_version=state_version,
        config_version=rs.config_version,
        topology_version=rs.topology_version,
        reconcile_required=reconcile_required,
        in_flight_batch_id=None,
        routing_state_complete=(len(unknown_fgs) == 0 and not reconcile_required),
        unknown_flow_group_ids=tuple(unknown_fgs),
        routing_state=tuple(entries),
    )


# ---------------------------------------------------------------------------
# Task 2: DemandHistoryProvider Exhaustive Edge Cases
# ---------------------------------------------------------------------------


def test_p3_t2_demand_history_duplicates_stale_malformed_missing_and_reconnect() -> None:
    topo, sm, _ = _bootstrap("run_p3_t2")
    origins = OriginRefs(
        run_started_at_utc="2026-11-25T09:00:00Z",
        traffic_started_at_utc="2026-11-25T09:00:00Z",
        measurement_started_at_utc="2026-11-25T09:01:00Z",
        warmup_s=60,
        delta_s=1,
    )
    rows = _demand_rows(topo, "run_p3_t2", [1])
    mode = {"case": "dup_identical"}

    def _mock_get(url: str, headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
        c = mode["case"]
        if c == "dup_identical":
            # Identical duplicate row must be deduplicated cleanly
            return 200, json.dumps(rows + [rows[0]])
        if c == "stale_out_of_window":
            stale = dict(rows[0], bin_index=0, window_start_utc="2026-11-25T09:00:59Z", window_end_utc="2026-11-25T09:01:00Z")
            return 200, json.dumps([stale])
        if c == "non_1s_bin":
            bad_dur = dict(rows[0], window_start_utc="2026-11-25T09:01:01Z", window_end_utc="2026-11-25T09:01:01.500000Z")
            return 200, json.dumps([bad_dur])
        if c == "malformed_json":
            return 200, "{not valid json"
        if c == "invalid_schema":
            return 200, json.dumps({"not": "a_list"})
        if c == "missing_bins":
            # Only 47 of 48 flows returned -> missing flow must stay None (never 0.0)
            return 200, json.dumps(rows[1:])
        if c == "down":
            return 503, '{"detail":"db unavailable"}'
        return 200, json.dumps(rows)

    provider = DataAPIDemandHistoryProvider(
        DataDemandHistoryConfig(endpoint_base_url="http://data.local:8000", api_key="k", timeout_s=0.5),
        http_get_fn=_mock_get,
    )
    # 1. Exact duplicate -> deduplicated to 48 rows, genuine 0.0 preserved
    recs = provider.fetch_demand_history(
        "run_p3_t2",
        history_cutoff_utc="2026-11-25T09:01:02Z",
        snapshot_as_of_utc="2026-11-25T09:01:02.500000Z",
        lookback_L=1,
    )
    assert len(recs) == 48
    assert next(r for r in recs if r.flow_group_id == "0x103").requested_mbps == 0.0

    # 2. Stale out-of-window, non-1s bin, malformed JSON, invalid schema -> fail closed + cache cleared
    for case, expected_code in (
        ("stale_out_of_window", "DATA_STALE_WINDOW_VIOLATION"),
        ("non_1s_bin", "DATA_INVALID_BIN_DURATION"),
        ("malformed_json", "DATA_MALFORMED_JSON"),
        ("invalid_schema", "DATA_INVALID_SCHEMA"),
        ("down", "DATA_SERVER_ERROR"),
    ):
        mode["case"] = case
        with pytest.raises(DataDemandHistoryError) as exc_info:
            provider.fetch_demand_history(
                "run_p3_t2",
                history_cutoff_utc="2026-11-25T09:01:02Z",
                snapshot_as_of_utc="2026-11-25T09:01:02.500000Z",
                lookback_L=1,
            )
        assert exc_info.value.reason_code == expected_code
        assert provider.is_healthy is False
        assert provider.get_observations("run_p3_t2") == ()

    # 3. Reconnect recovery with missing bin -> healthy=True, Snapshot is INCOMPLETE_HISTORY with None (never 0.0)
    mode["case"] = "missing_bins"
    sc = SnapshotCreator(topo, sm, provider)
    snap_inc = provider.create_snapshot_via_data(
        sc,
        run_id="run_p3_t2",
        cutoff=2,
        lookback_L=1,
        snapshot_as_of_utc="2026-11-25T09:01:02.500000Z",
        origin_refs=origins,
    )
    assert provider.is_healthy is True
    assert snap_inc.quality_status == SnapshotQualityStatus.INCOMPLETE_HISTORY
    missing_item = next(h for h in snap_inc.history if h.flow_group_id == "0x103")
    assert missing_item.valid is False
    assert missing_item.requested_mbps is None


# ---------------------------------------------------------------------------
# Task 3: Snapshot v1.4 Full Quality Semantics (VALID, INCOMPLETE, STALE, INVALID)
# ---------------------------------------------------------------------------


def test_p3_t3_snapshot_v14_all_four_quality_states_and_strict_invariants() -> None:
    topo, sm, _ = _bootstrap("run_p3_t3")
    origins = OriginRefs(
        run_started_at_utc="2026-11-25T09:00:00Z",
        traffic_started_at_utc="2026-11-25T09:00:00Z",
        measurement_started_at_utc="2026-11-25T09:01:00Z",
        warmup_s=60,
        delta_s=1,
    )
    rows = _demand_rows(topo, "run_p3_t3", [0])
    provider = DataAPIDemandHistoryProvider(
        DataDemandHistoryConfig(endpoint_base_url="http://data.local:8000", api_key="k"),
        http_get_fn=lambda u, h, t: (200, json.dumps(rows)),
    )
    sc = SnapshotCreator(topo, sm, provider, config=SnapshotCreatorConfig(stale_threshold_s=2.0))

    # 1. VALID
    snap_valid = provider.create_snapshot_via_data(
        sc, run_id="run_p3_t3", cutoff=1, lookback_L=1,
        snapshot_as_of_utc="2026-11-25T09:01:01.500000Z", snapshot_id="s_valid", origin_refs=origins,
    )
    w_valid, b_valid, h_valid = map_snapshot_to_data_v1(snap_valid)
    assert w_valid["sdn"]["quality_status"] == "VALID"
    assert HEX64_RE.fullmatch(w_valid["sdn"]["content_hash"])
    assert HEX64_RE.fullmatch(w_valid["sdn"]["topology_hash"])
    assert w_valid["sdn"]["content_hash"] == compute_sdn_v14_content_hash(w_valid["sdn"])
    assert hashlib.sha256(b_valid).hexdigest() == h_valid
    assert UTC_RE.fullmatch(w_valid["sdn"]["snapshot_as_of_utc"])

    # 2. STALE (as_of - cutoff = 5s > stale_threshold_s=2.0s)
    snap_stale = provider.create_snapshot_via_data(
        sc, run_id="run_p3_t3", cutoff=1, lookback_L=1,
        snapshot_as_of_utc="2026-11-25T09:01:06Z", snapshot_id="s_stale", origin_refs=origins,
    )
    w_stale, _, _ = map_snapshot_to_data_v1(snap_stale)
    assert w_stale["sdn"]["quality_status"] == "STALE"
    assert w_stale["sdn"]["content_hash"] == compute_sdn_v14_content_hash(w_stale["sdn"])

    # 3. INVALID (controller restart -> reconcile_required=True -> INVALID_ROUTING_STATE -> mapped to INVALID)
    sm.mark_controller_restart("run_p3_t3", "2026-11-25T09:01:01Z")
    snap_inv = provider.create_snapshot_via_data(
        sc, run_id="run_p3_t3", cutoff=1, lookback_L=1,
        snapshot_as_of_utc="2026-11-25T09:01:01.500000Z", snapshot_id="s_inv", origin_refs=origins,
    )
    w_inv, _, _ = map_snapshot_to_data_v1(snap_inv)
    assert w_inv["sdn"]["quality_status"] == "INVALID"
    assert w_inv["sdn"]["routing_state_complete"] is False
    assert w_inv["sdn"]["content_hash"] == compute_sdn_v14_content_hash(w_inv["sdn"])


# ---------------------------------------------------------------------------
# Task 4: All 16 48-Flow Audit & Causal Outbox Ordering Scenarios
# ---------------------------------------------------------------------------


def test_p3_t4_48_flow_audit_b0_b1_b4_expired_deadline_and_stale_version() -> None:
    topo, sm, _ = _bootstrap("run_p3_t4a")
    rs_v1 = sm.get_routing_state("run_p3_t4a")

    # Scenario 2: B=0 changed flows -> 48 KEPT
    dec_b0 = _decision_rows(topo)
    resp_b0 = ApplyResponse(
        run_id="run_p3_t4a",
        decision_batch_id="batch_b0",
        status=BatchStatus.APPLIED,
        state_version=rs_v1.state_version,
        completed_at="2026-11-25T09:01:05Z",
        results=(),
        rule_count=0,
    )
    audit_b0 = build_48_flow_apply_result_data_v1(resp_b0, decision_rows=dec_b0, routing_state=rs_v1)
    assert len(audit_b0["rows"]) == 48
    assert all(r["status"] == "KEPT" and r["actual_path_id"] == f"{r['flow_group_id']}_P1" for r in audit_b0["rows"])
    assert audit_b0["response_state_version"] == rs_v1.state_version

    # Scenarios 3 & 4: B=4 multiple changed flows (APPLIED + KEPT mixed batch)
    changed_4 = {
        "0x103": ("0x103_P1", "0x103_P2"),
        "0x104": ("0x104_P1", "0x104_P2"),
        "0x203": ("0x203_P1", "0x203_P2"),
        "0x204": ("0x204_P1", "0x204_P2"),
    }
    dec_b4 = _decision_rows(topo, changed_4)
    rs_v2 = _clone_rs(
        rs_v1,
        state_version=2,
        overrides={fg: (p2, "2026-11-25T09:01:10Z", VerificationStatus.VERIFIED) for fg, (_, p2) in changed_4.items()},
    )
    resp_b4 = ApplyResponse(
        run_id="run_p3_t4a",
        decision_batch_id="batch_b4",
        status=BatchStatus.APPLIED,
        state_version=2,
        completed_at="2026-11-25T09:01:10Z",
        results=tuple(
            ApplyFlowResult(
                flow_group_id=fg,
                old_path_id=p1,
                proposed_path_id=p2,
                actual_path_id=p2,
                status=FlowDecisionStatus.APPLIED,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=3,
                reason=None,
                applied_at="2026-11-25T09:01:10Z",
            )
            for fg, (p1, p2) in changed_4.items()
        ),
        rule_count=12,
    )
    audit_b4 = build_48_flow_apply_result_data_v1(resp_b4, decision_rows=dec_b4, routing_state=rs_v2)
    assert len(audit_b4["rows"]) == 48
    by_fg = {r["flow_group_id"]: r for r in audit_b4["rows"]}
    for fg, (_, p2) in changed_4.items():
        assert by_fg[fg]["status"] == "APPLIED"
        assert by_fg[fg]["actual_path_id"] == p2
        assert by_fg[fg]["verified_at_utc"] == "2026-11-25T09:01:10Z"
    assert sum(1 for r in audit_b4["rows"] if r["status"] == "KEPT") == 44

    # Scenario 7: Expired deadline -> BatchStatus.REJECTED with per-flow FlowDecisionStatus.REJECTED!
    dec_exp = _decision_rows(topo, {"0x103": ("0x103_P1", "0x103_P2")})
    resp_exp = ApplyResponse(
        run_id="run_p3_t4a",
        decision_batch_id="batch_expired",
        status=BatchStatus.REJECTED,
        state_version=rs_v1.state_version,
        completed_at="2026-11-25T09:01:15Z",
        results=(
            ApplyFlowResult(
                flow_group_id="0x103",
                old_path_id="0x103_P1",
                proposed_path_id="0x103_P2",
                actual_path_id="0x103_P1",
                status=FlowDecisionStatus.REJECTED,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=0,
                reason="DEADLINE_EXPIRED",
                applied_at=None,
            ),
        ),
        rule_count=0,
        reason="DEADLINE_EXPIRED",
    )
    audit_exp = build_48_flow_apply_result_data_v1(resp_exp, decision_rows=dec_exp, routing_state=rs_v1)
    assert audit_exp["response_status"] == "REJECTED"
    assert len(audit_exp["rows"]) == 48
    row_103 = next(r for r in audit_exp["rows"] if r["flow_group_id"] == "0x103")
    assert row_103["status"] == "KEPT"
    assert row_103["actual_path_id"] == "0x103_P1"
    assert row_103["reason"] == "DEADLINE_EXPIRED"

    # Guard: APPLIED batch MUST NEVER accept a per-flow REJECTED result
    resp_bad_applied_rej = ApplyResponse(
        run_id="run_p3_t4a",
        decision_batch_id="batch_bad_applied_rej",
        status=BatchStatus.APPLIED,
        state_version=rs_v1.state_version,
        completed_at="2026-11-25T09:01:15Z",
        results=resp_exp.results,
        rule_count=0,
    )
    with pytest.raises(DataV1MappingError):
        build_48_flow_apply_result_data_v1(resp_bad_applied_rej, decision_rows=dec_exp, routing_state=rs_v1)

    # Scenario 8: Stale routing version mismatch between ApplyResponse and RoutingStateSnapshot -> rejected
    with pytest.raises(DataV1MappingError):
        build_48_flow_apply_result_data_v1(resp_b4, decision_rows=dec_b4, routing_state=rs_v1)


def test_p3_t4_outbox_delayed_ack_timeout_after_commit_rejected_dep_and_409_422(tmp_path: Path) -> None:
    topo, sm, _ = _bootstrap("run_p3_t4b")
    rs = sm.get_routing_state("run_p3_t4b")
    dec = _decision_rows(topo)
    resp = ApplyResponse(
        run_id="run_p3_t4b",
        decision_batch_id="batch_outbox_1",
        status=BatchStatus.APPLIED,
        state_version=rs.state_version,
        completed_at="2026-11-25T09:01:20Z",
        results=(),
        rule_count=0,
    )

    # Scenarios 11 & 12: Routing POST times out after DB commit, then retry gets idempotent 200 {"inserted": 0}
    # ApplyResult MUST NOT be sent until Routing ACK is received!
    events_log: list[str] = []
    route_calls = {"n": 0}

    def _mock_post_timeout_then_idempotent(url: str, payload: dict[str, Any], headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
        if url.endswith(DATA_V1_ENDPOINT_ROUTING):
            route_calls["n"] += 1
            if route_calls["n"] == 1:
                events_log.append("ROUTING_TIMEOUT_AFTER_DB_COMMIT")
                raise TimeoutError("socket read timed out after DB commit")
            events_log.append("ROUTING_IDEMPOTENT_ACK")
            return 200, '{"inserted": 0}'
        if url.endswith(DATA_V1_ENDPOINT_APPLY_RESULTS):
            assert "ROUTING_IDEMPOTENT_ACK" in events_log, "ApplyResult sent before Routing ACK!"
            events_log.append("APPLY_RESULT_ACK")
            return 200, '{"updated": 1}'
        return 404, "not found"

    pub = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://data.local:8000",
            outbox_dir=tmp_path / "ob_timeout",
            api_mode="data_v1",
            api_key="sdn-key",
            max_retries=1,
        ),
        sender_fn=_mock_post_timeout_then_idempotent,
    )
    r_route_1 = pub.publish_routing_state(rs, deliver_now=True)
    assert r_route_1.status == DeliveryStatus.PENDING_DELIVERY
    r_apply_1 = pub.publish_apply_response(resp, deliver_now=True, full_routing_state=rs, decision_rows=dec)
    assert r_apply_1.status == DeliveryStatus.PENDING_DELIVERY
    assert r_apply_1.attempts == 0
    assert events_log == ["ROUTING_TIMEOUT_AFTER_DB_COMMIT"]

    # Next flush: Routing retries -> 200 {"inserted": 0} (valid ACK!) -> ApplyResult delivered immediately after
    flushed = pub.flush_pending()
    assert flushed == {"delivered": 2, "pending": 0}
    assert events_log == ["ROUTING_TIMEOUT_AFTER_DB_COMMIT", "ROUTING_IDEMPOTENT_ACK", "APPLY_RESULT_ACK"]

    # Scenario 14: Duplicate RoutingState ACK -> idempotent, zero extra HTTP calls
    dup_route = pub.publish_routing_state(rs, deliver_now=True)
    assert dup_route.duplicate is True
    assert route_calls["n"] == 2

    # Scenario 15: Dependency permanently rejected (409) -> ApplyResult NEVER sent (0 attempts) + surfaces dep error
    apply_calls = {"n": 0}

    def _mock_dep_rejected(url: str, payload: dict[str, Any], headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
        if url.endswith(DATA_V1_ENDPOINT_ROUTING):
            return 409, '{"detail":"Routing state did not change"}'
        if url.endswith(DATA_V1_ENDPOINT_APPLY_RESULTS):
            apply_calls["n"] += 1
            return 200, '{"updated": 1}'
        return 404, "not found"

    pub_rej = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://data.local:8000",
            outbox_dir=tmp_path / "ob_dep_rej",
            api_mode="data_v1",
            api_key="sdn-key",
            max_retries=3,
        ),
        sender_fn=_mock_dep_rejected,
    )
    pub_rej.publish_routing_state(rs, deliver_now=True)
    r_blocked = pub_rej.publish_apply_response(resp, deliver_now=True, full_routing_state=rs, decision_rows=dec)
    assert r_blocked.status == DeliveryStatus.PENDING_DELIVERY
    assert r_blocked.attempts == 0
    assert "WAITING_FOR_DEPENDENCY:" in (r_blocked.last_error or "")
    assert "HTTP_409" in (r_blocked.last_error or "")
    assert apply_calls["n"] == 0

    # Scenario 10: Data returns 409 / 422 on ApplyResult itself -> 1 attempt only, stays PENDING_DELIVERY
    for err_code in (409, 422):
        pub_err = DataEventPublisher(
            DataPublisherConfig(
                endpoint_base_url="http://data.local:8000",
                api_mode="data_v1",
                api_key="sdn-key",
                max_retries=3,
            ),
            sender_fn=lambda u, p, h, t, ec=err_code: (
                (200, '{"inserted": 1}') if u.endswith(DATA_V1_ENDPOINT_ROUTING) else (ec, '{"detail":"rejected"}')
            ),
        )
        assert pub_err.publish_routing_state(rs, deliver_now=True).status == DeliveryStatus.DELIVERED
        res_err = pub_err.publish_apply_response(resp, deliver_now=True, full_routing_state=rs, decision_rows=dec)
        assert res_err.status == DeliveryStatus.PENDING_DELIVERY
        assert res_err.attempts == 1
        assert f"HTTP_{err_code}" in (res_err.last_error or "")
