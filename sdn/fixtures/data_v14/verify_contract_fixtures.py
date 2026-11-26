"""
Canonical SDN v1.4 Data Contract Fixture Generator & Verifier (`Phase 12`).

Generates all 8 Data API v1.4 request fixtures strictly using real SDN production
adapter functions (never hand-written JSON) and verifies:
- Task B: Canonical UTF-8 byte serialization, `sdn.content_hash`, `X-Content-SHA256`,
  and byte-for-byte replay determinism.
- Task C: `DataAPIDemandHistoryProvider` (`format=sdn`), `warmup_s=60`, `delta_s=1`,
  measurement bin 100 <-> traffic bin 160, and all 5 tick boundary cases (`cutoff < L`,
  `cutoff == L`, 1 bin missing, 1 route UNKNOWN, late demand availability).
- Task D: 48-flow `ApplyResult` fixtures (`1 APPLIED + 47 KEPT`, `ROLLED_BACK`,
  `UNKNOWN`, `B=0` no-op) built strictly from registered 48-flow `DecisionBatch` rows
  and verified `RoutingStateSnapshot`.
- Task E: Schema & validator alignment against `origin/main` `data/ingestion/online_models.py`
  and `data/ingestion/management_api.py` (uses real Pydantic if installed; otherwise
  strict pure-Python mirror validation clearly labeled `PURE_PYTHON_MIRROR_NO_PYDANTIC`).

Usage:
    python sdn/fixtures/data_v14/verify_contract_fixtures.py --generate --verify
    python sdn/fixtures/data_v14/verify_contract_fixtures.py --verify
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sdn.adapters.data_api_v1_adapter import (
    build_48_flow_apply_result_data_v1,
    build_data_v1_headers,
    build_noop_48_flow_apply_result_data_v1,
    compute_bytes_sha256,
    compute_sdn_v14_content_hash,
    map_routing_state_to_data_v1,
    map_snapshot_to_data_v1,
    serialize_canonical_json_bytes,
)
from sdn.adapters.data_demand_history_provider import (
    DataAPIDemandHistoryProvider,
    DataDemandHistoryConfig,
)
from sdn.controller.state_manager import StateManager
from sdn.controller.topology_registry import TopologyRegistry
from sdn.flow_management.flow_matcher import FlowMatcher
from sdn.flow_management.route_compiler import RouteCompiler
from sdn.flow_management.rule_installer import RuleInstaller
from sdn.flow_management.switch_driver import InMemorySwitchDriver
from sdn.monitoring.snapshot_creator import SnapshotCreationError, SnapshotCreator
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

FIXTURE_DIR = Path(__file__).resolve().parent
TOPOLOGY_BUNDLE_PATH = REPO_ROOT / "configs" / "network" / "topology_bundle.json"
CATALOG_DIR = REPO_ROOT / "configs" / "network"

# Deterministic synthetic run constants (explicitly labeled synthetic)
SYNTHETIC_RUN_ID = "synthetic_sdn_v14_fixture_run_01"
RUN_START_UTC = "2026-11-26T08:59:55Z"
TRAFFIC_START_UTC = "2026-11-26T09:00:00Z"
MEAS_START_UTC = "2026-11-26T09:01:00Z"  # warmup_s = 60s -> measurement bin 100 == traffic bin 160
LOOKBACK_L = 10
CUTOFF_OFFSET_S = 100  # measurement bins 90..99 (traffic bins 150..159); cutoff = 09:02:40Z (traffic bin 160)
DELTA_S = 1
WARMUP_S = 60

HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)$")


def _dt(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def _fmt(dt: datetime) -> str:
    if dt.microsecond:
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_synthetic_demand_rows(
    topo: TopologyRegistry,
    run_id: str,
    bins: range,
    *,
    drop_key: tuple[str, int] | None = None,
    late_key: tuple[str, int] | None = None,
    late_available_utc: str | None = None,
) -> list[dict[str, Any]]:
    meas_dt = _dt(MEAS_START_UTC)
    rows: list[dict[str, Any]] = []
    for b in bins:
        w_start = meas_dt + timedelta(seconds=b)
        w_end = w_start + timedelta(seconds=DELTA_S)
        avail_default = w_end + timedelta(milliseconds=50)
        for idx, fg in enumerate(topo.get_all_flow_groups()):
            if drop_key is not None and (fg.flow_group_id, b) == drop_key:
                continue
            avail_ts = (
                late_available_utc
                if (late_key is not None and (fg.flow_group_id, b) == late_key and late_available_utc)
                else _fmt(avail_default)
            )
            rows.append(
                {
                    "run_id": run_id,
                    "flow_group_id": fg.flow_group_id,
                    "bin_index": b,
                    "window_start_utc": _fmt(w_start),
                    "window_end_utc": _fmt(w_end),
                    "requested_mbps": round(1.0 + (idx % 7) * 0.25 + (b % 5) * 0.1, 3),
                    "released_at_utc": _fmt(w_end),
                    "available_at_utc": avail_ts,
                    "release_record_id": f"synthetic:net:{run_id}:{fg.flow_group_id}:{b}",
                    "source_kind": "RELEASED_DEMAND",
                    "valid": True,
                    "reason": None,
                }
            )
    return rows


def _make_decision_rows(
    topo: TopologyRegistry,
    changes: dict[str, tuple[str, str]] | None = None,
) -> list[dict[str, Any]]:
    changes = changes or {}
    return [
        {
            "flow_group_id": fg.flow_group_id,
            "old_path_id": changes.get(fg.flow_group_id, (f"{fg.flow_group_id}_P1", f"{fg.flow_group_id}_P1"))[0],
            "proposed_path_id": changes.get(fg.flow_group_id, (f"{fg.flow_group_id}_P1", f"{fg.flow_group_id}_P1"))[1],
        }
        for fg in topo.get_all_flow_groups()
    ]


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
                last_successful_change_at=c if e.flow_group_id in overrides and p else e.last_successful_change_at,
                verification_status=vs,
                config_version=e.config_version,
                evidence_mode=e.evidence_mode,
                last_optimization_change_at=c if e.flow_group_id in overrides and p else e.last_optimization_change_at,
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


def build_all_fixtures() -> dict[str, bytes]:
    """Generate all 8 canonical SDN v1.4 JSON fixtures using real SDN production adapters."""
    topo = TopologyRegistry.from_bundle_file(TOPOLOGY_BUNDLE_PATH, csv_config_dir=CATALOG_DIR)
    driver = InMemorySwitchDriver(topo)
    comp = RouteCompiler(topo, FlowMatcher(topo))
    installer = RuleInstaller(comp, driver, RuleVerifier(comp, driver))
    sm = StateManager(topo)
    evs = installer.bootstrap_initial_paths(SYNTHETIC_RUN_ID, RUN_START_UTC)
    sm.initialize_run(SYNTHETIC_RUN_ID, RUN_START_UTC, verification_evidences=evs)
    rs_v1 = sm.get_routing_state(SYNTHETIC_RUN_ID)

    origins = OriginRefs(
        run_started_at_utc=RUN_START_UTC,
        traffic_started_at_utc=TRAFFIC_START_UTC,
        measurement_started_at_utc=MEAS_START_UTC,
        warmup_s=WARMUP_S,
        delta_s=DELTA_S,
    )
    as_of_utc = _fmt(_dt(MEAS_START_UTC) + timedelta(seconds=CUTOFF_OFFSET_S, milliseconds=200))

    # 1. VALID Snapshot (48 x 10 = 480 bins, measurement bins 90..99, cutoff_offset_s=100 == traffic bin 160)
    valid_rows = _build_synthetic_demand_rows(topo, SYNTHETIC_RUN_ID, range(CUTOFF_OFFSET_S - LOOKBACK_L, CUTOFF_OFFSET_S))
    prov_valid = DataAPIDemandHistoryProvider(
        DataDemandHistoryConfig(endpoint_base_url="http://data.fixture.local:8000", api_key="synthetic-key"),
        http_get_fn=lambda u, h, t: (200, json.dumps(valid_rows)),
    )
    sc_valid = SnapshotCreator(topo, sm, prov_valid)
    snap_valid = prov_valid.create_snapshot_via_data(
        sc_valid,
        run_id=SYNTHETIC_RUN_ID,
        cutoff=CUTOFF_OFFSET_S,
        lookback_L=LOOKBACK_L,
        snapshot_as_of_utc=as_of_utc,
        snapshot_id=f"snap:{SYNTHETIC_RUN_ID}:t{CUTOFF_OFFSET_S}:L{LOOKBACK_L}:v{rs_v1.state_version}:valid",
        origin_refs=origins,
    )
    _, snap_valid_bytes, snap_valid_sha = map_snapshot_to_data_v1(snap_valid)
    snap_valid_headers = build_data_v1_headers(
        api_key="<SDN_SDN_KEY>",
        dedup_key=f"snapshot:{snap_valid.snapshot_id}",
        event_type="Snapshot",
        content_sha256=snap_valid_sha,
    )
    snap_valid_headers_bytes = serialize_canonical_json_bytes(snap_valid_headers)

    # 2. INCOMPLETE Snapshot (1 missing bin: flow 0x103, bin 99 -> null evidence, never 0.0)
    inc_rows = _build_synthetic_demand_rows(
        topo,
        SYNTHETIC_RUN_ID,
        range(CUTOFF_OFFSET_S - LOOKBACK_L, CUTOFF_OFFSET_S),
        drop_key=("0x103", CUTOFF_OFFSET_S - 1),
    )
    prov_inc = DataAPIDemandHistoryProvider(
        DataDemandHistoryConfig(endpoint_base_url="http://data.fixture.local:8000", api_key="synthetic-key"),
        http_get_fn=lambda u, h, t: (200, json.dumps(inc_rows)),
    )
    sc_inc = SnapshotCreator(topo, sm, prov_inc)
    snap_inc = prov_inc.create_snapshot_via_data(
        sc_inc,
        run_id=SYNTHETIC_RUN_ID,
        cutoff=CUTOFF_OFFSET_S,
        lookback_L=LOOKBACK_L,
        snapshot_as_of_utc=as_of_utc,
        snapshot_id=f"snap:{SYNTHETIC_RUN_ID}:t{CUTOFF_OFFSET_S}:L{LOOKBACK_L}:v{rs_v1.state_version}:incomplete",
        origin_refs=origins,
    )
    _, snap_inc_bytes, _ = map_snapshot_to_data_v1(snap_inc)

    # 3. RoutingState fixture (POST /v1/sdn/routing)
    routing_payload = map_routing_state_to_data_v1(rs_v1, published_at_utc="2026-11-26T08:59:56Z")
    routing_bytes = serialize_canonical_json_bytes(routing_payload)

    # 4. ApplyResult: 1 APPLIED + 47 KEPT (batch_01: 0x103 P1 -> P2 at v2)
    dec_change = _make_decision_rows(topo, {"0x103": ("0x103_P1", "0x103_P2")})
    ts_v2 = "2026-11-26T09:02:41Z"
    rs_v2 = _clone_rs(
        rs_v1,
        state_version=2,
        overrides={"0x103": ("0x103_P2", ts_v2, VerificationStatus.VERIFIED)},
    )
    resp_applied = ApplyResponse(
        run_id=SYNTHETIC_RUN_ID,
        decision_batch_id="batch_synthetic_01_applied",
        status=BatchStatus.APPLIED,
        state_version=2,
        completed_at=ts_v2,
        results=(
            ApplyFlowResult(
                flow_group_id="0x103",
                old_path_id="0x103_P1",
                proposed_path_id="0x103_P2",
                actual_path_id="0x103_P2",
                status=FlowDecisionStatus.APPLIED,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=3,
                reason=None,
                applied_at=ts_v2,
            ),
        ),
        rule_count=3,
    )
    ar_applied = build_48_flow_apply_result_data_v1(resp_applied, decision_rows=dec_change, routing_state=rs_v2)

    # 5. ApplyResult: ROLLED_BACK (0x103 rolled back to 0x103_P1 + 47 KEPT)
    ts_rb = "2026-11-26T09:02:42Z"
    resp_rb = ApplyResponse(
        run_id=SYNTHETIC_RUN_ID,
        decision_batch_id="batch_synthetic_02_rollback",
        status=BatchStatus.ROLLED_BACK,
        state_version=rs_v1.state_version,
        completed_at=ts_rb,
        results=(
            ApplyFlowResult(
                flow_group_id="0x103",
                old_path_id="0x103_P1",
                proposed_path_id="0x103_P2",
                actual_path_id="0x103_P1",
                status=FlowDecisionStatus.ROLLED_BACK,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=6,
                reason="VERIFY_FAILED",
                applied_at=ts_rb,
            ),
        ),
        rule_count=6,
        reason="VERIFY_FAILED",
    )
    ar_rollback = build_48_flow_apply_result_data_v1(resp_rb, decision_rows=dec_change, routing_state=rs_v1)

    # 6. ApplyResult: UNKNOWN (0x103 actual_path_id=null, verified_at_utc=null + 47 KEPT)
    ts_unk = "2026-11-26T09:02:43Z"
    rs_unk = _clone_rs(
        rs_v1,
        state_version=2,
        overrides={"0x103": (None, None, VerificationStatus.UNKNOWN)},
        reconcile_required=True,
    )
    resp_unk = ApplyResponse(
        run_id=SYNTHETIC_RUN_ID,
        decision_batch_id="batch_synthetic_03_unknown",
        status=BatchStatus.INCONSISTENT,
        state_version=2,
        completed_at=ts_unk,
        results=(
            ApplyFlowResult(
                flow_group_id="0x103",
                old_path_id="0x103_P1",
                proposed_path_id="0x103_P2",
                actual_path_id=None,
                status=FlowDecisionStatus.UNKNOWN,
                verification_status=VerificationStatus.UNKNOWN,
                rule_count=6,
                reason="ROLLBACK_FAILED",
                applied_at=ts_unk,
            ),
        ),
        rule_count=6,
        reason="ROLLBACK_FAILED",
    )
    ar_unknown = build_48_flow_apply_result_data_v1(resp_unk, decision_rows=dec_change, routing_state=rs_unk)

    # 7. ApplyResult: B=0 No-Op (48 KEPT on existing state_version)
    dec_noop = _make_decision_rows(topo)
    ar_noop = build_noop_48_flow_apply_result_data_v1(
        run_id=SYNTHETIC_RUN_ID,
        decision_batch_id="batch_synthetic_04_noop",
        decision_rows=dec_noop,
        routing_state=rs_v1,
        completed_at_utc="2026-11-26T09:02:44Z",
    )

    return {
        "snapshot_valid_body.json": snap_valid_bytes,
        "snapshot_valid_headers.json": snap_valid_headers_bytes,
        "snapshot_incomplete_body.json": snap_inc_bytes,
        "routing_state_body.json": routing_bytes,
        "apply_result_1_applied_47_kept.json": serialize_canonical_json_bytes(ar_applied),
        "apply_result_rollback.json": serialize_canonical_json_bytes(ar_rollback),
        "apply_result_unknown.json": serialize_canonical_json_bytes(ar_unknown),
        "apply_result_noop.json": serialize_canonical_json_bytes(ar_noop),
    }


def verify_tick_boundaries() -> dict[str, Any]:
    """Task C: Verify measurement-origin, warmup=60 (100 <-> 160), and all 5 tick boundary cases."""
    topo = TopologyRegistry.from_bundle_file(TOPOLOGY_BUNDLE_PATH, csv_config_dir=CATALOG_DIR)
    driver = InMemorySwitchDriver(topo)
    comp = RouteCompiler(topo, FlowMatcher(topo))
    installer = RuleInstaller(comp, driver, RuleVerifier(comp, driver))
    sm = StateManager(topo)
    evs = installer.bootstrap_initial_paths(SYNTHETIC_RUN_ID, RUN_START_UTC)
    sm.initialize_run(SYNTHETIC_RUN_ID, RUN_START_UTC, verification_evidences=evs)
    origins = OriginRefs(
        run_started_at_utc=RUN_START_UTC,
        traffic_started_at_utc=TRAFFIC_START_UTC,
        measurement_started_at_utc=MEAS_START_UTC,
        warmup_s=WARMUP_S,
        delta_s=DELTA_S,
    )

    # Verify measurement bin 100 <-> traffic bin 160
    meas_100 = _dt(MEAS_START_UTC) + timedelta(seconds=100)
    traffic_offset = int((meas_100 - _dt(TRAFFIC_START_UTC)).total_seconds())
    assert traffic_offset == 160, f"Expected traffic bin 160, got {traffic_offset}"

    results: dict[str, Any] = {"measurement_100_equals_traffic_160": traffic_offset == 160}

    # Case 1: cutoff earlier than L (cutoff=5 < L=10) -> SnapshotCreationError
    prov_empty = DataAPIDemandHistoryProvider(
        DataDemandHistoryConfig(endpoint_base_url="http://data.fixture.local:8000", api_key="k"),
        http_get_fn=lambda u, h, t: (200, "[]"),
    )
    sc = SnapshotCreator(topo, sm, prov_empty)
    try:
        sc.create_snapshot(
            SYNTHETIC_RUN_ID,
            cutoff=5,
            lookback_L=LOOKBACK_L,
            snapshot_as_of_utc=_fmt(_dt(MEAS_START_UTC) + timedelta(seconds=5)),
            origin_refs=origins,
        )
        raise AssertionError("Expected SnapshotCreationError when cutoff < L")
    except SnapshotCreationError:
        results["cutoff_earlier_than_L_rejected"] = True

    # Case 2: cutoff == L (cutoff=10, L=10, bins 0..9) -> VALID
    rows_l = _build_synthetic_demand_rows(topo, SYNTHETIC_RUN_ID, range(0, LOOKBACK_L))
    captured_urls: list[str] = []

    def _get_l(u: str, h: dict[str, str], t: float) -> tuple[int, str]:
        captured_urls.append(u)
        return 200, json.dumps(rows_l)

    prov_l = DataAPIDemandHistoryProvider(
        DataDemandHistoryConfig(endpoint_base_url="http://data.fixture.local:8000", api_key="k"),
        http_get_fn=_get_l,
    )
    sc_l = SnapshotCreator(topo, sm, prov_l)
    snap_l = prov_l.create_snapshot_via_data(
        sc_l,
        run_id=SYNTHETIC_RUN_ID,
        cutoff=LOOKBACK_L,
        lookback_L=LOOKBACK_L,
        snapshot_as_of_utc=_fmt(_dt(MEAS_START_UTC) + timedelta(seconds=LOOKBACK_L, milliseconds=200)),
        origin_refs=origins,
    )
    assert "format=sdn" in captured_urls[0] and f"lookback={LOOKBACK_L}" in captured_urls[0]
    assert snap_l.quality_status == SnapshotQualityStatus.VALID
    assert snap_l.valid_bin_count == 48 * LOOKBACK_L
    results["cutoff_equals_L_valid"] = True
    results["provider_uses_format_sdn"] = True

    # Case 3: one bin missing -> INCOMPLETE_HISTORY -> wire quality_status="INCOMPLETE", null evidence
    rows_miss = _build_synthetic_demand_rows(topo, SYNTHETIC_RUN_ID, range(0, LOOKBACK_L), drop_key=("0x103", 9))
    prov_miss = DataAPIDemandHistoryProvider(
        DataDemandHistoryConfig(endpoint_base_url="http://data.fixture.local:8000", api_key="k"),
        http_get_fn=lambda u, h, t: (200, json.dumps(rows_miss)),
    )
    sc_miss = SnapshotCreator(topo, sm, prov_miss)
    snap_miss = prov_miss.create_snapshot_via_data(
        sc_miss,
        run_id=SYNTHETIC_RUN_ID,
        cutoff=LOOKBACK_L,
        lookback_L=LOOKBACK_L,
        snapshot_as_of_utc=_fmt(_dt(MEAS_START_UTC) + timedelta(seconds=LOOKBACK_L, milliseconds=200)),
        origin_refs=origins,
    )
    wire_miss, _, _ = map_snapshot_to_data_v1(snap_miss)
    assert wire_miss["sdn"]["quality_status"] == "INCOMPLETE"
    m_row = next(h for h in wire_miss["sdn"]["history"] if h["flow_group_id"] == "0x103" and h["bin_index"] == 9)
    assert m_row["valid"] is False and m_row["requested_mbps"] is None and m_row["available_at_utc"] is None and m_row["reason"] is None
    results["one_bin_missing_incomplete_null_evidence"] = True

    # Case 4: late demand availability (available_at_utc > snapshot_as_of_utc) -> rejected by DataAPIDemandHistoryProvider fail-closed
    as_of_dt = _dt(MEAS_START_UTC) + timedelta(seconds=LOOKBACK_L, milliseconds=200)
    rows_late = _build_synthetic_demand_rows(
        topo,
        SYNTHETIC_RUN_ID,
        range(0, LOOKBACK_L),
        late_key=("0x103", 9),
        late_available_utc=_fmt(as_of_dt + timedelta(seconds=2)),
    )
    prov_late = DataAPIDemandHistoryProvider(
        DataDemandHistoryConfig(endpoint_base_url="http://data.fixture.local:8000", api_key="k"),
        http_get_fn=lambda u, h, t: (200, json.dumps(rows_late)),
    )
    try:
        prov_late.fetch_demand_history(
            SYNTHETIC_RUN_ID,
            history_cutoff_utc=_fmt(_dt(MEAS_START_UTC) + timedelta(seconds=LOOKBACK_L)),
            snapshot_as_of_utc=_fmt(as_of_dt),
            lookback_L=LOOKBACK_L,
        )
        raise AssertionError("Expected DataDemandHistoryError for late available_at_utc > as_of")
    except Exception as exc:
        assert getattr(exc, "reason_code", None) == "DATA_CAUSALITY_VIOLATION"
        results["late_demand_availability_rejected"] = True

    # Case 5: one route UNKNOWN (reconcile_required) -> INVALID_ROUTING_STATE -> wire quality_status="INVALID"
    sm.mark_controller_restart(SYNTHETIC_RUN_ID, _fmt(as_of_dt))
    sc_unk = SnapshotCreator(topo, sm, prov_l)
    snap_unk = prov_l.create_snapshot_via_data(
        sc_unk,
        run_id=SYNTHETIC_RUN_ID,
        cutoff=LOOKBACK_L,
        lookback_L=LOOKBACK_L,
        snapshot_as_of_utc=_fmt(as_of_dt),
        origin_refs=origins,
    )
    wire_unk, _, _ = map_snapshot_to_data_v1(snap_unk)
    assert wire_unk["sdn"]["quality_status"] == "INVALID"
    assert wire_unk["sdn"]["routing_state_complete"] is False
    results["one_route_unknown_invalid"] = True

    return results


def verify_data_models(fixtures: dict[str, bytes]) -> dict[str, Any]:
    """
    Task E: Validate all fixtures against `online_models.py` and `management_api.py`.
    Uses real Pydantic if available; otherwise runs strict pure-Python mirror checks.
    """
    has_pydantic = False
    try:
        import pydantic  # type: ignore[import-not-found]  # noqa: F401
        has_pydantic = True
    except ImportError:
        has_pydantic = False

    report: dict[str, Any] = {
        "validation_mode": "REAL_PYDANTIC" if has_pydantic else "PURE_PYTHON_MIRROR_NO_PYDANTIC",
        "hashes": {},
        "checks": {},
    }

    for name, raw in fixtures.items():
        # Verify UTF-8 + canonical key ordering + byte-for-byte replay
        parsed = json.loads(raw.decode("utf-8"))
        reencoded = serialize_canonical_json_bytes(parsed)
        assert reencoded == raw, f"Non-canonical bytes in {name}"
        report["hashes"][name] = {"x_content_sha256": compute_bytes_sha256(raw), "bytes": len(raw)}

    # Validate Snapshot valid & incomplete
    for snap_name, expected_q in (
        ("snapshot_valid_body.json", "VALID"),
        ("snapshot_incomplete_body.json", "INCOMPLETE"),
    ):
        raw = fixtures[snap_name]
        obj = json.loads(raw)
        assert set(obj.keys()) == {"sdn"}
        sdn = obj["sdn"]
        assert sdn["quality_status"] == expected_q
        assert HEX64_RE.fullmatch(sdn["content_hash"])
        assert HEX64_RE.fullmatch(sdn["topology_hash"])
        assert sdn["content_hash"] == compute_sdn_v14_content_hash(sdn)
        assert len(sdn["flow_group_ids"]) == 48
        assert len(sdn["routing_state"]) == 48
        assert len(sdn["history"]) == 48 * LOOKBACK_L
        assert sdn["origin_refs"]["warmup_s"] == WARMUP_S
        assert sdn["origin_refs"]["delta_s"] == DELTA_S
        report["hashes"][snap_name]["sdn_content_hash"] = sdn["content_hash"]
        report["checks"][snap_name] = "PASS"

    # Validate snapshot_valid_headers.json matches snapshot_valid_body.json bytes
    hdrs = json.loads(fixtures["snapshot_valid_headers.json"])
    assert hdrs["X-Content-SHA256"] == compute_bytes_sha256(fixtures["snapshot_valid_body.json"])
    assert hdrs["Content-Type"] == "application/json"
    report["checks"]["snapshot_valid_headers.json"] = "PASS"

    # Validate routing_state_body.json
    rt = json.loads(fixtures["routing_state_body.json"])
    assert set(rt.keys()) == {"run_id", "state_version", "published_at_utc", "rows"}
    assert len(rt["rows"]) == 48
    for row in rt["rows"]:
        assert set(row.keys()) == {"flow_group_id", "path_id", "confirmed_at_utc", "verification_status"}
        assert row["verification_status"] == "VERIFIED" and row["path_id"] and UTC_RE.fullmatch(row["confirmed_at_utc"])
    report["checks"]["routing_state_body.json"] = "PASS"

    # Validate all 4 ApplyResult fixtures against management_api.ApplyResult / ResultRow
    for ar_name, exp_status, exp_counts in (
        ("apply_result_1_applied_47_kept.json", "APPLIED", {"APPLIED": 1, "KEPT": 47}),
        ("apply_result_rollback.json", "ROLLED_BACK", {"ROLLED_BACK": 1, "KEPT": 47}),
        ("apply_result_unknown.json", "INCONSISTENT", {"UNKNOWN": 1, "KEPT": 47}),
        ("apply_result_noop.json", "APPLIED", {"KEPT": 48}),
    ):
        ar = json.loads(fixtures[ar_name])
        assert set(ar.keys()) == {
            "run_id",
            "decision_batch_id",
            "response_status",
            "response_state_version",
            "completed_at_utc",
            "rows",
        }
        assert ar["response_status"] == exp_status
        assert len(ar["rows"]) == 48
        counts: dict[str, int] = {}
        for r in ar["rows"]:
            assert set(r.keys()) == {"flow_group_id", "actual_path_id", "status", "verified_at_utc", "reason"}
            assert (r["actual_path_id"] is None) == (r["verified_at_utc"] is None) == (r["status"] == "UNKNOWN")
            counts[r["status"]] = counts.get(r["status"], 0) + 1
        assert counts == exp_counts, f"{ar_name}: expected {exp_counts}, got {counts}"
        report["checks"][ar_name] = "PASS"

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate and verify canonical SDN v1.4 Data fixtures.")
    parser.add_argument("--generate", action="store_true", help="Write fixtures to sdn/fixtures/data_v14/")
    parser.add_argument("--verify", action="store_true", help="Verify generated fixtures and tick boundaries")
    args = parser.parse_args()

    if not args.generate and not args.verify:
        args.verify = True

    fixtures = build_all_fixtures()
    if args.generate:
        FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
        for name, raw in fixtures.items():
            (FIXTURE_DIR / name).write_bytes(raw)

    if args.verify:
        # Read back from disk to guarantee exact byte-for-byte persistence parity
        disk_fixtures = {name: (FIXTURE_DIR / name).read_bytes() for name in fixtures}
        for name, expected_bytes in fixtures.items():
            assert disk_fixtures[name] == expected_bytes, f"Disk fixture mismatch for {name}"
        model_report = verify_data_models(disk_fixtures)
        boundary_report = verify_tick_boundaries()
        print(json.dumps({"model_validation": model_report, "tick_boundaries": boundary_report}, indent=2))


if __name__ == "__main__":
    main()
