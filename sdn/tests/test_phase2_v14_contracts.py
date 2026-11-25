"""
Phase 2 Cross-Role Integration Contract Tests (`origin/main` @ `8ee1782`).

Validates all SDN-owned changes for the updated Data Integration Contracts:
- Task A: `DataAPIDemandHistoryProvider` reading `GET /v1/runs/{run_id}/demand-history?format=sdn`
  with `X-API-Key`, preserving Data's original `available_at_utc`, failing closed on errors,
  and never mixing Data API history with local SQLite history.
- Task B: Canonical SDN Snapshot v1.4 wire format `{"sdn": <SDNSnapshotV14>}` with exact
  `content_hash` (matching `online_store.py:snapshot_content_hash`) and byte-level
  `X-Content-SHA256`, measurement-origin bins, and nullable missing evidence.
- Task C: 48-flow audit `ApplyResult` strictly from registered `DecisionBatch` rows + verified
  `RoutingStateSnapshot` (never fabricating 48 rows) AND durable causal outbox dependency
  enforcing `RoutingState` ACK before `ApplyResult` delivery across retries, reordering,
  outages, and process restarts.
- Task D: Legacy mode (`api_mode="legacy"`, `demand_provider_mode="http_sqlite"`) remains
  completely unchanged.

Classification: `PASS_CONTRACT_MOCK` (pure-Python Windows/Ubuntu, no live PostgreSQL).
"""

from __future__ import annotations

import hashlib
import io
import json
import urllib.parse
from pathlib import Path
from typing import Any

import pytest

from sdn.adapters.data_api_v1_adapter import (
    DATA_V1_ENDPOINT_APPLY_RESULTS,
    DATA_V1_ENDPOINT_ROUTING,
    DATA_V1_ENDPOINT_SNAPSHOTS,
    DataV1ContractBlockedError,
    DataV1MappingError,
    build_48_flow_apply_result_data_v1,
    compute_sdn_v14_content_hash,
    fetch_decision_batch_from_data_v1,
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
from sdn.adapters.http_control_api import (
    NorthboundAPIConfig,
    SDNNorthboundHTTPApp,
)
from sdn.apps.ryu_s0_app import S0AppConfig, S0ControllerOrchestrator
from sdn.controller.decision_handler import DecisionHandler, DecisionHandlerConfig
from sdn.controller.state_manager import StateManager
from sdn.controller.topology_registry import TopologyRegistry
from sdn.flow_management.flow_matcher import FlowMatcher
from sdn.flow_management.route_compiler import RouteCompiler
from sdn.flow_management.rule_installer import RuleInstaller
from sdn.flow_management.switch_driver import InMemorySwitchDriver
from sdn.monitoring.snapshot_creator import SnapshotCreator
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


def _bootstrap_env(run_id: str = "run_p2"):
    topo = TopologyRegistry.from_bundle_file(TOPOLOGY_BUNDLE_PATH, csv_config_dir=CATALOG_DIR)
    driver = InMemorySwitchDriver(topo)
    comp = RouteCompiler(topo, FlowMatcher(topo))
    installer = RuleInstaller(comp, driver, RuleVerifier(comp, driver))
    sm = StateManager(topo)
    boot_ts = "2026-11-24T09:00:00Z"
    evs = installer.bootstrap_initial_paths(run_id, boot_ts)
    sm.initialize_run(run_id, boot_ts, verification_evidences=evs)
    return topo, sm, installer


def _sample_data_demand_rows(topo: TopologyRegistry, run_id: str, bins: list[int]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for b in bins:
        w_start = f"2026-11-24T09:01:{b:02d}Z"
        w_end = f"2026-11-24T09:01:{b + 1:02d}Z"
        for fg in topo.get_all_flow_groups():
            rows.append(
                {
                    "run_id": run_id,
                    "flow_group_id": fg.flow_group_id,
                    "bin_index": b,
                    "window_start_utc": w_start,
                    "window_end_utc": w_end,
                    "requested_mbps": 2.5,
                    "released_at_utc": w_end,
                    # PostgreSQL-stamped availability time from Data (MUST be preserved verbatim)
                    "available_at_utc": f"2026-11-24T09:01:{b + 1:02d}.123456Z",
                    "release_record_id": f"net:{run_id}:{fg.flow_group_id}:{b}",
                    "source_kind": "RELEASED_DEMAND",
                    "valid": True,
                    "reason": None,
                }
            )
    return rows


def _make_decision_rows(
    topo: TopologyRegistry,
    overrides: dict[str, tuple[str, str]] | None = None,
) -> list[dict[str, Any]]:
    overrides = overrides or {}
    rows: list[dict[str, Any]] = []
    for fg in topo.get_all_flow_groups():
        fid = fg.flow_group_id
        if fid in overrides:
            old_p, prop_p = overrides[fid]
        else:
            old_p = f"{fid}_P1"
            prop_p = f"{fid}_P1"
        rows.append(
            {
                "flow_group_id": fid,
                "old_path_id": old_p,
                "proposed_path_id": prop_p,
            }
        )
    return rows


def _wsgi_call(
    app: SDNNorthboundHTTPApp,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    raw = json.dumps(body).encode("utf-8") if body is not None else b""
    env = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "QUERY_STRING": "",
        "CONTENT_LENGTH": str(len(raw)),
        "wsgi.input": io.BytesIO(raw),
        "HTTP_X_SDN_TOKEN": "tok-p2",
    }
    out: dict[str, Any] = {}

    def _start(status: str, headers: list[tuple[str, str]]) -> None:
        out["code"] = int(status.split(" ", 1)[0])

    chunks = app(env, _start)
    return out["code"], json.loads(b"".join(chunks).decode("utf-8"))


# ---------------------------------------------------------------------------
# Task A: Data-backed DemandHistoryProvider
# ---------------------------------------------------------------------------


def test_p2_a01_data_demand_provider_fetches_preserves_available_at_and_builds_valid_snapshot() -> None:
    topo, sm, _ = _bootstrap_env("run_a01")
    origins = OriginRefs(
        run_started_at_utc="2026-11-24T09:00:00Z",
        traffic_started_at_utc="2026-11-24T09:00:00Z",
        measurement_started_at_utc="2026-11-24T09:01:00Z",
        warmup_s=60,
        delta_s=1,
    )
    rows = _sample_data_demand_rows(topo, "run_a01", [0, 1])
    seen_req: list[dict[str, Any]] = []

    def _mock_get(url: str, headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
        seen_req.append({"url": url, "headers": dict(headers), "timeout_s": timeout_s})
        if headers.get("X-API-Key") != "sdn-secret-key":
            return 401, '{"detail":"bad key"}'
        return 200, json.dumps(rows)

    provider = DataAPIDemandHistoryProvider(
        DataDemandHistoryConfig(
            endpoint_base_url="http://data.local:8000",
            api_key="sdn-secret-key",
            timeout_s=1.5,
        ),
        http_get_fn=_mock_get,
    )
    sc = SnapshotCreator(topo, sm, provider)
    snap = provider.create_snapshot_via_data(
        sc,
        run_id="run_a01",
        cutoff=2,
        lookback_L=2,
        snapshot_as_of_utc="2026-11-24T09:01:02.500000Z",
        origin_refs=origins,
    )

    assert len(seen_req) == 1
    parsed_url = urllib.parse.urlparse(seen_req[0]["url"])
    qs = urllib.parse.parse_qs(parsed_url.query)
    assert parsed_url.path == "/v1/runs/run_a01/demand-history"
    assert qs["format"] == ["sdn"]
    assert qs["lookback"] == ["2"]
    assert qs["cutoff"] == ["2026-11-24T09:01:02Z"]
    assert qs["as_of"] == ["2026-11-24T09:01:02.5Z"]
    assert seen_req[0]["headers"]["X-API-Key"] == "sdn-secret-key"

    assert snap.quality_status == SnapshotQualityStatus.VALID
    assert snap.valid_bin_count == 96
    assert snap.provenance.demand_provider_name == "DataAPIDemandHistoryProvider"
    assert snap.provenance.demand_channel_status == "DATA_API_V1_PULL"
    # Data's PostgreSQL-stamped available_at_utc MUST be preserved verbatim
    item_0 = next(h for h in snap.history if h.flow_group_id == "0x103" and h.bin_index == 0)
    assert item_0.available_at_utc == "2026-11-24T09:01:01.123456Z"
    assert "sdn-secret-key" not in json.dumps(provider.status_summary())


def test_p2_a02_data_demand_provider_fails_closed_on_errors_conflicts_and_clears_cache() -> None:
    topo, _, _ = _bootstrap_env("run_a02")
    good_rows = _sample_data_demand_rows(topo, "run_a02", [0])

    # First succeed to populate cache
    state = {"mode": "ok"}

    def _mock_get(url: str, headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
        m = state["mode"]
        if m == "ok":
            return 200, json.dumps(good_rows)
        if m == "401":
            return 401, '{"detail":"unauthorized"}'
        if m == "403":
            return 403, '{"detail":"forbidden"}'
        if m == "404":
            return 404, '{"detail":"unknown run"}'
        if m == "409":
            return 409, '{"detail":"missing measurement origin"}'
        if m == "422":
            return 422, '{"detail":"invalid params"}'
        if m == "503":
            return 503, '{"detail":"pg down"}'
        if m == "timeout":
            raise TimeoutError("read timed out")
        if m == "conflict":
            dup = dict(good_rows[0], requested_mbps=99.9)
            return 200, json.dumps([good_rows[0], dup])
        if m == "future":
            bad = dict(good_rows[0], window_end_utc="2026-11-24T09:01:05Z")
            return 200, json.dumps([bad])
        if m == "forbidden_source":
            bad = dict(good_rows[0], source_kind="port_tx_mbps")
            return 200, json.dumps([bad])
        return 500, "unexpected"

    provider = DataAPIDemandHistoryProvider(
        DataDemandHistoryConfig(endpoint_base_url="http://data.local:8000", api_key="k", timeout_s=0.5),
        http_get_fn=_mock_get,
    )
    recs = provider.fetch_demand_history(
        "run_a02",
        history_cutoff_utc="2026-11-24T09:01:01Z",
        snapshot_as_of_utc="2026-11-24T09:01:01.500000Z",
        lookback_L=1,
    )
    assert len(recs) == 48
    assert provider.is_healthy is True

    # Every error mode MUST raise DataDemandHistoryError AND clear cached rows (fail-closed)
    for err_mode, expected_code in (
        ("401", "DATA_AUTH_FAILED"),
        ("403", "DATA_AUTH_FAILED"),
        ("404", "DATA_RUN_NOT_FOUND"),
        ("409", "DATA_RUN_CONFLICT"),
        ("422", "DATA_CONTRACT_VALIDATION_ERROR"),
        ("503", "DATA_SERVER_ERROR"),
        ("timeout", "DATA_TIMEOUT"),
        ("conflict", "DATA_DUPLICATE_CONFLICT"),
        ("future", "DATA_CAUSALITY_VIOLATION"),
        ("forbidden_source", "FORBIDDEN_DEMAND_SOURCE"),
    ):
        state["mode"] = err_mode
        with pytest.raises(DataDemandHistoryError) as exc_info:
            provider.fetch_demand_history(
                "run_a02",
                history_cutoff_utc="2026-11-24T09:01:01Z",
                snapshot_as_of_utc="2026-11-24T09:01:01.500000Z",
                lookback_L=1,
            )
        assert exc_info.value.reason_code == expected_code
        assert provider.is_healthy is False
        assert provider.get_observations("run_a02") == ()


def test_p2_a03_explicit_provider_selection_never_mixes_data_and_sqlite(tmp_path: Path) -> None:
    topo, sm, installer = _bootstrap_env("run_a03")
    # 1. S0ControllerOrchestrator with demand_provider_mode="data_v1" MUST NOT open SQLite demand ledger
    sqlite_path = tmp_path / "should_not_be_created.sqlite3"
    orch = S0ControllerOrchestrator(
        S0AppConfig(
            topology_bundle_path=TOPOLOGY_BUNDLE_PATH,
            csv_config_dir=CATALOG_DIR,
            run_id="run_a03",
            demand_provider_mode="data_v1",
            demand_db_path=sqlite_path,
            data_endpoint_url="http://data.local:8000",
            data_api_key="sdn-key",
        )
    )
    assert isinstance(orch._demand_provider, DataAPIDemandHistoryProvider)
    assert orch._demand_ledger is None
    assert not sqlite_path.exists()

    # 2. Northbound API with DataAPIDemandHistoryProvider rejects POST /demand-releases with 409
    #    and fails closed (503) on POST /snapshots when Data is unavailable
    provider = DataAPIDemandHistoryProvider(
        DataDemandHistoryConfig(endpoint_base_url="http://data.local:8000", api_key="k"),
        http_get_fn=lambda u, h, t: (503, '{"detail":"db unavailable"}'),
    )
    sc = SnapshotCreator(topo, sm, provider)
    dh = DecisionHandler(topo, sm, installer, config=DecisionHandlerConfig.for_integration())
    app = SDNNorthboundHTTPApp(
        state_manager=sm,
        decision_handler=dh,
        snapshot_creator=sc,
        demand_provider=provider,
        config=NorthboundAPIConfig(auth_token="tok-p2", default_run_id="run_a03"),
    )
    code_push, body_push = _wsgi_call(
        app,
        "POST",
        "/demand-releases",
        body={
            "event_id": "ev1",
            "run_id": "run_a03",
            "flow_group_id": "0x103",
            "bin_index": 0,
            "window_start_utc": "2026-11-24T09:01:00Z",
            "window_end_utc": "2026-11-24T09:01:01Z",
            "requested_mbps": 1.0,
            "released_at_utc": "2026-11-24T09:01:01Z",
        },
    )
    assert code_push == 409
    assert body_push["error"] == "DEMAND_PROVIDER_IS_DATA_V1_READ_ONLY"

    code_snap, body_snap = _wsgi_call(
        app,
        "POST",
        "/snapshots",
        body={"run_id": "run_a03", "cutoff": 1, "lookback_L": 1},
    )
    assert code_snap == 503
    assert body_snap["error"] == "DATA_DEMAND_HISTORY_UNAVAILABLE"


# ---------------------------------------------------------------------------
# Task B: Canonical SDN Snapshot v1.4 Wire Format {"sdn": {...}}
# ---------------------------------------------------------------------------


def test_p2_b01_canonical_snapshot_v14_envelope_hashes_and_ml_fields(tmp_path: Path) -> None:
    topo, sm, _ = _bootstrap_env("run_b01")
    origins = OriginRefs(
        run_started_at_utc="2026-11-24T09:00:00Z",
        traffic_started_at_utc="2026-11-24T09:00:00Z",
        measurement_started_at_utc="2026-11-24T09:01:00Z",
        warmup_s=60,
        delta_s=1,
    )
    rows = _sample_data_demand_rows(topo, "run_b01", [0])
    provider = DataAPIDemandHistoryProvider(
        DataDemandHistoryConfig(endpoint_base_url="http://data.local:8000", api_key="k"),
        http_get_fn=lambda u, h, t: (200, json.dumps(rows)),
    )
    sc = SnapshotCreator(topo, sm, provider)
    snap = provider.create_snapshot_via_data(
        sc,
        run_id="run_b01",
        cutoff=1,
        lookback_L=1,
        snapshot_as_of_utc="2026-11-24T09:01:01.500000Z",
        origin_refs=origins,
    )

    wire1, raw1, sha1 = map_snapshot_to_data_v1(snap)
    wire2, raw2, sha2 = map_snapshot_to_data_v1(snap)
    # Deterministic across retries
    assert raw1 == raw2
    assert sha1 == sha2
    assert set(wire1.keys()) == {"sdn"}

    sdn_obj = wire1["sdn"]
    # Exact online_store.py:snapshot_content_hash verification
    assert sdn_obj["content_hash"] == compute_sdn_v14_content_hash(sdn_obj)
    # Exact online_api.py:X-Content-SHA256 verification
    assert hashlib.sha256(raw1).hexdigest() == sha1
    assert raw1 == serialize_canonical_json_bytes(wire1)

    # Verify ML-expected and Data-expected rich fields are preserved losslessly
    assert sdn_obj["origin_refs"]["warmup_s"] == 60
    assert len(sdn_obj["flow_group_ids"]) == 48
    assert len(sdn_obj["routing_state"]) == 48
    assert len(sdn_obj["routing_history"]) == 48
    assert sdn_obj["expected_bin_count"] == 48
    assert sdn_obj["valid_bin_count"] == 48
    assert sdn_obj["quality_status"] == "VALID"
    assert sdn_obj["routing_state_complete"] is True
    assert sdn_obj["unknown_flow_group_ids"] == []
    assert sdn_obj["provenance"]["demand_provider_name"] == "DataAPIDemandHistoryProvider"

    # End-to-end delivery through DataEventPublisher checks exact X-Content-SHA256 header
    captured: list[dict[str, Any]] = []

    def _mock_post(url: str, payload: dict[str, Any], headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
        body_bytes = serialize_canonical_json_bytes(payload)
        assert headers["X-Content-SHA256"] == hashlib.sha256(body_bytes).hexdigest()
        assert "sdn" in payload
        assert compute_sdn_v14_content_hash(payload["sdn"]) == payload["sdn"]["content_hash"]
        captured.append({"url": url, "headers": dict(headers), "payload": payload})
        return 200, '{"inserted":1}'

    pub = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://data.local:8000",
            outbox_dir=tmp_path / "ob_b01",
            api_mode="data_v1",
            api_key="sdn-key",
        ),
        sender_fn=_mock_post,
    )
    res = pub.publish_snapshot(snap, deliver_now=True)
    assert res.status == DeliveryStatus.DELIVERED
    assert captured[0]["url"].endswith(DATA_V1_ENDPOINT_SNAPSHOTS)


# ---------------------------------------------------------------------------
# Task C: 48-Flow Audit Builder & Causal Outbox Delivery Ordering
# ---------------------------------------------------------------------------


def test_p2_c01_48_flow_apply_result_builder_applied_rolled_back_unknown_and_missing_decision() -> None:
    topo, sm, installer = _bootstrap_env("run_c01_48")
    rs_initial = sm.get_routing_state("run_c01_48")

    # 1. Missing decision_rows -> MUST fail closed (never fabricate 48 rows!)
    resp_applied = ApplyResponse(
        run_id="run_c01_48",
        decision_batch_id="batch_1",
        status=BatchStatus.APPLIED,
        state_version=rs_initial.state_version,
        completed_at="2026-11-24T09:01:10Z",
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
                applied_at="2026-11-24T09:01:10Z",
            ),
        ),
        rule_count=3,
    )
    with pytest.raises(DataV1ContractBlockedError):
        build_48_flow_apply_result_data_v1(
            resp_applied,
            decision_rows=None,
            routing_state=rs_initial,
        )

    # 2. Build a realistic post-apply RoutingStateSnapshot where 0x103 moved to P2 at v2
    updated_entries = []
    for e in rs_initial.routing_state:
        if e.flow_group_id == "0x103":
            updated_entries.append(
                RoutingStateEntry(
                    run_id=e.run_id,
                    flow_group_id=e.flow_group_id,
                    path_id="0x103_P2",
                    state_version=2,
                    confirmed_at="2026-11-24T09:01:10Z",
                    last_successful_change_at="2026-11-24T09:01:10Z",
                    verification_status=VerificationStatus.VERIFIED,
                    config_version=e.config_version,
                    evidence_mode=e.evidence_mode,
                    last_optimization_change_at="2026-11-24T09:01:10Z",
                )
            )
        else:
            updated_entries.append(
                RoutingStateEntry(
                    run_id=e.run_id,
                    flow_group_id=e.flow_group_id,
                    path_id=e.path_id,
                    state_version=2,
                    confirmed_at=e.confirmed_at,
                    last_successful_change_at=e.last_successful_change_at,
                    verification_status=e.verification_status,
                    config_version=e.config_version,
                    evidence_mode=e.evidence_mode,
                    last_optimization_change_at=e.last_optimization_change_at,
                )
            )
    rs_v2 = RoutingStateSnapshot(
        run_id="run_c01_48",
        state_version=2,
        config_version=rs_initial.config_version,
        topology_version=rs_initial.topology_version,
        reconcile_required=False,
        in_flight_batch_id=None,
        routing_state_complete=True,
        unknown_flow_group_ids=(),
        routing_state=tuple(updated_entries),
    )
    resp_v2 = ApplyResponse(
        run_id="run_c01_48",
        decision_batch_id="batch_v2",
        status=BatchStatus.APPLIED,
        state_version=2,
        completed_at="2026-11-24T09:01:10Z",
        results=resp_applied.results,
        rule_count=3,
    )
    decision_rows = _make_decision_rows(topo, {"0x103": ("0x103_P1", "0x103_P2")})
    built = build_48_flow_apply_result_data_v1(
        resp_v2,
        decision_rows=decision_rows,
        routing_state=rs_v2,
    )
    assert len(built["rows"]) == 48
    by_fg = {r["flow_group_id"]: r for r in built["rows"]}
    assert by_fg["0x103"]["status"] == "APPLIED"
    assert by_fg["0x103"]["actual_path_id"] == "0x103_P2"
    assert by_fg["0x103"]["verified_at_utc"] == "2026-11-24T09:01:10Z"
    assert sum(1 for r in built["rows"] if r["status"] == "KEPT") == 47
    assert by_fg["0x104"]["actual_path_id"] == "0x104_P1"
    assert by_fg["0x104"]["verified_at_utc"] == "2026-11-24T09:00:00Z"

    # 3. ROLLED_BACK changed flow -> actual_path_id == old_path_id + 47 KEPT
    resp_rb = ApplyResponse(
        run_id="run_c01_48",
        decision_batch_id="batch_rb",
        status=BatchStatus.ROLLED_BACK,
        state_version=rs_initial.state_version,
        completed_at="2026-11-24T09:01:12Z",
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
                applied_at="2026-11-24T09:01:12Z",
            ),
        ),
        rule_count=6,
        reason="VERIFY_FAILED",
    )
    built_rb = build_48_flow_apply_result_data_v1(
        resp_rb,
        decision_rows=decision_rows,
        routing_state=rs_initial,
    )
    by_fg_rb = {r["flow_group_id"]: r for r in built_rb["rows"]}
    assert by_fg_rb["0x103"]["status"] == "ROLLED_BACK"
    assert by_fg_rb["0x103"]["actual_path_id"] == "0x103_P1"
    assert sum(1 for r in built_rb["rows"] if r["status"] == "KEPT") == 47

    # 4. UNKNOWN flow -> actual_path_id=None, verified_at_utc=None (never faked!)
    unk_entries = [
        RoutingStateEntry(
            run_id=e.run_id,
            flow_group_id=e.flow_group_id,
            path_id=None if e.flow_group_id == "0x103" else e.path_id,
            state_version=3,
            confirmed_at=None if e.flow_group_id == "0x103" else e.confirmed_at,
            last_successful_change_at=e.last_successful_change_at,
            verification_status=(
                VerificationStatus.UNKNOWN if e.flow_group_id == "0x103" else e.verification_status
            ),
            config_version=e.config_version,
            evidence_mode=e.evidence_mode,
            last_optimization_change_at=e.last_optimization_change_at,
        )
        for e in rs_initial.routing_state
    ]
    rs_unk = RoutingStateSnapshot(
        run_id="run_c01_48",
        state_version=3,
        config_version=rs_initial.config_version,
        topology_version=rs_initial.topology_version,
        reconcile_required=True,
        in_flight_batch_id=None,
        routing_state_complete=False,
        unknown_flow_group_ids=("0x103",),
        routing_state=tuple(unk_entries),
    )
    resp_unk = ApplyResponse(
        run_id="run_c01_48",
        decision_batch_id="batch_unk",
        status=BatchStatus.INCONSISTENT,
        state_version=3,
        completed_at="2026-11-24T09:01:15Z",
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
                applied_at="2026-11-24T09:01:15Z",
            ),
        ),
        rule_count=6,
        reason="ROLLBACK_FAILED",
    )
    built_unk = build_48_flow_apply_result_data_v1(
        resp_unk,
        decision_rows=decision_rows,
        routing_state=rs_unk,
    )
    by_fg_unk = {r["flow_group_id"]: r for r in built_unk["rows"]}
    assert by_fg_unk["0x103"]["status"] == "UNKNOWN"
    assert by_fg_unk["0x103"]["actual_path_id"] is None
    assert by_fg_unk["0x103"]["verified_at_utc"] is None

    # 5. DecisionBatch lookup helper
    fetched = fetch_decision_batch_from_data_v1(
        "http://data.local:8000",
        "run_c01_48",
        "batch_v2",
        api_key="sdn-key",
        http_get_fn=lambda u, h, t: (200, json.dumps({"request_payload": {"rows": decision_rows}})),
    )
    assert len(fetched) == 48
    with pytest.raises(DataV1ContractBlockedError):
        fetch_decision_batch_from_data_v1(
            "http://data.local:8000",
            "run_c01_48",
            "missing_batch",
            api_key="sdn-key",
            http_get_fn=lambda u, h, t: (404, '{"detail":"Unknown decision_batch_id"}'),
        )


def test_p2_c02_causal_outbox_enforces_routing_state_ack_before_apply_result_across_restart(
    tmp_path: Path,
) -> None:
    topo, sm, _ = _bootstrap_env("run_c02_order")
    rs = sm.get_routing_state("run_c02_order")
    decision_rows = _make_decision_rows(topo, {"0x103": ("0x103_P1", "0x103_P2")})
    resp = ApplyResponse(
        run_id="run_c02_order",
        decision_batch_id="batch_order_1",
        status=BatchStatus.ROLLED_BACK,
        state_version=rs.state_version,
        completed_at="2026-11-24T09:01:20Z",
        results=(
            ApplyFlowResult(
                flow_group_id="0x103",
                old_path_id="0x103_P1",
                proposed_path_id="0x103_P2",
                actual_path_id="0x103_P1",
                status=FlowDecisionStatus.ROLLED_BACK,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=6,
                reason="ROLLBACK_TEST",
                applied_at="2026-11-24T09:01:20Z",
            ),
        ),
        rule_count=6,
        reason="ROLLBACK_TEST",
    )

    delivery_log: list[str] = []
    outage = {"down": True}

    def _mock_post(url: str, payload: dict[str, Any], headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
        if outage["down"]:
            return 503, '{"detail":"pg down"}'
        if url.endswith(DATA_V1_ENDPOINT_ROUTING):
            delivery_log.append("ROUTING")
            return 200, '{"inserted":1}'
        if url.endswith(DATA_V1_ENDPOINT_APPLY_RESULTS):
            assert "ROUTING" in delivery_log, "ApplyResult MUST NOT arrive before RoutingState ACK!"
            assert len(payload["rows"]) == 48
            delivery_log.append("APPLY_RESULT")
            return 200, '{"updated":1}'
        return 404, "not found"

    outbox_dir = tmp_path / "causal_outbox"
    pub1 = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://data.local:8000",
            outbox_dir=outbox_dir,
            api_mode="data_v1",
            api_key="sdn-key",
            max_retries=1,
        ),
        sender_fn=_mock_post,
    )

    # Deliberately enqueue ApplyResult BEFORE RoutingState (reordered!) while Data is down
    r_apply_first = pub1.publish_apply_response(
        resp,
        deliver_now=True,
        full_routing_state=rs,
        decision_rows=decision_rows,
    )
    assert r_apply_first.status == DeliveryStatus.PENDING_DELIVERY
    assert r_apply_first.attempts == 0  # Zero HTTP attempts because RoutingState is not ACKed!
    assert "WAITING_FOR_DEPENDENCY:routing_state:run_c02_order:v" in (r_apply_first.last_error or "")

    # Enqueue RoutingState while Data is still down -> fails with 503 -> ApplyResult still 0 attempts
    r_route_down = pub1.publish_routing_state(rs, deliver_now=True)
    assert r_route_down.status == DeliveryStatus.PENDING_DELIVERY
    assert delivery_log == []

    # Simulate process restart: reload durable outbox from disk!
    pub2 = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://data.local:8000",
            outbox_dir=outbox_dir,
            api_mode="data_v1",
            api_key="sdn-key",
            max_retries=1,
        ),
        sender_fn=_mock_post,
    )
    apply_rec_after_restart = pub2.get_event(r_apply_first.dedup_key)
    assert apply_rec_after_restart is not None
    assert apply_rec_after_restart.depends_on_dedup_key == r_route_down.dedup_key

    # Data comes back online -> single flush_pending() MUST deliver ROUTING first, then APPLY_RESULT!
    outage["down"] = False
    flushed = pub2.flush_pending()
    assert flushed == {"delivered": 2, "pending": 0}
    assert delivery_log == ["ROUTING", "APPLY_RESULT"]

    # Duplicate replay after delivery -> idempotent, zero extra HTTP sends
    r_dup = pub2.publish_apply_response(
        resp,
        deliver_now=True,
        full_routing_state=rs,
        decision_rows=decision_rows,
    )
    assert r_dup.duplicate is True
    assert delivery_log == ["ROUTING", "APPLY_RESULT"]
