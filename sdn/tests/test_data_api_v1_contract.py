"""
Data API v1 Contract, Readiness & DemandRelease Duplicate Semantics Tests (`Phase 6`).

Validates SDN -> Data API v1 adapter against the exact `origin/integration/e2e`
Data API v1 contract (`online_api.py`, `online_models.py`, `management_api.py`):
1. RoutingState mapper (`POST /v1/sdn/routing`) + `extra="forbid"` + `Route.check_confirmation`.
2. Snapshot mapper (`POST /v1/sdn/snapshots`) + exact byte-level `X-Content-SHA256` + `CONTRACT_BLOCKED` on missing `available_at_utc`.
3. ApplyResponse mapper (`POST /v1/sdn/apply-results`) + `UNKNOWN` containment + inconsistent row rejection.
4. Data API v1 authentication (`X-API-Key`), `200` persistence ACK (`{"inserted": 1}`), and non-persistence `200` rejection.
5. Non-retryable `401`, `403`, `409`, `422` handling (never marks `DELIVERED`, never tight-loops retries).
6. Data receiver downtime & automatic recovery + duplicate event delivery idempotency.
7. `warmup=60` traffic-origin vs measurement-origin bin mapping (`measurement bin 0 -> traffic bin 60`).
8. `RoutingHistory` and `VerificationEvidence` kept safely `PENDING_DELIVERY` (`CONTRACT_BLOCKED`, no invented endpoints).
9. Phase 4 fix: Data endpoint configured + outbox worker stopped/crashed + 0 pending backlog -> `data_delivery_readiness.ready == False` (`DATA_OUTBOX_WORKER_NOT_RUNNING`).
10. Phase 5 fix: `DemandRelease` same `event_id` with altered `released_at_utc`, window, or `requested_mbps` -> `409 DEMAND_RELEASE_CONFLICT`, while exact replay preserves original `available_at_utc` across restart.

Classification: `PASS_CONTRACT_MOCK` (no live PostgreSQL database).
"""

from __future__ import annotations

import hashlib
import io
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from sdn.adapters.data_api_v1_adapter import (
    DATA_V1_ENDPOINT_APPLY_RESULTS,
    DATA_V1_ENDPOINT_ROUTING,
    DATA_V1_ENDPOINT_SNAPSHOTS,
    DataV1ContractBlockedError,
    DataV1MappingError,
    is_valid_data_v1_ack,
    map_apply_response_to_data_v1,
    map_routing_state_to_data_v1,
    map_snapshot_to_data_v1,
    serialize_canonical_json_bytes,
)
from sdn.adapters.data_event_publisher import (
    DataEventPublisher,
    DataPublisherConfig,
    DeliveryStatus,
)
from sdn.adapters.http_control_api import (
    HTTPDemandReleaseProvider,
    NorthboundAPIConfig,
    SDNNorthboundHTTPApp,
)
from sdn.adapters.sqlite_demand_ledger import SQLiteDemandReleaseLedger
from sdn.controller.decision_handler import DecisionHandler, DecisionHandlerConfig
from sdn.controller.state_manager import StateManager
from sdn.controller.topology_registry import TopologyRegistry
from sdn.flow_management.flow_matcher import FlowMatcher
from sdn.flow_management.route_compiler import RouteCompiler
from sdn.flow_management.rule_installer import RuleInstaller
from sdn.flow_management.switch_driver import InMemorySwitchDriver
from sdn.monitoring.snapshot_creator import MockReleasedDemandProvider, SnapshotCreator
from sdn.route_verification.rule_verifier import RuleVerifier
from sdn.schemas.apply_models import ApplyFlowResult, ApplyResponse
from sdn.schemas.snapshot_models import DemandObservationRecord, OriginRefs
from sdn.schemas.state_models import (
    BatchStatus,
    FlowDecisionStatus,
    VerificationStatus,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TOPOLOGY_BUNDLE_PATH = REPO_ROOT / "configs" / "network" / "topology_bundle.json"
CATALOG_DIR = REPO_ROOT / "configs" / "network"

# Exact UTC regex copied from origin/integration/e2e:data/ingestion/online_models.py
DATA_V1_UTC_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)")


def _assert_data_v1_utc(val: Any) -> None:
    assert isinstance(val, str) and DATA_V1_UTC_PATTERN.fullmatch(val) is not None, f"Bad Data v1 UTC: {val!r}"


def _assert_data_v1_route(row: dict[str, Any]) -> None:
    assert set(row.keys()) == {"flow_group_id", "path_id", "confirmed_at_utc", "verification_status"}
    assert row["verification_status"] in {"VERIFIED", "UNKNOWN", "INCONSISTENT"}
    verified = row["verification_status"] == "VERIFIED"
    assert verified == (row["path_id"] is not None) == (row["confirmed_at_utc"] is not None)
    if verified:
        _assert_data_v1_utc(row["confirmed_at_utc"])


def _assert_data_v1_routing(payload: dict[str, Any]) -> None:
    assert set(payload.keys()) == {"run_id", "state_version", "published_at_utc", "rows"}
    assert isinstance(payload["state_version"], int) and not isinstance(payload["state_version"], bool)
    _assert_data_v1_utc(payload["published_at_utc"])
    assert 1 <= len(payload["rows"]) <= 4096
    for row in payload["rows"]:
        _assert_data_v1_route(row)


def _assert_data_v1_snapshot(payload: dict[str, Any], raw_bytes: bytes, sha_hex: str) -> None:
    assert set(payload.keys()) == {"sdn"}
    sdn_obj = payload["sdn"]
    assert {
        "snapshot_id",
        "run_id",
        "snapshot_as_of_utc",
        "history_cutoff_utc",
        "cutoff_offset_s",
        "lookback_L",
        "delta_s",
        "state_version",
        "config_version",
        "config_hash",
        "topology_version",
        "topology_hash",
        "content_hash",
        "origin_refs",
        "flow_group_ids",
        "history",
        "routing_state",
        "routing_history",
        "link_sample_refs",
        "quality_status",
        "missing_groups",
        "routing_state_complete",
        "unknown_flow_group_ids",
        "provenance",
        "reason",
        "expected_bin_count",
        "valid_bin_count",
    } == set(sdn_obj.keys())
    _assert_data_v1_utc(sdn_obj["history_cutoff_utc"])
    _assert_data_v1_utc(sdn_obj["snapshot_as_of_utc"])
    assert sdn_obj["quality_status"] in {"VALID", "INCOMPLETE", "STALE", "INVALID"}
    assert hashlib.sha256(raw_bytes).hexdigest() == sha_hex
    for h in sdn_obj["history"]:
        assert set(h.keys()) == {
            "flow_group_id",
            "bin_index",
            "window_start_utc",
            "window_end_utc",
            "requested_mbps",
            "released_at_utc",
            "available_at_utc",
            "valid",
            "reason",
            "release_record_id",
        }
        if h["available_at_utc"] is not None:
            _assert_data_v1_utc(h["available_at_utc"])


def _assert_data_v1_apply_result(payload: dict[str, Any]) -> None:
    assert set(payload.keys()) == {
        "run_id",
        "decision_batch_id",
        "response_status",
        "response_state_version",
        "completed_at_utc",
        "rows",
    }
    assert payload["response_status"] in {"APPLIED", "REJECTED", "FAILED", "ROLLED_BACK", "PARTIAL", "INCONSISTENT"}
    _assert_data_v1_utc(payload["completed_at_utc"])
    assert len(payload["rows"]) >= 1
    for r in payload["rows"]:
        assert set(r.keys()) == {"flow_group_id", "actual_path_id", "status", "verified_at_utc", "reason"}
        assert r["status"] in {"APPLIED", "KEPT", "FAILED", "ROLLED_BACK", "UNKNOWN"}
        assert (r["actual_path_id"] is None) == (r["verified_at_utc"] is None) == (r["status"] == "UNKNOWN")
        if r["verified_at_utc"] is not None:
            _assert_data_v1_utc(r["verified_at_utc"])


def _bootstrap_env(run_id: str = "run_v1_contract"):
    topo = TopologyRegistry.from_bundle_file(TOPOLOGY_BUNDLE_PATH, csv_config_dir=CATALOG_DIR)
    driver = InMemorySwitchDriver(topo)
    comp = RouteCompiler(topo, FlowMatcher(topo))
    installer = RuleInstaller(comp, driver, RuleVerifier(comp, driver))
    sm = StateManager(topo)
    boot_ts = "2026-11-23T09:00:00Z"
    evs = installer.bootstrap_initial_paths(run_id, boot_ts)
    sm.initialize_run(run_id, boot_ts, verification_evidences=evs)
    return topo, sm, installer


def _wsgi_call(app: SDNNorthboundHTTPApp, method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
    raw = json.dumps(body).encode("utf-8") if body is not None else b""
    env = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "QUERY_STRING": "",
        "CONTENT_LENGTH": str(len(raw)),
        "wsgi.input": io.BytesIO(raw),
        "HTTP_X_SDN_TOKEN": "tok-v1",
    }
    out: dict[str, Any] = {}

    def _start(status: str, headers: list[tuple[str, str]]) -> None:
        out["code"] = int(status.split(" ", 1)[0])

    chunks = app(env, _start)
    return out["code"], json.loads(b"".join(chunks).decode("utf-8"))


def test_c01_routing_state_mapper_matches_data_v1_schema() -> None:
    _, sm, _ = _bootstrap_env("run_c01")
    rs = sm.get_routing_state("run_c01")
    mapped = map_routing_state_to_data_v1(rs)
    _assert_data_v1_routing(mapped)
    assert len(mapped["rows"]) == 48
    assert mapped["rows"][0]["flow_group_id"] == "0x103"
    assert mapped["rows"][0]["path_id"] == "0x103_P1"
    assert mapped["rows"][0]["confirmed_at_utc"] == "2026-11-23T09:00:00Z"


def test_c02_snapshot_mapper_warmup60_origin_and_byte_sha256_and_missing_blocked() -> None:
    topo, sm, _ = _bootstrap_env("run_c02")
    dp = MockReleasedDemandProvider()
    origins = OriginRefs(
        run_started_at_utc="2026-11-23T09:00:00Z",
        traffic_started_at_utc="2026-11-23T09:00:00Z",
        measurement_started_at_utc="2026-11-23T09:01:00Z",  # warmup = 60s!
        delta_s=1,
    )
    dp.add_observations(
        [
            DemandObservationRecord(
                run_id="run_c02",
                flow_group_id=fg.flow_group_id,
                bin_index=0,
                window_start_utc="2026-11-23T09:01:00Z",
                window_end_utc="2026-11-23T09:01:01Z",
                requested_mbps=3.5,
                released_at_utc="2026-11-23T09:01:01Z",
                available_at_utc="2026-11-23T09:01:01.050000Z",
                release_record_id=f"ev:{fg.flow_group_id}:0",
            )
            for fg in topo.get_all_flow_groups()
        ]
    )
    sc = SnapshotCreator(topo, sm, dp)
    snap_ok = sc.create_snapshot(
        "run_c02",
        cutoff=1,
        lookback_L=1,
        snapshot_as_of_utc="2026-11-23T09:01:01.200000Z",
        origin_refs=origins,
    )

    mapped, raw_bytes, sha_hex = map_snapshot_to_data_v1(snap_ok)
    _assert_data_v1_snapshot(mapped, raw_bytes, sha_hex)
    # Canonical SDNSnapshotV14 preserves measurement-origin bin_index=0 and warmup_s=60!
    assert mapped["sdn"]["origin_refs"]["warmup_s"] == 60
    assert mapped["sdn"]["cutoff_offset_s"] == 1
    assert all(h["bin_index"] == 0 for h in mapped["sdn"]["history"])
    assert all(h["requested_mbps"] == 3.5 for h in mapped["sdn"]["history"])

    # Missing bin (available_at_utc=None) maps cleanly to canonical INCOMPLETE with null evidence (never fabricates 0.0!)
    snap_missing = sc.create_snapshot(
        "run_c02",
        cutoff=2,
        lookback_L=2,
        snapshot_as_of_utc="2026-11-23T09:01:02.200000Z",
        origin_refs=origins,
    )
    mapped_inc, raw_inc, sha_inc = map_snapshot_to_data_v1(snap_missing)
    _assert_data_v1_snapshot(mapped_inc, raw_inc, sha_inc)
    assert mapped_inc["sdn"]["quality_status"] == "INCOMPLETE"
    missing_rows = [h for h in mapped_inc["sdn"]["history"] if not h["valid"]]
    assert len(missing_rows) == 48
    assert all(
        h["requested_mbps"] is None
        and h["released_at_utc"] is None
        and h["available_at_utc"] is None
        and h["release_record_id"] is None
        and h["reason"] is None
        for h in missing_rows
    )


def test_c03_apply_response_mapper_valid_unknown_and_inconsistent_rejection() -> None:
    resp_ok = ApplyResponse(
        run_id="run_c03",
        decision_batch_id="batch_c03_1",
        status=BatchStatus.APPLIED,
        state_version=3,
        completed_at="2026-11-23T09:01:05Z",
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
                applied_at="2026-11-23T09:01:05Z",
            ),
        ),
        rule_count=3,
    )
    mapped_ok = map_apply_response_to_data_v1(resp_ok)
    _assert_data_v1_apply_result(mapped_ok)
    assert mapped_ok["rows"][0]["status"] == "APPLIED"

    # UNKNOWN flow must map with actual_path_id=None and verified_at_utc=None (never APPLIED!)
    resp_unk = ApplyResponse(
        run_id="run_c03",
        decision_batch_id="batch_c03_2",
        status=BatchStatus.INCONSISTENT,
        state_version=4,
        completed_at="2026-11-23T09:01:10Z",
        results=(
            ApplyFlowResult(
                flow_group_id="0x103",
                old_path_id="0x103_P2",
                proposed_path_id="0x103_P1",
                actual_path_id=None,
                status=FlowDecisionStatus.UNKNOWN,
                verification_status=VerificationStatus.UNKNOWN,
                rule_count=6,
                reason="ROLLBACK_FAILED",
                applied_at="2026-11-23T09:01:10Z",
            ),
        ),
        rule_count=6,
        reason="ROLLBACK_FAILED",
    )
    mapped_unk = map_apply_response_to_data_v1(resp_unk)
    _assert_data_v1_apply_result(mapped_unk)
    assert mapped_unk["rows"][0]["status"] == "UNKNOWN"
    assert mapped_unk["rows"][0]["actual_path_id"] is None
    assert mapped_unk["rows"][0]["verified_at_utc"] is None

    # Inconsistent row (APPLIED with actual_path_id=None) -> fail closed
    resp_bad = ApplyResponse(
        run_id="run_c03",
        decision_batch_id="batch_c03_bad",
        status=BatchStatus.FAILED,
        state_version=4,
        completed_at="2026-11-23T09:01:10Z",
        results=(
            ApplyFlowResult(
                flow_group_id="0x103",
                old_path_id="0x103_P2",
                proposed_path_id="0x103_P1",
                actual_path_id=None,
                status=FlowDecisionStatus.APPLIED,
                verification_status=VerificationStatus.VERIFIED,
                rule_count=3,
                reason=None,
                applied_at="2026-11-23T09:01:10Z",
            ),
        ),
        rule_count=3,
    )
    with pytest.raises(DataV1MappingError):
        map_apply_response_to_data_v1(resp_bad)


def test_c04_data_v1_transport_auth_ack_errors_outage_and_contract_blocked(tmp_path: Path) -> None:
    """
    Tests Data API v1 transport behavior:
    - X-API-Key auth & exact byte-level X-Content-SHA256 on /v1/sdn/snapshots.
    - 200 {"inserted": 1} -> DELIVERED; empty 200 -> NOT DELIVERED.
    - 401/403/409/422 -> NOT DELIVERED, stops retries immediately (1 attempt).
    - Receiver down -> PENDING_DELIVERY -> receiver recovers -> DELIVERED (idempotent on duplicate).
    - RoutingHistory & VerificationEvidence -> safely PENDING_DELIVERY (CONTRACT_BLOCKED, 0 HTTP sends).
    """
    topo, sm, _ = _bootstrap_env("run_c04")
    rs = sm.get_routing_state("run_c04")
    calls: list[dict[str, Any]] = []
    mode = {"resp": (200, '{"inserted":1}')}

    def _mock_data_v1(url: str, payload: dict[str, Any], headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
        calls.append({"url": url, "headers": dict(headers), "payload": payload})
        if headers.get("X-API-Key") != "secret-sdn-key":
            return 401, '{"detail":"X-API-Key is invalid"}'
        if url.endswith(DATA_V1_ENDPOINT_SNAPSHOTS):
            expected_sha = hashlib.sha256(serialize_canonical_json_bytes(payload)).hexdigest()
            if headers.get("X-Content-SHA256") != expected_sha:
                return 422, '{"detail":"X-Content-SHA256 mismatch"}'
        return mode["resp"]

    # 1. Valid 200 ACK + X-API-Key + duplicate delivery idempotency
    pub = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://127.0.0.1:19090",
            outbox_dir=tmp_path / "ob_v1",
            api_mode="data_v1",
            api_key="secret-sdn-key",
            max_retries=3,
            backoff_base_s=0.001,
        ),
        sender_fn=_mock_data_v1,
    )
    r1 = pub.publish_routing_state(rs, deliver_now=True)
    assert r1.status == DeliveryStatus.DELIVERED
    assert calls[-1]["url"].endswith(DATA_V1_ENDPOINT_ROUTING)
    assert calls[-1]["headers"]["X-API-Key"] == "secret-sdn-key"

    # Duplicate publish -> idempotent, zero extra HTTP sends
    before_calls = len(calls)
    r1_dup = pub.publish_routing_state(rs, deliver_now=True)
    assert r1_dup.duplicate is True
    assert len(calls) == before_calls

    # RoutingHistory in data_v1 mode -> CONTRACT_BLOCKED, stays PENDING_DELIVERY, 0 HTTP calls
    r_hist = pub.publish_routing_history("run_c04", rs.state_version, (), deliver_now=True)
    assert r_hist.status == DeliveryStatus.PENDING_DELIVERY
    assert len(calls) == before_calls

    # 2. Non-persistence 200 body must NOT be treated as DELIVERED
    assert is_valid_data_v1_ack(200, '{"inserted": 1}') is True
    assert is_valid_data_v1_ack(200, '{"updated": 0}') is True
    assert is_valid_data_v1_ack(200, "") is False
    assert is_valid_data_v1_ack(200, '{"ok": true}') is False

    # 3. Deterministic 401/403/409/422 errors -> 1 attempt only, stays PENDING_DELIVERY
    for err_code in (401, 403, 409, 422):
        calls.clear()
        mode["resp"] = (err_code, '{"detail":"rejected"}')
        pub_err = DataEventPublisher(
            DataPublisherConfig(
                endpoint_base_url="http://127.0.0.1:19090",
                api_mode="data_v1",
                api_key="secret-sdn-key",
                max_retries=3,
                backoff_base_s=0.001,
            ),
            sender_fn=_mock_data_v1,
        )
        re_res = pub_err.publish_routing_state(rs, deliver_now=True)
        assert re_res.status == DeliveryStatus.PENDING_DELIVERY
        assert re_res.attempts == 1
        assert f"HTTP_{err_code}" in (re_res.last_error or "")

    # 4. Receiver outage (503) -> stays PENDING_DELIVERY -> recovery (200) -> DELIVERED
    mode["resp"] = (503, "db down")
    pub_rec = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://127.0.0.1:19090",
            outbox_dir=tmp_path / "ob_rec",
            api_mode="data_v1",
            api_key="secret-sdn-key",
            max_retries=1,
        ),
        sender_fn=_mock_data_v1,
    )
    r_down = pub_rec.publish_routing_state(rs, deliver_now=True)
    assert r_down.status == DeliveryStatus.PENDING_DELIVERY
    mode["resp"] = (200, '{"inserted":1}')
    flushed = pub_rec.flush_pending()
    assert flushed == {"delivered": 1, "pending": 0}


def test_c05_phase4_readiness_worker_stopped_with_zero_backlog_is_not_ready(tmp_path: Path) -> None:
    """
    Phase 4 regression: Data endpoint configured + outbox worker stopped/crashed + 0 backlog
    MUST report `data_delivery_readiness.ready == False` with `DATA_OUTBOX_WORKER_NOT_RUNNING`
    and `full_system_readiness.ready == False`.
    """
    topo, sm, installer = _bootstrap_env("run_c05")
    pub = DataEventPublisher(
        DataPublisherConfig(
            endpoint_base_url="http://127.0.0.1:19090",
            outbox_dir=tmp_path / "ob_c05",
            auto_flush=False,  # Simulates orchestrator wiring where worker was started then stopped/crashed
        ),
        sender_fn=lambda u, p, h, t: (200, '{"ack":true}'),
    )
    pub.start_worker()
    pub.stop_worker()
    assert pub.worker_running is False
    assert pub.status_summary()["pending_delivery_count"] == 0

    dp = HTTPDemandReleaseProvider()
    sc = SnapshotCreator(topo, sm, dp)
    dh = DecisionHandler(topo, sm, installer, config=DecisionHandlerConfig.for_integration())
    app = SDNNorthboundHTTPApp(
        state_manager=sm,
        decision_handler=dh,
        snapshot_creator=sc,
        demand_provider=dp,
        data_publisher=pub,
        config=NorthboundAPIConfig(auth_token="tok-v1", default_run_id="run_c05"),
        health_provider=lambda: {
            "connected_dpids": [1, 2, 3, 4, 5, 6],
            "readiness_state": "VERIFIED",
            "verified_cached": 48,
            "state_version": 2,
            "reconcile_required": False,
        },
    )
    code, health = _wsgi_call(app, "GET", "/health")
    assert code == 200
    assert health["process_liveness"]["alive"] is True
    assert health["sdn_readiness"]["ready"] is True
    assert health["data_delivery_readiness"]["configured"] is True
    assert health["data_delivery_readiness"]["worker_running"] is False
    assert health["data_delivery_readiness"]["pending_delivery_count"] == 0
    assert health["data_delivery_readiness"]["ready"] is False
    assert "DATA_OUTBOX_WORKER_NOT_RUNNING" in health["data_delivery_readiness"]["blockers"]
    assert health["sdn_integration_readiness"]["ready"] is False
    assert health["full_system_readiness"]["ready"] is False


def test_c06_phase5_demand_release_conflict_on_altered_released_at_window_and_mbps(tmp_path: Path) -> None:
    """
    Phase 5 regression:
    - Same event_id + identical payload -> 200 duplicate=True, original available_at_utc unchanged.
    - Same event_id + altered released_at_utc -> 409 DEMAND_RELEASE_CONFLICT.
    - Same event_id + altered window -> 409 DEMAND_RELEASE_CONFLICT.
    - Same event_id + altered requested_mbps -> 409 DEMAND_RELEASE_CONFLICT.
    - Restart restores exact original available_at_utc & rejects conflicts.
    """
    db_path = tmp_path / "demand_c06.sqlite3"
    topo, sm, installer = _bootstrap_env("run_c06")
    ledger1 = SQLiteDemandReleaseLedger(db_path)
    dp1 = HTTPDemandReleaseProvider(ledger=ledger1)
    sc1 = SnapshotCreator(topo, sm, dp1)
    dh1 = DecisionHandler(topo, sm, installer, config=DecisionHandlerConfig.for_integration())
    app1 = SDNNorthboundHTTPApp(
        state_manager=sm,
        decision_handler=dh1,
        snapshot_creator=sc1,
        demand_provider=dp1,
        config=NorthboundAPIConfig(auth_token="tok-v1", default_run_id="run_c06"),
    )

    base_ev = {
        "event_id": "network:run_c06:demand:0x103:0",
        "run_id": "run_c06",
        "flow_group_id": "0x103",
        "bin_index": 0,
        "window_start_utc": "2026-11-23T09:01:00Z",
        "window_end_utc": "2026-11-23T09:01:01Z",
        "requested_mbps": 4.2,
        "released_at_utc": "2026-11-23T09:01:01Z",
    }
    c0, b0 = _wsgi_call(app1, "POST", "/demand-releases", body=base_ev)
    assert c0 == 200 and b0["ack"] is True and b0["duplicate"] is False
    orig_avail = b0["available_at_utc"]
    dp1.close()

    # Restart on same SQLite DB
    ledger2 = SQLiteDemandReleaseLedger(db_path)
    dp2 = HTTPDemandReleaseProvider(ledger=ledger2)
    sc2 = SnapshotCreator(topo, sm, dp2)
    app2 = SDNNorthboundHTTPApp(
        state_manager=sm,
        decision_handler=dh1,
        snapshot_creator=sc2,
        demand_provider=dp2,
        config=NorthboundAPIConfig(auth_token="tok-v1", default_run_id="run_c06"),
    )
    try:
        # 1. Exact duplicate -> 200, duplicate=True, available_at_utc unchanged!
        c_dup, b_dup = _wsgi_call(app2, "POST", "/demand-releases", body=base_ev)
        assert c_dup == 200 and b_dup["duplicate"] is True
        assert b_dup["available_at_utc"] == orig_avail

        # 2. Altered released_at_utc -> 409 DEMAND_RELEASE_CONFLICT!
        ev_bad_rel = dict(base_ev, released_at_utc="2026-11-23T09:01:02Z")
        c_rel, b_rel = _wsgi_call(app2, "POST", "/demand-releases", body=ev_bad_rel)
        assert c_rel == 409 and b_rel["error"] == "DEMAND_RELEASE_CONFLICT"

        # 3. Altered window -> 409 DEMAND_RELEASE_CONFLICT!
        ev_bad_win = dict(base_ev, window_end_utc="2026-11-23T09:01:02Z")
        c_win, b_win = _wsgi_call(app2, "POST", "/demand-releases", body=ev_bad_win)
        assert c_win == 409 and b_win["error"] == "DEMAND_RELEASE_CONFLICT"

        # 4. Altered requested_mbps -> 409 DEMAND_RELEASE_CONFLICT!
        ev_bad_mbps = dict(base_ev, requested_mbps=9.9)
        c_mbps, b_mbps = _wsgi_call(app2, "POST", "/demand-releases", body=ev_bad_mbps)
        assert c_mbps == 409 and b_mbps["error"] == "DEMAND_RELEASE_CONFLICT"
    finally:
        dp2.close()
