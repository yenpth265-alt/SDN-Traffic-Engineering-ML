"""
CF-21 Integration Contract Test — ML L=30 Snapshot (`48 x 30 = 1,440` genuine observations).

NOTE ON EVIDENCE SCOPE:
- This test validates `SnapshotCreator` + `DataAPIDemandHistoryProvider` + `map_snapshot_to_data_v1`
  using isolated synthetic HTTP mock responses (`PASS_CONTRACT_MOCK`).
- Real PostgreSQL-backed retrieval via `data.ingestion.online_api` is covered separately in
  `sdn/tests/integration_postgres_v14.py::test_cf21_ml_l30_snapshot_48x30_1440_observations`.

Validates:
- `lookback_L = 30`, `cutoff >= 30` measurement bins (`cutoff = 30`)
- 48 canonical flow groups x 30 bins = 1,440 genuine non-padded, non-zero-filled observations
- 48 `VERIFIED` routing paths
- Deterministic freshness boundary (`<= 2.0s` -> `VALID`, `> 2.0s` -> `STALE` / `DATA_STALE`)
- Canonical `SDNSnapshotV14` wire payload (`{"sdn": {...}}`), `content_hash`, and `X-Content-SHA256`
- Does NOT alter the existing deterministic `L=10` fixture under `sdn/fixtures/data_v14/`.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from sdn.adapters.data_api_v1_adapter import map_snapshot_to_data_v1
from sdn.adapters.data_demand_history_provider import (
    DataAPIDemandHistoryProvider,
    DataDemandHistoryConfig,
)
from sdn.controller.state_manager import StateManager, format_utc_timestamp
from sdn.controller.topology_registry import TopologyRegistry
from sdn.flow_management.flow_matcher import FlowMatcher
from sdn.flow_management.route_compiler import RouteCompiler
from sdn.flow_management.rule_installer import RuleInstaller
from sdn.flow_management.switch_driver import InMemorySwitchDriver
from sdn.monitoring.snapshot_creator import SnapshotCreator
from sdn.route_verification.rule_verifier import RuleVerifier
from sdn.schemas.snapshot_models import (
    OriginRefs,
    SnapshotQualityStatus,
    compute_snapshot_content_hash,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
BUNDLE_PATH = REPO_ROOT / "configs" / "network" / "topology_bundle.json"
CSV_DIR = REPO_ROOT / "configs" / "network"


def test_cf21_ml_l30_snapshot_48x30_1440_genuine_observations_and_hashes() -> None:
    topo = TopologyRegistry.from_bundle_file(BUNDLE_PATH, csv_config_dir=CSV_DIR)
    drv = InMemorySwitchDriver(topo)
    comp = RouteCompiler(topo, FlowMatcher(topo))
    inst = RuleInstaller(comp, drv, RuleVerifier(comp, drv))
    sm = StateManager(topo)

    run_id = "run_cf21_ml_l30"
    meas_start = datetime(2026, 11, 27, 12, 1, 0, tzinfo=timezone.utc)
    traffic_start = meas_start - timedelta(seconds=60)
    run_start = traffic_start - timedelta(seconds=5)
    boot_ts = format_utc_timestamp(run_start + timedelta(seconds=1))

    evs = inst.bootstrap_initial_paths(run_id, boot_ts)
    sm.initialize_run(run_id, boot_ts, verification_evidences=evs)

    cutoff = 30
    lookback_L = 30
    cutoff_dt = meas_start + timedelta(seconds=cutoff)
    as_of_dt = cutoff_dt + timedelta(milliseconds=250)  # <= T_stale (2.0s)

    fgs = topo.get_all_flow_groups()
    assert len(fgs) == 48

    # Simulate Data API `/v1/runs/{run_id}/demand?format=sdn` returning 48 x 30 = 1,440 genuine rows
    demand_rows: list[dict[str, Any]] = []
    for k in range(cutoff - lookback_L, cutoff):
        ws = format_utc_timestamp(meas_start + timedelta(seconds=k))
        we = format_utc_timestamp(meas_start + timedelta(seconds=k + 1))
        avail = format_utc_timestamp(meas_start + timedelta(seconds=k + 1) + timedelta(milliseconds=50))
        for idx, fg in enumerate(fgs):
            demand_rows.append(
                {
                    "run_id": run_id,
                    "flow_group_id": fg.flow_group_id,
                    "bin_index": k,
                    "window_start_utc": ws,
                    "window_end_utc": we,
                    "requested_mbps": round(1.25 + (idx * 0.04) + (k * 0.03), 4),
                    "released_at_utc": we,
                    "available_at_utc": avail,
                    "release_record_id": f"net:{run_id}:{fg.flow_group_id}:{60 + k}",
                    "source_kind": "RELEASED_DEMAND",
                    "valid": True,
                    "reason": None,
                }
            )
    assert len(demand_rows) == 1440

    seen_queries: list[dict[str, list[str]]] = []

    def _mock_data_get(url: str, headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
        del timeout_s
        assert headers.get("X-API-Key") == "qa-sdn-l30"
        parsed = urlparse(url)
        q = parse_qs(parsed.query)
        seen_queries.append(q)
        assert q.get("format") == ["sdn"]
        assert q.get("lookback") == ["30"]
        return 200, json.dumps(demand_rows)

    provider = DataAPIDemandHistoryProvider(
        DataDemandHistoryConfig(
            endpoint_base_url="http://data.local:8000",
            api_key="qa-sdn-l30",
            timeout_s=5.0,
        ),
        http_get_fn=_mock_data_get,
    )
    sc = SnapshotCreator(topo, sm, provider)
    origins = OriginRefs(
        run_started_at_utc=format_utc_timestamp(run_start),
        traffic_started_at_utc=format_utc_timestamp(traffic_start),
        measurement_started_at_utc=format_utc_timestamp(meas_start),
        warmup_s=60,
        delta_s=1,
    )

    snap = provider.create_snapshot_via_data(
        sc,
        run_id=run_id,
        cutoff=cutoff,
        lookback_L=lookback_L,
        snapshot_as_of_utc=format_utc_timestamp(as_of_dt),
        origin_refs=origins,
    )

    # Core CF-21 invariants: L=30, 48x30=1440 genuine observations, 48 VERIFIED paths, VALID
    assert len(seen_queries) == 1
    assert snap.lookback_L == 30
    assert snap.cutoff_offset_s == 30
    assert snap.expected_bin_count == 1440
    assert snap.valid_bin_count == 1440
    assert len(snap.history) == 1440
    assert len(snap.flow_group_ids) == 48
    assert len(snap.routing_state) == 48
    assert snap.routing_state_complete is True
    assert snap.missing_groups == ()
    assert snap.unknown_flow_group_ids == ()
    assert snap.quality_status == SnapshotQualityStatus.VALID
    assert snap.reason is None
    assert all(r.verification_status.value == "VERIFIED" for r in snap.routing_state)
    assert all(
        h.valid and h.requested_mbps is not None and h.requested_mbps > 0.0 and h.reason is None
        for h in snap.history
    )
    assert snap.content_hash == compute_snapshot_content_hash(snap)

    # Canonical Data API v1 wire serialization + X-Content-SHA256 verification
    wire_dict, exact_bytes, x_sha = map_snapshot_to_data_v1(snap)
    assert set(wire_dict.keys()) == {"sdn"}
    sdn_obj = wire_dict["sdn"]
    assert sdn_obj["lookback_L"] == 30
    assert sdn_obj["cutoff_offset_s"] == 30
    assert len(sdn_obj["history"]) == 1440
    assert len(sdn_obj["routing_state"]) == 48
    assert sdn_obj["quality_status"] == "VALID"
    assert hashlib.sha256(exact_bytes).hexdigest() == x_sha

    # Verify canonical sdn.content_hash matches Data's `snapshot_content_hash` algorithm
    sdn_no_hash = {k: v for k, v in sdn_obj.items() if k != "content_hash"}
    recomputed_content_hash = hashlib.sha256(
        json.dumps(sdn_no_hash, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    ).hexdigest()
    assert sdn_obj["content_hash"] == recomputed_content_hash

    # Deterministic freshness boundary coverage (T_stale = 2.0s):
    # Exact 2.000s -> VALID; 2.001s -> STALE (DATA_STALE)
    snap_at_bound = provider.create_snapshot_via_data(
        sc,
        run_id=run_id,
        cutoff=cutoff,
        lookback_L=lookback_L,
        snapshot_as_of_utc=format_utc_timestamp(cutoff_dt + timedelta(seconds=2)),
        origin_refs=origins,
        snapshot_id="snap_cf21_l30_bound_ok",
    )
    assert snap_at_bound.quality_status == SnapshotQualityStatus.VALID
    assert snap_at_bound.reason is None

    snap_over_bound = provider.create_snapshot_via_data(
        sc,
        run_id=run_id,
        cutoff=cutoff,
        lookback_L=lookback_L,
        snapshot_as_of_utc=format_utc_timestamp(cutoff_dt + timedelta(seconds=2, milliseconds=1)),
        origin_refs=origins,
        snapshot_id="snap_cf21_l30_bound_stale",
    )
    assert snap_over_bound.quality_status == SnapshotQualityStatus.STALE
    assert snap_over_bound.reason == "DATA_STALE"
