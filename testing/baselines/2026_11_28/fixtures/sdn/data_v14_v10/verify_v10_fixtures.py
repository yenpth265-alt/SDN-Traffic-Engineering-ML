"""
Generate and verify canonical Network `v1.0` (`ec6c6969...`) SDN Snapshot v1.4 fixtures
without altering the historical `1.0` (`2fa43f87...`) fixtures in `sdn/fixtures/data_v14/`.

Fixtures generated/verified in `sdn/fixtures/data_v14_v10/`:
- `snapshot_valid_l10_body.json` (`L=10`, 480/480 valid observations)
- `snapshot_valid_l30_body.json` (`L=30`, 1440/1440 genuine observations, no padding/fake zeros)
- `snapshot_incomplete_body.json` (`L=10`, 479/480 valid, 1 missing observation -> `requested_mbps=null`, `valid=false`)
- `snapshot_stale_body.json` (`L=10`, `quality_status=STALE`, `reason=DATA_STALE`)

Usage:
    python sdn/fixtures/data_v14_v10/verify_v10_fixtures.py --generate
    python sdn/fixtures/data_v14_v10/verify_v10_fixtures.py --verify
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_DIR = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sdn.adapters.data_api_v1_adapter import map_snapshot_to_data_v1
from sdn.adapters.data_demand_history_provider import (
    DataAPIDemandHistoryProvider,
    DataDemandHistoryConfig,
)
from sdn.controller.state_manager import StateManager, format_utc_timestamp
from sdn.controller.topology_registry import TopologyRegistry, compute_network_topology_hash
from sdn.flow_management.flow_matcher import FlowMatcher
from sdn.flow_management.route_compiler import RouteCompiler
from sdn.flow_management.rule_installer import RuleInstaller
from sdn.flow_management.switch_driver import InMemorySwitchDriver
from sdn.monitoring.snapshot_creator import SnapshotCreator
from sdn.route_verification.rule_verifier import RuleVerifier
from sdn.schemas.snapshot_models import OriginRefs

CANONICAL_NETWORK_SHA = "a2d97687ced118879e0908516beb50f1feb3992d"
APPROVED_TOPO_VERSION = "v1.0"
APPROVED_TOPO_HASH = "ec6c6969019b1c36744339fb54f0f608c95402ae972859b30710dc44b5dc3956"
NETWORK_FILES = (
    "topology_bundle.json",
    "flow_groups.csv",
    "candidate_paths.csv",
    "links.csv",
    "initial_paths.csv",
)


def _load_canonical_v10_registry() -> TopologyRegistry:
    """
    Load canonical `v1.0` bundle + CSVs from `SDN_TOPOLOGY_BUNDLE_PATH` (if set to a `v1.0` bundle),
    else from `git show a2d9768...:configs/network/*` into a temp directory, else by deterministically
    resigning the local bundle to `v1.0` (`ovs_port_mapping_verified=False`) and asserting exact
    `APPROVED_TOPO_HASH` parity.
    """
    env_bundle = os.environ.get("SDN_TOPOLOGY_BUNDLE_PATH")
    if env_bundle:
        bp = Path(env_bundle)
        cd = Path(os.environ.get("SDN_CSV_CONFIG_DIR") or str(bp.parent))
        reg = TopologyRegistry.from_bundle_file(bp, csv_config_dir=cd, verify_hash=True)
        if reg.topology_version == APPROVED_TOPO_VERSION and reg.topology_hash == APPROVED_TOPO_HASH:
            return reg

    with tempfile.TemporaryDirectory() as td_str:
        td = Path(td_str)
        try:
            for fname in NETWORK_FILES:
                raw = subprocess.check_output(
                    ["git", "show", f"{CANONICAL_NETWORK_SHA}:configs/network/{fname}"],
                    cwd=str(REPO_ROOT),
                    stderr=subprocess.DEVNULL,
                )
                (td / fname).write_bytes(raw)
            reg = TopologyRegistry.from_bundle_file(td / "topology_bundle.json", csv_config_dir=td, verify_hash=True)
        except Exception:
            local_net = REPO_ROOT / "configs" / "network"
            data = copy.deepcopy(json.loads((local_net / "topology_bundle.json").read_text(encoding="utf-8")))
            data["topology_version"] = APPROVED_TOPO_VERSION
            data["ovs_port_mapping_verified"] = False
            data["topology_hash"] = compute_network_topology_hash(data)
            (td / "topology_bundle.json").write_text(json.dumps(data), encoding="utf-8")
            reg = TopologyRegistry.from_bundle_file(
                td / "topology_bundle.json", csv_config_dir=local_net, verify_hash=True
            )
    assert reg.topology_version == APPROVED_TOPO_VERSION
    assert reg.topology_hash == APPROVED_TOPO_HASH
    return reg


def _build_fixture_payloads() -> dict[str, tuple[bytes, str]]:
    topo = _load_canonical_v10_registry()
    drv = InMemorySwitchDriver(topo)
    comp = RouteCompiler(topo, FlowMatcher(topo))
    inst = RuleInstaller(comp, drv, RuleVerifier(comp, drv))
    sm = StateManager(topo)

    run_id = "run_cf05_v10_fixtures"
    meas_start = datetime(2026, 11, 27, 14, 1, 0, tzinfo=timezone.utc)
    traffic_start = meas_start - timedelta(seconds=60)
    run_start = traffic_start - timedelta(seconds=5)
    boot_ts = format_utc_timestamp(run_start + timedelta(seconds=1))
    sm.initialize_run(run_id, boot_ts, verification_evidences=inst.bootstrap_initial_paths(run_id, boot_ts))
    origins = OriginRefs(
        run_started_at_utc=format_utc_timestamp(run_start),
        traffic_started_at_utc=format_utc_timestamp(traffic_start),
        measurement_started_at_utc=format_utc_timestamp(meas_start),
        warmup_s=60,
        delta_s=1,
    )
    fgs = topo.get_all_flow_groups()

    def _make(name: str, cutoff_val: int, lookback_val: int, as_of_ms: int, drop_one: bool = False) -> tuple[bytes, str]:
        rows: list[dict[str, Any]] = []
        for k in range(cutoff_val - lookback_val, cutoff_val):
            ws = format_utc_timestamp(meas_start + timedelta(seconds=k))
            we = format_utc_timestamp(meas_start + timedelta(seconds=k + 1))
            avail = format_utc_timestamp(meas_start + timedelta(seconds=k + 1, milliseconds=50))
            for i, fg in enumerate(fgs):
                if drop_one and k == cutoff_val - 1 and i == 0:
                    continue
                rows.append(
                    {
                        "run_id": run_id,
                        "flow_group_id": fg.flow_group_id,
                        "bin_index": k,
                        "window_start_utc": ws,
                        "window_end_utc": we,
                        "requested_mbps": round(1.25 + i * 0.04 + k * 0.03, 4),
                        "released_at_utc": we,
                        "available_at_utc": avail,
                        "release_record_id": f"net:{run_id}:{fg.flow_group_id}:{60 + k}",
                        "source_kind": "RELEASED_DEMAND",
                        "valid": True,
                        "reason": None,
                    }
                )
        prov = DataAPIDemandHistoryProvider(
            DataDemandHistoryConfig(endpoint_base_url="http://data.local:8000", api_key="k"),
            http_get_fn=lambda u, h, t: (200, json.dumps(rows)),
        )
        sc = SnapshotCreator(topo, sm, prov)
        as_of = format_utc_timestamp(meas_start + timedelta(seconds=cutoff_val, milliseconds=as_of_ms))
        snap = prov.create_snapshot_via_data(
            sc,
            run_id=run_id,
            cutoff=cutoff_val,
            lookback_L=lookback_val,
            snapshot_as_of_utc=as_of,
            origin_refs=origins,
            snapshot_id=f"snap_v10_{name}",
        )
        _, raw_bytes, x_sha = map_snapshot_to_data_v1(snap)
        return raw_bytes, x_sha

    return {
        "snapshot_valid_l10_body.json": _make("valid_l10", 10, 10, 200),
        "snapshot_valid_l30_body.json": _make("valid_l30", 30, 30, 200),
        "snapshot_incomplete_body.json": _make("incomplete_l10", 10, 10, 200, drop_one=True),
        "snapshot_stale_body.json": _make("stale_l10", 10, 10, 2500),
    }


def _load_real_pydantic_snapshot_model() -> Any:
    try:
        from data.ingestion.online_models import Snapshot  # type: ignore[import-not-found]
        return Snapshot
    except ImportError:
        pass
    try:
        src = subprocess.check_output(
            ["git", "show", f"{CANONICAL_NETWORK_SHA}:data/ingestion/online_models.py"],
            cwd=str(REPO_ROOT),
            stderr=subprocess.DEVNULL,
        ).decode("utf-8")
        mod = types.ModuleType("online_models_v10")
        sys.modules["online_models_v10"] = mod
        exec(compile(src, "online_models.py", "exec"), mod.__dict__)
        mod.Snapshot.model_rebuild()
        return mod.Snapshot
    except Exception:
        return None


def generate() -> dict[str, Any]:
    payloads = _build_fixture_payloads()
    summary: dict[str, Any] = {}
    for fname, (raw, x_sha) in payloads.items():
        (FIXTURE_DIR / fname).write_bytes(raw)
        obj = json.loads(raw)["sdn"]
        summary[fname] = {
            "bytes": len(raw),
            "x_content_sha256": x_sha,
            "sdn_content_hash": obj["content_hash"],
            "topology_version": obj["topology_version"],
            "topology_hash": obj["topology_hash"],
            "quality_status": obj["quality_status"],
            "lookback_L": obj["lookback_L"],
            "valid_bin_count": obj["valid_bin_count"],
        }
    return summary


def verify() -> dict[str, Any]:
    expected_payloads = _build_fixture_payloads()
    pydantic_model = _load_real_pydantic_snapshot_model()
    mode = "REAL_PYDANTIC" if pydantic_model is not None else "PURE_PYTHON_MIRROR"
    results: dict[str, Any] = {"validation_mode": mode, "checks": {}, "hashes": {}}

    for fname, (expected_bytes, expected_sha) in expected_payloads.items():
        fpath = FIXTURE_DIR / fname
        actual_bytes = fpath.read_bytes()
        assert actual_bytes == expected_bytes, f"Fixture bytes drift in {fname}"
        actual_sha = hashlib.sha256(actual_bytes).hexdigest()
        assert actual_sha == expected_sha, f"X-Content-SHA256 mismatch in {fname}"

        parsed = json.loads(actual_bytes)
        sdn_obj = parsed["sdn"]
        assert sdn_obj["topology_version"] == APPROVED_TOPO_VERSION
        assert sdn_obj["topology_hash"] == APPROVED_TOPO_HASH

        sdn_no_hash = {k: v for k, v in sdn_obj.items() if k != "content_hash"}
        recomp = hashlib.sha256(
            json.dumps(sdn_no_hash, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        ).hexdigest()
        assert sdn_obj["content_hash"] == recomp, f"content_hash mismatch in {fname}"

        if fname == "snapshot_incomplete_body.json":
            missing_rows = [h for h in sdn_obj["history"] if not h["valid"]]
            assert len(missing_rows) == 1
            assert missing_rows[0]["requested_mbps"] is None
            assert missing_rows[0]["released_at_utc"] is None
            assert missing_rows[0]["available_at_utc"] is None
            assert missing_rows[0]["reason"] is None
        elif fname == "snapshot_valid_l30_body.json":
            assert len(sdn_obj["history"]) == 1440
            assert sdn_obj["valid_bin_count"] == 1440
            assert all(h["valid"] and h["requested_mbps"] is not None and h["requested_mbps"] > 0.0 for h in sdn_obj["history"])
        elif fname == "snapshot_stale_body.json":
            assert sdn_obj["quality_status"] == "STALE"
            assert sdn_obj["reason"] == "DATA_STALE"

        if pydantic_model is not None:
            validated = pydantic_model.model_validate_json(actual_bytes)
            assert validated.sdn.topology_version == APPROVED_TOPO_VERSION
            assert validated.sdn.topology_hash == APPROVED_TOPO_HASH

        results["checks"][fname] = "PASS"
        results["hashes"][fname] = {
            "bytes": len(actual_bytes),
            "x_content_sha256": actual_sha,
            "sdn_content_hash": sdn_obj["content_hash"],
            "quality_status": sdn_obj["quality_status"],
            "lookback_L": sdn_obj["lookback_L"],
        }
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--generate", action="store_true")
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    if args.generate:
        print(json.dumps(generate(), indent=2))
    if args.verify or not args.generate:
        print(json.dumps(verify(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
