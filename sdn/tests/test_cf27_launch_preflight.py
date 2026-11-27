"""
CF-27 Runner Launch Preflight & Readiness Gate Tests (`PASS_CONTRACT_MOCK`).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import pytest

from sdn.scripts.preflight_and_launch_v14 import (
    evaluate_sdn_readiness_payload,
    run_preflight,
    wait_for_sdn_readiness,
)


def _valid_env(tmp_path: Path) -> dict[str, str]:
    return {
        "SDN_STRICT_INTEGRATION_PROFILE": "1",
        "SDN_DATA_API_MODE": "data_v1",
        "SDN_DEMAND_PROVIDER_MODE": "data_v1",
        "SDN_DATA_ENDPOINT_URL": "http://127.0.0.1:8000",
        "SDN_DATA_API_KEY": "super-secret-sdn-key",
        "SDN_DATA_OUTBOX_DIR": str(tmp_path / "outbox"),
        "SDN_IDEMPOTENCY_DB_PATH": str(tmp_path / "idem.sqlite3"),
        "SDN_NORTHBOUND_ENABLE": "1",
        "SDN_NORTHBOUND_HOST": "127.0.0.1",
        "SDN_NORTHBOUND_PORT": "18080",
        "SDN_NORTHBOUND_TOKEN": "super-secret-nb-token",
        "SDN_DATA_TIMEOUT_S": "1.0",
    }


def test_cf27_preflight_fail_closed_and_no_secret_leak(tmp_path: Path) -> None:
    env = _valid_env(tmp_path)
    report = run_preflight(env)
    assert report["preflight_ok"] is True
    assert report["switch_count"] == 6
    assert report["flow_group_count"] == 48
    assert report["data_api_key_configured"] is True
    assert report["northbound_token_configured"] is True
    serialized = json.dumps(report)
    assert "super-secret-sdn-key" not in serialized
    assert "super-secret-nb-token" not in serialized

    # Fail closed if strict flag missing
    bad_strict = dict(env, SDN_STRICT_INTEGRATION_PROFILE="0")
    with pytest.raises(ValueError, match="SDN_STRICT_INTEGRATION_PROFILE"):
        run_preflight(bad_strict)

    # Fail closed on legacy data mode or sqlite demand mode
    with pytest.raises(ValueError, match="SDN_DATA_API_MODE"):
        run_preflight(dict(env, SDN_DATA_API_MODE="legacy"))
    with pytest.raises(ValueError, match="SDN_DEMAND_PROVIDER_MODE"):
        run_preflight(dict(env, SDN_DEMAND_PROVIDER_MODE="http_sqlite"))

    # Fail closed on missing Northbound token or topology hash mismatch
    bad_tok = dict(env)
    del bad_tok["SDN_NORTHBOUND_TOKEN"]
    with pytest.raises(ValueError, match="SDN_NORTHBOUND_TOKEN"):
        run_preflight(bad_tok)
    with pytest.raises(ValueError, match="topology_hash mismatch"):
        run_preflight(env, expected_topology_hash="0" * 64)

    # Fail closed if Data API /health/ready is not 200
    with pytest.raises(ValueError, match="Data API /health/ready failed"):
        run_preflight(
            env,
            check_data_health=True,
            http_get_fn=lambda u, h, t: (503, {"status": "not_ready"}),
        )
    # Succeeds when Data API /health/ready returns 200
    ok_with_data = run_preflight(
        env,
        check_data_health=True,
        http_get_fn=lambda u, h, t: (200, {"status": "ready"}),
    )
    assert ok_with_data["data_health"]["http_status"] == 200

    # CF-05 Canonical v1.0 bundle override via SDN_TOPOLOGY_BUNDLE_PATH + SDN_CSV_CONFIG_DIR
    import copy
    from sdn.controller.topology_registry import compute_network_topology_hash

    repo_root = Path(__file__).resolve().parents[2]
    v10_bundle = copy.deepcopy(
        json.loads((repo_root / "configs" / "network" / "topology_bundle.json").read_text(encoding="utf-8"))
    )
    v10_bundle["topology_version"] = "v1.0"
    v10_bundle["ovs_port_mapping_verified"] = False
    v10_bundle["topology_hash"] = compute_network_topology_hash(v10_bundle)
    v10_path = tmp_path / "topology_bundle_v10.json"
    v10_path.write_text(json.dumps(v10_bundle), encoding="utf-8")

    env_v10 = dict(
        env,
        SDN_TOPOLOGY_BUNDLE_PATH=str(v10_path),
        SDN_CSV_CONFIG_DIR=str(repo_root / "configs" / "network"),
    )
    rep_v10 = run_preflight(
        env_v10,
        expected_topology_version="v1.0",
        expected_topology_hash="ec6c6969019b1c36744339fb54f0f608c95402ae972859b30710dc44b5dc3956",
    )
    assert rep_v10["preflight_ok"] is True
    assert rep_v10["topology_version"] == "v1.0"
    assert rep_v10["topology_hash"] == "ec6c6969019b1c36744339fb54f0f608c95402ae972859b30710dc44b5dc3956"


def test_cf27_wait_for_sdn_readiness_enforces_6_switches_48_verified_and_sync_gate() -> None:
    polls = {"n": 0}

    def _mock_get(url: str, headers: Mapping[str, str], timeout_s: float) -> tuple[int, dict[str, Any]]:
        del timeout_s
        assert url == "http://127.0.0.1:18080/health"
        assert headers.get("X-SDN-Token") == "tok-123"
        polls["n"] += 1
        if polls["n"] == 1:
            return 200, {
                "sdn_readiness": {
                    "connected_switches": 6,
                    "verified_flow_groups": 48,
                    "reconcile_required": False,
                },
                "data_sync_gate": {
                    "api_mode": "data_v1",
                    "optimization_tick_eligible": False,
                    "optimization_tick_block_reason": "DATA_ROUTING_SYNC_PENDING",
                },
            }
        return 200, {
            "sdn_readiness": {
                "connected_switches": 6,
                "verified_flow_groups": 48,
                "reconcile_required": False,
            },
            "data_sync_gate": {
                "api_mode": "data_v1",
                "optimization_tick_eligible": True,
                "optimization_tick_block_reason": None,
            },
        }

    ok_bad, reasons = evaluate_sdn_readiness_payload(
        {
            "sdn_readiness": {"connected_switches": 5, "verified_flow_groups": 47, "reconcile_required": True},
            "data_sync_gate": {"api_mode": "data_v1", "optimization_tick_eligible": False},
        }
    )
    assert ok_bad is False
    assert len(reasons) >= 4

    res = wait_for_sdn_readiness(
        "http://127.0.0.1:18080",
        auth_token="tok-123",
        timeout_s=2.0,
        poll_interval_s=0.01,
        http_get_fn=_mock_get,
    )
    assert res["ready"] is True
    assert polls["n"] == 2

    # Fail closed (TimeoutError) if optimization_tick_eligible stays False
    with pytest.raises(TimeoutError, match="optimization_tick_eligible=false"):
        wait_for_sdn_readiness(
            "http://127.0.0.1:18080",
            auth_token="tok-123",
            timeout_s=0.12,
            poll_interval_s=0.02,
            http_get_fn=lambda u, h, t: (
                200,
                {
                    "sdn_readiness": {
                        "connected_switches": 6,
                        "verified_flow_groups": 48,
                        "reconcile_required": False,
                    },
                    "data_sync_gate": {
                        "api_mode": "data_v1",
                        "optimization_tick_eligible": False,
                        "optimization_tick_block_reason": "APPLY_AUDIT_PENDING",
                    },
                },
            ),
        )
