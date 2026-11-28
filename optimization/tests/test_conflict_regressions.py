from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from optimization.config_loader import load_optimization_config
from optimization.input_validator import validate_forecast
from optimization.planner import run_planner
from optimization.service_client import OptimizationServiceClient
from optimization.load_estimator import load_topology_from_configs
from optimization.tests.test_optimization_sdn_contract import (
    _base_config, _build_forecast, _build_routing_state, _build_topology,
)


NOW = datetime(2026, 11, 26, 14, tzinfo=timezone.utc)


def test_native_topology_hash_is_preserved_for_sdn_apply():
    from sdn.controller.topology_registry import TopologyRegistry
    loaded = load_topology_from_configs("configs/network")
    registry = TopologyRegistry.from_bundle_file("configs/network/topology_bundle.json", csv_config_dir="configs/network")
    assert loaded["topology_hash"] == registry.topology_hash
    assert loaded["topology_version"] == registry.topology_version


@pytest.mark.parametrize("field", ["run_id", "snapshot_id"])
def test_missing_identifiers_never_generate_defaults(field):
    topology = _build_topology()
    forecast = _build_forecast(topology)
    del forecast[field]
    result = run_planner(forecast, _build_routing_state(topology), [.95], _base_config(0),
                         topology_override=topology, now_utc=NOW)
    assert result["status"] == "MISSING_CONTEXT"
    assert "decision_batch" not in result


def test_h1_rejects_five_second_utc_window():
    topology = _build_topology()
    forecast = _build_forecast(topology)
    forecast.update(delta_s=1, forecast_start=NOW.isoformat(),
                    forecast_end=(NOW + timedelta(seconds=5)).isoformat())
    ok, status, _ = validate_forecast(forecast, topology["F"], expected_horizon=1, now_utc=NOW,
                                     apply_deadline_utc=(NOW + timedelta(seconds=4)).isoformat())
    assert not ok and status == "INVALID_FORECAST_WINDOW"


def test_pending_apply_is_not_overwritten_and_corruption_fails_closed(tmp_path):
    path = tmp_path / "pending.json"
    client = OptimizationServiceClient(None, "http://data", "http://sdn", journal_path=path)
    first = {"run_id": "run", "decision_batch_id": "first"}
    client._save_pending_journal(apply_request=first)
    before = path.read_bytes()
    for request in ({"run_id": "run", "decision_batch_id": "second"},
                    {**first, "changes": ["different"]}):
        with pytest.raises(RuntimeError, match="Outstanding apply"):
            client._save_pending_journal(apply_request=request)
        assert path.read_bytes() == before
    path.write_text("corrupt")
    with pytest.raises(RuntimeError, match="unreadable"):
        client._save_pending_journal(apply_request=first)


def test_native_sdn_job_envelope_waits_until_wrapped_apply_response(tmp_path, monkeypatch):
    client = OptimizationServiceClient(None, "http://data", "http://sdn", apply_poll_interval_sec=0,
                                       apply_max_polls=3, journal_path=tmp_path / "pending.json")
    final = {"run_id": "run", "decision_batch_id": "batch", "status": "APPLIED", "results": []}
    replies = iter([
        (202, {"http_status": "ACCEPTED", "job_state": "IN_PROGRESS", "poll_url": "/apply-status"}),
        (200, {"http_status": "ACCEPTED", "job_state": "IN_PROGRESS", "apply_response": None}),
        (200, {"http_status": "FINALIZED", "job_state": "FINALIZED", "apply_response": final}),
    ])
    monkeypatch.setattr(client, "_request_json_with_status", lambda *a, **kw: next(replies))
    assert client.post_apply_request_to_sdn({"run_id": "run", "decision_batch_id": "batch"}) == final


def test_run_hash_is_distinct_from_optimizer_hash(tmp_path):
    cfg_file = tmp_path / "optimization.yaml"
    cfg_file.write_text("config_hash: run-network-hash\nB: 0\n")
    cfg = load_optimization_config(cfg_file)
    assert cfg["config_hash"] == "run-network-hash"
    assert cfg["optimizer_config_hash"].startswith("sha256:")
    topology = _build_topology()
    forecast, routing = _build_forecast(topology), _build_routing_state(topology)
    forecast["config_hash"] = routing["config_hash"] = "run-network-hash"
    result = run_planner(forecast, routing, [.95] * 3, cfg, topology_override=topology,
                         now_utc=NOW, apply_deadline_utc=(NOW + timedelta(milliseconds=900)).isoformat())
    assert result["status"] == "KEPT"


def test_frozen_model_bytes_and_horizon_are_verified(tmp_path):
    selected = Path("configs/ml/selected.yaml").resolve()
    cfg_file = tmp_path / "optimizer.yaml"
    cfg_file.write_text(yaml.safe_dump({"ml_deployment_config": str(selected), "H": 5}))
    with pytest.raises(ValueError, match="H differs"):
        load_optimization_config(cfg_file)
    deployment = yaml.safe_load(selected.read_text())
    deployment["artifact_dir"] = str((selected.parent / deployment["artifact_dir"]).resolve())
    deployment["model_sha256"] = "0" * 64
    corrupt = tmp_path / "selected.yaml"
    corrupt.write_text(yaml.safe_dump(deployment))
    cfg_file.write_text(yaml.safe_dump({"ml_deployment_config": str(corrupt)}))
    with pytest.raises(ValueError, match="model_sha256 mismatch"):
        load_optimization_config(cfg_file)
