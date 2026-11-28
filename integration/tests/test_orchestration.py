from datetime import datetime, timedelta, timezone
import json

import pytest

from integration.orchestration.context import OracleProvider, manifest_to_run, ready_forecast_context
from integration.orchestration.runner import ClosedLoopRunner, DurableAudit, measured_utilization


NOW = datetime(2026, 11, 28, tzinfo=timezone.utc)


def run_info():
    topology = {"topology_hash": "a" * 64, "topology_version": "v1.0"}
    selected = {"ml_config_sha256": "b" * 64, "lookback_L": 30, "horizon_H": 1, "model_version": "rf_v1"}
    manifest = {"run_id": "run", "method": "M3", "config_hash": "network-hash",
                "run_started_at_utc": "2026-11-27T23:58:55Z",
                "traffic_started_at_utc": "2026-11-27T23:59:00Z"}
    return manifest_to_run(manifest, topology, selected, git_commit="commit", config_version="v1.0")


def test_manifest_requires_actual_launch_origins_and_frozen_model():
    run = run_info()
    assert run["measurement_started_at_utc"] == "2026-11-28T00:00:00Z"
    assert run["config_hash"] == "network-hash"
    assert run["lookback_bins"] == 30 and run["horizon_bins"] == 1
    with pytest.raises(KeyError):
        manifest_to_run({"run_start_utc": "old sample date"}, {}, {}, git_commit="x", config_version="v1.0")


def test_ready_adapter_preserves_prediction_rows_and_binds_context():
    run = run_info()
    rows = [{"forecast_start": 30, "forecast_end": 31, "predicted_mbps": 4.4}]
    ready = {"status": "READY", "run_id": "run", "snapshot_id": "snap", "rows": rows}
    snapshot = {"run_id": "run", "snapshot_id": "snap", "quality_status": "VALID", "state_version": 1,
                "config_hash": "network-hash", "config_version": "v1.0", "topology_hash": "a" * 64,
                "topology_version": "v1.0", "cutoff_offset_s": 30, "lookback_L": 30,
                "history_cutoff_utc": "2026-11-28T00:00:30Z", "snapshot_as_of_utc": "2026-11-28T00:00:30.1Z"}
    routing = {"run_id": "run", "state_version": 1}
    enriched = ready_forecast_context(ready, run, snapshot, routing, tick_at_utc="2026-11-28T00:00:30.2Z")
    assert enriched["rows"] is rows
    assert enriched["apply_deadline_utc"] == "2026-11-28T00:00:31.000000Z"
    with pytest.raises(ValueError, match="Routing changed"):
        ready_forecast_context(ready, run, snapshot, dict(routing, state_version=2),
                               tick_at_utc="2026-11-28T00:00:30.2Z")


def test_audit_survives_restart_and_reuses_exact_event_after_lost_ack(tmp_path):
    sent = []
    def lost_ack(method, path, body):
        sent.append(body)
        raise OSError("lost ACK")
    db = tmp_path / "audit.sqlite3"
    audit = DurableAudit(db, "run", lost_ack, lambda: NOW)
    audit.record("CONTROL_TICK", {"status": "NO_TRIGGER"})
    with pytest.raises(OSError):
        audit.flush()
    def recovered(method, path, body):
        sent.append(body)
        return {"inserted": 0}
    resumed = DurableAudit(db, "run", recovered, lambda: NOW + timedelta(seconds=5))
    assert resumed.flush() == 1
    assert resumed.flush() == 0
    assert sent[0] == sent[1]


def test_telemetry_never_substitutes_missing_link_or_zero(tmp_path):
    path = tmp_path / "telemetry.jsonl"
    batch = {"run_id": "run", "sampled_at_utc": "2026-11-28T00:00:00Z",
             "samples": [{"link_id": "e1>c1", "utilization": .8}]}
    path.write_text(json.dumps(batch) + "\n")
    assert measured_utilization(path, "run", {"e1>c1"}, NOW) == .8
    with pytest.raises(ValueError, match="Incomplete"):
        measured_utilization(path, "run", {"e1>c1", "e2>c1"}, NOW)
    with pytest.raises(ValueError, match="stale"):
        measured_utilization(path, "run", {"e1>c1"}, NOW + timedelta(seconds=3))


def test_initial_ticks_are_audited_without_padding_history(tmp_path):
    audit = DurableAudit(tmp_path / "audit.sqlite3", "run", lambda *a: {"inserted": 1}, lambda: NOW)
    runner = ClosedLoopRunner(run_info(), None, None, None, None, audit, None)
    assert runner.tick(0)["status"] == "SKIPPED_WARMUP"
    assert runner.tick(29)["status"] == "SKIPPED_WARMUP"
    assert audit.flush() == 2


def test_runner_registers_ticks_drains_and_finalizes_with_controlled_clock(tmp_path):
    from types import SimpleNamespace
    run = run_info()
    run.update(method_id="M0", lookback_bins=1, measurement_s=3)
    run["manifest"]["phases"] = {"drain_seconds": 0}
    now = [NOW]
    calls = []
    def data(method, path, body=None):
        calls.append((method, path, body))
        return {"inserted": 1}
    def sdn(method, path, body=None):
        return {"run_id": "run", "config_version": "v1.0", "topology_hash": "a" * 64,
                "connected_switches": 6, "verified_flow_groups": 48,
                "data_sync_gate": {"api_mode": "data_v1", "optimization_tick_eligible": True}}
    optimizer = SimpleNamespace(journal_path=tmp_path / "pending.json",
        recover_pending_apply=lambda **kwargs: None,
        get_routing_state=lambda **kwargs: {"data_sync_gate": {"optimization_tick_eligible": True}})
    audit = DurableAudit(tmp_path / "audit.sqlite3", "run", data, lambda: now[0])
    runner = ClosedLoopRunner(run, data, sdn, None, optimizer, audit, None,
        clock=lambda: now[0], sleep=lambda seconds: now.__setitem__(0, now[0] + timedelta(seconds=seconds)))
    assert runner.execute()["status"] == "ok"
    assert calls[1][1] == "/v1/runs"
    ticks = [r[2]["detail"] for r in calls if r[2] and r[2].get("event_type") == "CONTROL_TICK"]
    assert [r["cutoff"] for r in ticks] == [0, 1, 2]
    assert calls[-1] == ("PATCH", "/v1/runs/run/status", {"status": "ok"})


def test_oracle_is_m4_only_and_uses_measurement_to_traffic_mapping(tmp_path):
    from ml.common import file_hash
    run = run_info()
    run.update(method_id="M4", mode="oracle", schedule_id="schedule")
    schedule = tmp_path / "schedule.csv"
    schedule.write_text("schedule_id,time_offset_s,flow_group_id,requested_mbps\nschedule,90,0x103,8\n")
    run["manifest"]["schedule_sha256"] = file_hash(schedule)
    provider = OracleProvider(run, schedule)
    batch = provider(run, {"snapshot_id": "snap", "cutoff_offset_s": 30, "flow_group_ids": ["0x103"]}, NOW)
    assert batch["rows"][0]["forecast_start"] == 30
    assert batch["rows"][0]["predicted_mbps"] == 8
    with pytest.raises(ValueError, match="reserved"):
        OracleProvider(dict(run, method_id="M3", mode="live"), schedule)


def test_network_drain_detects_unacked_or_missing_journal(tmp_path):
    from integration.orchestration.runner import network_delivery_pending
    assert network_delivery_pending(tmp_path, "run")
    for name in [*(f"demand_outbox_h{i}.jsonl" for i in range(1, 9)), "telemetry_outbox.jsonl", "traffic_outbox.jsonl"]:
        (tmp_path / name).touch()
    assert not network_delivery_pending(tmp_path, "run")
    telemetry = tmp_path / "telemetry_outbox.jsonl"
    telemetry.write_text(json.dumps({"op": "add", "batch_id": "batch", "payload": {"run_id": "run"}}) + "\n")
    assert network_delivery_pending(tmp_path, "run")
    with telemetry.open("a") as stream:
        stream.write(json.dumps({"op": "ack", "batch_id": "batch"}) + "\n")
    assert not network_delivery_pending(tmp_path, "run")
