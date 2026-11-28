import importlib.util
import io
from pathlib import Path
from unittest.mock import patch

import pytest


ROOT = Path(__file__).resolve().parents[2]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_telemetry_write_ahead_survives_crash_before_first_http(tmp_path):
    import sys
    with patch.object(sys, "path", [str(ROOT / "network/telemetry"), *sys.path]):
        telemetry = load("cf_telemetry", ROOT / "network/telemetry/telemetry_collector.py")
    journal = tmp_path / "outbox.jsonl"
    outbox = telemetry.TelemetryOutbox(str(journal))
    payload = {"batch_id": "batch", "run_id": "run", "samples": []}
    def crash(**kwargs):
        assert telemetry.TelemetryOutbox(str(journal)).items() == [("batch", payload)]
        raise SystemExit("process lost before ACK")
    with patch.object(telemetry, "_post_batch_with_retry", side_effect=crash), pytest.raises(SystemExit):
        telemetry.emit_batch_event(payload, io.StringIO(), "http://data", outbox=outbox)
    restored = telemetry.TelemetryOutbox(str(journal))
    with patch.object(telemetry, "_post_batch_with_retry", return_value=(True, 200, "")):
        telemetry.flush_telemetry_outbox(restored, "http://data")
    assert telemetry.TelemetryOutbox(str(journal)).items() == []


def test_receive_overcount_is_unknown_loss_not_negative_or_clamped():
    correlator = load("cf_correlator", ROOT / "network/traffic_generator/correlate_traffic.py")
    manifest = {"run_id": "run", "t0": 0, "warmup": 0, "measurement": 1, "drain": 0}
    groups = [{"flow_group_id": "0x103", "src_host": "h1", "dst_host": "h3"}]
    sent = {(0, "0x103"): {"has_sent_log": True, "packets_sent": 5, "bytes_sent": 500}}
    received = {(0, "0x103"): {"packets_received": 10, "bytes_received": 1000}}
    detail, _ = correlator.build_correlated_rows(manifest, groups, {}, sent, received, {})
    assert detail[0]["cohort_status"] == "INVALID_RECEIVE_COUNT"
    assert detail[0]["loss_ratio_cohort"] is None and detail[0]["packets_lost_cohort"] is None
