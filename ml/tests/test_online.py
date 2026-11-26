"""Integration tests for canonical Snapshot v1.4 handling and ML online Data API runner."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
import sys
import urllib.error
import urllib.parse

import pytest
import yaml

from ml.__main__ import main
from ml.common import ContractError, ROW_FIELDS, digest, read_json, stamp
from ml.demo import create_demo_inputs
from ml.inference.online import DataApiClient, DataApiError, OnlineForecastRunner
from ml.inference.predict import Predictor
from ml.preprocessing.bundle import prepare_bundle
from ml.training.train import train

CATALOG = Path(__file__).resolve().parents[2] / "configs/network/flow_groups.csv"


def make_v14_snapshot(cfg, cutoff: datetime, *, quality_status: str = "VALID",
                      incomplete_last: bool = False) -> tuple[dict, datetime]:
    """Build a canonical `{"sdn": SDNSnapshotV14}` payload matching Data API v1.4."""
    L = cfg["lookback_L"]
    ids = cfg["flow_group_ids"]
    traffic_origin = cutoff - timedelta(seconds=160)
    measurement_origin = traffic_origin + timedelta(seconds=60)
    asof = cutoff + timedelta(milliseconds=200)

    history = []
    valid_count = 0
    for fg in ids:
        for i in range(L):
            ws = cutoff - timedelta(seconds=L - i)
            we = ws + timedelta(seconds=1)
            bin_idx = 100 - L + i
            missing = incomplete_last and fg == ids[-1] and i == L - 1
            if missing:
                history.append({
                    "flow_group_id": fg,
                    "bin_index": bin_idx,
                    "window_start_utc": stamp(ws),
                    "window_end_utc": stamp(we),
                    "requested_mbps": None,
                    "released_at_utc": None,
                    "available_at_utc": None,
                    "valid": False,
                    "reason": "missing_observation",
                    "release_record_id": None,
                })
            else:
                valid_count += 1
                history.append({
                    "flow_group_id": fg,
                    "bin_index": bin_idx,
                    "window_start_utc": stamp(ws),
                    "window_end_utc": stamp(we),
                    "requested_mbps": float(i + 2),
                    "released_at_utc": stamp(we),
                    "available_at_utc": stamp(we + timedelta(milliseconds=50)),
                    "valid": True,
                    "reason": None,
                    "release_record_id": f"qa:{fg}:{bin_idx}",
                })

    sdn = {
        "snapshot_id": "snap:online-test:t100:v1",
        "run_id": "online-test-run",
        "snapshot_as_of_utc": stamp(asof),
        "history_cutoff_utc": stamp(cutoff),
        "cutoff_offset_s": 100,
        "lookback_L": L,
        "delta_s": 1,
        "state_version": 1,
        "config_version": "1.0",
        "config_hash": "sdn_config_v1_hash",
        "topology_version": "1.0",
        "topology_hash": "2fa43f873e8530d934661d455fad946cd28d4d77b8f0f87b0b0cb4aa4594d0dc",
        "origin_refs": {
            "run_started_at_utc": stamp(traffic_origin),
            "traffic_started_at_utc": stamp(traffic_origin),
            "measurement_started_at_utc": stamp(measurement_origin),
            "warmup_s": 60,
            "delta_s": 1,
        },
        "flow_group_ids": ids,
        "history": history,
        "routing_state": [
            {
                "run_id": "online-test-run",
                "flow_group_id": fg,
                "path_id": f"{fg}_P1",
                "state_version": 1,
                "confirmed_at": stamp(traffic_origin),
                "last_successful_change_at": None,
                "verification_status": "VERIFIED",
                "config_version": "1.0",
                "evidence_mode": "MOCK_SIMULATED",
                "last_optimization_change_at": None,
            }
            for fg in ids
        ],
        "routing_history": [
            {
                "run_id": "online-test-run",
                "flow_group_id": fg,
                "path_id": f"{fg}_P1",
                "valid_from": stamp(traffic_origin),
                "valid_to": None,
                "status": "INITIAL",
                "batch_id": None,
                "evidence_mode": "MOCK_SIMULATED",
            }
            for fg in ids
        ],
        "link_sample_refs": [],
        "quality_status": quality_status,
        "missing_groups": [],
        "routing_state_complete": True,
        "unknown_flow_group_ids": [],
        "provenance": {
            "producer": "sdn_controller",
            "demand_channel_status": "RELEASED_DEMAND",
            "demand_provider_name": "DataDemandProvider",
            "schema_version": "1.0",
            "evidence_mode": "MOCK_SIMULATED",
        },
        "reason": None if quality_status == "VALID" else "incomplete_history",
        "expected_bin_count": 48 * L,
        "valid_bin_count": valid_count,
    }
    sdn["content_hash"] = digest(sdn)
    return {"sdn": sdn}, measurement_origin


def make_context_for_v14(envelope: dict, metadata: dict, measurement_origin: datetime) -> dict:
    sdn = envelope["sdn"]
    return {
        "run_id": sdn["run_id"],
        "snapshot_id": sdn["snapshot_id"],
        "snapshot_content_hash": sdn["content_hash"],
        "config_hash": sdn["config_hash"],
        "topology_hash": sdn["topology_hash"],
        "ml_config_sha256": metadata["ml_config_sha256"],
        "model_sha256": metadata["model_sha256"],
        "measurement_started_at_utc": stamp(measurement_origin),
        "measurement_end_utc": stamp(measurement_origin + timedelta(seconds=360)),
        "max_snapshot_age_s": 5.0,
        "forecast_timeout_ms": 5000.0,
        "fallback_policy": "skip",
        "fallback_epsilon": 0.0,
    }


@pytest.fixture(scope="module")
def model_dir(tmp_path_factory):
    root = tmp_path_factory.mktemp("online_model")
    cfg = create_demo_inputs(root / "input", CATALOG)
    cfg["lookback_candidates"] = [3]
    cfg["horizon_candidates"] = [5]
    (root / "input/training.yaml").write_text(yaml.safe_dump(cfg), encoding="utf-8")
    prepare_bundle(root / "input/history.csv", root / "input/runs.json", CATALOG,
                   root / "bundle", 3, 5, 5)
    train(root / "bundle", root / "input/training.yaml", root / "model")
    return root / "model"


def test_predict_batch_accepts_canonical_v14_envelope_and_rejects_incomplete(model_dir):
    predictor = Predictor(model_dir)
    cutoff = datetime(2026, 11, 26, 10, 2, 40, tzinfo=timezone.utc)
    envelope, meas_origin = make_v14_snapshot(predictor.config, cutoff)
    ctx = make_context_for_v14(envelope, predictor.metadata, meas_origin)
    now = cutoff + timedelta(milliseconds=300)

    rows = predictor.predict_batch(envelope, ctx, method="rf", clock=lambda: now)
    assert len(rows) == 240
    assert all(tuple(r) == ROW_FIELDS for r in rows)
    assert all(type(r["forecast_start"]) is int and type(r["forecast_end"]) is int for r in rows)
    assert {r["forecast_start"] for r in rows} == {100, 101, 102, 103, 104}
    assert {r["forecast_end"] for r in rows} == {101, 102, 103, 104, 105}

    # INCOMPLETE snapshot with null demand fields must fail cleanly before feature building.
    incomplete, _ = make_v14_snapshot(predictor.config, cutoff, quality_status="INCOMPLETE",
                                      incomplete_last=True)
    ctx_inc = make_context_for_v14(incomplete, predictor.metadata, meas_origin)
    with pytest.raises(ContractError, match="INVALID snapshot quality"):
        predictor.predict_batch(incomplete, ctx_inc, clock=lambda: now)

    # Mismatched v1.4 metadata (e.g., origin_refs or bin_index) must be rejected.
    bad = json.loads(json.dumps(envelope))
    bad["sdn"]["origin_refs"]["warmup_s"] = 70
    bad["sdn"].pop("content_hash")
    bad["sdn"]["content_hash"] = digest(bad["sdn"])
    ctx_bad = make_context_for_v14(bad, predictor.metadata, meas_origin)
    with pytest.raises(ContractError, match="warmup mismatch"):
        predictor.predict_batch(bad, ctx_bad, clock=lambda: now)


class _MockResponse:
    def __init__(self, raw: bytes, headers: dict[str, str]):
        self._raw = raw
        self.headers = headers

    def read(self) -> bytes:
        return self._raw

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class MockDataServer:
    """In-process transport mock of Data API v1 enforcing X-API-Key, SHA-256, and strict int rows."""

    def __init__(self, run_info: dict, snapshots: dict[str, dict]):
        self.run_info = run_info
        self.snapshots = snapshots
        self.forecasts: list[dict] = []
        self.events: list[dict] = []
        self.post_attempts = 0
        self.transient_503_once = False
        self.force_422 = False
        self.tamper_snapshot_sha = False
        self.ml_key = "ml-secret"
        self.reader_key = "reader-secret"
        self.base_url = "http://data-api.local"

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def _response(self, code: int, url: str, payload: dict | bytes,
                  headers: dict[str, str] | None = None):
        raw = payload if isinstance(payload, bytes) else json.dumps(
            payload, separators=(",", ":")).encode("utf-8")
        hdrs = {"Content-Type": "application/json", **(headers or {})}
        if code >= 400:
            raise urllib.error.HTTPError(url, code, "mock error", hdrs, io.BytesIO(raw))
        return _MockResponse(raw, hdrs)

    def open(self, req, timeout=None):
        del timeout
        url = req.full_url
        assert url.startswith(self.base_url)
        path = url[len(self.base_url):]
        headers = {k.lower(): v for k, v in req.header_items()}
        key = headers.get("x-api-key")
        method = req.get_method()

        if method == "GET":
            if path == "/health/ready":
                return self._response(200, url, {"status": "ready"})
            if path == f"/v1/runs/{self.run_info['run_id']}":
                if key != self.ml_key:
                    return self._response(401, url, {"detail": "invalid ml key"})
                return self._response(200, url, self.run_info)
            prefix = f"/v1/runs/{self.run_info['run_id']}/snapshots/"
            if path.startswith(prefix):
                if key != self.reader_key:
                    return self._response(401, url, {"detail": "reader key required"})
                snap_id = urllib.parse.unquote(path[len(prefix):])
                snap = self.snapshots.get(snap_id)
                if snap is None:
                    return self._response(404, url, {"detail": "unknown snapshot"})
                raw = json.dumps(snap, separators=(",", ":")).encode("utf-8")
                sha = "0" * 64 if self.tamper_snapshot_sha else hashlib.sha256(raw).hexdigest()
                return self._response(200, url, raw, {"X-Content-SHA256": sha})
            return self._response(404, url, {"detail": "not found"})

        if method == "POST":
            body = json.loads(req.data.decode("utf-8"))
            if path == "/v1/ml/forecasts":
                if key != self.ml_key:
                    return self._response(401, url, {"detail": "ml key required"})
                self.post_attempts += 1
                if self.transient_503_once:
                    self.transient_503_once = False
                    return self._response(503, url, {"detail": "temporary db unavailable"})
                if self.force_422:
                    return self._response(422, url, {"detail": "forced validation error"})
                for row in body["rows"]:
                    if (type(row["forecast_start"]) is not int
                            or type(row["forecast_end"]) is not int):
                        return self._response(422, url, {"detail": "strict int required"})
                existing = {b["forecast_batch_id"]: b for b in self.forecasts}
                bid = body["forecast_batch_id"]
                if bid in existing:
                    if existing[bid] != body:
                        return self._response(409, url, {"detail": "conflicting batch"})
                    return self._response(200, url, {"inserted": 0})
                self.forecasts.append(body)
                return self._response(200, url, {"inserted": len(body["rows"])})
            if path == f"/v1/runs/{self.run_info['run_id']}/events/ml":
                if key != self.ml_key:
                    return self._response(401, url, {"detail": "ml key required"})
                self.events.append(body)
                return self._response(200, url, {"inserted": 1})
            return self._response(404, url, {"detail": "not found"})
        return self._response(405, url, {"detail": "method not allowed"})


def _live_run_fixture(predictor: Predictor):
    # as_of is cutoff + 200ms; keep it safely before wall time even near a second boundary.
    cutoff = datetime.now(timezone.utc) - timedelta(milliseconds=500)
    envelope, meas_origin = make_v14_snapshot(predictor.config, cutoff)
    incomplete, _ = make_v14_snapshot(predictor.config, cutoff, quality_status="INCOMPLETE",
                                      incomplete_last=True)
    incomplete["sdn"]["snapshot_id"] = "snap:online-test:incomplete"
    incomplete["sdn"].pop("content_hash")
    incomplete["sdn"]["content_hash"] = digest(incomplete["sdn"])
    run_info = {
        "run_id": "online-test-run",
        "topology_id": envelope["sdn"]["topology_hash"],
        "method_id": "M3",
        "mode": "live",
        "run_started_at_utc": stamp(meas_origin - timedelta(seconds=60)),
        "traffic_started_at_utc": stamp(meas_origin - timedelta(seconds=60)),
        "measurement_started_at_utc": stamp(meas_origin),
        "warmup_s": 60,
        "measurement_s": 360,
        "config_hash": envelope["sdn"]["config_hash"],
        "ml_config_hash": predictor.metadata["ml_config_sha256"],
        "lookback_bins": predictor.config["lookback_L"],
        "horizon_bins": predictor.config["horizon_H"],
    }
    snapshots = {
        envelope["sdn"]["snapshot_id"]: envelope,
        incomplete["sdn"]["snapshot_id"]: incomplete,
    }
    return run_info, snapshots, cutoff


def test_online_runner_e2e_retry_and_skip(model_dir):
    predictor = Predictor(model_dir)
    run_info, snapshots, cutoff = _live_run_fixture(predictor)
    valid_id = "snap:online-test:t100:v1"
    inc_id = "snap:online-test:incomplete"
    now = cutoff + timedelta(milliseconds=350)

    with MockDataServer(run_info, snapshots) as srv:
        client = DataApiClient(srv.base_url, ml_api_key=srv.ml_key, reader_api_key=srv.reader_key,
                               max_retries=2, retry_backoff_s=0.01, opener=srv.open)
        assert client.health_ready() == {"status": "ready"}
        runner = OnlineForecastRunner(model_dir, client)

        # Transient 503 retries idempotently with identical forecast_batch_id.
        srv.transient_503_once = True
        res = runner.process_snapshot(run_info["run_id"], valid_id, emit_event=True,
                                      clock=lambda: now)
        assert res["status"] == "OK"
        assert res["method"] == "rf"
        assert res["row_count"] == 240
        assert res["inserted"] == 240
        assert srv.post_attempts == 2
        assert len(srv.forecasts) == 1
        assert srv.forecasts[0]["forecast_batch_id"] == f"{run_info['run_id']}:{valid_id}:rf"
        assert srv.events[-1]["event_type"] == "ML_FORECAST_PUBLISHED"

        # 422 must never retry.
        srv.post_attempts = 0
        srv.force_422 = True
        with pytest.raises(DataApiError) as exc_info:
            runner.process_snapshot(run_info["run_id"], valid_id, clock=lambda: now)
        assert exc_info.value.status_code == 422
        assert srv.post_attempts == 1
        srv.force_422 = False

        # INCOMPLETE snapshot skips without posting forecast, emits skip event.
        srv.post_attempts = 0
        skipped = runner.process_snapshot(run_info["run_id"], inc_id, emit_event=True,
                                          clock=lambda: now)
        assert skipped["status"] == "SKIPPED"
        assert srv.post_attempts == 0
        assert srv.events[-1]["event_type"] == "ML_FORECAST_SKIPPED"

        # Tampered X-Content-SHA256 must fail before prediction or POST.
        srv.tamper_snapshot_sha = True
        with pytest.raises(ContractError, match="X-Content-SHA256 mismatch"):
            runner.process_snapshot(run_info["run_id"], valid_id, clock=lambda: now)
        assert srv.post_attempts == 0


def test_online_cli_single_and_watch_stdin(model_dir, tmp_path, monkeypatch, capsys):
    predictor = Predictor(model_dir)
    run_info, snapshots, _ = _live_run_fixture(predictor)
    valid_id = "snap:online-test:t100:v1"
    inc_id = "snap:online-test:incomplete"

    with MockDataServer(run_info, snapshots) as srv:
        import urllib.request
        monkeypatch.setattr(urllib.request, "urlopen", srv.open)
        monkeypatch.setenv("SDN_ML_KEY", srv.ml_key)
        monkeypatch.setenv("SDN_READER_KEY", srv.reader_key)
        out_file = tmp_path / "batch.json"
        audit_file = tmp_path / "audit.json"

        code = main([
            "online", "--artifact", str(model_dir), "--base-url", srv.base_url,
            "--run-id", run_info["run_id"], "--snapshot-id", valid_id,
            "--max-snapshot-age-s", "10", "--forecast-timeout-ms", "5000",
            "--out", str(out_file), "--audit", str(audit_file),
        ])
        assert code == 0
        batch = read_json(out_file)
        assert len(batch["rows"]) == 240
        assert all(type(r["forecast_start"]) is int for r in batch["rows"])
        assert read_json(audit_file)["status"] == "OK"

        # Watch stdin mode keeps model loaded across ticks and emits one JSON status per line.
        stdin_data = (
            json.dumps({"snapshot_id": valid_id, "forecast_batch_id": "tick-2"}) + "\n"
            + inc_id + "\n"
        )
        monkeypatch.setattr(sys, "stdin", io.StringIO(stdin_data))
        capsys.readouterr()
        code2 = main([
            "online", "--artifact", str(model_dir), "--base-url", srv.base_url,
            "--run-id", run_info["run_id"], "--watch-stdin",
            "--max-snapshot-age-s", "10", "--forecast-timeout-ms", "5000",
        ])
        assert code2 == 0
        lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
        assert [item["status"] for item in lines] == ["OK", "SKIPPED"]
        assert lines[0]["forecast_batch_id"] == "tick-2"


def test_online_cli_uses_frozen_deployment_yaml(model_dir, tmp_path, monkeypatch):
    from ml.deployment import export_deployment
    import urllib.request
    filename = tmp_path / "selected.yaml"
    frozen = export_deployment(model_dir, filename)
    predictor = Predictor(filename)
    run_info, snapshots, cutoff = _live_run_fixture(predictor)
    with MockDataServer(run_info, snapshots) as server:
        monkeypatch.setattr(urllib.request, "urlopen", server.open)
        monkeypatch.setenv("SDN_ML_KEY", server.ml_key)
        monkeypatch.setenv("SDN_READER_KEY", server.reader_key)
        assert main([
            "online", "--deployment-config", str(filename), "--base-url", server.base_url,
            "--run-id", run_info["run_id"], "--snapshot-id", "snap:online-test:t100:v1",
            "--max-snapshot-age-s", "10", "--forecast-timeout-ms", "5000",
        ]) == 0
        assert len(server.forecasts[0]["rows"]) == 48 * frozen["horizon_H"]


def test_online_rejects_wrong_snapshot_id_and_wrong_run_method(model_dir):
    from ml.inference.online import resolve_method
    predictor = Predictor(model_dir)
    run, snapshots, cutoff = _live_run_fixture(predictor)
    wrong_id = "request-with-mismatched-body"
    snapshots[wrong_id] = snapshots["snap:online-test:t100:v1"]
    with MockDataServer(run, snapshots) as server:
        client = DataApiClient(server.base_url, ml_api_key=server.ml_key,
                               reader_api_key=server.reader_key, opener=server.open)
        runner = OnlineForecastRunner(model_dir, client)
        result = runner.process_snapshot(run["run_id"], wrong_id,
                                          clock=lambda: cutoff + timedelta(milliseconds=350))
        assert result["status"] == "SKIPPED" and server.post_attempts == 0
        assert result["audit"]["status"] == "INVALID"
        assert result["audit"]["snapshot_id"] == wrong_id
    with pytest.raises(ContractError, match="registered run"):
        resolve_method("rf", {"method_id": "M1"})
