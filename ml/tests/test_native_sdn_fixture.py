"""Native SDN contract fixtures, including the actual frozen L=30 model."""
from copy import deepcopy
from datetime import timedelta
import hashlib
from pathlib import Path

import pytest
import yaml
from data.ingestion.online_models import Snapshot
from ml.common import ContractError, digest, read_json, utc
from ml.demo import create_demo_inputs
from ml.inference.predict import Predictor
from ml.preprocessing.bundle import prepare_bundle
from ml.tests.test_online import make_context_for_v14
from ml.training.train import train
from ml.inference.online import build_forecast_batch

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "sdn/fixtures/data_v14"


@pytest.fixture(scope="module")
def fixture_predictor(tmp_path_factory):
    # Small synthetic model for parser/contract verification only; never deployed.
    folder = tmp_path_factory.mktemp("native_sdn_l10")
    config = create_demo_inputs(folder / "input", ROOT / "configs/network/flow_groups.csv")
    config.update(lookback_candidates=[10], horizon_candidates=[1], epsilon_candidates=[0])
    (folder / "search.yaml").write_text(yaml.safe_dump(config), encoding="utf-8")
    prepare_bundle(folder / "input/history.csv", folder / "input/runs.json",
                   ROOT / "configs/network/flow_groups.csv", folder / "bundle", 10, 1, 5)
    train(folder / "bundle", folder / "search.yaml", folder / "model")
    return Predictor(folder / "model")


def context(payload, predictor):
    origin = utc(payload["sdn"]["origin_refs"]["measurement_started_at_utc"])
    return make_context_for_v14(payload, predictor.metadata, origin)


def test_native_valid_fixture_inference_and_both_hashes(fixture_predictor):
    raw = (FIXTURE / "snapshot_valid_body.json").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == "a48f7093ed630840620699cdab72659dc1429c765a9271a05bbf126b0f7af0c7"
    payload = read_json(FIXTURE / "snapshot_valid_body.json")
    Snapshot.model_validate(payload)
    assert digest({k: v for k, v in payload["sdn"].items() if k != "content_hash"}) == payload["sdn"]["content_hash"]
    now = utc(payload["sdn"]["snapshot_as_of_utc"]) + timedelta(milliseconds=100)
    rows = fixture_predictor.predict_batch(payload, context(payload, fixture_predictor), clock=lambda: now)
    assert len(rows) == 48
    assert all(type(r["forecast_start"]) is int and r["forecast_start"] == 100
               and type(r["forecast_end"]) is int and r["forecast_end"] == 101 for r in rows)


@pytest.mark.parametrize("quality", ["INCOMPLETE", "STALE", "INVALID", "BAD_HASH"])
def test_native_bad_snapshot_emits_no_forecast(fixture_predictor, quality):
    filename = "snapshot_incomplete_body.json" if quality == "INCOMPLETE" else "snapshot_valid_body.json"
    payload = read_json(FIXTURE / filename)
    if quality in ("STALE", "INVALID"):
        payload["sdn"]["quality_status"] = quality
        payload["sdn"].pop("content_hash")
        payload["sdn"]["content_hash"] = digest(payload["sdn"])
    if quality == "BAD_HASH": payload["sdn"]["content_hash"] = "0" * 64
    now = utc(payload["sdn"]["snapshot_as_of_utc"]) + timedelta(milliseconds=100)
    with pytest.raises(ContractError):
        fixture_predictor.predict_batch(payload, context(payload, fixture_predictor), clock=lambda: now)
    assert fixture_predictor.last_audit["status"] == "INVALID"


def test_deployed_l30_rejects_native_l10_without_padding():
    predictor = Predictor(ROOT / "configs/ml/selected.yaml")
    payload = read_json(FIXTURE / "snapshot_valid_body.json")
    now = utc(payload["sdn"]["snapshot_as_of_utc"]) + timedelta(milliseconds=100)
    with pytest.raises(ContractError, match="lookback_L mismatch"):
        predictor.predict_batch(payload, context(payload, predictor), clock=lambda: now)


def test_frozen_model_consumes_native_sdn_l30_producer(monkeypatch):
    """Real producer and frozen model; synthetic demand/in-memory OVS only."""
    from sdn.fixtures.data_v14 import verify_contract_fixtures as producer
    from data.ingestion.online_models import ForecastBatch
    predictor = Predictor(ROOT / "configs/ml/selected.yaml")
    monkeypatch.setattr(producer, "LOOKBACK_L", predictor.config["lookback_L"])
    fixtures = producer.build_all_fixtures()
    import json
    payload = json.loads(fixtures["snapshot_valid_body.json"])
    Snapshot.model_validate(payload)
    sdn = payload["sdn"]
    assert sdn["lookback_L"] == 30 and len(sdn["history"]) == 1440
    assert sdn["quality_status"] == "VALID" and len(sdn["routing_state"]) == 48
    now = utc(sdn["snapshot_as_of_utc"]) + timedelta(milliseconds=100)
    rows = predictor.predict_batch(payload, context(payload, predictor), clock=lambda: now)
    batch = build_forecast_batch(sdn["run_id"], sdn["snapshot_id"], rows,
                                 snapshot=payload, horizon_H=predictor.config["horizon_H"])
    accepted = ForecastBatch.model_validate(batch)
    assert len(accepted.rows) == 48
    assert {r.forecast_start for r in accepted.rows} == {100}
