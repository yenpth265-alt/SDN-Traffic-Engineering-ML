"""Executable checks traced to Testing v2 TC-D03/D04, TC-M01..04, TC-I03."""
import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil

import numpy as np
import pandas as pd
import pytest
import yaml

from ml.baselines import moving_average, persistence
from ml.common import ContractError, ROW_FIELDS, digest, file_hash, read_json, split_check, write_json
from ml.demo import create_demo_inputs, synthetic_snapshot
from ml.inference.predict import Predictor
from ml.model_registry import load_artifact
from ml.preprocessing.bundle import TrainingBundle, prepare_bundle
from ml.preprocessing.features import build_features
from ml.training.train import train
from ml.validation.evaluate import evaluate
from ml.validation.metrics import calibrate, choose_epsilon, errors

CATALOG = Path(__file__).resolve().parents[2] / "configs/network/flow_groups.csv"


@pytest.fixture(scope="module")
def pipeline(tmp_path_factory):
    root = tmp_path_factory.mktemp("ml_pipeline")
    cfg = create_demo_inputs(root / "input", CATALOG)
    cfg["lookback_candidates"] = [3]
    cfg["horizon_candidates"] = [5]
    (root / "input/training.yaml").write_text(yaml.safe_dump(cfg))
    prepare_bundle(root / "input/history.csv", root / "input/runs.json",
                   CATALOG, root / "bundle", 5, 5, 5)
    train(root / "bundle", root / "input/training.yaml", root / "model")
    return root


@pytest.fixture
def online(pipeline):
    p = Predictor(pipeline / "model")
    cutoff = datetime(2026, 11, 12, 9, 2, 40, tzinfo=timezone.utc)
    snapshot, context = synthetic_snapshot(p.config, p.metadata, cutoff)
    return p, snapshot, context, cutoff + timedelta(milliseconds=150)


def rehash(snapshot, context):
    snapshot["content_hash"] = digest({k: v for k, v in snapshot.items() if k != "content_hash"})
    context["snapshot_content_hash"] = snapshot["content_hash"]


def test_baselines_and_features_tc_m01():
    a = np.array([[2., 3., 4.]])
    np.testing.assert_array_equal(persistence.predict(a, 5), [[4] * 5])
    np.testing.assert_array_equal(moving_average.predict(a, 5, 3), [[3] * 5])
    x = build_features(a, ["0x103"], ["0x103"], 3)
    np.testing.assert_array_equal(x[0, :3], [4, 3, 2])
    assert x[0, -1] == 1
    with pytest.raises(ContractError):
        moving_average.predict(a, 5, 4)


def test_epsilon_tc_m04_and_mean_metrics():
    np.testing.assert_allclose(calibrate([[4]], .1), [[4.4]])
    np.testing.assert_array_equal(calibrate([[-2, 20]], .1), [[0, 22]])
    assert choose_epsilon(np.array([[0.]]), np.array([[0.]]), [0, .05, .1])[0] == 0
    assert choose_epsilon(np.array([[4.]]), np.array([[4.]]), [0, .1])[0] == 0
    report = errors([[1, 3]], [[2, 2]])
    assert report["pooled_mae_mbps"] == 1
    assert report["mean_h_mae_mbps"] == 0


def test_window_alignment_tc_d04(pipeline):
    b = TrainingBundle(pipeline / "bundle")
    x, y, m = b.load("train")
    sample = m.iloc[0]
    raw = pd.read_csv(pipeline / "input/history.csv")
    raw = raw[(raw.run_id == sample.run_id) & (raw.flow_group_id == sample.flow_group_id)]
    values = raw.sort_values("time_offset_s").requested_mbps.to_numpy()
    traffic_cutoff = int(sample.cutoff) + 10
    np.testing.assert_array_equal(x[0], values[traffic_cutoff - 5:traffic_cutoff])
    np.testing.assert_array_equal(y[0], values[traffic_cutoff:traffic_cutoff + 5])


def test_split_rejects_50_25_25_and_shared_family_tc_d03(pipeline):
    runs = read_json(pipeline / "input/runs.json")["runs"]
    bad = copy.deepcopy(runs)
    for r in bad:
        r["split"] = "train" if r["seed"] <= 10 else "validation" if r["seed"] <= 15 else "test"
    with pytest.raises(ContractError, match="70/15/15"):
        split_check(bad)
    runs[-1]["parent_family_id"] = runs[0]["parent_family_id"]
    with pytest.raises(ContractError, match="leakage"):
        split_check(runs)


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "nan", "fingerprint"])
def test_prepare_rejects_bad_data(pipeline, tmp_path, mutation):
    data = pd.read_csv(pipeline / "input/history.csv")
    if mutation == "missing":
        data = data.iloc[1:]
    elif mutation == "duplicate":
        data = pd.concat([data, data.iloc[:1]])
    elif mutation == "nan":
        data.loc[0, "requested_mbps"] = np.nan
    else:
        # Rename/replay the same series under a test ID: content fingerprint must catch it.
        values = data.loc[data.run_id == "demo_1", "requested_mbps"].to_numpy()
        data.loc[data.run_id == "demo_20", "requested_mbps"] = values
    data.to_csv(tmp_path / "bad.csv", index=False)
    with pytest.raises(ContractError):
        prepare_bundle(tmp_path / "bad.csv", pipeline / "input/runs.json", CATALOG,
                       tmp_path / "bundle", 5, 5, 5)


def test_train_never_loads_test_and_test_evaluation_is_separate(pipeline, tmp_path, monkeypatch):
    seen = []
    original = TrainingBundle.load
    def watched(self, split):
        seen.append(split)
        assert split != "test", "train() accessed held-out labels"
        return original(self, split)
    with monkeypatch.context() as m:
        m.setattr(TrainingBundle, "load", watched)
        train(pipeline / "bundle", pipeline / "input/training.yaml", tmp_path / "model")
    assert seen == ["train", "validation"]
    report = evaluate(pipeline / "bundle", tmp_path / "model", tmp_path / "evaluation")
    assert report["split"] == "test"
    assert set(report["metrics"]) == {"rf", "persistence", "moving_average"}
    # Grouped test metrics must reconstruct the overall MAE, not average runs blindly.
    groups = [r["metrics"]["rf"] for r in report["by_run"].values()]
    weighted = sum(g["pooled_mae_mbps"] * g["samples"] for g in groups) / sum(g["samples"] for g in groups)
    assert weighted == pytest.approx(report["metrics"]["rf"]["pooled_mae_mbps"])
    assert len(report["by_flow"]) == 48
    assert report["data_provenance"]["kind"] == "synthetic_demo"


def test_bundle_joins_by_sample_id(pipeline, tmp_path):
    dest = tmp_path / "bundle"
    shutil.copytree(pipeline / "bundle", dest)
    frame = pd.read_csv(dest / "train.csv").iloc[::-1]
    frame.to_csv(dest / "train.csv", index=False)
    info = read_json(dest / "bundle.json")
    info["files"]["train.csv"] = file_hash(dest / "train.csv")
    write_json(dest / "bundle.json", info)
    x1, y1, m1 = TrainingBundle(pipeline / "bundle").load("train")
    x2, y2, m2 = TrainingBundle(dest).load("train")
    np.testing.assert_array_equal(x1, x2)
    np.testing.assert_array_equal(y1, y2)
    assert m1.sample_id.tolist() == m2.sample_id.tolist()


def test_exact_seven_fields_and_origin_tc_i03(online):
    p, snap, ctx, now = online
    rows = p.predict_batch(snap, ctx, method="persistence", clock=lambda: now)
    assert len(rows) == 240
    assert all(tuple(r) == ROW_FIELDS for r in rows)
    assert {r["predicted_mbps"] for r in rows} == {4.0}
    assert {r["forecast_start"] for r in rows} == {100, 101, 102, 103, 104}
    assert {r["forecast_end"] for r in rows} == {101, 102, 103, 104, 105}
    assert len({r["issued_at"] for r in rows}) == 1
    assert rows[0]["issued_at"] == "2026-11-12T09:02:40.150000Z"


@pytest.mark.parametrize("mutation", [
    "missing", "duplicate", "future_window", "late_release", "late_available",
    "invalid_value", "invalid_flag", "unknown_route", "hash", "model_pin", "end"])
def test_invalid_snapshot_skips_even_with_fallback_tc_m03(online, mutation):
    p, snap, ctx, now = online
    ctx["fallback_policy"] = "persistence"
    row = snap["history"][0]
    if mutation == "missing": snap["history"].pop()
    elif mutation == "duplicate": snap["history"][1] = dict(row)
    elif mutation == "future_window": row["window_end_utc"] = "2026-11-12T09:02:41Z"
    elif mutation == "late_release": row["released_at_utc"] = "2026-11-12T09:02:41Z"
    elif mutation == "late_available": row["available_at_utc"] = "2026-11-12T09:02:41Z"
    elif mutation == "invalid_value": row["requested_mbps"] = -1
    elif mutation == "invalid_flag": row["valid"] = False
    elif mutation == "unknown_route": snap["routing_state"][0]["verification_status"] = "UNKNOWN"
    elif mutation == "model_pin": ctx["model_sha256"] = "wrong"
    elif mutation == "end": ctx["measurement_end_utc"] = "2026-11-12T09:02:44Z"
    if mutation != "hash": rehash(snap, ctx)
    else: row["requested_mbps"] = 123
    with pytest.raises(ContractError):
        p.predict_batch(snap, ctx, clock=lambda: now)
    assert p.last_audit["status"] == "INVALID"
    assert p.counts["fallback"] == 0


def test_stale_and_timeout(online):
    p, snap, ctx, now = online
    with pytest.raises(ContractError, match="STALE"):
        p.predict_batch(snap, ctx, clock=lambda: now + timedelta(seconds=3))
    ctx["forecast_timeout_ms"] = 0.000001
    with pytest.raises(ContractError, match="TIMEOUT"):
        p.predict_batch(snap, ctx, method="persistence", clock=lambda: now)


class ConstantModel:
    def __init__(self, value=4):
        self.value = value
    def predict(self, x):
        return np.full((len(x), 5), self.value)


@pytest.mark.parametrize("value,expected", [(4, 4.4), (-2, 0), (20, 22)])
def test_final_normalization_once_no_capacity_clip_tc_m02_m04(online, value, expected):
    p, snap, ctx, now = online
    p.model = ConstantModel(value)
    p.config["epsilon"] = .1
    rows = p.predict_batch(snap, ctx, clock=lambda: now)
    np.testing.assert_allclose([r["predicted_mbps"] for r in rows], expected)


@pytest.mark.parametrize("failure", ["nan", "exception", "shape"])
def test_whole_batch_fallback_not_mixed(online, failure):
    p, snap, ctx, now = online
    class BrokenModel:
        def predict(self, x):
            if failure == "exception":
                raise RuntimeError("worker failed")
            if failure == "shape":
                return np.zeros((47, 5))
            return np.full((48, 5), np.nan)
    p.model = BrokenModel()
    with pytest.raises(ContractError):
        p.predict_batch(snap, ctx, clock=lambda: now)
    ctx["fallback_policy"] = "persistence"
    rows = p.predict_batch(snap, ctx, clock=lambda: now)
    assert len(rows) == 240
    assert {r["source"] for r in rows} == {"fallback_persistence"}
    assert {r["predicted_mbps"] for r in rows} == {4}
    assert p.last_audit["method"] == "rf"
    assert p.counts == {"eligible": 2, "native": 0, "fallback": 1, "skipped": 1}


@pytest.mark.parametrize("target", ["model", "config", "feature_order"])
def test_tampered_artifact_rejected_tc_m02(pipeline, tmp_path, target):
    dest = tmp_path / "model"
    shutil.copytree(pipeline / "model", dest)
    if target == "model":
        with open(dest / "model.joblib", "ab") as f: f.write(b"corruption")
    else:
        cfg = read_json(dest / "ml_config.json")
        cfg["feature_names"] = cfg["feature_names"][::-1]
        write_json(dest / "ml_config.json", cfg)
        if target == "feature_order":
            meta = read_json(dest / "metadata.json")
            meta["ml_config_sha256"] = file_hash(dest / "ml_config.json")
            write_json(dest / "metadata.json", meta)
    with pytest.raises(ContractError, match="MODEL_LOAD_FAIL"):
        load_artifact(dest)


def test_single_step_rf_and_future_label_isolation(pipeline, tmp_path):
    cfg = yaml.safe_load((pipeline / "input/training.yaml").read_text())
    cfg["horizon_candidates"] = [1]
    (tmp_path / "one.yaml").write_text(yaml.safe_dump(cfg))
    train(pipeline / "bundle", tmp_path / "one.yaml", tmp_path / "model")
    p = Predictor(tmp_path / "model")
    cutoff = datetime(2026, 11, 12, 9, 2, 40, tzinfo=timezone.utc)
    snap, ctx = synthetic_snapshot(p.config, p.metadata, cutoff)
    now = cutoff + timedelta(milliseconds=150)
    rows = p.predict_batch(snap, ctx, clock=lambda: now)
    assert len(rows) == 48
    # Labels are stored separately and never passed to the feature builder.
    x, y, meta = TrainingBundle(pipeline / "bundle").load("test")
    before = build_features(x, meta.flow_group_id.tolist(), p.config["flow_group_ids"], 3)
    y[:] = 99999
    after = build_features(x, meta.flow_group_id.tolist(), p.config["flow_group_ids"], 3)
    np.testing.assert_array_equal(before, after)


def test_demo_default_snapshot_has_time_left_for_horizon_one(pipeline, monkeypatch):
    from ml import demo
    from ml.common import utc
    now = datetime(2026, 11, 18, 9, 0, 0, tzinfo=timezone.utc)
    class FixedDateTime:
        @staticmethod
        def now(_):
            return now
    monkeypatch.setattr(demo, "datetime", FixedDateTime)
    predictor = Predictor(pipeline / "model")
    cfg = {**predictor.config, "horizon_H": 1}
    snapshot, _ = demo.synthetic_snapshot(cfg, predictor.metadata)
    assert utc(snapshot["snapshot_as_of_utc"]) <= now
    assert utc(snapshot["history_cutoff_utc"]) + timedelta(seconds=1) > now
