"""Train one shared multi-output RF; select only on validation."""
from itertools import product
from pathlib import Path
import platform
from importlib.metadata import version

import joblib
import numpy as np
import sklearn
import yaml
from sklearn.ensemble import RandomForestRegressor

from ml.baselines import moving_average, persistence
from ml.common import (FEATURE_VERSION, digest, file_hash, new_directory, require,
                       stamp, whole, write_json)
from ml.preprocessing.bundle import TrainingBundle
from ml.preprocessing.features import build_features, feature_names
from ml.validation.metrics import calibrate, choose_epsilon, errors


def rf_predict(model, x, horizon):
    result = np.asarray(model.predict(x), dtype=float)
    if horizon == 1 and result.ndim == 1:
        result = result.reshape(-1, 1)
    require(result.shape == (len(x), horizon), "RF output shape mismatch")
    return result


def versions():
    return {"python": platform.python_version(), "numpy": np.__version__,
            "scikit_learn": sklearn.__version__, "joblib": joblib.__version__}


def train(bundle_path, config_path, out):
    with open(config_path, encoding="utf-8") as f:
        search = yaml.safe_load(f)
    b = TrainingBundle(bundle_path)
    tx, ty, tm = b.load("train")
    vx, vy, vm = b.load("validation")
    ids = b.info["flow_group_ids"]
    ls = sorted({whole(v, "lookback", 1) for v in search["lookback_candidates"]})
    hs = sorted({whole(v, "horizon", 1) for v in search["horizon_candidates"]})
    ws = sorted({whole(v, "MA window", 1) for v in search["ma_window_candidates"]})
    require(ls and hs and ws, "Empty tuning candidates")
    require(max(ls) <= tx.shape[1] and max(hs) <= ty.shape[1], "Bundle is too short for search")
    require(min(ws) <= min(ls), "At least one MA window must fit every L candidate")
    eps = sorted(set(float(v) for v in search["epsilon_candidates"]))
    require(eps and eps[0] == 0 and np.isfinite(eps).all(), "Invalid epsilon candidates")
    baseline_epsilon = float(search["baseline_epsilon"])
    require(np.isfinite(baseline_epsilon) and baseline_epsilon >= 0, "Invalid baseline epsilon")
    seed = whole(search["random_state"], "random_state")
    limit = whole(search["max_train_samples"], "max_train_samples", 1)
    selected = np.arange(len(tx))
    if len(tx) > limit:
        selected = np.sort(np.random.default_rng(seed).choice(len(tx), limit, replace=False))
    trials, best = [], None
    require(search["rf_candidates"], "Empty RF search")
    allowed = {"n_estimators", "max_depth", "min_samples_leaf", "max_features",
               "max_leaf_nodes", "min_samples_split", "max_samples"}
    for params in search["rf_candidates"]:
        require(set(params) <= allowed, "Unsupported RF candidate parameter")
    for L in ls:
        xtr = build_features(tx[selected], tm.flow_group_id.iloc[selected].tolist(), ids, L)
        xval = build_features(vx, vm.flow_group_id.tolist(), ids, L)
        for H, params in product(hs, search["rf_candidates"]):
            model = RandomForestRegressor(**params, random_state=seed, n_jobs=search["n_jobs"])
            labels = ty[selected, :H]
            model.fit(xtr, labels[:, 0] if H == 1 else labels)
            raw = rf_predict(model, xval, H)
            epsilon, score = choose_epsilon(vy[:, :H], raw, eps)
            trial = {"lookback_L": L, "horizon_H": H, "epsilon": epsilon,
                     "rf_params": params, "validation_mae_mbps": score}
            trials.append(trial)
            print(f"L={L} H={H} epsilon={epsilon}: validation MAE={score:.6f}", flush=True)
            if best is None or score < best["validation_mae_mbps"] - 1e-12:
                best = {**trial, "model": model, "raw": raw}
    L, H = best["lookback_L"], best["horizon_H"]
    ma_trials = []
    for W in ws:
        if W <= L:
            pred = calibrate(moving_average.predict(vx[:, -L:], H, W), baseline_epsilon)
            ma_trials.append((errors(vy[:, :H], pred)["pooled_mae_mbps"], W))
    _, W = min(ma_trials)
    frozen = {
        "schema_version": "ml_config_v1", "model_version": "rf_v1", "source": "rf_v1",
        "target": "requested_mbps", "delta_s": 1,
        "lookback_L": L, "horizon_H": H, "epsilon": best["epsilon"], "ma_window": W,
        "feature_schema_version": FEATURE_VERSION,
        "feature_names": feature_names(L, ids), "flow_group_ids": ids,
        "baseline_epsilon": baseline_epsilon,
        "baseline_sources": {"persistence": "persistence", "moving_average": "moving_avg"},
        "rf_params": best["rf_params"], "random_state": seed,
        "selection_metric": "validation_pooled_vector_mae_mbps",
        "selection_note": "Same cutoffs across L/H; different H represent different targets. "
                          "Selected H minimizes validation MAE, not proven network gain.",
        "policy_status": "ML selection complete; integration/benchmark owner approval separate",
    }
    out = new_directory(out)
    joblib.dump(best["model"], out / "model.joblib")
    write_json(out / "ml_config.json", frozen)
    metadata = {
        "created_at_utc": stamp(), "model_sha256": file_hash(out / "model.joblib"),
        "ml_config_sha256": file_hash(out / "ml_config.json"),
        "bundle_sha256": b.sha256, "bundle_files": b.info["files"],
        "search_config_sha256": file_hash(config_path), "libraries": versions(),
        "train_sample_count": len(tx), "fit_sample_count": len(selected),
        "fit_sample_ids_sha256": digest(tm.sample_id.iloc[selected].tolist()),
        "validation_sample_ids_sha256": digest(vm.sample_id.tolist()),
        "refit_train_validation": False, "test_used_for_selection": False,
        "source_hashes": b.info["source_hashes"],
        "data_provenance": b.info.get("data_provenance", {"kind": "unspecified"}),
    }
    write_json(out / "metadata.json", metadata)
    (out / "requirements-lock.txt").write_text(
        "".join(f"{name}=={version(name)}\n" for name in
                ("numpy", "pandas", "scikit-learn", "scipy", "joblib", "PyYAML")),
        encoding="utf-8")
    predictions = {
        "rf": calibrate(best["raw"], frozen["epsilon"]),
        "persistence": calibrate(persistence.predict(vx[:, -L:], H), baseline_epsilon),
        "moving_average": calibrate(moving_average.predict(vx[:, -L:], H, W), baseline_epsilon),
    }
    write_json(out / "validation.json", {
        "common_sample_ids_sha256": digest(vm.sample_id.tolist()),
        "trials": trials, "moving_average_trials": [{"W": w, "mae": s} for s, w in ma_trials],
        "selected_comparison": {name: errors(vy[:, :H], p) for name, p in predictions.items()},
    })
    (out / "MODEL_CARD.md").write_text(
        "# SDN demand forecasting\n\n"
        f"- Target: requested_mbps; shared RF for 48 flows; L={L}, H={H}.\n"
        f"- Epsilon={frozen['epsilon']}; MA W={W}; baseline epsilon={baseline_epsilon}.\n"
        "- Features: past demand lags/stats/trend and canonical flow one-hot only.\n"
        "- Split: seeds/families before windowing; 70/15/15. No test tuning or refit.\n"
        f"- Fitted {len(selected)} / {len(tx)} train windows; seed={seed}.\n"
        f"- Data source: {metadata['data_provenance']['kind']}; see metadata.json for provenance.\n"
        "- See validation.json for comparisons; RF superiority is not assumed.\n"
        "- Lower forecasting MAE does not establish better network performance.\n"
        "- Pilot latency/fallback/transport policies still need integration approval.\n",
        encoding="utf-8")
    return frozen
