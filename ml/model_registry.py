"""Verify local artifacts once before loading a model for multiple ticks."""
from pathlib import Path
import joblib

from ml.common import FEATURE_VERSION, file_hash, numbers, read_json, require, whole
from ml.deployment import FROZEN_FIELDS, read_deployment
from ml.preprocessing.features import feature_names
from ml.training.train import versions


def load_artifact(directory):
    path = Path(directory)
    deployment = None
    if path.suffix.lower() in (".yaml", ".yml"):
        deployment = read_deployment(path)
        path = (path.parent / deployment["artifact_dir"]).resolve()
    meta = read_json(path / "metadata.json")
    for name, key in (("model.joblib", "model_sha256"), ("ml_config.json", "ml_config_sha256")):
        require(file_hash(path / name) == meta[key], f"MODEL_LOAD_FAIL: {name} hash mismatch")
    cfg = read_json(path / "ml_config.json")
    if deployment is not None:
        for key in ("model_sha256", "ml_config_sha256"):
            require(deployment.get(key) == meta[key], f"MODEL_LOAD_FAIL: deployment {key} mismatch")
        for key in ("delta_s", "lookback_L", "horizon_H", "ma_window"):
            require(type(deployment.get(key)) is int, f"MODEL_LOAD_FAIL: deployment {key} must be int")
        for key in ("epsilon", "baseline_epsilon"):
            require(type(deployment.get(key)) in (int, float),
                    f"MODEL_LOAD_FAIL: deployment {key} must be numeric")
        for key in FROZEN_FIELDS:
            require(deployment.get(key) == cfg[key], f"MODEL_LOAD_FAIL: deployment {key} mismatch")
    require(cfg["schema_version"] == "ml_config_v1" and cfg["target"] == "requested_mbps"
            and cfg["delta_s"] == 1, "MODEL_LOAD_FAIL: config schema")
    L, H = whole(cfg["lookback_L"], "L", 1), whole(cfg["horizon_H"], "H", 1)
    require(1 <= whole(cfg["ma_window"], "W", 1) <= L, "MODEL_LOAD_FAIL: MA window")
    numbers([cfg["epsilon"], cfg["baseline_epsilon"]], "epsilon")
    expected = sorted((hex(s * 256 + d) for s in range(1, 9) for d in range(1, 9)
                       if (s - 1) // 2 != (d - 1) // 2), key=lambda x: int(x, 16))
    require(cfg["flow_group_ids"] == expected, "MODEL_LOAD_FAIL: flow order")
    require(cfg["feature_schema_version"] == FEATURE_VERSION
            and cfg["feature_names"] == feature_names(L, expected),
            "MODEL_LOAD_FAIL: feature order/schema")
    current = versions()
    for library in ("scikit_learn", "numpy", "joblib"):
        require(meta["libraries"][library] == current[library],
                f"MODEL_LOAD_FAIL: library version changed: {library}")
    # Only load locally trusted artifacts; joblib uses pickle.
    model = joblib.load(path / "model.joblib")
    require(model.n_features_in_ == len(cfg["feature_names"]) and model.n_outputs_ == H,
            "MODEL_LOAD_FAIL: model dimensions")
    return model, cfg, meta
