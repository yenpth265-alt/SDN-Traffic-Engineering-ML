"""Verify local artifacts once before loading a model for multiple ticks."""
from pathlib import Path
import joblib

from ml.common import FEATURE_VERSION, file_hash, numbers, read_json, require, whole
from ml.preprocessing.features import feature_names
from ml.training.train import versions


def load_artifact(directory):
    path = Path(directory)
    meta = read_json(path / "metadata.json")
    for name, key in (("model.joblib", "model_sha256"), ("ml_config.json", "ml_config_sha256")):
        require(file_hash(path / name) == meta[key], f"MODEL_LOAD_FAIL: {name} hash mismatch")
    cfg = read_json(path / "ml_config.json")
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
