"""Freeze offline selection in a portable YAML used by online inference."""
import os
from pathlib import Path
import tempfile

import yaml

from ml.common import file_hash, read_json, require

SCHEMA = "ml_deployment_v1"
FROZEN_FIELDS = (
    "target", "delta_s", "model_version", "lookback_L", "horizon_H", "ma_window",
    "epsilon", "baseline_epsilon", "selection_metric",
    "rf_params", "random_state", "feature_schema_version",
)


def read_deployment(filename):
    with open(filename, encoding="utf-8") as stream:
        try:
            deployment = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            require(False, f"MODEL_LOAD_FAIL: invalid deployment YAML: {exc}")
    require(isinstance(deployment, dict) and deployment.get("schema_version") == SCHEMA,
            "MODEL_LOAD_FAIL: deployment schema")
    require(isinstance(deployment.get("artifact_dir"), str) and deployment["artifact_dir"],
            "MODEL_LOAD_FAIL: deployment artifact_dir")
    return deployment


def verify_deployment(filename):
    """Verify frozen metadata and bytes without deserializing the model."""
    deployment = read_deployment(filename)
    artifact = (Path(filename).resolve().parent / deployment["artifact_dir"]).resolve()
    config = read_json(artifact / "ml_config.json")
    metadata = read_json(artifact / "metadata.json")
    for name, key in (("model.joblib", "model_sha256"), ("ml_config.json", "ml_config_sha256")):
        require(deployment.get(key) == metadata[key] == file_hash(artifact / name),
                f"MODEL_LOAD_FAIL: deployment {key} mismatch")
    for key in FROZEN_FIELDS:
        require(deployment.get(key) == config[key], f"MODEL_LOAD_FAIL: deployment {key} mismatch")
    require(type(deployment["horizon_H"]) is int and deployment["horizon_H"] > 0,
            "MODEL_LOAD_FAIL: invalid horizon")
    require(type(deployment["delta_s"]) is int and deployment["delta_s"] == 1,
            "MODEL_LOAD_FAIL: expected one-second bins")
    return deployment


def export_deployment(artifact_dir, filename):
    """Export only an already selected model; never inspect test labels or retune."""
    artifact, output = Path(artifact_dir).resolve(), Path(filename).resolve()
    require(output.suffix.lower() in (".yaml", ".yml"), "Deployment output must be a YAML file")
    metadata = read_json(artifact / "metadata.json")
    for name, key in (("model.joblib", "model_sha256"), ("ml_config.json", "ml_config_sha256")):
        require(file_hash(artifact / name) == metadata[key], f"Cannot freeze corrupted {name}")
    config = read_json(artifact / "ml_config.json")
    validation = read_json(artifact / "validation.json")
    deployment = {
        "schema_version": SCHEMA,
        "artifact_dir": Path(os.path.relpath(artifact, output.parent)).as_posix(),
        **{key: config[key] for key in FROZEN_FIELDS},
        "model_sha256": metadata["model_sha256"],
        "ml_config_sha256": metadata["ml_config_sha256"],
        "selection": {
            "validation_mae_mbps": validation["selected_comparison"]["rf"]["pooled_mae_mbps"],
            "rf_trial_count": len(validation["trials"]),
            "fit_sample_count": metadata["fit_sample_count"],
            "train_sample_count": metadata["train_sample_count"],
            "bundle_sha256": metadata["bundle_sha256"],
            "search_config_sha256": metadata["search_config_sha256"],
            "test_used_for_selection": metadata["test_used_for_selection"],
            "data_provenance": metadata["data_provenance"],
        },
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=output.name + ".", dir=output.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            yaml.safe_dump(deployment, stream, sort_keys=False, allow_unicode=True)
        os.replace(temporary, output)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return deployment
