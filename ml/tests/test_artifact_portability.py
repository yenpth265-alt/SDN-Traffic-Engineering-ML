"""Verify the bytes Git delivers on Linux match the frozen deployment hashes."""
import hashlib
import json
from pathlib import Path
import subprocess

import yaml


def test_frozen_config_hash_matches_repository_blob():
    root = Path(__file__).resolve().parents[2]
    selected_file = root / "configs/ml/selected.yaml"
    selected = yaml.safe_load(selected_file.read_text(encoding="utf-8"))
    artifact = (selected_file.parent / selected["artifact_dir"]).resolve()
    metadata = json.loads((artifact / "metadata.json").read_bytes())
    config = artifact / "ml_config.json"
    relative_path = config.relative_to(root).as_posix()
    # Reading the index also catches CRLF normalization on Windows, before CI.
    blob = subprocess.check_output(["git", "cat-file", "blob", f":{relative_path}"], cwd=root)
    assert hashlib.sha256(blob).hexdigest() == metadata["ml_config_sha256"] == selected["ml_config_sha256"]
    assert config.read_bytes() == blob
