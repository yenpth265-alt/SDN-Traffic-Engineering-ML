from ml.__main__ import main
from ml.common import read_json


def test_predict_model_failure_creates_audit_not_predictions(tmp_path):
    output = tmp_path / "rows.json"
    audit = tmp_path / "audit.json"
    code = main(["predict", "--artifact", str(tmp_path / "missing"),
                 "--snapshot", str(tmp_path / "snapshot.json"),
                 "--context", str(tmp_path / "context.json"),
                 "--out", str(output), "--audit", str(audit)])
    assert code == 2
    assert not output.exists()
    assert read_json(audit)["status"] == "INVALID"


def test_cli_refuses_rows_audit_same_file(tmp_path):
    target = tmp_path / "rows.json"
    code = main(["predict", "--artifact", "missing", "--snapshot", "missing",
                 "--context", "missing", "--out", str(target), "--audit", str(target)])
    assert code == 2
    assert not target.exists()


def test_missing_snapshot_after_model_load_still_writes_error_reason(tmp_path, monkeypatch):
    from ml.inference import predict
    class LoadedPredictor:
        last_audit = {}
        def __init__(self, _):
            pass
        def predict_batch(self, *_):
            raise AssertionError("Missing input must be rejected before inference")
    monkeypatch.setattr(predict, "Predictor", LoadedPredictor)
    audit = tmp_path / "audit.json"
    output = tmp_path / "rows.json"
    assert main(["predict", "--artifact", "loaded", "--snapshot", str(tmp_path / "missing.json"),
                 "--context", "unused", "--out", str(output), "--audit", str(audit)]) == 2
    result = read_json(audit)
    assert result["status"] == "INVALID" and result["reason"]
    assert not output.exists()
