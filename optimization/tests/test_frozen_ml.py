from pathlib import Path
import pytest
import yaml
from optimization.ml_contract import normalize_forecast
from optimization.input_validator import normalize_routing_entries
from optimization.load_estimator import calculate_mean_demand


def test_frozen_h_and_final_values(tmp_path):
    cfg = tmp_path / "selected.yaml"
    cfg.write_text(yaml.safe_dump({"schema_version": "ml_deployment_v1", "artifact_dir": ".", "horizon_H": 1}))
    batch = {"run_id": "run", "measurement_started_at_utc": "2026-11-26T00:00:00Z",
             "rows": [{"run_id": "run", "flow_group_id": "0x103", "issued_at": "2026-11-26T00:00:01Z",
                       "forecast_start": 1, "forecast_end": 2, "predicted_mbps": 4.4, "source": "rf_v1"}]}
    normalized = normalize_forecast(batch, ["0x103"], cfg)
    assert calculate_mean_demand(normalized, ["0x103"]) == {"0x103": 4.4}
    assert normalized["forecast_end"] == 2
    assert normalized["forecast_end_utc"] == "2026-11-26T00:00:02.000000Z"
    normalized["predictions"]["0x103"]["predicted_mbps_array"] *= 5
    normalized.pop("rows")
    with pytest.raises(ValueError, match="Forecast H"):
        normalize_forecast(normalized, ["0x103"], cfg)


def test_native_routing_names_are_preserved():
    routes = normalize_routing_entries({"routing_state": [{"flow_group_id": "0x103", "path_id": "0x103_P1",
                                                           "verification_status": "UNKNOWN"}]})
    assert routes["0x103"]["status"] == "UNKNOWN"
    assert routes["0x103"]["current_path"] == "0x103_P1"
