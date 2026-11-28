from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from data.ingestion.online_models import ForecastBatch, Snapshot
from data.ingestion import online_store as store
from ml.common import ContractError, ROW_FIELDS, read_json
from ml.inference.online import build_forecast_batch

FIXTURE = Path(__file__).resolve().parents[2] / "testing/fixtures/ml_data_v14"


def test_real_producer_batch_passes_data_schema_and_ingestion_guards(monkeypatch):
    batch = ForecastBatch.model_validate(read_json(FIXTURE / "forecast_batch.json"))
    snapshot = Snapshot.model_validate(read_json(FIXTURE / "snapshot.json")).sdn
    rows = read_json(FIXTURE / "forecast_batch.json")["rows"]
    assert len(rows) == 48 and {r["forecast_start"] for r in rows} == {100}
    assert all(set(r) == set(ROW_FIELDS) and type(r["forecast_start"]) is int
               and type(r["forecast_end"]) is int and r["forecast_end"] == 101 for r in rows)
    run = {"run_id": batch.run_id, "now": batch.rows[0].issued_at + timedelta(milliseconds=100),
           "measurement_started_at_utc": snapshot.origin_refs.measurement_started_at_utc,
           "horizon_bins": 1, "measurement_s": 360, "method_id": "M3"}

    class Cursor:
        inserted_rows = None
        def execute(self, *_args):
            pass
        def fetchone(self):
            return {"quality_status": "VALID", "history_cutoff_utc": snapshot.history_cutoff_utc,
                    "available_at_utc": snapshot.snapshot_as_of_utc}
        def executemany(self, _sql, values):
            self.inserted_rows = values

    cursor = Cursor()
    with patch.object(store, "flow_ids", return_value=set(snapshot.flow_group_ids)), \
            patch.object(store, "insert_once", return_value=1):
        assert store.ingest_forecast(cursor, run, batch) == 48
    assert len(cursor.inserted_rows) == 48
    from fastapi.testclient import TestClient
    from data.ingestion import online_api
    monkeypatch.setenv("SDN_ML_KEY", "fixture-only-ml-key")
    with patch.object(store, "flow_ids", return_value=set(snapshot.flow_group_ids)), \
            patch.object(store, "insert_once", return_value=1), \
            patch.object(online_api, "db_call", side_effect=lambda operation, run_id, payload:
                         operation(cursor, run, payload)):
        client = TestClient(online_api.app)
        payload = read_json(FIXTURE / "forecast_batch.json")
        response = client.post("/v1/ml/forecasts", json=payload, headers={"X-API-Key": "fixture-only-ml-key"})
        assert response.status_code == 200 and response.json() == {"inserted": 48}
        payload["rows"][0]["forecast_start"] = 100.0
        assert client.post("/v1/ml/forecasts", json=payload,
                           headers={"X-API-Key": "fixture-only-ml-key"}).status_code == 422


@pytest.mark.parametrize("mutation", ["float", "missing", "duplicate", "run", "end", "snapshot", "cutoff"])
def test_producer_rejects_incompatible_complete_batch(mutation):
    batch = read_json(FIXTURE / "forecast_batch.json")
    snapshot = read_json(FIXTURE / "snapshot.json")
    rows = batch["rows"]
    if mutation == "float": rows[0]["forecast_start"] = 100.0
    elif mutation == "missing": rows.pop()
    elif mutation == "duplicate": rows[0] = deepcopy(rows[1])
    elif mutation == "run": rows[0]["run_id"] = "wrong-run"
    elif mutation == "end": rows[0]["forecast_end"] = 102
    elif mutation == "snapshot": snapshot["sdn"]["snapshot_id"] = "wrong-snapshot"
    else: snapshot["sdn"]["cutoff_offset_s"] = 99
    with pytest.raises(ContractError):
        build_forecast_batch(batch["run_id"], batch["snapshot_id"], rows, snapshot=snapshot, horizon_H=1)
