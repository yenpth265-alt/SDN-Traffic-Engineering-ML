"""Generate deterministic handoff fixtures with the actual frozen Predictor."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

from data.ingestion.online_models import ForecastBatch, Snapshot
from ml.common import digest, write_json
from ml.inference.online import build_forecast_batch
from ml.inference.predict import Predictor
from ml.tests.test_online import make_context_for_v14, make_v14_snapshot

ROOT = Path(__file__).resolve().parents[2]


def generate(output=None):
    output = Path(output or ROOT / "testing/fixtures/ml_data_v14")
    predictor = Predictor(ROOT / "configs/ml/selected.yaml")
    cutoff = datetime(2026, 11, 26, 9, 2, 40, tzinfo=timezone.utc)
    envelope, origin = make_v14_snapshot(predictor.config, cutoff)
    topology = json.loads((ROOT / "configs/network/topology_bundle.json").read_text(encoding="utf-8"))
    inner = envelope["sdn"]
    inner["topology_hash"] = topology["topology_hash"]
    inner.pop("content_hash")
    inner["content_hash"] = digest(inner)
    context = make_context_for_v14(envelope, predictor.metadata, origin)
    rows = predictor.predict_batch(envelope, context, clock=lambda: cutoff + timedelta(milliseconds=350))
    batch = build_forecast_batch(inner["run_id"], inner["snapshot_id"], rows,
                                 snapshot=envelope, horizon_H=predictor.config["horizon_H"])
    Snapshot.model_validate(envelope)
    ForecastBatch.model_validate(batch)
    output.mkdir(parents=True, exist_ok=True)
    write_json(output / "snapshot.json", envelope)
    write_json(output / "context.json", context)
    write_json(output / "forecast_batch.json", batch)
    hashes = {name: hashlib.sha256((output / name).read_bytes()).hexdigest()
              for name in ("snapshot.json", "context.json", "forecast_batch.json")}
    summary = {"fixture_kind": "SYNTHETIC_SNAPSHOT_REAL_FROZEN_PREDICTOR_NOT_LIVE_NETWORK",
               "run_id": inner["run_id"], "snapshot_id": inner["snapshot_id"],
               "L": predictor.config["lookback_L"], "H": predictor.config["horizon_H"],
               "rows": len(rows), "flow_count": len({r["flow_group_id"] for r in rows}),
               "anchor": inner["cutoff_offset_s"], "strict_integer_offsets": True,
               "prediction_fields": list(rows[0]), "content_hash": inner["content_hash"],
               "snapshot_http_sha256": hashes["snapshot.json"], "file_sha256": hashes,
               "model_sha256": predictor.metadata["model_sha256"]}
    write_json(output / "verification.json", summary)
    return summary


if __name__ == "__main__":
    print(json.dumps(generate(), ensure_ascii=True))
