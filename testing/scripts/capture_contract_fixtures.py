"""Capture wire payloads by running the pinned producers with controlled inputs."""
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import time

import yaml

from ml.common import catalog, file_hash, read_json
from ml.demo import create_demo_inputs
from ml.inference.predict import Predictor
from ml.preprocessing.bundle import prepare_bundle
from ml.training.train import train
from optimization.result_serializer import serialize_apply_request
from sdn.adapters.data_api_v1_adapter import (
    map_apply_response_to_data_v1,
    map_routing_state_to_data_v1,
    map_snapshot_to_data_v1,
)
from sdn.controller.decision_handler import DecisionHandler, DecisionHandlerConfig
from sdn.controller.state_manager import StateManager
from sdn.controller.topology_registry import TopologyRegistry
from sdn.flow_management.flow_matcher import FlowMatcher
from sdn.flow_management.route_compiler import RouteCompiler
from sdn.flow_management.rule_installer import RuleInstaller
from sdn.flow_management.switch_driver import InMemorySwitchDriver
from sdn.monitoring.snapshot_creator import MockReleasedDemandProvider, SnapshotCreator
from sdn.route_verification.rule_verifier import RuleVerifier
from sdn.schemas.apply_models import ApplyRequest
from sdn.schemas.snapshot_models import DemandObservationRecord, OriginRefs


ROOT = Path(__file__).resolve().parents[2]
TOPOLOGY_DIR = ROOT / "configs/network"
RUN_ID = "qa-contract-capture"
TRAFFIC = datetime(2026, 1, 1, 10, tzinfo=timezone.utc)
MEASUREMENT = TRAFFIC + timedelta(seconds=60)
CUTOFF = MEASUREMENT + timedelta(seconds=100)
AS_OF = CUTOFF + timedelta(milliseconds=200)
ISSUED = CUTOFF + timedelta(milliseconds=300)


def stamp(value):
    return value.isoformat().replace("+00:00", "Z")


def write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n",
                    encoding="utf-8", newline="\n")


def capture_telemetry(directory):
    manifest = directory / "run.json"
    write(manifest, dict(run_id=RUN_ID, t0=time.time() + 0.4, warmup_s=0,
                         measurement_s=1, drain_s=0))
    subprocess.run(
        [sys.executable, str(ROOT / "network/telemetry/telemetry_collector.py"),
         "--manifest", str(manifest), "--config-dir", str(TOPOLOGY_DIR),
         "--outdir", str(directory), "--dry-run"],
        check=True,
    )
    batches = (directory / "link_samples.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(batches) == 1
    return json.loads(batches[0])


def capture_sdn():
    topo = TopologyRegistry.from_bundle_file(
        TOPOLOGY_DIR / "topology_bundle.json", csv_config_dir=TOPOLOGY_DIR)
    driver = InMemorySwitchDriver(topo)
    compiler = RouteCompiler(topo, FlowMatcher(topo))
    installer = RuleInstaller(compiler, driver, RuleVerifier(compiler, driver))
    state = StateManager(topo)
    initial = installer.bootstrap_initial_paths(RUN_ID, stamp(TRAFFIC))
    state.initialize_run(RUN_ID, stamp(TRAFFIC), verification_evidences=initial)
    routing_before = map_routing_state_to_data_v1(
        state.get_routing_state(RUN_ID), published_at_utc=stamp(TRAFFIC))

    provider = MockReleasedDemandProvider()
    ids = catalog(TOPOLOGY_DIR / "flow_groups.csv")
    provider.add_observations([
        DemandObservationRecord(
            run_id=RUN_ID, flow_group_id=group, bin_index=index,
            window_start_utc=stamp(MEASUREMENT + timedelta(seconds=index)),
            window_end_utc=stamp(MEASUREMENT + timedelta(seconds=index + 1)),
            requested_mbps=2.0 + index / 100,
            released_at_utc=stamp(MEASUREMENT + timedelta(seconds=index + 1)),
            available_at_utc=stamp(MEASUREMENT + timedelta(seconds=index + 1, milliseconds=50)),
            release_record_id=f"qa:{group}:{index}",
        )
        for group in ids for index in (98, 99)
    ])
    creator = SnapshotCreator(topo, state, provider)
    origins = OriginRefs(run_started_at_utc=stamp(TRAFFIC), traffic_started_at_utc=stamp(TRAFFIC),
                         measurement_started_at_utc=stamp(MEASUREMENT), delta_s=1)
    full = creator.create_snapshot(RUN_ID, cutoff=100, lookback_L=2,
                                   snapshot_as_of_utc=stamp(AS_OF), origin_refs=origins)
    compact, _, _ = map_snapshot_to_data_v1(full, use_traffic_origin_bins=True)

    versions = {"config_version": full.config_version, "topology_version": full.topology_version,
                "topology_hash": full.topology_hash, "config_hash": full.config_hash}
    proposal = dict(versions, run_id=RUN_ID, snapshot_id=full.snapshot_id,
                    expected_state_version=full.state_version,
                    changes=[{"flow_group_id": ids[0], "old_path_id": ids[0] + "_P1",
                              "new_path_id": ids[0] + "_P2"}])
    request = serialize_apply_request(
        proposal, {"forecast_start": stamp(CUTOFF),
                   "forecast_end": stamp(CUTOFF + timedelta(seconds=5)),
                   "apply_deadline_utc": stamp(CUTOFF + timedelta(seconds=4)),
                   "snapshot_as_of_utc": stamp(AS_OF),
                   "history_cutoff_utc": stamp(CUTOFF)}, {})
    handler = DecisionHandler(topo, state, installer,
                              config=DecisionHandlerConfig.for_mock_unit_tests())
    handler.register_snapshot(full)
    expired_request = serialize_apply_request(
        proposal, {"forecast_start": stamp(CUTOFF),
                   "forecast_end": stamp(CUTOFF + timedelta(seconds=5)),
                   "apply_deadline_utc": stamp(CUTOFF + timedelta(milliseconds=250)),
                   "snapshot_as_of_utc": stamp(AS_OF),
                   "history_cutoff_utc": stamp(CUTOFF)}, {})
    expired = handler.handle_apply_request(
        ApplyRequest.from_dict(expired_request, strict=True), decision_time_utc=stamp(ISSUED))
    assert expired.reason == "DEADLINE_EXPIRED", expired.to_dict()
    assert expired.state_version == full.state_version
    applied = CUTOFF + timedelta(milliseconds=400)
    completed = CUTOFF + timedelta(milliseconds=500)
    response = handler.handle_apply_request(
        ApplyRequest.from_dict(request, strict=True), decision_time_utc=stamp(ISSUED),
        applied_at_utc=stamp(applied), completed_at_utc=stamp(completed))
    assert response.status.value == "APPLIED", response.to_dict()
    replay = handler.handle_apply_request(
        ApplyRequest.from_dict(request, strict=True), decision_time_utc=stamp(ISSUED),
        applied_at_utc=stamp(applied), completed_at_utc=stamp(completed))
    assert replay.idempotent_replay and replay.state_version == response.state_version
    final_state = state.get_routing_state(RUN_ID)
    routing_after = map_routing_state_to_data_v1(
        final_state, published_at_utc=stamp(completed))
    return full.to_dict(), compact, {
        "request": request, "response": response.to_dict(),
        "result": map_apply_response_to_data_v1(response),
        "result_full": map_apply_response_to_data_v1(
            response, full_routing_state=final_state, expand_unchanged_as_kept=True),
        "expired_request": expired_request,
        "expired_response": expired.to_dict(),
        "routing_before": routing_before, "routing_after": routing_after,
    }


def forecast_model(directory):
    catalog_path = TOPOLOGY_DIR / "flow_groups.csv"
    config = create_demo_inputs(directory, catalog_path)
    config.update(lookback_candidates=[2], horizon_candidates=[5], ma_window_candidates=[2])
    config_path = directory / "training.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    bundle, artifact = directory / "bundle", directory / "artifact"
    prepare_bundle(directory / "history.csv", directory / "runs.json", catalog_path,
                   bundle, 2, 5, 5)
    train(bundle, config_path, artifact)
    return Predictor(artifact)


def predict_forecast(predictor, snapshot, method="persistence"):
    metadata = predictor.metadata
    context = {key: snapshot[key] for key in ("run_id", "snapshot_id", "config_hash", "topology_hash")}
    context.update(
        snapshot_content_hash=snapshot["content_hash"],
        ml_config_sha256=metadata["ml_config_sha256"],
        model_sha256=metadata["model_sha256"],
        measurement_started_at_utc=stamp(MEASUREMENT),
        measurement_end_utc=stamp(MEASUREMENT + timedelta(seconds=360)),
        max_snapshot_age_s=2.0, forecast_timeout_ms=5000,
        fallback_policy="skip", fallback_epsilon=0.0,
    )
    rows = predictor.predict_batch(snapshot, context, method, clock=lambda: ISSUED)
    assert len(rows) == 240
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory() as name:
        directory = Path(name)
        write(args.output / "telemetry.json", capture_telemetry(directory))
        full, compact, apply = capture_sdn()
        write(args.output / "snapshot.json", {"sdn": full, "data_v1": compact})
        write(args.output / "apply.json", apply)
        write(args.output / "forecast.json", predict_forecast(forecast_model(directory), full))
    for path in sorted(args.output.glob("*.json")):
        print(f"{file_hash(path)}  {path.name}")


if __name__ == "__main__":
    main()
