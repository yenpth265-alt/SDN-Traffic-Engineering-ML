"""Sequential, deadline-aware Runner for already launched Network/Data/SDN processes."""
import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import hashlib
import math
import os
from pathlib import Path
import sqlite3
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

import yaml

from integration.orchestration.context import OracleProvider, manifest_to_run, ready_forecast_context
from ml.common import require, stamp, utc
from ml.deployment import verify_deployment
from ml.inference.online import DataApiClient, OnlineForecastRunner
from optimization.config_loader import load_optimization_config
from optimization.planner import OptimizationController
from optimization.service_client import OptimizationServiceClient
from sdn.scripts.preflight_and_launch_v14 import evaluate_sdn_readiness_payload


class JsonHTTP:
    def __init__(self, base_url, headers, timeout_s=1):
        self.base_url, self.headers, self.timeout_s = base_url.rstrip("/"), headers, timeout_s

    def __call__(self, method, path, body=None):
        data = None if body is None else json.dumps(body, allow_nan=False).encode()
        request = urllib.request.Request(self.base_url + path, data=data, method=method,
                                         headers={"Content-Type": "application/json", **self.headers})
        with urllib.request.urlopen(request, timeout=self.timeout_s) as reply:
            return json.load(reply)


@contextmanager
def exclusive_run(filename):
    """Hold an OS lock so two Runners cannot race on the same apply journal."""
    with open(filename, "a+b") as stream:
        stream.seek(0)
        stream.write(b"\0")
        stream.flush()
        stream.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def network_delivery_pending(directory, run_id):
    """Observe producer-owned journals; never rewrite an active sender's outbox."""
    directory = Path(directory)
    expected = [directory / f"demand_outbox_h{i}.jsonl" for i in range(1, 9)]
    expected += [directory / "telemetry_outbox.jsonl", directory / "traffic_outbox.jsonl"]
    if not all(path.exists() for path in expected):
        return True
    for path in expected:
        pending = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            record = json.loads(line)
            key = record.get("event_id") or record.get("batch_id") or record.get("batch_key")
            if record.get("op") == "ack":
                pending.pop(key, None)
            else:
                pending[key] = record.get("payload", {})
        if any(payload.get("run_id") == run_id for payload in pending.values()):
            return True
    return False


class DurableAudit:
    """Append immutable events in SQLite before delivery; checkpoint each Data ACK."""
    def __init__(self, filename, run_id, sender, clock):
        self.filename, self.run_id, self.sender, self.clock = Path(filename), run_id, sender, clock
        self.filename.parent.mkdir(parents=True, exist_ok=True)
        from contextlib import closing
        with closing(self.connect()) as db, db:
            db.execute("CREATE TABLE IF NOT EXISTS audit (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, acked INTEGER NOT NULL DEFAULT 0)")

    def connect(self):
        connection = sqlite3.connect(self.filename)
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def record(self, kind, detail):
        from contextlib import closing
        with closing(self.connect()) as db, db:
            record_id = db.execute("INSERT INTO audit(payload) VALUES ('')").lastrowid
            payload = {"event_id": f"runner:{self.run_id}:{record_id}", "event_type": kind,
                       "event_at_utc": stamp(self.clock()), "detail": detail}
            db.execute("UPDATE audit SET payload=? WHERE id=?", (json.dumps(payload, allow_nan=False), record_id))
        return payload

    def flush(self):
        from contextlib import closing
        with closing(self.connect()) as db, db:
            rows = db.execute("SELECT id,payload FROM audit WHERE acked=0 ORDER BY id").fetchall()
        for record_id, raw in rows:
            reply = self.sender("POST", f"/v1/runs/{urllib.parse.quote(self.run_id, safe='')}/events/runner", json.loads(raw))
            require(type(reply.get("inserted")) is int and reply["inserted"] in (0, 1), "Invalid audit ACK")
            with closing(self.connect()) as db, db:
                db.execute("UPDATE audit SET acked=1 WHERE id=?", (record_id,))
        return len(rows)


def measured_utilization(filename, run_id, fabric_links, now, max_age_s=2):
    """Read Network's actual telemetry; require every directed fabric counter."""
    latest = None
    with Path(filename).open(encoding="utf-8") as stream:
        for line in stream:
            if not line.endswith("\n"):
                break  # A producer may still be writing the final record.
            batch = json.loads(line)
            if batch.get("run_id") == run_id:
                latest = batch
    require(latest is not None, "No telemetry for this run")
    age = (now - utc(latest["sampled_at_utc"])).total_seconds()
    require(0 <= age <= max_age_s, "Telemetry is stale or from the future")
    samples = [r for r in latest["samples"] if r["link_id"] in fabric_links]
    require(len(samples) == len(fabric_links) and {r["link_id"] for r in samples} == fabric_links,
            "Incomplete/duplicate fabric telemetry")
    values = [r.get("utilization") for r in samples]
    require(all(type(v) in (int, float) and math.isfinite(v) and v >= 0 for v in values),
            "Invalid fabric counter measurement")
    return max(values)


class ClosedLoopRunner:
    def __init__(self, run, data, sdn, forecast_runner, optimizer, audit, utilization,
                 *, clock=lambda: datetime.now(timezone.utc), sleep=time.sleep,
                 snapshot_wait_s=.2, drain_timeout_s=10, forecast_timeout_ms=300,
                 oracle_provider=None, readiness_timeout_s=30):
        self.run_info, self.data, self.sdn = run, data, sdn
        self.forecast_runner, self.optimizer, self.audit = forecast_runner, optimizer, audit
        self.utilization, self.clock, self.sleep = utilization, clock, sleep
        self.snapshot_wait_s, self.drain_timeout_s = snapshot_wait_s, drain_timeout_s
        self.forecast_timeout_ms = forecast_timeout_ms
        self.oracle_provider, self.readiness_timeout_s = oracle_provider, readiness_timeout_s
        self.safe_run_id = urllib.parse.quote(run["run_id"], safe="")
        self.network_pending = lambda: False
        self.registered = False

    def preflight(self):
        self.data("GET", "/health/ready")
        self.data("POST", "/v1/runs", self.run_info)
        self.registered = True
        deadline = time.monotonic() + self.readiness_timeout_s
        while True:
            ready = self.sdn("GET", "/health")
            ok, reasons = evaluate_sdn_readiness_payload(ready)
            if ok:
                break
            require(time.monotonic() < deadline, "SDN not ready: " + "; ".join(reasons))
            self.sleep(.1)
        require(ready.get("run_id") == self.run_info["run_id"], "SDN is serving a different run")
        for key in ("config_version", "topology_version", "topology_hash"):
            expected = self.run_info["config_version"] if key == "config_version" else self.run_info["topology_id"] if key == "topology_hash" else None
            if expected is not None:
                require(ready.get(key) == expected, f"SDN {key} mismatch")

    def tick(self, cutoff):
        run = self.run_info
        if cutoff < run["lookback_bins"]:
            result = {"status": "SKIPPED_WARMUP", "cutoff": cutoff}
        elif run["method_id"] == "M0":
            result = {"status": "STATIC", "cutoff": cutoff}
        else:
            if run["method_id"] == "M4":
                require(self.oracle_provider is not None, "M4 requires its declared oracle schedule")
            if self.optimizer.journal_path.exists():
                recovery = self.optimizer.recover_pending_apply(now_utc=self.clock())
                if self.optimizer.journal_path.exists():
                    result = {"status": "SKIPPED_IN_FLIGHT", "cutoff": cutoff, "recovery": recovery}
                    self.audit.record("CONTROL_TICK", result)
                    return result
            routing = self.optimizer.get_routing_state(run_id=run["run_id"])
            gate = routing.get("data_sync_gate", {})
            require(gate.get("optimization_tick_eligible") is True, "SDN/Data backpressure")
            origins = {k: run[k] for k in ("run_started_at_utc", "traffic_started_at_utc", "measurement_started_at_utc")}
            origins.update(warmup_s=run["warmup_s"], delta_s=1)
            snapshot = self.sdn("POST", "/snapshots", {"run_id": run["run_id"], "cutoff": cutoff,
                                "lookback_L": run["lookback_bins"], "origin_refs": origins})
            snapshot_id = snapshot["snapshot_id"]
            target_end = utc(run["measurement_started_at_utc"]) + timedelta(seconds=cutoff + run["horizon_bins"])
            wait_end = time.monotonic() + self.snapshot_wait_s
            while True:
                try:
                    self.data("GET", f"/v1/runs/{self.safe_run_id}/snapshots/{urllib.parse.quote(snapshot_id, safe='')}")
                    break
                except urllib.error.HTTPError as exc:
                    if exc.code != 404 or time.monotonic() >= wait_end or self.clock() >= target_end:
                        raise
                    self.sleep(.01)
            if run["method_id"] == "M4":
                ready = self.oracle_provider(run, snapshot, self.clock())
                predicted = {"status": "OK"}
            else:
                predicted = self.forecast_runner.process_snapshot(
                    run["run_id"], snapshot_id, forecast_timeout_ms=self.forecast_timeout_ms,
                    fallback_policy="skip", emit_event=True, clock=self.clock)
            if predicted["status"] != "OK":
                result = predicted
            else:
                if run["method_id"] != "M4":
                    ready = self.optimizer.get_forecast_batch(run["run_id"], predicted["forecast_batch_id"])
                routing = self.optimizer.get_routing_state(run_id=run["run_id"])
                now = self.clock()
                context = ready_forecast_context(ready, run, snapshot, routing, tick_at_utc=stamp(now))
                result = self.optimizer.execute_tick(
                    u_current=self.utilization(now), forecast_data=context, routing_state=routing,
                    now_utc=now, tick_id=str(cutoff), tick_at_utc=stamp(now),
                    apply_deadline_utc=context["apply_deadline_utc"])
        self.audit.record("CONTROL_TICK", {"cutoff": cutoff, **result})
        return result

    def execute(self):
        run = self.run_info
        origin = utc(run["measurement_started_at_utc"])
        next_cutoff, failed = 0, False
        try:
            self.preflight()
            while next_cutoff + run["horizon_bins"] <= run["measurement_s"]:
                now = self.clock()
                elapsed = math.floor((now - origin).total_seconds())
                if elapsed < next_cutoff:
                    self.sleep(min(.1, (origin + timedelta(seconds=next_cutoff) - now).total_seconds()))
                    continue
                if elapsed > next_cutoff:
                    for missed in range(next_cutoff, min(elapsed, run["measurement_s"])):
                        self.audit.record("CONTROL_TICK", {"cutoff": missed, "status": "SKIPPED_LATE"})
                    next_cutoff = elapsed
                if next_cutoff + run["horizon_bins"] > run["measurement_s"]:
                    break
                try:
                    self.tick(next_cutoff)
                    self.audit.flush()
                except (ValueError, RuntimeError, OSError) as exc:
                    self.audit.record("CONTROL_TICK", {"cutoff": next_cutoff, "status": "SKIPPED_ERROR", "reason": str(exc)})
                next_cutoff += 1
            phases = run["manifest"].get("phases", {})
            finish_at = origin + timedelta(seconds=run["measurement_s"] + phases.get("drain_seconds", 10))
            while self.clock() < finish_at:
                self.sleep(min(.1, (finish_at - self.clock()).total_seconds()))
        except BaseException:
            failed = True
            raise
        finally:
            if self.registered:
                try:
                    self.drain()
                except (ValueError, RuntimeError, OSError):
                    failed = True
                self.audit.record("RUN_FINISHED", {"status": "failed" if failed else "ok"})
                self.audit.flush()
                self.data("PATCH", f"/v1/runs/{self.safe_run_id}/status", {"status": "failed" if failed else "ok"})
            else:
                self.audit.record("RUN_START_FAILED", {"status": "failed"})
        return {"run_id": run["run_id"], "status": "failed" if failed else "ok"}


    def drain(self):
        deadline = time.monotonic() + self.drain_timeout_s
        while time.monotonic() < deadline:
            self.optimizer.recover_pending_apply(now_utc=self.clock())
            self.audit.flush()
            state = self.optimizer.get_routing_state(run_id=self.run_info["run_id"])
            if (not self.optimizer.journal_path.exists()
                    and state.get("data_sync_gate", {}).get("optimization_tick_eligible") is True
                    and not self.network_pending()):
                return
            self.sleep(.05)
        raise TimeoutError("Pending apply/Data delivery did not drain; durable evidence is retained")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--profile", type=Path, default=Path("configs/experiments/runtime.yaml"))
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    profile = yaml.safe_load(args.profile.read_text(encoding="utf-8"))
    require(profile["first_tick_policy"] == "wait_for_measurement_history" and profile["fallback_policy"] == "skip",
            "This Runner profile supports waiting for complete measurement history and skipping invalid ticks")
    selected = verify_deployment(profile["ml_deployment_config"])
    topology = json.loads(Path(profile["network_config_dir"], "topology_bundle.json").read_bytes())
    network = yaml.safe_load(Path(profile["network_config_dir"], "config.yaml").read_text(encoding="utf-8"))
    run = manifest_to_run(json.loads(args.manifest.read_bytes()), topology, selected,
                          git_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                          config_version=network["config_version"])
    if not args.execute:
        print(json.dumps(run, indent=2))
        return
    clock = lambda: datetime.now(timezone.utc)
    state_dir = Path(profile["state_dir"]) / hashlib.sha256(run["run_id"].encode()).hexdigest()
    state_dir.mkdir(parents=True, exist_ok=True)
    data = JsonHTTP(profile["data_base_url"], {"X-API-Key": os.environ["SDN_RUNNER_KEY"]}, profile["http_timeout_s"])
    reader = JsonHTTP(profile["data_base_url"], {"X-API-Key": os.environ["SDN_READER_KEY"]}, profile["http_timeout_s"])
    def data_access(method, path, body=None):
        return reader(method, path, body) if "/snapshots/" in path else data(method, path, body)
    sdn = JsonHTTP(profile["sdn_base_url"], {"X-SDN-Token": os.environ["SDN_NORTHBOUND_TOKEN"]}, profile["http_timeout_s"])
    config = load_optimization_config(profile["optimization_config"])
    config.update(run_id=run["run_id"], config_hash=run["config_hash"], config_version=run["config_version"],
                  audit_log_path=str(state_dir / "optimizer.jsonl"))
    client = OptimizationServiceClient(
        OptimizationController(config), profile["data_base_url"], profile["sdn_base_url"],
        data_api_key=os.environ["SDN_OPTIMIZATION_KEY"], sdn_api_key=os.environ["SDN_NORTHBOUND_TOKEN"],
        sdn_auth_header="X-SDN-Token", sdn_auth_scheme="", timeout_sec=profile["http_timeout_s"],
        sdn_apply_endpoint="/apply?async=1", max_retries=0, apply_max_polls=profile["apply_max_polls"],
        journal_path=state_dir / "pending-apply.json")
    ml_client = DataApiClient(profile["data_base_url"], timeout_s=profile["http_timeout_s"], max_retries=0)
    forecasts = (OnlineForecastRunner(profile["ml_deployment_config"], ml_client, replay_db=state_dir / "ml.sqlite3")
                 if run["method_id"] in ("M1", "M2", "M3") else None)
    oracle = OracleProvider(run, run["manifest"]["schedule_file"]) if run["method_id"] == "M4" else None
    fabric = {r["link_id"] for r in topology["links"] if r["is_fabric"]}
    audit = DurableAudit(state_dir / "audit.sqlite3", run["run_id"], data, clock)
    runner = ClosedLoopRunner(run, data_access, sdn, forecasts, client, audit,
        lambda now: measured_utilization(profile["telemetry_jsonl"], run["run_id"], fabric, now),
        clock=clock, snapshot_wait_s=profile["snapshot_wait_s"], drain_timeout_s=profile["drain_timeout_s"],
        forecast_timeout_ms=profile["forecast_timeout_ms"], oracle_provider=oracle)
    runner.network_pending = lambda: network_delivery_pending(profile["network_outbox_dir"], run["run_id"])
    with exclusive_run(state_dir / "runner.lock"):
        result = runner.execute()
    print(json.dumps(result))
    if result["status"] != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
