"""Replay pinned producer captures through the Data API and a disposable PostgreSQL DB."""
from copy import deepcopy
from datetime import datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from uuid import uuid4

from fastapi.testclient import TestClient
from psycopg2 import sql

from data.ingestion.online_api import app
from data.ingestion.online_store import transaction


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "testing/fixtures/online_contract"
BASE_COMMIT = "6e5e3b9d18ac1919d47111b901a454154ed8f7af"
ROLES = ("RUNNER", "NETWORK", "SDN", "ML", "OPTIMIZATION", "READER")


def fixture(name):
    return json.loads((FIXTURES / f"{name}.json").read_bytes())


def utc(text):
    return datetime.fromisoformat(text)


def stamp(moment):
    return moment.isoformat().replace("+00:00", "Z")


class PostgreSQLContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.snapshot_capture = fixture("snapshot")
        cls.apply_capture = fixture("apply")
        cls.forecast_capture = fixture("forecast")
        cls.telemetry_capture = fixture("telemetry")
        cls.topology = json.loads((ROOT / "configs/network/topology_bundle.json").read_bytes())
        cls.topology_id = cls.topology["topology_hash"]
        cls.client = TestClient(app)
        cls.addClassCleanup(cls.client.close)
        keys = patch.dict(
            "os.environ", {f"SDN_{role}_KEY": f"qa-{role.lower()}" for role in ROLES}
        )
        keys.start()
        cls.addClassCleanup(keys.stop)
        ready = cls.client.get("/health/ready")
        if ready.status_code != 200:
            raise RuntimeError(f"Run the suite against PostgreSQL with schema revisions 1–3: {ready.text}")
        with transaction() as cursor:
            cursor.execute("SELECT current_setting('server_version') AS version")
            print(f"PostgreSQL {cursor.fetchone()['version']}", flush=True)
            cursor.execute(
                "INSERT INTO public.topologies VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (cls.topology_id, cls.topology["topology_version"], cls.topology_id,
                 (ROOT / "configs/network/topology_bundle.json").read_bytes()),
            )
            nodes = [(cls.topology_id, row["host_id"], "host", None) for row in cls.topology["hosts"]]
            nodes += [(cls.topology_id, row["switch_id"], row["role"], row["dpid"])
                      for row in cls.topology["switches"]]
            cursor.executemany(
                "INSERT INTO public.topology_nodes VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING", nodes
            )
            for table, fields in (
                ("flow_groups", ("flow_group_id", "flow_cookie", "src_host", "dst_host", "src_ip",
                                 "dst_ip", "src_edge", "dst_edge", "udp_port")),
                ("links", ("link_id", "from_node", "to_node", "from_port", "to_port",
                           "is_fabric", "capacity_mbps", "delay_ms")),
                ("candidate_paths", ("flow_group_id", "path_id", "path_label")),
            ):
                query = sql.SQL("INSERT INTO public.{} VALUES ({}) ON CONFLICT DO NOTHING").format(
                    sql.Identifier(table), sql.SQL(",").join([sql.Placeholder()] * (len(fields) + 1))
                )
                cursor.executemany(query, [
                    (cls.topology_id, *(row[field] for field in fields)) for row in cls.topology[table]
                ])

    def setUp(self):
        self.run_id = f"qa-contract-{uuid4().hex}"
        self.url = f"/v1/runs/{self.run_id}"
        snapshot = self.snapshot_capture["sdn"]
        self.origin = utc(snapshot["origin_refs"]["traffic_started_at_utc"])
        self.measurement = self.origin + timedelta(seconds=60)
        self.cutoff = utc(snapshot["history_cutoff_utc"])
        self.schedule = f"{self.run_id}-schedule"

    def at(self, bin_index):
        return stamp(self.origin + timedelta(seconds=bin_index))

    @staticmethod
    def now():
        with transaction() as cursor:
            cursor.execute("SELECT clock_timestamp() AS now")
            return cursor.fetchone()["now"]

    @staticmethod
    def headers(role):
        return {"X-API-Key": f"qa-{role.lower()}"}

    def response(self, verb, url, role, status=200, **kwargs):
        response = self.client.request(verb, url, headers=self.headers(role), **kwargs)
        if status >= 400:
            print(json.dumps({"test": self._testMethodName, "request": f"{verb} {url}",
                              "expected_http": status, "actual_http": response.status_code}), flush=True)
        self.assertEqual(response.status_code, status, response.text[:800])
        return response

    def check(self, subject, actual, expected):
        print(json.dumps({"test": self._testMethodName, "run_id": self.run_id,
                          "check": subject, "expected": expected, "actual": actual,
                          "status": "PASS" if actual == expected else "FAIL"},
                         sort_keys=True, default=stamp), flush=True)
        self.assertEqual(actual, expected, subject)

    def post(self, path, role, payload, ack=None, status=200):
        reply = self.response("POST", "/v1/" + path, role, status=status, json=payload).json()
        if ack is not None:
            self.assertEqual(reply, ack)
        return reply

    def get(self, suffix, role, **kwargs):
        return self.response("GET", f"{self.url}/{suffix}", role, **kwargs)

    def count(self, table):
        with transaction() as cursor:
            cursor.execute(sql.SQL("SELECT count(*) FROM public.{} WHERE run_id=%s").format(
                sql.Identifier(table)), (self.run_id,))
            return cursor.fetchone()["count"]

    def register(self, method="M1", mode="live"):
        body = dict(run_id=self.run_id, topology_id=self.topology_id, schedule_id=self.schedule,
                    mode=mode, method_id=method, run_started_at_utc=self.at(0),
                    traffic_started_at_utc=self.at(0), measurement_started_at_utc=self.at(60),
                    warmup_s=60, measurement_s=360, schema_version=self.snapshot_capture["data_v1"]["schema_version"],
                    config_version=self.snapshot_capture["sdn"]["config_version"],
                    config_hash=self.snapshot_capture["sdn"]["config_hash"], ml_config_hash="qa-fixture",
                    lookback_bins=2, horizon_bins=5, git_commit=BASE_COMMIT, manifest={"suite": "contracts"})
        with transaction() as cursor:
            cursor.execute("INSERT INTO public.split_assignments VALUES (%s,'qa','test')", (self.run_id,))
            cursor.execute("""
                INSERT INTO public.schedules
                    (schedule_id,topology_id,scenario_id,seed,split_manifest_id,seed_family,file_hash,
                     first_offset_s,duration_s)
                VALUES (%s,%s,'qa',1,%s,'qa','qa-fixture',0,420)
            """, (self.schedule, self.topology_id, self.run_id))
        self.post("runs", "RUNNER", dict(body, warmup_s=59), status=422)
        self.post("runs", "RUNNER", body, {"inserted": 1})
        return body

    def start_routing(self):
        state = dict(self.apply_capture["routing_before"], run_id=self.run_id)
        self.post("sdn/routing", "SDN", state, {"inserted": 1})
        self.post("sdn/routing", "SDN", state, {"inserted": 0})
        return state

    def demand(self, group, index, rate):
        return dict(run_id=self.run_id, flow_group_id=group, bin_index=index,
                    event_id=f"network:{self.run_id}:demand:{group}:{index}", requested_mbps=rate,
                    window_start_utc=self.at(index), window_end_utc=self.at(index + 1),
                    released_at_utc=self.at(index + 1))

    def captured_demands(self):
        return [
            self.demand(row["flow_group_id"], row["bin_index"], row["requested_mbps"])
            for row in self.snapshot_capture["data_v1"]["history"]
        ]

    def send_demand(self, row, inserted=1):
        self.post("network/demand", "NETWORK", row, {"inserted": inserted})

    def history(self, as_of, representation="compact", cutoff=None, lookback=2):
        response = self.get("demand-history", "SDN", params={
            "cutoff": cutoff or stamp(self.cutoff),
            "as_of": stamp(as_of),
            "lookback": lookback,
            "format": representation,
        })
        return response.json()

    def snapshot(self, as_of, history):
        return dict(self.snapshot_capture["data_v1"], run_id=self.run_id,
                    snapshot_as_of_utc=stamp(as_of), history=history)

    def predict_input(self, history):
        from ml.common import digest

        full = deepcopy(self.snapshot_capture["sdn"])
        values = {(row["flow_group_id"], row["bin_index"] - 60): row["requested_mbps"] for row in history}
        self.assertEqual(len(values), len(full["history"]))
        for row in full["history"]:
            row["requested_mbps"] = values[(row["flow_group_id"], row["bin_index"])]
        full.pop("content_hash")
        full["content_hash"] = digest(full)
        return full

    def send_snapshot(self, payload, inserted=None, status=200, wrong_hash=False):
        raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        digest = sha256(raw).hexdigest()
        headers = dict(self.headers("SDN"), **{
            "Content-Type": "application/json", "X-Content-SHA256": "bad" if wrong_hash else digest,
        })
        response = self.client.post("/v1/sdn/snapshots", content=raw, headers=headers)
        if status >= 400:
            print(json.dumps({"test": self._testMethodName, "request": "POST /v1/sdn/snapshots",
                              "expected_http": status, "actual_http": response.status_code}), flush=True)
        self.assertEqual(response.status_code, status, response.text[:800])
        if inserted is not None:
            self.assertEqual(response.json(), {"inserted": inserted})
        return raw

    def forecast_payload(self, snapshot, cast_offsets=False):
        issue = stamp(self.now())
        rows = [dict(row, run_id=self.run_id, issued_at=issue) for row in self.forecast_capture]
        if cast_offsets:
            for row in rows:
                for key in ("forecast_start", "forecast_end"):
                    value = row[key]
                    self.assertEqual(value, int(value))
                    row[key] = int(value)
        return dict(run_id=self.run_id, snapshot_id=snapshot["snapshot_id"],
                    forecast_batch_id=f"{self.run_id}-forecast", rows=rows)

    def test_telemetry_retry_and_topology(self):
        self.register()
        self.assertEqual(self.get("topology", "NETWORK").json(), self.topology)
        original = self.telemetry_capture
        payload = dict(original, run_id=self.run_id, batch_id=f"network:{self.run_id}:telemetry:0")
        sample_count = len(payload["samples"])
        self.assertEqual(payload["sample_count"], sample_count)
        self.post("sdn/links", "NETWORK", dict(payload, sample_count=sample_count - 1), status=422)
        self.post("sdn/links", "NETWORK", payload, {"inserted": sample_count})
        self.post("sdn/links", "NETWORK", payload, {"inserted": 0})
        self.check("link_samples after replay", self.count("link_samples"), sample_count)
        first, *rest = payload["samples"]
        invalid = dict(first, counter_status="DIFFERENT")
        self.post("sdn/links", "NETWORK", dict(payload, samples=[invalid, *rest]), status=422)
        delta_bytes = first["delta_bytes"] + 100
        changed = dict(first, delta_bytes=delta_bytes,
                       port_tx_mbps=delta_bytes * 8 / (first["delta_t_s"] * 1_000_000))
        self.post("sdn/links", "NETWORK", dict(payload, samples=[changed, *rest]), status=409)
        self.check("link_samples after invalid and conflicting payloads", self.count("link_samples"), sample_count)

    def test_i02_cutoff_late_missing_and_future_schedule(self):
        self.register()
        self.start_routing()
        *on_time, late = self.captured_demands()
        for row in on_time:
            self.send_demand(row)
        self.send_demand(on_time[0], inserted=0)
        self.post("network/demand", "NETWORK", dict(on_time[0], requested_mbps=1000), status=409)
        self.check("demand_observations after retry and same-ID conflict",
                   self.count("demand_observations"), 95)
        group = late["flow_group_id"]
        future = self.demand(group, 160, 789)
        self.send_demand(future)
        with transaction() as cursor:
            cursor.execute("INSERT INTO public.demand_plan VALUES (%s,%s,%s,160,1)",
                           (self.schedule, self.topology_id, group))

        as_of = self.now()
        first = self.history(as_of)
        self.check("history at first as_of", {"rows": len(first), "bins": sorted({r["bin_index"] for r in first})},
                   {"rows": 95, "bins": [158, 159]})
        self.send_snapshot(self.snapshot(as_of, first), status=422)
        partial = dict(self.snapshot(as_of, first), snapshot_id="incomplete", quality_status="INCOMPLETE")
        self.send_snapshot(partial, inserted=1)
        self.post("ml/forecasts", "ML", self.forecast_payload(partial, cast_offsets=True), status=409)

        self.send_demand(late)
        with transaction() as cursor:
            cursor.execute("""
                UPDATE public.demand_plan SET requested_mbps=999
                WHERE schedule_id=%s AND flow_group_id=%s AND time_offset_s=160
                RETURNING requested_mbps
            """, (self.schedule, group))
            self.assertEqual(cursor.fetchone()["requested_mbps"], 999)
        self.check("original as_of returns identical history after late ingest and schedule edit",
                   self.history(as_of) == first, True)
        refreshed_as_of = self.now()
        late_row = next(row for row in self.history(refreshed_as_of)
                        if row["flow_group_id"] == group and row["bin_index"] == late["bin_index"])
        future_row = next(row for row in self.history(refreshed_as_of, cutoff=self.at(161), lookback=3)
                          if row["bin_index"] == 160)
        self.assertGreater(utc(late_row["available_at_utc"]), as_of)
        self.assertLessEqual(utc(future_row["available_at_utc"]), as_of)
        self.send_snapshot(self.snapshot(as_of, first + [late_row]), status=409)
        self.send_snapshot(self.snapshot(as_of, first + [future_row]), status=409)

        complete = self.history(refreshed_as_of)
        self.check("history at newer as_of and PostgreSQL observations",
                   [len(complete), self.count("demand_observations")], [96, 97])
        payload = self.snapshot(refreshed_as_of, complete)
        raw = self.send_snapshot(payload, inserted=1)
        self.send_snapshot(payload, inserted=0)
        self.send_snapshot(dict(payload, quality_status="STALE"), status=409)
        saved = self.get("snapshots/" + payload["snapshot_id"], "READER")
        self.assertEqual(saved.content, raw)
        self.assertEqual(saved.headers["X-Content-SHA256"], sha256(raw).hexdigest())
        self.check("snapshot counts after exact replay: incomplete plus valid", self.count("snapshots"), 2)
        self.assertEqual(self.count("forecast_batches"), 0)

        # Reuse producer timing for ML replay, with feature values from PostgreSQL.
        # The only change between the two reads is in the schedule's future bin.
        from testing.scripts.capture_contract_fixtures import forecast_model, predict_forecast

        before = self.predict_input(saved.json()["history"])
        with transaction() as cursor:
            cursor.execute("""
                UPDATE public.demand_plan SET requested_mbps=1999
                WHERE schedule_id=%s AND flow_group_id=%s AND time_offset_s=160
                RETURNING requested_mbps
            """, (self.schedule, group))
            self.assertEqual(cursor.fetchone()["requested_mbps"], 1999)
        after = self.predict_input(self.history(refreshed_as_of))
        with TemporaryDirectory() as directory:
            predictor = forecast_model(Path(directory))
            for method in ("persistence", "moving_average", "rf"):
                with self.subTest(method=method):
                    self.assertEqual(predict_forecast(predictor, before, method),
                                     predict_forecast(predictor, after, method))
        print("TC-I02: 95 as-of rows; 96 after refresh; M1–M3 invariant to future schedule",
              flush=True)

    def valid_snapshot(self):
        for row in self.captured_demands():
            self.send_demand(row)
        as_of = self.now()
        valid = self.snapshot(as_of, self.history(as_of))
        self.send_snapshot(valid, wrong_hash=True, status=422)
        self.send_snapshot(valid, inserted=1)
        return valid

    def ready_forecast(self, snapshot, test_invalid=False):
        raw = self.forecast_payload(snapshot)
        if test_invalid:
            rejection = self.post("ml/forecasts", "ML", raw, status=422)
            self.check("raw ML float bin rejected by field validator",
                       "forecast_start" in rejection["detail"][0]["loc"], True)
        payload = self.forecast_payload(snapshot, cast_offsets=True)
        if test_invalid:
            self.post("ml/forecasts", "ML", dict(payload, rows=payload["rows"][:-1]), status=422)
            traffic_offsets = [
                dict(row, forecast_start=row["forecast_start"] + 60, forecast_end=row["forecast_end"] + 60)
                for row in payload["rows"]
            ]
            self.post("ml/forecasts", "ML", dict(payload, rows=traffic_offsets), status=422)
            self.check("invalid forecast leaves no batches and no predictions",
                       [self.count("forecast_batches"), self.count("predictions")], [0, 0])
        self.post("ml/forecasts", "ML", payload, {"inserted": 240})
        self.post("ml/forecasts", "ML", payload, {"inserted": 0})
        first, *rest = payload["rows"]
        self.post("ml/forecasts", "ML",
                  dict(payload, rows=[dict(first, predicted_mbps=999), *rest]), status=409)
        self.check("forecast_batches and predictions after replay and conflict",
                   [self.count("forecast_batches"), self.count("predictions")], [1, 240])
        ready = self.get("forecasts/" + payload["forecast_batch_id"], "OPTIMIZATION").json()
        self.assertEqual((ready["expected_rows"], ready["actual_rows"], len(ready["rows"])), (240, 240, 240))
        return payload, ready

    def test_i03_warmup_and_forecast_horizon(self):
        self.register()
        self.start_routing()
        snapshot = self.valid_snapshot()
        as_of = utc(snapshot["snapshot_as_of_utc"])
        compact = self.history(as_of)
        sdn = self.history(as_of, representation="sdn")
        offsets = {(row["flow_group_id"], row["bin_index"], row["requested_mbps"]) for row in compact}
        self.assertEqual(offsets, {(row["flow_group_id"], row["bin_index"] + 60, row["requested_mbps"])
                                  for row in sdn})
        with transaction() as cursor:
            cursor.execute("""
                SELECT s.cutoff_bin, EXTRACT(EPOCH FROM
                    s.history_cutoff_utc - r.measurement_started_at_utc)::integer AS measurement_bin
                FROM public.snapshots s JOIN public.runs r USING (run_id)
                WHERE s.run_id=%s AND s.snapshot_id=%s
            """, (self.run_id, snapshot["snapshot_id"]))
            row = cursor.fetchone()
        self.check("PostgreSQL traffic bin and measurement bin",
                   [row["cutoff_bin"], row["measurement_bin"]], [160, 100])
        self.assertEqual(self.at(160), "2026-01-01T10:02:40Z")
        _, ready = self.ready_forecast(snapshot, test_invalid=True)
        rows = ready["rows"]
        self.assertEqual({row["forecast_start"] for row in rows}, set(range(100, 105)))
        self.assertEqual({row["forecast_end"] for row in rows}, set(range(101, 106)))
        self.assertEqual(len({row["flow_group_id"] for row in rows}), 48)
        self.assertEqual(self.measurement + timedelta(seconds=max(row["forecast_end"] for row in rows)),
                         utc("2026-01-01T10:02:45Z"))
        print("TC-I03: cutoff traffic=160, measurement=100; 48 groups × H=5 → 240 rows", flush=True)

    def test_version_change_deadline_and_apply_replay(self):
        self.register()
        before = self.start_routing()
        snapshot = self.valid_snapshot()
        forecast, _ = self.ready_forecast(snapshot)
        trace = self.apply_capture
        tick_at = self.now()
        change = trace["request"]["changes"][0]
        rows = [
            dict(flow_group_id=row["flow_group_id"], old_path_id=row["path_id"],
                 proposed_path_id=change["new_path_id"] if row["flow_group_id"] == change["flow_group_id"]
                 else row["path_id"])
            for row in before["rows"]
        ]
        proposal = dict(run_id=self.run_id, decision_batch_id=trace["request"]["decision_batch_id"],
                        tick_id="apply-test", tick_at_utc=stamp(tick_at), snapshot_id=snapshot["snapshot_id"],
                        forecast_batch_id=forecast["forecast_batch_id"], expected_state_version=1,
                        apply_deadline_utc=stamp(tick_at + timedelta(minutes=5)), optimizer_result={}, rows=rows)
        expired = dict(proposal, decision_batch_id="expired", tick_id="expired", apply_deadline_utc=stamp(tick_at))
        rejection = self.post("optimization/decisions", "OPTIMIZATION", expired, status=422)
        self.assertIn("deadline has expired", rejection["detail"])
        self.check("control_ticks and decision_batches after expired deadline",
                   [self.count("control_ticks"), self.count("decision_batches")], [0, 0])
        self.post("optimization/decisions", "OPTIMIZATION", proposal, {"inserted": 1})
        self.post("optimization/decisions", "OPTIMIZATION", proposal, {"inserted": 0})

        completed_at = stamp(self.now())
        response = dict(trace["result_full"], run_id=self.run_id, completed_at_utc=completed_at)
        self.post("sdn/apply-results", "SDN", response, status=409)
        after = dict(trace["routing_after"], run_id=self.run_id)
        self.post("sdn/routing", "SDN", dict(after, state_version=3), status=409)
        self.post("sdn/routing", "SDN", after, {"inserted": 1})
        self.post("sdn/routing", "SDN", after, {"inserted": 0})

        stale = dict(proposal, decision_batch_id="stale", tick_id="stale", tick_at_utc=stamp(self.now()))
        rejection = self.post("optimization/decisions", "OPTIMIZATION", stale, status=409)
        self.assertIn("Routing version has changed", rejection["detail"])
        stale_snapshot = dict(snapshot, snapshot_id="stale", snapshot_as_of_utc=stamp(self.now()))
        self.send_snapshot(stale_snapshot, status=409)
        self.send_snapshot(snapshot, inserted=0)

        narrow = dict(trace["result"], run_id=self.run_id, completed_at_utc=completed_at)
        rejection = self.post("sdn/apply-results", "SDN", narrow, status=422)
        self.assertIn("cover exactly", rejection["detail"])
        saved_before = self.get("decisions/" + proposal["decision_batch_id"], "SDN").json()
        self.assertIsNone(saved_before["response_payload"])
        self.post("sdn/apply-results", "SDN", response, {"updated": 1})
        self.post("sdn/apply-results", "SDN", response, {"updated": 0})
        self.post("sdn/apply-results", "SDN", dict(response, response_status="PARTIAL"), status=409)
        saved = self.get("decisions/" + proposal["decision_batch_id"], "SDN").json()
        result = saved["response_payload"]
        self.check("stored state_version, APPLIED count, KEPT count",
                   [result["response_state_version"],
                    sum(row["status"] == "APPLIED" for row in result["rows"]),
                    sum(row["status"] == "KEPT" for row in result["rows"])], [2, 1, 47])
        with transaction() as cursor:
            cursor.execute("SELECT state_version FROM public.routing_versions WHERE run_id=%s ORDER BY state_version",
                           (self.run_id,))
            versions = [row["state_version"] for row in cursor]
        self.check("routing_versions after switch and retries", versions, [1, 2])
        self.assertEqual(self.count("routing_history"), 49)
        self.assertEqual(self.count("control_ticks"), 1)
        self.assertEqual(self.count("decision_batches"), 1)
        self.assertEqual(self.count("routing_decisions"), 48)
        self.assertEqual(self.count("snapshots"), 1)
        self.assertEqual(trace["expired_response"]["reason"], "DEADLINE_EXPIRED")
        self.assertEqual(trace["expired_response"]["state_version"], 1)

    def test_runner_and_event_replay(self):
        self.register()
        event = dict(event_id=f"{self.run_id}-ready", event_at_utc=stamp(self.now()),
                     event_type="READY", detail={"suite": "contracts"})
        path = f"runs/{self.run_id}/events/network"
        self.post(path, "RUNNER", event, status=401)
        self.post(path, "NETWORK", event, {"inserted": 1})
        self.post(path, "NETWORK", event, {"inserted": 0})
        self.post(path, "NETWORK", dict(event, detail={"suite": "other"}), status=409)
        self.check("network event rows after replay and same-ID conflict", self.count("events"), 1)
        reply = self.response("PATCH", self.url + "/status", "RUNNER", json={"status": "ok"})
        self.assertEqual(reply.json(), {"updated": 1})
        repeat = self.response("PATCH", self.url + "/status", "RUNNER", json={"status": "ok"})
        self.assertEqual(repeat.json(), {"updated": 0})
        self.response("PATCH", self.url + "/status", "RUNNER", status=409, json={"status": "failed"})

    def test_runner_registration_replay(self):
        body = self.register()
        response = self.client.post("/v1/runs", headers=self.headers("RUNNER"), json=body)
        self.check("registered run rows after identical replay", self.count("runs"), 1)
        self.check("HTTP and ACK for identical run registration",
                   {"http": response.status_code, "body": response.json()},
                   {"http": 200, "body": {"inserted": 0}})
