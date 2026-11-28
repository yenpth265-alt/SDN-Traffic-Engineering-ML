"""Data ingestion integration tests against a disposable PostgreSQL database."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import unittest
from uuid import uuid4

from fastapi.testclient import TestClient

from data.ingestion.online_api import app
from data.ingestion.online_store import transaction
from testing.fixtures.snapshot_factory import canonical_snapshot


class OnlineIngestionTest(unittest.TestCase):
    def setUp(self):
        os.environ.update(SDN_NETWORK_KEY="test-network", SDN_SDN_KEY="test-sdn", SDN_READER_KEY="test-reader")
        self.net = {"X-API-Key": "test-network"}
        self.sdn = {"X-API-Key": "test-sdn"}
        self.client = TestClient(app)
        self.origin = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.run = "fixture-" + uuid4().hex
        with transaction() as cursor:
            cursor.execute("""
                INSERT INTO public.topologies VALUES (%(run)s,'v1',%(topology_hash)s,'{}');
                INSERT INTO public.topology_nodes VALUES
                  (%(run)s,'h1','host',NULL), (%(run)s,'h3','host',NULL),
                  (%(run)s,'e1','edge',1), (%(run)s,'e2','edge',2);
                INSERT INTO public.flow_groups VALUES
                  (%(run)s,'0x103','0x103','h1','h3','10.0.0.1','10.0.0.3','e1','e2',5001);
                INSERT INTO public.links VALUES
                  (%(run)s,'e1>e2','e1','e2',1,1,true,10,0);
                INSERT INTO public.candidate_paths VALUES
                  (%(run)s,'0x103','0x103_P1','P1'), (%(run)s,'0x103','0x103_P2','P2');
                INSERT INTO public.runs
                  (run_id,topology_id,mode,method_id,run_started_at_utc,traffic_started_at_utc,
                   measurement_started_at_utc,warmup_s,measurement_s,schema_version,status,
                   config_version,config_hash,lookback_bins,git_commit,manifest)
                VALUES (%(run)s,%(run)s,'live','M0',%(origin)s,%(origin)s,%(origin)s,0,360,'v1','running',
                        'v1','config',1,'fixture','{}');
            """, {"run": self.run, "origin": self.origin, "topology_hash": sha256(self.run.encode()).hexdigest()})
        self.addCleanup(self.client.close)

    @staticmethod
    def db_now():
        with transaction() as cursor:
            cursor.execute("SELECT clock_timestamp() AS now")
            return cursor.fetchone()["now"].isoformat()

    def at(self, seconds):
        return (self.origin + timedelta(seconds=seconds)).isoformat()

    def demand(self, index=0):
        return dict(run_id=self.run, flow_group_id="0x103", bin_index=index,
                    event_id=f"network:{self.run}:demand:0x103:{index}", requested_mbps=0,
                    window_start_utc=self.at(index), window_end_utc=self.at(index + 1),
                    released_at_utc=self.at(index + 1))

    def assert_post(self, path, body, headers, status=200, inserted=None):
        response = self.client.post("/v1/" + path, json=body, headers=headers)
        self.assertEqual(response.status_code, status, response.text)
        if inserted is not None:
            self.assertEqual(response.json(), {"inserted": inserted})
        return response

    def test_network_concurrent_retry_and_invalid_timestamps(self):
        payload = self.demand()
        missing_key = self.assert_post("network/demand", payload, {}, 401)
        self.assertIn("X-API-Key", missing_key.json()["detail"])
        security = self.client.get("/openapi.json").json()["paths"]["/v1/network/demand"]["post"]["security"]
        self.assertEqual(security, [{"APIKeyHeader": []}])
        self.assert_post("network/demand", payload, self.sdn, 401)
        self.assert_post("network/demand", dict(payload, released_at_utc="2026-01-01T07:00:01+07:00"),
                         self.net, 422)
        self.assert_post("network/demand", dict(payload, window_end_utc=self.at(2)), self.net, 422)
        self.assert_post("network/demand", dict(payload, run_id="missing"), self.net, 404)
        invalid = json.dumps(dict(payload, requested_mbps=float("nan")))
        response = self.client.post("/v1/network/demand", content=invalid,
                                    headers=dict(self.net, **{"Content-Type": "application/json"}))
        self.assertEqual(response.status_code, 422, response.text)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(
                lambda _: self.assert_post("network/demand", payload, self.net).json()["inserted"], range(2)))
        self.assertEqual(sorted(results), [0, 1])
        self.assert_post("network/demand", dict(payload, requested_mbps=1), self.net, 409)
        self.assert_post("network/demand", dict(self.demand(1), event_id=payload["event_id"]), self.net, 409)

    def test_traffic_fk_rolls_back_batch(self):
        row = dict(flow_group_id="0x103", window_start_utc=self.at(0), window_end_utc=self.at(1),
                   sent_bytes=100, received_bytes=None, sent_packets=1, received_packets=None)
        self.assert_post("network/traffic", dict(run_id=self.run, rows=[row, dict(row, flow_group_id="foreign")]),
                         self.net, 422)
        body = dict(run_id=self.run, rows=[row])
        self.assert_post("network/traffic", body, self.net, inserted=1)
        self.assert_post("network/traffic", body, self.net, inserted=0)

    def test_links_matches_collector_shape_and_topology(self):
        sample = dict(link_id="e1>e2", from_node="e1", to_node="e2", is_fabric=True,
                      measure_switch="e1", measure_port=1, tx_bytes_total=2**64 - 1,
                      delta_bytes=100, delta_t_s=1, port_tx_mbps=0.0008, utilization=0.00008,
                      tx_drops_delta=0, counter_status="OK")
        body = dict(run_id=self.run, batch_id=f"network:{self.run}:telemetry:0", bin_index=0,
                    sampled_at_utc=self.at(1), interval_s=1, sample_count=1, samples=[sample])
        producer_sample = {key: value for key, value in sample.items() if key != "tx_bytes_total"}
        empty_poll = dict(body, samples=[], sample_count=0)
        self.assert_post("sdn/links", empty_poll, self.net, 422)
        self.assert_post("sdn/links", dict(body, samples=[]), self.net, 422)
        self.assert_post("sdn/links", dict(body, samples=[producer_sample], batch_id="network-shaped"),
                         self.net, inserted=1)
        self.assert_post("sdn/links", dict(body, samples=[dict(sample, measure_port=2)]), self.net, 422)
        self.assert_post("sdn/links", body, self.net, inserted=1)
        self.assert_post("sdn/links", empty_poll, self.net, 422)
        self.assert_post("sdn/links", body, self.net, inserted=0)
        self.assert_post("sdn/links", dict(body, samples=[dict(sample, delta_bytes=200)]), self.net, 409)

    def test_routing_and_snapshot_causality(self):
        route = dict(flow_group_id="0x103", path_id="0x103_P1", confirmed_at_utc=self.at(0),
                     verification_status="VERIFIED")
        batch = dict(run_id=self.run, state_version=0, published_at_utc=self.at(0), rows=[route])
        self.assert_post("sdn/routing", dict(batch, rows=[dict(route, path_id="0x103_P3")]), self.sdn, 422)
        self.assert_post("sdn/routing", batch, self.sdn, inserted=1)
        self.assert_post("sdn/routing", batch, self.sdn, inserted=0)
        self.assert_post("network/demand", self.demand(), self.net, inserted=1)
        as_of = self.db_now()
        query = dict(cutoff=self.at(1), as_of=as_of, lookback=1)
        history_url = f"/v1/runs/{self.run}/demand-history"
        response = self.client.get(history_url, params=query, headers=self.sdn)
        self.assertEqual(response.status_code, 200, response.text)
        history = response.json()
        self.assertEqual(len(history), 1)
        self.assertEqual(self.client.get(history_url, params=dict(query, as_of=self.at(1)),
                                         headers=self.sdn).json(), [])
        body = dict(run_id=self.run, snapshot_id="s1", cutoff_bin=1, state_version=0,
                    history_cutoff_utc=self.at(1), snapshot_as_of_utc=as_of, schema_version="v1",
                    quality_status="VALID", history=history, routing_state=[route])

        def upload(payload, expected_status, expected_inserted=None, bad_hash=False):
            raw = json.dumps(canonical_snapshot(payload)).encode()
            digest = "wrong" if bad_hash else sha256(raw).hexdigest()
            response = self.client.post("/v1/sdn/snapshots", content=raw,
                                        headers=dict(self.sdn, **{"Content-Type": "application/json",
                                                                  "X-Content-SHA256": digest}))
            self.assertEqual(response.status_code, expected_status, response.text)
            if expected_inserted is not None:
                self.assertEqual(response.json(), {"inserted": expected_inserted})
            return raw

        upload(body, 422, bad_hash=True)
        upload(dict(body, history=[]), 422)
        upload(dict(body, history=[dict(history[0], requested_mbps=9)]), 409)
        raw = upload(body, 200, 1)
        upload(body, 200, 0)
        upload(dict(body, quality_status="STALE"), 409)
        next_route = dict(route, path_id="0x103_P2", confirmed_at_utc=self.at(2))
        next_batch = dict(batch, state_version=1, published_at_utc=self.at(2), rows=[next_route])
        self.assert_post("sdn/routing", dict(next_batch, state_version=2), self.sdn, 409)
        self.assert_post("sdn/routing", next_batch, self.sdn, inserted=1)
        upload(body, 200, 0)
        saved = self.client.get(f"/v1/runs/{self.run}/snapshots/s1", headers={"X-API-Key": "test-reader"})
        self.assertEqual(saved.content, raw)
        self.assertEqual(saved.headers["X-Content-SHA256"], sha256(raw).hexdigest())
        with transaction() as cursor:
            cursor.execute("SELECT valid_to_utc FROM public.routing_history WHERE run_id = %s ORDER BY state_version",
                           (self.run,))
            self.assertEqual([row["valid_to_utc"] for row in cursor],
                             [self.origin + timedelta(seconds=2), None])

    def test_sdn_bootstrap_and_measurement_history(self):
        with transaction() as cursor:
            cursor.execute("""
                UPDATE public.runs SET warmup_s=2, measurement_started_at_utc=%s
                WHERE run_id=%s
            """, (self.origin + timedelta(seconds=2), self.run))
        route = dict(flow_group_id="0x103", path_id="0x103_P1", confirmed_at_utc=self.at(0),
                     verification_status="VERIFIED")
        batch = dict(run_id=self.run, state_version=1, published_at_utc=self.at(0), rows=[route])
        self.assert_post("sdn/routing", dict(batch, state_version=2), self.sdn, 409)
        self.assert_post("sdn/routing", batch, self.sdn, inserted=1)
        event = self.demand(2)
        self.assert_post("network/demand", event, self.net, inserted=1)
        url = f"/v1/runs/{self.run}/demand-history"
        query = dict(cutoff=self.at(3), as_of=self.db_now(), lookback=1)
        response = self.client.get(url, params=dict(query, format="sdn"), headers=self.sdn)
        self.assertEqual(response.status_code, 200, response.text)
        [row] = response.json()
        self.assertEqual(row["bin_index"], 0)
        self.assertEqual(row["release_record_id"], event["event_id"])
        self.assertEqual(row["source_kind"], "RELEASED_DEMAND")
        self.assertTrue(row["valid"])
        for field in ("window_start_utc", "window_end_utc", "released_at_utc"):
            self.assertEqual(datetime.fromisoformat(row[field]), datetime.fromisoformat(event[field]))
        compact = self.client.get(url, params=query, headers=self.sdn).json()
        self.assertEqual(compact[0]["bin_index"], 2)
        self.assertEqual(self.client.get(url, params=dict(query, format="sdn", lookback=2),
                                         headers=self.sdn).status_code, 422)

    def test_mock_48_flow_pipeline(self):
        topology_file = Path(__file__).resolve().parents[2] / "configs/network/topology_bundle.json"
        raw_topology = topology_file.read_bytes()
        topology = json.loads(raw_topology)
        groups = [row["flow_group_id"] for row in topology["flow_groups"]]
        self.assertEqual(len(set(groups)), 48)
        topology_id = topology["topology_hash"]
        measurement = self.origin + timedelta(seconds=2)
        with transaction() as cursor:
            cursor.execute("INSERT INTO public.topologies VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                           (topology_id, topology["topology_version"], topology_id, raw_topology))
            hosts = [(topology_id, row["host_id"], "host", None) for row in topology["hosts"]]
            switches = [(topology_id, row["switch_id"], row["role"], row["dpid"]) for row in topology["switches"]]
            cursor.executemany("INSERT INTO public.topology_nodes VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                               hosts + switches)
            flow_fields = ("flow_group_id", "flow_cookie", "src_host", "dst_host", "src_ip", "dst_ip",
                           "src_edge", "dst_edge", "udp_port")
            cursor.executemany("""
                INSERT INTO public.flow_groups VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, [(topology_id, *(row[key] for key in flow_fields)) for row in topology["flow_groups"]])
            link_fields = ("link_id", "from_node", "to_node", "from_port", "to_port", "is_fabric",
                           "capacity_mbps", "delay_ms")
            cursor.executemany("""
                INSERT INTO public.links VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
            """, [(topology_id, *(row[key] for key in link_fields)) for row in topology["links"]])
            cursor.executemany("INSERT INTO public.candidate_paths VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                               [(topology_id, row["flow_group_id"], row["path_id"], row["path_label"])
                                for row in topology["candidate_paths"]])
            cursor.execute("""
                INSERT INTO public.runs
                  (run_id,topology_id,mode,method_id,run_started_at_utc,traffic_started_at_utc,
                   measurement_started_at_utc,warmup_s,measurement_s,schema_version,status,config_version,
                   config_hash,ml_config_hash,lookback_bins,horizon_bins,git_commit,manifest)
                VALUES (%s,%s,'live','M1',%s,%s,%s,2,360,'v1','running','v1','config','ml',2,5,'mock','{}')
            """, (self.run + "-48", topology_id, self.origin, self.origin, measurement))
        self.run += "-48"
        os.environ.update(SDN_ML_KEY="test-ml", SDN_OPTIMIZATION_KEY="test-opt")
        ml = {"X-API-Key": "test-ml"}
        opt = {"X-API-Key": "test-opt"}
        routes = [dict(flow_group_id=group, path_id=f"{group}_P1", confirmed_at_utc=self.at(0),
                       verification_status="VERIFIED") for group in groups]
        routing = dict(run_id=self.run, state_version=1, published_at_utc=self.at(0), rows=routes)
        self.assert_post("sdn/routing", routing, self.sdn, inserted=1)

        events = [dict(self.demand(bin_index), flow_group_id=group,
                       event_id=f"mock:{self.run}:{group}:{bin_index}", requested_mbps=index / 10)
                  for bin_index in (2, 3) for index, group in enumerate(groups)]
        for event in events[:-1]:
            self.assert_post("network/demand", event, self.net, inserted=1)
        self.assert_post("network/demand", events[0], self.net, inserted=0)
        self.assert_post("network/demand", dict(events[-1], flow_group_id="missing"), self.net, 422)
        self.assert_post("network/demand", dict(events[-1], event_id=events[0]["event_id"]), self.net, 409)
        traffic = [dict(flow_group_id=group, window_start_utc=self.at(2), window_end_utc=self.at(3),
                        sent_bytes=100, received_bytes=90, sent_packets=10, received_packets=9)
                   for group in groups]
        self.assert_post("network/traffic", dict(run_id=self.run, rows=traffic), self.net, inserted=48)
        links = [dict(link_id=row["link_id"], measure_switch=row["from_node"], measure_port=row["from_port"],
                      delta_bytes=100, delta_t_s=1, port_tx_mbps=0.0008, counter_status="OK")
                 for row in topology["links"] if row["from_node"].startswith(("e", "c"))]
        self.assertEqual(len(links), 24)
        self.assert_post("sdn/links", dict(run_id=self.run, batch_id="mock-fabric", sampled_at_utc=self.at(3),
                                           sample_count=len(links), samples=links), self.sdn, inserted=24)

        root = f"/v1/runs/{self.run}"
        query = dict(cutoff=self.at(4), as_of=self.db_now(), lookback=2)
        self.assert_post("network/demand", events[-1], self.net, inserted=1)
        late_history = self.client.get(root + "/demand-history", params=query, headers=self.sdn).json()
        self.assertEqual(len(late_history), 95)
        query["as_of"] = self.db_now()
        history = self.client.get(root + "/demand-history", params=query, headers=self.sdn).json()
        self.assertEqual(len(history), 96)
        snapshot = dict(run_id=self.run, snapshot_id="mock-snapshot", cutoff_bin=4, state_version=1,
                        history_cutoff_utc=query["cutoff"], snapshot_as_of_utc=query["as_of"], schema_version="v1",
                        quality_status="VALID", routing_state=routes, history=history)

        def publish_snapshot(rows, expected_status):
            raw = json.dumps(canonical_snapshot(dict(snapshot, history=rows))).encode()
            response = self.client.post("/v1/sdn/snapshots", content=raw, headers=dict(
                self.sdn, **{"Content-Type": "application/json", "X-Content-SHA256": sha256(raw).hexdigest()}))
            self.assertEqual(response.status_code, expected_status, response.text)
            return raw

        publish_snapshot(late_history, 422)
        raw = publish_snapshot(history, 200)
        download = self.client.get(root + "/snapshots/mock-snapshot", headers={"X-API-Key": "test-reader"})
        self.assertEqual(download.content, raw)
        self.assertEqual(download.headers["X-Content-SHA256"], sha256(raw).hexdigest())
        issued = self.db_now()
        predictions = [dict(run_id=self.run, flow_group_id=group, issued_at=issued, forecast_start=bin_index,
                            forecast_end=bin_index + 1, predicted_mbps=2.0, source="mock")
                       for group in groups for bin_index in range(2, 7)]
        forecast = dict(run_id=self.run, snapshot_id="mock-snapshot", forecast_batch_id="mock-forecast",
                        rows=predictions)
        forecast_url = root + "/forecasts/mock-forecast"
        self.assert_post("ml/forecasts", dict(forecast, rows=predictions[:-1]), ml, 422)
        self.assert_post("ml/forecasts", dict(forecast, rows=predictions[:-1] + predictions[:1]), ml, 422)
        self.assert_post("ml/forecasts", dict(forecast, rows=[dict(predictions[0], predicted_mbps=-1)]
                                             + predictions[1:]), ml, 422)
        self.assertEqual(self.client.get(forecast_url, headers=opt).status_code, 404)
        self.assert_post("ml/forecasts", forecast, ml, inserted=240)
        self.assert_post("ml/forecasts", dict(forecast, rows=list(reversed(predictions))), ml, inserted=0)
        self.assert_post("ml/forecasts", dict(forecast, rows=[dict(predictions[0], predicted_mbps=8)]
                                             + predictions[1:]), ml, 409)
        batch = self.client.get(forecast_url, headers=opt).json()
        self.assertEqual((batch["expected_rows"], batch["actual_rows"]), (240, 240))
        self.assertEqual(len(batch["rows"]), 240)
        changed = dict(routes[0], path_id=f"{groups[0]}_P2", confirmed_at_utc=self.at(5))
        self.assert_post("sdn/routing", dict(routing, state_version=2, published_at_utc=self.at(5),
                                             rows=[changed] + routes[1:]), self.sdn, inserted=1)
        with transaction() as cursor:
            cursor.execute("""
                SELECT (SELECT count(*) FROM public.demand_observations WHERE run_id=%(run)s) AS demand,
                       (SELECT count(*) FROM public.traffic_actual WHERE run_id=%(run)s) AS traffic,
                       (SELECT count(*) FROM public.link_samples WHERE run_id=%(run)s) AS links,
                       (SELECT count(*) FROM public.routing_state WHERE run_id=%(run)s) AS state,
                       (SELECT count(*) FROM public.routing_history WHERE run_id=%(run)s) AS history,
                       (SELECT count(*) FROM public.predictions WHERE run_id=%(run)s) AS predictions
            """, {"run": self.run})
            self.assertEqual(dict(cursor.fetchone()), dict(demand=96, traffic=48, links=24, state=96,
                                                           history=49, predictions=240))
