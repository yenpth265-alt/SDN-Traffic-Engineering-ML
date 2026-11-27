import json
import threading
import unittest
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from optimization.input_validator import validate_forecast, validate_routing_state
from optimization.planner import OptimizationController, run_planner
from optimization.result_serializer import (
    bind_decision_batch_id,
    build_decision_wire_payload,
    serialize_apply_request,
)
from optimization.service_client import OptimizationServiceClient


def _build_topology(num_flows: int = 48) -> dict[str, Any]:
    flows = [f"0x{100 + i:03x}" for i in range(num_flows)]
    paths = {fg: [f"p_{fg}_a", f"p_{fg}_b"] for fg in flows}
    links = {
        "l_a": {"capacity_mbps": 500.0},
        "l_b": {"capacity_mbps": 2000.0},
    }
    path_links: dict[str, list[str]] = {}
    for fg in flows:
        path_links[f"p_{fg}_a"] = ["l_a"]
        path_links[f"p_{fg}_b"] = ["l_b"]

    a_fpe: dict[str, dict[str, dict[str, int]]] = {}
    for fg in flows:
        a_fpe[fg] = {}
        for p in paths[fg]:
            active_links = set(path_links.get(p, ()))
            a_fpe[fg][p] = {e: (1 if e in active_links else 0) for e in links}

    return {
        "topology_version": "topo_v1_4",
        "topology_hash": "hash_topo_v1_4",
        "F": flows,
        "P": paths,
        "E": list(links.keys()),
        "links": links,
        "path_links": path_links,
        "a_fpe": a_fpe,
    }


def _build_routing_state(
    topology: dict[str, Any],
    eligible: bool = True,
    include_gate: bool = True,
    state_version: int = 1,
) -> dict[str, Any]:
    routes: dict[str, Any] = {}
    for fg in topology["F"]:
        routes[fg] = {
            "flow_group_id": fg,
            "current_path": topology["P"][fg][0],
            "verification_status": "VERIFIED",
        }
    state: dict[str, Any] = {
        "run_id": "run_test_01",
        "state_version": state_version,
        "routing_state_complete": True,
        "routes": routes,
    }
    if include_gate:
        state["data_sync_gate"] = {
            "optimization_tick_eligible": eligible,
            "synced": eligible,
            "pending_count": 0 if eligible else 1,
        }
    return state


def _build_forecast(
    topology: dict[str, Any],
    h_bins: int = 3,
    high_demand_count: int = 8,
    forecast_start: int = 0,
    forecast_end: int = 1,
) -> dict[str, Any]:
    rows = []
    for idx, fg in enumerate(topology["F"]):
        demand = 120.0 if idx < high_demand_count else 20.0
        for off in range(h_bins):
            rows.append(
                {
                    "flow_group_id": fg,
                    "offset": off,
                    "predicted_mbps": demand,
                }
            )
    return {
        "run_id": "run_test_01",
        "forecast_batch_id": "fb_001",
        "snapshot_id": "snap_001",
        "snapshot_as_of_utc": "2026-11-26T14:00:00Z",
        "history_cutoff_utc": "2026-11-26T13:59:00Z",
        "measurement_started_at_utc": "2026-11-26T14:00:00Z",
        "delta_s": 1.0,
        "forecast_start": forecast_start,
        "forecast_end": forecast_end,
        "status": "READY",
        "rows": rows,
    }


def _base_config(budget_b: int = 4, t_hold: float = 30.0) -> dict[str, Any]:
    return {
        "config_version": "cfg_v1_4",
        "TH": 0.5,
        "N": 1,
        "B": budget_b,
        "T_hold": t_hold,
        "h": 0.0,
        "lambda_weight": 0.0,
        "deadline_ms": 200,
        "apply_guard_ms": 200,
        "C_e": 1000.0,
        "w_u": 1.0,
        "w_m": 0.01,
        "w_c": 0.01,
    }


class TestOptimizationSDNContract(unittest.TestCase):
    def setUp(self) -> None:
        self.topo = _build_topology(48)
        self.rs = _build_routing_state(self.topo, eligible=True)
        self.fc = _build_forecast(self.topo, h_bins=3, high_demand_count=8)
        self.now = datetime(2026, 11, 26, 14, 0, 0, tzinfo=timezone.utc)
        self.tick_at = "2026-11-26T14:00:00Z"
        self.deadline = "2026-11-26T14:00:10Z"

    def test_01_topology_mapping_counts(self) -> None:
        self.assertEqual(len(self.topo["F"]), 48)
        self.assertEqual(len(self.topo["P"]), 48)

    def test_02_block_unknown_or_missing_verification_status(self) -> None:
        bad_rs = _build_routing_state(self.topo, eligible=True)
        first_fg = self.topo["F"][0]
        del bad_rs["routes"][first_fg]["verification_status"]
        ok, code, _ = validate_routing_state(bad_rs, self.topo["F"], self.topo["P"])
        self.assertFalse(ok)
        self.assertEqual(code, "STATE_UNKNOWN")

    def test_03_block_stale_deadline(self) -> None:
        short_deadline = (self.now + timedelta(milliseconds=100)).isoformat().replace("+00:00", "Z")
        ok, code, _ = validate_forecast(
            self.fc,
            self.topo["F"],
            deadline_ms=200,
            apply_guard_ms=200,
            now_utc=self.now,
            apply_deadline_utc=short_deadline,
        )
        self.assertFalse(ok)
        self.assertEqual(code, "DATA_STALE")

    def test_04_milp_reroute_and_strict_sdn_apply_request(self) -> None:
        ctrl = OptimizationController(_base_config(budget_b=4), topology_override=self.topo)
        res = ctrl.step(
            0.92,
            self.fc,
            self.rs,
            now_utc=self.now,
            decision_batch_id="dec_004",
            apply_deadline_utc=self.deadline,
            tick_id="tick_004",
            tick_at_utc=self.tick_at,
        )
        self.assertEqual(res["status"], "APPLY_REQUESTED")
        wire = build_decision_wire_payload(res["decision_batch"])
        expected_keys = {
            "run_id",
            "decision_batch_id",
            "tick_id",
            "tick_at_utc",
            "snapshot_id",
            "forecast_batch_id",
            "expected_state_version",
            "apply_deadline_utc",
            "optimizer_result",
            "rows",
        }
        self.assertEqual(set(wire.keys()), expected_keys)
        self.assertIsInstance(wire["tick_id"], str)
        self.assertEqual(len(wire["rows"]), 48)
        for r in wire["rows"]:
            self.assertEqual(set(r.keys()), {"flow_group_id", "old_path_id", "proposed_path_id"})

        app_req = res["apply_request"]
        self.assertIsNotNone(app_req)
        self.assertLessEqual(len(app_req["changes"]), 4)
        self.assertGreater(len(app_req["changes"]), 0)
        self.assertIn("payload_hash", app_req)

    def test_05_decision_batch_48_rows_shared_id_and_zero_budget(self) -> None:
        res = run_planner(
            self.fc,
            self.rs,
            [0.92],
            _base_config(budget_b=0),
            topology_override=self.topo,
            now_utc=self.now,
            decision_batch_id="dec_005",
            apply_deadline_utc=self.deadline,
            tick_id="tick_005",
            tick_at_utc=self.tick_at,
        )
        self.assertEqual(res["status"], "KEPT")
        self.assertIsNone(res["apply_request"])
        wire = build_decision_wire_payload(res["decision_batch"])
        self.assertEqual(len(wire["rows"]), 48)
        for r in wire["rows"]:
            self.assertEqual(r["proposed_path_id"], r["old_path_id"])

    def test_06_data_sync_gate_blocks_missing_or_ineligible(self) -> None:
        no_gate_rs = _build_routing_state(self.topo, include_gate=False)
        ok1, code1, _ = validate_routing_state(no_gate_rs, self.topo["F"], self.topo["P"])
        self.assertFalse(ok1)
        self.assertEqual(code1, "DATA_NOT_SYNCED")

        false_gate_rs = _build_routing_state(self.topo, eligible=False, include_gate=True)
        ok2, code2, _ = validate_routing_state(false_gate_rs, self.topo["F"], self.topo["P"])
        self.assertFalse(ok2)
        self.assertEqual(code2, "DATA_NOT_SYNCED")

    def test_07_hold_down_only_on_verified_path_change(self) -> None:
        ctrl = OptimizationController(_base_config(budget_b=4, t_hold=60.0), topology_override=self.topo)
        first = ctrl.step(
            0.92,
            self.fc,
            self.rs,
            now_utc=self.now,
            decision_batch_id="dec_007_a",
            apply_deadline_utc=self.deadline,
            tick_id="tick_007_a",
            tick_at_utc=self.tick_at,
        )
        self.assertEqual(first["status"], "APPLY_REQUESTED")
        changed_fg = first["apply_request"]["changes"][0]["flow_group_id"]
        old_p = first["apply_request"]["changes"][0]["old_path_id"]
        new_p = first["apply_request"]["changes"][0]["new_path_id"]

        results_48 = []
        for fg in self.topo["F"]:
            cur_p = str(self.rs["routes"][fg]["current_path"])
            if fg == changed_fg:
                results_48.append(
                    {
                        "flow_group_id": fg,
                        "old_path_id": old_p,
                        "actual_path_id": new_p,
                        "changed": True,
                        "status": "APPLIED",
                        "verification_status": "VERIFIED",
                        "verified_at_utc": self.tick_at,
                    }
                )
            else:
                results_48.append(
                    {
                        "flow_group_id": fg,
                        "old_path_id": cur_p,
                        "actual_path_id": cur_p,
                        "changed": False,
                        "status": "KEPT",
                        "verification_status": "VERIFIED",
                        "verified_at_utc": self.tick_at,
                    }
                )

        apply_res = {
            "status": "APPLIED",
            "results": results_48,
        }
        pub_rs = _build_routing_state(self.topo, eligible=True)
        pub_rs["routes"][changed_fg]["current_path"] = new_p
        upd = ctrl.ingest_apply_result(apply_res, now_utc=self.now, published_routing_state=pub_rs)
        self.assertTrue(upd["complete_48"])
        self.assertIn(changed_fg, upd["locked_updated"])

    def test_08_duplicate_or_non_contiguous_bins_rejected(self) -> None:
        dup_fc = _build_forecast(self.topo, h_bins=2, high_demand_count=2)
        dup_fc["rows"].append(
            {"flow_group_id": self.topo["F"][0], "offset": 1, "predicted_mbps": 700.0}
        )
        ok_dup, _, _ = validate_forecast(dup_fc, self.topo["F"], now_utc=self.now, apply_deadline_utc=self.deadline)
        self.assertFalse(ok_dup)

        gap_fc = _build_forecast(self.topo, h_bins=2, high_demand_count=2)
        gap_fc["rows"] = [
            r for r in gap_fc["rows"] if not (r["flow_group_id"] == self.topo["F"][0] and r["offset"] == 1)
        ]
        gap_fc["rows"].append({"flow_group_id": self.topo["F"][0], "offset": 2, "predicted_mbps": 700.0})
        ok_gap, code_gap, _ = validate_forecast(
            gap_fc, self.topo["F"], now_utc=self.now, apply_deadline_utc=self.deadline
        )
        self.assertFalse(ok_gap)
        self.assertEqual(code_gap, "INVALID_FORECAST_WINDOW")

    def test_09_bind_decision_batch_id_recomputes_sdn_payload_hash(self) -> None:
        ctrl = OptimizationController(_base_config(budget_b=4), topology_override=self.topo)
        res = ctrl.step(
            0.92,
            self.fc,
            self.rs,
            now_utc=self.now,
            decision_batch_id="dec_tmp_009",
            apply_deadline_utc=self.deadline,
            tick_id="tick_009",
            tick_at_utc=self.tick_at,
        )
        self.assertEqual(res["status"], "APPLY_REQUESTED")
        old_hash = res["apply_request"]["payload_hash"]
        bind_decision_batch_id(res, "dec_canonical_009")
        self.assertEqual(res["decision_batch"]["decision_batch_id"], "dec_canonical_009")
        self.assertEqual(res["apply_request"]["decision_batch_id"], "dec_canonical_009")
        self.assertNotEqual(res["apply_request"]["payload_hash"], old_hash)

    def test_10_service_client_get_forecast_post_data_before_sdn_and_retry_id(self) -> None:
        received_order: list[str] = []
        decision_post_count = 0
        captured_decision_payloads: list[dict[str, Any]] = []
        forecast_payload = self.fc

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                return

            def do_GET(self) -> None:
                received_order.append(f"GET:{self.path}")
                body = json.dumps(forecast_payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self) -> None:
                nonlocal decision_post_count
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length).decode("utf-8")
                parsed = json.loads(raw)
                received_order.append(f"POST:{self.path}:{parsed.get('decision_batch_id')}")

                if self.path == "/v1/optimization/decisions":
                    captured_decision_payloads.append(parsed)
                    decision_post_count += 1
                    if decision_post_count == 1:
                        self.send_response(503)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", "2")
                        self.end_headers()
                        self.wfile.write(b"{}")
                        return
                    resp = json.dumps({"status": "ACCEPTED", "decision_batch_id": parsed["decision_batch_id"]}).encode(
                        "utf-8"
                    )
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(resp)))
                    self.end_headers()
                    self.wfile.write(resp)
                    return

                resp = json.dumps({"status": "APPLIED", "results": []}).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(resp)))
                self.end_headers()
                self.wfile.write(resp)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        try:
            base_url = f"http://127.0.0.1:{port}"
            ctrl = OptimizationController(_base_config(budget_b=4), topology_override=self.topo)
            client = OptimizationServiceClient(
                controller=ctrl,
                data_base_url=base_url,
                sdn_base_url=base_url,
                max_retries=2,
                retry_backoff_sec=0.01,
            )
            out = client.execute_tick(
                u_current=0.92,
                run_id="run_test_01",
                forecast_batch_id="fb_001",
                routing_state=self.rs,
                now_utc=self.now,
                decision_batch_id="dec_retry_fixed_010",
                apply_deadline_utc=self.deadline,
                tick_id=10,
                tick_at_utc=self.tick_at,
            )
        finally:
            server.shutdown()
            server.server_close()

        self.assertEqual(out["status"], "APPLY_REQUESTED")
    self.assertEqual(
            received_order,
            [
                "GET:/v1/runs/run_test_01/forecasts/fb_001",
                "POST:/v1/optimization/decisions:dec_retry_fixed_010",
                "POST:/v1/optimization/decisions:dec_retry_fixed_010",
                "POST:/apply:dec_retry_fixed_010",
            ],
        )
        self.assertEqual(len(captured_decision_payloads), 2)
        self.assertEqual(captured_decision_payloads[0], captured_decision_payloads[1])
        self.assertEqual(
            set(captured_decision_payloads[0].keys()),
            {
                "run_id",
                "decision_batch_id",
                "tick_id",
                "tick_at_utc",
                "snapshot_id",
                "forecast_batch_id",
                "expected_state_version",
                "apply_deadline_utc",
                "optimizer_result",
                "rows",
            },
        )
        self.assertIsInstance(captured_decision_payloads[0]["tick_id"], str)

    def test_11_over_budget_changes_not_silently_sliced(self) -> None:
        fake_rows = [
            {
                "flow_group_id": fg,
                "old_path_id": "p_old",
                "proposed_path_id": "p_new",
                "new_path_id": "p_new",
                "changed": True,
            }
            for fg in self.topo["F"][:3]
        ]
        with self.assertRaises(ValueError):
            serialize_apply_request(
                decision_batch_id="dec_over_b",
                decision_rows=fake_rows,
                forecast_data=self.fc,
                state_version=1,
                config=_base_config(budget_b=2),
                apply_deadline_utc=self.deadline,
                topology=self.topo,
                now_utc=self.now,
            )


if __name__ == "__main__":
    unittest.main()
