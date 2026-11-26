import copy
import json
import threading
import unittest
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from optimization.config_loader import load_optimization_config
from optimization.load_estimator import load_topology_from_configs
from optimization.planner import OptimizationController, run_planner
from optimization.result_serializer import bind_decision_batch_id
from optimization.service_client import OptimizationServiceClient

try:
    from sdn.schemas.apply_models import ApplyRequest, ApplyResponse, compute_apply_payload_hash
except ImportError:
    ApplyRequest = None
    ApplyResponse = None
    compute_apply_payload_hash = None


class TestOptimizationSDNContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.topology = load_topology_from_configs("configs/network")
        cls.config = load_optimization_config("configs/optimization/optimization.yaml")

    def _make_routing_state(
        self,
        state_version: int = 3,
        unknown_flow: str | None = None,
        omit_status_flow: str | None = None,
    ) -> dict:
        entries = []
        for fg in self.topology["F"]:
            p1 = self.topology["P"][fg][0]
            if fg == unknown_flow:
                entries.append(
                    {
                        "flow_group_id": fg,
                        "current_path": None,
                        "actual_path_id": None,
                        "verification_status": "UNKNOWN",
                        "last_successful_change_at": None,
                    }
                )
            elif fg == omit_status_flow:
                entries.append(
                    {
                        "flow_group_id": fg,
                        "current_path": p1,
                        "actual_path_id": p1,
                        "last_successful_change_at": None,
                    }
                )
            else:
                entries.append(
                    {
                        "flow_group_id": fg,
                        "current_path": p1,
                        "actual_path_id": p1,
                        "verification_status": "VERIFIED",
                        "last_successful_change_at": None,
                    }
                )
        return {
            "run_id": "run_test_01",
            "state_version": state_version,
            "routing_state_complete": True,
            "reconcile_required": False,
            "in_flight_batch_id": None,
            "data_sync_gate": {"synced": True, "pending_count": 0},
            "routing_state": entries,
        }

    def _find_congested_pair(self) -> tuple[set[str], float]:
        F = self.topology["F"]
        P = self.topology["P"]
        E = self.topology["E"]
        a_fpe = self.topology["a_fpe"]
        C_map = self.topology.get("C", {})

        best_ratio = 1.0
        best_pair: list[str] = []
        best_m_before = 0.0

        for fg1 in F:
            if len(P[fg1]) < 2:
                continue
            p1_1, p2_1 = P[fg1][0], P[fg1][1]
            for fg2 in F:
                if fg2 == fg1:
                    continue
                p1_2 = P[fg2][0]
                m_before = 0.0
                m_after = 0.0
                for e in E:
                    cap = float(C_map.get(e, 100.0))
                    if cap <= 0:
                        continue
                    u_b = (a_fpe[fg1][p1_1][e] + a_fpe[fg2][p1_2][e]) / cap
                    u_a = (a_fpe[fg1][p2_1][e] + a_fpe[fg2][p1_2][e]) / cap
                    if u_b > m_before:
                        m_before = u_b
                    if u_a > m_after:
                        m_after = u_a
                if m_before > 0:
                    ratio = m_after / m_before
                    if ratio < best_ratio:
                        best_ratio = ratio
                        best_pair = [fg1, fg2]
                        best_m_before = m_before

        high_mbps = (0.96 / best_m_before) if best_m_before > 0 else 48.0
        return set(best_pair), high_mbps

    def _make_forecast(
        self,
        now_dt: datetime,
        congested: bool = True,
        expired: bool = False,
        include_deadline: bool = True,
    ) -> dict:
        f_start = now_dt.isoformat().replace("+00:00", "Z")
        f_end = (now_dt + timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
        deadline = (
            (now_dt - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
            if expired
            else (now_dt + timedelta(seconds=4)).isoformat().replace("+00:00", "Z")
        )

        selected, high_mbps = self._find_congested_pair()
        preds = {}
        for fg in self.topology["F"]:
            mbps = high_mbps if (congested and fg in selected) else 0.0
            preds[fg] = {"predicted_mbps_array": [mbps, mbps, mbps]}

        fc = {
            "run_id": "run_test_01",
            "snapshot_id": "snap_001",
            "forecast_batch_id": "fc_001",
            "status": "READY",
            "forecast_start": f_start,
            "forecast_end": f_end,
            "topology_version": self.topology["topology_version"],
            "topology_hash": self.topology["topology_hash"],
            "snapshot_as_of_utc": f_start,
            "history_cutoff_utc": f_start,
            "predictions": preds,
        }
        if include_deadline:
            fc["apply_deadline_utc"] = deadline
        return fc

    def test_01_topology_mapping_counts(self) -> None:
        self.assertEqual(len(self.topology["F"]), 48)
        self.assertEqual(len(self.topology["E"]), 32)
        total_paths = sum(len(v) for v in self.topology["P"].values())
        self.assertEqual(total_paths, 96)

    def test_02_block_unknown_or_missing_verification_status(self) -> None:
        now_dt = datetime.now(timezone.utc)
        res_unknown = run_planner(
            self._make_forecast(now_dt),
            self._make_routing_state(unknown_flow=self.topology["F"][0]),
            [0.90, 0.91, 0.92],
            self.config,
            topology_override=self.topology,
            now_utc=now_dt,
        )
        self.assertEqual(res_unknown["status"], "STATE_UNKNOWN")

        res_missing = run_planner(
            self._make_forecast(now_dt),
            self._make_routing_state(omit_status_flow=self.topology["F"][1]),
            [0.90, 0.91, 0.92],
            self.config,
            topology_override=self.topology,
            now_utc=now_dt,
        )
        self.assertEqual(res_missing["status"], "STATE_UNKNOWN")

    def test_03_block_stale_deadline(self) -> None:
        now_dt = datetime.now(timezone.utc)
        res = run_planner(
            self._make_forecast(now_dt, expired=True),
            self._make_routing_state(),
            [0.90, 0.91, 0.92],
            self.config,
            topology_override=self.topology,
            now_utc=now_dt,
        )
        self.assertEqual(res["status"], "DATA_STALE")

    def test_04_milp_reroute_and_strict_sdn_apply_request(self) -> None:
        now_dt = datetime.now(timezone.utc)
        ctrl = OptimizationController(self.config, topology_override=self.topology)
        rs = self._make_routing_state(state_version=7)
        fc = self._make_forecast(now_dt, congested=True)

        self.assertEqual(ctrl.step(0.90, fc, rs, now_utc=now_dt)["status"], "NO_TRIGGER")
        self.assertEqual(ctrl.step(0.92, fc, rs, now_utc=now_dt)["status"], "NO_TRIGGER")

        res = ctrl.step(0.95, fc, rs, now_utc=now_dt)
        self.assertEqual(res["status"], "APPLY_REQUESTED")
        apply_dict = res["apply_request"]

        self.assertGreaterEqual(len(apply_dict["changes"]), 1)
        self.assertLessEqual(len(apply_dict["changes"]), self.config["B"])
        self.assertEqual(len(apply_dict["config_hash"]), 64)

        if ApplyRequest is not None:
            sdn_parsed = ApplyRequest.from_dict(apply_dict, strict=True)
            self.assertEqual(sdn_parsed.expected_state_version, 7)
            self.assertEqual(sdn_parsed.config_version, "1.0")
            self.assertEqual(sdn_parsed.effective_config_hash, apply_dict["config_hash"])
            self.assertEqual(sdn_parsed.effective_payload_hash, apply_dict["payload_hash"])
            if compute_apply_payload_hash is not None:
                self.assertEqual(compute_apply_payload_hash(apply_dict), apply_dict["payload_hash"])

    def test_05_decision_batch_48_rows_shared_id_and_zero_budget(self) -> None:
        now_dt = datetime.now(timezone.utc)
        rs = self._make_routing_state(state_version=8)
        fc = self._make_forecast(now_dt, congested=True, include_deadline=False)
        shared_id = "dec_data_ack_20261126_001"
        deadline_str = (now_dt + timedelta(seconds=3)).isoformat().replace("+00:00", "Z")

        res = run_planner(
            fc,
            rs,
            [0.91, 0.92, 0.94],
            self.config,
            topology_override=self.topology,
            now_utc=now_dt,
            decision_batch_id=shared_id,
            apply_deadline_utc=deadline_str,
            tick_id=100,
        )
        self.assertEqual(res["status"], "APPLY_REQUESTED")
        db = res["decision_batch"]
        req = res["apply_request"]

        self.assertEqual(db["tick_id"], 100)
        self.assertNotIn("tick_id", req)
        self.assertEqual(db["decision_batch_id"], shared_id)
        self.assertEqual(req["decision_batch_id"], shared_id)
        self.assertEqual(len(db["decision_rows"]), 48)
        self.assertEqual(db["total_flows"], 48)
        self.assertEqual(db["changed_flows_count"], len(req["changes"]))

        cfg_b0 = dict(self.config, B=0)
        res_b0 = run_planner(
            fc,
            rs,
            [0.91, 0.92, 0.94],
            cfg_b0,
            topology_override=self.topology,
            now_utc=now_dt,
            decision_batch_id="dec_b0_tick",
            tick_id=101,
        )
        self.assertEqual(res_b0["status"], "KEPT")
        self.assertIsNone(res_b0["apply_request"])
        self.assertEqual(len(res_b0["decision_batch"]["decision_rows"]), 48)
        self.assertEqual(res_b0["decision_batch"]["changed_flows_count"], 0)

    def test_06_data_sync_gate_preserves_verified(self) -> None:
        now_dt = datetime.now(timezone.utc)
        ctrl = OptimizationController(self.config, topology_override=self.topology)
        rs = self._make_routing_state(state_version=9)
        rs["data_synced"] = False
        rs["acked_routing_version"] = 8
        rs_snapshot = copy.deepcopy(rs["routing_state"])
        fc = self._make_forecast(now_dt, congested=True)

        res = ctrl.step(0.95, fc, rs, now_utc=now_dt)
        self.assertEqual(res["status"], "DATA_NOT_SYNCED")
        self.assertEqual(rs["routing_state"], rs_snapshot)

    def test_07_hold_down_only_on_verified_path_change(self) -> None:
        now_dt = datetime.now(timezone.utc)
        ctrl = OptimizationController(self.config, topology_override=self.topology)
        rs = self._make_routing_state(state_version=10)
        fc = self._make_forecast(now_dt, congested=True)

        ctrl.step(0.90, fc, rs, now_utc=now_dt)
        ctrl.step(0.92, fc, rs, now_utc=now_dt)
        res = ctrl.step(0.95, fc, rs, now_utc=now_dt)
        self.assertEqual(res["status"], "APPLY_REQUESTED")

        changed_row = next(r for r in res["decision_batch"]["decision_rows"] if r["changed"])
        kept_row = next(r for r in res["decision_batch"]["decision_rows"] if not r["changed"])

        summary = ctrl.ingest_apply_result(
            {
                "decision_batch_id": res["decision_batch"]["decision_batch_id"],
                "status": "APPLIED",
                "completed_at": now_dt.isoformat().replace("+00:00", "Z"),
                "results": [
                    {
                        "flow_group_id": changed_row["flow_group_id"],
                        "old_path_id": changed_row["old_path_id"],
                        "actual_path_id": changed_row["new_path_id"],
                        "proposed_path_id": changed_row["new_path_id"],
                        "status": "APPLIED",
                        "verification_status": "VERIFIED",
                        "rule_count": 2,
                        "applied_at": now_dt.isoformat().replace("+00:00", "Z"),
                    },
                    {
                        "flow_group_id": kept_row["flow_group_id"],
                        "old_path_id": kept_row["old_path_id"],
                        "actual_path_id": kept_row["old_path_id"],
                        "proposed_path_id": kept_row["old_path_id"],
                        "status": "KEPT",
                        "verification_status": "VERIFIED",
                        "rule_count": 0,
                        "applied_at": None,
                    },
                ],
            },
            now_utc=now_dt,
        )
        self.assertIn(changed_row["flow_group_id"], summary["locked_updated"])
        self.assertNotIn(kept_row["flow_group_id"], summary["locked_updated"])

    def test_08_integer_offsets_to_utc_apply_request_bridge(self) -> None:
        now_dt = datetime.now(timezone.utc)
        rs = self._make_routing_state(state_version=11)
        selected, high_mbps = self._find_congested_pair()

        rows = []
        for fg in self.topology["F"]:
            mbps = high_mbps if fg in selected else 0.0
            for offset in range(100, 105):
                rows.append({"flow_group_id": fg, "offset": offset, "predicted_mbps": mbps})

        fc_rows = {
            "run_id": "run_test_01",
            "tick_id": 100,
            "snapshot_id": "snap_001",
            "forecast_batch_id": "fc_int_100",
            "status": "READY",
            "forecast_start": 100,
            "forecast_end": 105,
            "measurement_started_at_utc": "2026-11-26T04:00:00Z",
            "delta_s": 1,
            "rows": rows,
        }
        res = run_planner(
            fc_rows,
            rs,
            [0.91, 0.92, 0.94],
            self.config,
            topology_override=self.topology,
            now_utc=now_dt,
        )
        self.assertEqual(res["status"], "APPLY_REQUESTED")
        self.assertEqual(res["decision_batch"]["forecast_start"], 100)
        self.assertEqual(res["decision_batch"]["forecast_end"], 105)
        self.assertEqual(res["apply_request"]["forecast_start"], "2026-11-26T04:01:40Z")
        self.assertEqual(res["apply_request"]["forecast_end"], "2026-11-26T04:01:45Z")
        if ApplyRequest is not None:
            parsed = ApplyRequest.from_dict(res["apply_request"], strict=True)
            self.assertEqual(parsed.effective_payload_hash, res["apply_request"]["payload_hash"])

        fc_float = dict(fc_rows, forecast_start=100.0, forecast_end=105.0)
        res_float = run_planner(
            fc_float,
            rs,
            [0.91, 0.92, 0.94],
            self.config,
            topology_override=self.topology,
            now_utc=now_dt,
        )
        self.assertEqual(res_float["status"], "INVALID_FORECAST_OFFSET")

    def test_09_nested_sdn_data_sync_gate_blocks_unsynced_tick(self) -> None:
        now_dt = datetime.now(timezone.utc)
        rs = self._make_routing_state(state_version=12)
        rs["data_sync_gate"] = {"synced": False, "pending_count": 1}
        fc = self._make_forecast(now_dt, congested=True)

        res = run_planner(
            fc,
            rs,
            [0.91, 0.92, 0.94],
            self.config,
            topology_override=self.topology,
            now_utc=now_dt,
        )
        self.assertEqual(res["status"], "DATA_NOT_SYNCED")

    def test_10_service_client_http_order_data_before_sdn(self) -> None:
        now_dt = datetime.now(timezone.utc)
        call_log: list[tuple[str, dict]] = []

        class MockContractHandler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args) -> None:
                return

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                call_log.append((self.path, body))

                if self.path == "/v1/optimization/decisions":
                    resp = {"accepted": True, "decision_batch_id": "dec_ack_from_data_999"}
                elif self.path == "/v1/routing/apply":
                    if ApplyRequest is not None:
                        ApplyRequest.from_dict(body, strict=True)
                    chg = body["changes"][0]
                    resp = {
                        "run_id": body["run_id"],
                        "decision_batch_id": body["decision_batch_id"],
                        "status": "APPLIED",
                        "state_version": body["expected_state_version"] + 1,
                        "completed_at": now_dt.isoformat().replace("+00:00", "Z"),
                        "results": [
                            {
                                "flow_group_id": chg["flow_group_id"],
                                "old_path_id": chg["old_path_id"],
                                "actual_path_id": chg["new_path_id"],
                                "proposed_path_id": chg["new_path_id"],
                                "status": "APPLIED",
                                "verification_status": "VERIFIED",
                                "rule_count": 2,
                                "applied_at": now_dt.isoformat().replace("+00:00", "Z"),
                            }
                        ],
                    }
                    if ApplyResponse is not None:
                        ApplyResponse.from_dict(resp, strict=True)
                else:
                    self.send_response(404)
                    self.end_headers()
                    return

                encoded = json.dumps(resp).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

        server = ThreadingHTTPServer(("127.0.0.1", 0), MockContractHandler)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        try:
            ctrl = OptimizationController(self.config, topology_override=self.topology)
            client = OptimizationServiceClient(
                controller=ctrl,
                data_base_url=f"http://127.0.0.1:{port}",
                sdn_base_url=f"http://127.0.0.1:{port}",
                api_key="test_opt_key",
            )
            rs = self._make_routing_state(state_version=15)
            fc = self._make_forecast(now_dt, congested=True)

            ctrl.step(0.90, fc, rs, now_utc=now_dt)
            ctrl.step(0.92, fc, rs, now_utc=now_dt)
            res = client.execute_tick(
                u_current=0.95,
                forecast_data=fc,
                routing_state=rs,
                now_utc=now_dt,
                decision_batch_id="dec_initial_015",
                tick_id=15,
            )

            self.assertEqual(res["status"], "APPLY_REQUESTED")
            self.assertEqual(len(call_log), 2)
            self.assertEqual(call_log[0][0], "/v1/optimization/decisions")
            self.assertEqual(len(call_log[0][1]["decision_rows"]), 48)
            self.assertEqual(call_log[1][0], "/v1/routing/apply")
            # Xác nhận ID từ Data ACK được truyền sang SDN
            self.assertEqual(call_log[1][1]["decision_batch_id"], "dec_ack_from_data_999")
            self.assertEqual(len(res["hold_down_update"]["locked_updated"]), 1)
        finally:
            server.shutdown()
            server.server_close()

    def test_11_bind_decision_batch_id_recomputes_sdn_payload_hash(self) -> None:
        now_dt = datetime.now(timezone.utc)
        rs = self._make_routing_state(state_version=16)
        fc = self._make_forecast(now_dt, congested=True)
        res = run_planner(
            fc,
            rs,
            [0.91, 0.92, 0.94],
            self.config,
            topology_override=self.topology,
            now_utc=now_dt,
            tick_id=16,
        )
        bind_decision_batch_id(res, "dec_runner_data_ack_20261126")
        self.assertEqual(res["decision_batch"]["decision_batch_id"], "dec_runner_data_ack_20261126")
        self.assertEqual(res["apply_request"]["decision_batch_id"], "dec_runner_data_ack_20261126")
        if ApplyRequest is not None:
            parsed = ApplyRequest.from_dict(res["apply_request"], strict=True)
            self.assertEqual(parsed.effective_payload_hash, res["apply_request"]["payload_hash"])


if __name__ == "__main__":
    unittest.main()