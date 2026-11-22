import unittest
from datetime import datetime, timedelta, timezone

from optimization.load_estimator import load_topology_from_configs
from optimization.planner import OptimizationController, run_planner

try:
    from sdn.schemas.apply_models import ApplyRequest
except ImportError:
    ApplyRequest = None


class TestOptimizationSDNContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.topology = load_topology_from_configs("configs/network")
        cls.config = {
            "config_version": "1.0",
            "topology_version": "1.0",
            "TH": 0.85,
            "N": 3,
            "h": 0.05,
            "T_hold": 10.0,
            "B": 4,
            "lambda_weight": 0.01,
            "C_e": 100.0,
            "deadline_ms": 200,
            "apply_guard_ms": 200,
        }

    def _make_routing_state(self, state_version: int = 3, unknown_flow: str | None = None) -> dict:
        entries = []
        for fg in self.topology["F"]:
            p1 = self.topology["P"][fg][0]
            if fg == unknown_flow:
                entries.append(
                    {
                        "flow_group_id": fg,
                        "current_path": None,
                        "actual_path_id": None,
                        "status": "UNKNOWN",
                        "last_successful_change_at": None,
                    }
                )
            else:
                entries.append(
                    {
                        "flow_group_id": fg,
                        "current_path": p1,
                        "actual_path_id": p1,
                        "status": "VERIFIED",
                        "last_successful_change_at": None,
                    }
                )
        return {
            "run_id": "run_test_01",
            "state_version": state_version,
            "routing_state_complete": True,
            "reconcile_required": False,
            "in_flight_batch_id": None,
            "routing_state": entries,
        }

    def _make_forecast(self, now_dt: datetime, congested: bool = True, expired: bool = False) -> dict:
        f_start = now_dt.isoformat().replace("+00:00", "Z")
        f_end = (now_dt + timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
        deadline = (
            (now_dt - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
            if expired
            else (now_dt + timedelta(seconds=4)).isoformat().replace("+00:00", "Z")
        )
        preds = {}
        for idx, fg in enumerate(self.topology["F"]):
            mbps = 22.0 if (congested and idx < 6) else 0.5
            preds[fg] = {"predicted_mbps_array": [mbps, mbps, mbps]}

        return {
            "run_id": "run_test_01",
            "snapshot_id": "snap_001",
            "forecast_batch_id": "fc_001",
            "forecast_start": f_start,
            "forecast_end": f_end,
            "apply_deadline_utc": deadline,
            "topology_version": self.topology["topology_version"],
            "topology_hash": self.topology["topology_hash"],
            "config_hash": None,
            "snapshot_as_of_utc": f_start,
            "history_cutoff_utc": f_start,
            "predictions": preds,
        }

    def test_01_topology_mapping_counts(self) -> None:
        self.assertEqual(len(self.topology["F"]), 48)
        self.assertEqual(len(self.topology["E"]), 32)
        total_paths = sum(len(v) for v in self.topology["P"].values())
        self.assertEqual(total_paths, 96)

    def test_02_block_unknown_state(self) -> None:
        now_dt = datetime.now(timezone.utc)
        res = run_planner(
            self._make_forecast(now_dt),
            self._make_routing_state(unknown_flow=self.topology["F"][0]),
            [0.90, 0.91, 0.92],
            self.config,
            topology_override=self.topology,
            now_utc=now_dt,
        )
        self.assertEqual(res["status"], "STATE_UNKNOWN")

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

        if ApplyRequest is not None:
            sdn_parsed = ApplyRequest.from_dict(apply_dict, strict=True)
            self.assertEqual(sdn_parsed.expected_state_version, 7)
            self.assertEqual(sdn_parsed.config_version, "1.0")
            self.assertEqual(sdn_parsed.effective_payload_hash, apply_dict["payload_hash"])
        else:
            required_keys = {
                "run_id",
                "decision_batch_id",
                "snapshot_id",
                "expected_state_version",
                "config_version",
                "forecast_start",
                "forecast_end",
                "apply_deadline_utc",
                "changes",
                "payload_hash",
            }
            self.assertTrue(required_keys.issubset(apply_dict.keys()))
            self.assertEqual(apply_dict["expected_state_version"], 7)
            self.assertEqual(apply_dict["config_version"], "1.0")


if __name__ == "__main__":
    unittest.main()