"""Contract checks without requiring a live PostgreSQL instance."""
import json
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient
from pydantic import ValidationError

from .online_api import app
from .management_api import Decision, RunCreate, ResultRow, record_decision, set_status
from .online_store import Conflict


class ManagementContractTest(unittest.TestCase):
    def test_health_and_routes(self):
        with TestClient(app) as client:
            self.assertEqual(client.get('/health/live').json(), {'status': 'alive'})
            paths = client.get('/openapi.json').json()['paths']
            for path in ('/v1/runs', '/v1/optimization/decisions', '/v1/sdn/apply-results',
                         '/v1/runs/{run_id}/topology', '/v1/runs/{run_id}/events/network'):
                self.assertIn(path, paths)

    def test_writer_role_is_enforced(self):
        with patch.dict(os.environ, {'SDN_RUNNER_KEY': 'runner', 'SDN_NETWORK_KEY': 'network'}):
            with TestClient(app) as client:
                self.assertEqual(client.post('/v1/runs', json={}, headers={'X-API-Key': 'network'}).status_code, 401)
                self.assertEqual(client.post('/v1/runs', json={}, headers={'X-API-Key': 'runner'}).status_code, 422)

    def test_origins_and_ml_context(self):
        run = dict(run_id='r', topology_id='t', mode='live', method_id='M0',
            run_started_at_utc='2026-01-01T00:00:00Z', traffic_started_at_utc='2026-01-01T00:00:01Z',
            measurement_started_at_utc='2026-01-01T00:01:01Z', warmup_s=60, measurement_s=360,
            schema_version='v1', config_version='v1', config_hash='hash', git_commit='abc', manifest={})
        RunCreate(**run)
        for update in ({'warmup_s': 59}, {'method_id': 'M1'}, {'mode': 'oracle'}):
            with self.assertRaises(ValidationError):
                RunCreate(**dict(run, **update))

    def test_unknown_result_cannot_claim_verified_path(self):
        ResultRow(flow_group_id='f', status='UNKNOWN', actual_path_id=None, verified_at_utc=None)
        with self.assertRaises(ValidationError):
            ResultRow(flow_group_id='f', status='APPLIED', actual_path_id=None, verified_at_utc=None)

    def test_terminal_run_cannot_be_reopened_or_rewritten(self):
        from .management_api import RunStatus
        self.assertEqual(set_status(None, {'status': 'ok'}, RunStatus(status='ok')), {'updated': 0})
        with self.assertRaises(Conflict):
            set_status(None, {'status': 'ok'}, RunStatus(status='failed'))

    def test_noop_decision_completes_and_replays_without_apply_result(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        verified_at = now - timedelta(seconds=5)
        flows = [f"flow-{i:02d}" for i in range(48)]
        routes = {
            flow: {"verification_status": "VERIFIED", "path_id": "p1",
                   "confirmed_at_utc": verified_at}
            for flow in flows
        }
        proposal = Decision(
            run_id="run", decision_batch_id="noop", tick_id="tick",
            tick_at_utc=now.isoformat(), snapshot_id="snapshot", expected_state_version=7,
            apply_deadline_utc=(now + timedelta(seconds=1)).isoformat(), optimizer_result={},
            rows=[dict(flow_group_id=flow, old_path_id="p1", proposed_path_id="p1")
                  for flow in reversed(flows)],
        )
        run = {"status": "running", "now": now, "topology_id": "topology",
               "run_started_at_utc": verified_at, "method_id": "M0"}
        cursor = Mock()
        cursor.fetchone.side_effect = [
            None, {"tick_at_utc": None},
            {"quality_status": "VALID", "state_version": 7, "available_at_utc": now},
            {"version": 7},
        ]
        with (patch("data.ingestion.management_api.store.route_rows", return_value=routes),
              patch("data.ingestion.management_api.store.flow_ids", return_value=set(flows)),
              patch("data.ingestion.management_api.store.insert_once") as insert):
            self.assertEqual(record_decision(cursor, run, proposal), {"inserted": 1})
            writes = [(call.args[1], call.args[2]) for call in insert.call_args_list]
            self.assertEqual([table for table, row in writes],
                             ["control_ticks", "decision_batches"] + ["routing_decisions"] * 48)
            batch = writes[1][1]
            self.assertEqual(batch["response_state_version"], 7)
            response = json.loads(batch["response_payload"])
            self.assertEqual(response["response_state_version"], 7)
            self.assertEqual([row["flow_group_id"] for row in response["rows"]], flows)
            for _, row in writes[2:]:
                self.assertEqual((row["status"], row["actual_path_id"], row["verified_at_utc"]),
                                 ("KEPT", "p1", verified_at))

            cursor.fetchone.side_effect = None
            cursor.fetchone.return_value = {"request_payload": batch["request_payload"]}
            insert.reset_mock()
            self.assertEqual(record_decision(cursor, run, proposal), {"inserted": 0})
            insert.assert_not_called()


if __name__ == '__main__':
    unittest.main()
