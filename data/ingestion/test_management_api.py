"""Contract checks without requiring a live PostgreSQL instance."""
import os
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from pydantic import ValidationError

from .online_api import app
from .management_api import RunCreate, ResultRow, set_status
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


if __name__ == '__main__':
    unittest.main()
