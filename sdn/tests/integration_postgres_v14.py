"""
Isolated SDN-owned Real PostgreSQL + Data API v1.4 Integration Entrypoint (`Phase 7`, Task C).

Intentionally named `integration_postgres_v14.py` (not `test_*.py`) so regular mock
`pytest sdn/tests/` runs remain 100% isolated from PostgreSQL/FastAPI dependencies.

QA / CI invocation against a disposable PostgreSQL database with migrations 001..004:
    SDN_DATABASE_URL="postgresql://sdn_qa:local_qa@127.0.0.1:5432/sdn_qa" \\
        python -m unittest -v sdn.tests.integration_postgres_v14

Validates all three approved team contracts end-to-end against real `data.ingestion.online_api:app`:
1. Contract 1 (Network -> Data -> SDN): `POST /v1/network/demand` -> `DataAPIDemandHistoryProvider`
   with PostgreSQL `available_at_utc` parity.
2. Contract 2 (SDN -> Data -> ML): `SnapshotCreator` -> `map_snapshot_to_data_v1` (`{"sdn": {...}}`
   + `X-Content-SHA256`) -> `POST /v1/sdn/snapshots` -> `GET /v1/runs/{run_id}/snapshots/{id}`
   exact byte round-trip + `public.snapshots` DB read-back.
3. Contract 3 (Optimization -> SDN -> Data): `POST /v1/optimization/decisions` (48 flows) ->
   `fetch_decision_batch_from_data_v1` -> `build_48_flow_apply_result_data_v1` ->
   `DataEventPublisher(api_mode="data_v1")` enforcing `RoutingState` ACK before `ApplyResult` ->
   PostgreSQL `public.routing_versions`, `public.decision_batches`, `public.routing_decisions`
   (`1 APPLIED + 47 KEPT`) read-back.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import uuid4

from sdn.adapters.data_api_v1_adapter import (
    fetch_decision_batch_from_data_v1,
    map_snapshot_to_data_v1,
    serialize_canonical_json_bytes,
)
from sdn.adapters.data_demand_history_provider import (
    DataAPIDemandHistoryProvider,
    DataDemandHistoryConfig,
)
from sdn.adapters.data_event_publisher import (
    DataEventPublisher,
    DataPublisherConfig,
    DeliveryStatus,
)
from sdn.controller.state_manager import StateManager
from sdn.controller.topology_registry import TopologyRegistry
from sdn.flow_management.flow_matcher import FlowMatcher
from sdn.flow_management.route_compiler import RouteCompiler
from sdn.flow_management.rule_installer import RuleInstaller
from sdn.flow_management.switch_driver import InMemorySwitchDriver
from sdn.monitoring.snapshot_creator import SnapshotCreator
from sdn.route_verification.rule_verifier import RuleVerifier
from sdn.schemas.apply_models import ApplyFlowResult, ApplyResponse
from sdn.schemas.snapshot_models import OriginRefs, SnapshotQualityStatus
from sdn.schemas.state_models import (
    BatchStatus,
    FlowDecisionStatus,
    RoutingStateEntry,
    RoutingStateSnapshot,
    VerificationStatus,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
TOPOLOGY_BUNDLE_PATH = Path(
    os.environ.get("SDN_TOPOLOGY_BUNDLE_PATH")
    or str(REPO_ROOT / "configs" / "network" / "topology_bundle.json")
)
CATALOG_DIR = Path(
    os.environ.get("SDN_CSV_CONFIG_DIR")
    or str(TOPOLOGY_BUNDLE_PATH.parent)
)
ROLES = ("RUNNER", "NETWORK", "SDN", "ML", "OPTIMIZATION", "READER")


def _fmt_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _fmt_utc_sec(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class SDNRealPostgresV14IntegrationTest(unittest.TestCase):
    """End-to-end SDN v1.4 adapter verification against real Data FastAPI + disposable PostgreSQL."""

    @classmethod
    def setUpClass(cls) -> None:
        if not os.environ.get("SDN_DATABASE_URL"):
            raise unittest.SkipTest("SDN_DATABASE_URL not set; skipping real PostgreSQL integration test.")
        try:
            from fastapi.testclient import TestClient  # type: ignore[import-not-found]
            from psycopg2 import sql  # type: ignore[import-not-found]
            from data.ingestion.online_api import app  # type: ignore[import-not-found]
            from data.ingestion.online_store import transaction  # type: ignore[import-not-found]
        except ImportError as exc:
            raise unittest.SkipTest(f"Data API / PostgreSQL runtime dependencies unavailable: {exc}") from exc

        cls._sql = sql
        cls._transaction = staticmethod(transaction)
        cls._topo = TopologyRegistry.from_bundle_file(TOPOLOGY_BUNDLE_PATH, csv_config_dir=CATALOG_DIR)
        cls._bundle_bytes = TOPOLOGY_BUNDLE_PATH.read_bytes()
        cls._bundle = json.loads(cls._bundle_bytes)
        cls._topology_id = cls._bundle["topology_hash"]

        cls._env_patch = patch.dict(
            "os.environ", {f"SDN_{role}_KEY": f"qa-{role.lower()}" for role in ROLES}
        )
        cls._env_patch.start()
        cls.addClassCleanup(cls._env_patch.stop)

        cls._client = TestClient(app)
        cls.addClassCleanup(cls._client.close)

        ready = cls._client.get("/health/ready")
        if ready.status_code != 200:
            raise RuntimeError(
                f"Data API /health/ready failed ({ready.status_code}): {ready.text}. "
                f"Ensure PostgreSQL migrations 001..004 are applied."
            )

        with cls._transaction() as cursor:
            cursor.execute(
                "INSERT INTO public.topologies VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (cls._topology_id, cls._bundle["topology_version"], cls._topology_id, cls._bundle_bytes),
            )
            nodes = [(cls._topology_id, r["host_id"], "host", None) for r in cls._bundle["hosts"]]
            nodes += [
                (cls._topology_id, r["switch_id"], r["role"], r["dpid"])
                for r in cls._bundle["switches"]
            ]
            cursor.executemany(
                "INSERT INTO public.topology_nodes VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                nodes,
            )
            for table, fields in (
                (
                    "flow_groups",
                    (
                        "flow_group_id",
                        "flow_cookie",
                        "src_host",
                        "dst_host",
                        "src_ip",
                        "dst_ip",
                        "src_edge",
                        "dst_edge",
                        "udp_port",
                    ),
                ),
                (
                    "links",
                    (
                        "link_id",
                        "from_node",
                        "to_node",
                        "from_port",
                        "to_port",
                        "is_fabric",
                        "capacity_mbps",
                        "delay_ms",
                    ),
                ),
                ("candidate_paths", ("flow_group_id", "path_id", "path_label")),
            ):
                query = sql.SQL("INSERT INTO public.{} VALUES ({}) ON CONFLICT DO NOTHING").format(
                    sql.Identifier(table),
                    sql.SQL(",").join([sql.Placeholder()] * (len(fields) + 1)),
                )
                cursor.executemany(
                    query,
                    [(cls._topology_id, *(row[f] for f in fields)) for row in cls._bundle[table]],
                )

    def _testclient_get(self, url: str, headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
        del timeout_s
        path = url.split("http://testserver", 1)[-1]
        resp = self._client.get(path, headers=headers)
        return int(resp.status_code), resp.text

    def _testclient_post(
        self, url: str, payload: dict[str, Any], headers: dict[str, str], timeout_s: float
    ) -> tuple[int, str]:
        del timeout_s
        path = url.split("http://testserver", 1)[-1]
        raw = serialize_canonical_json_bytes(payload)
        resp = self._client.post(path, content=raw, headers=headers)
        return int(resp.status_code), resp.text

    def _seed_fresh_history(self, run_id, meas_start, lookback):
        """Bulk-seed real store validation, then bounded catch-up using DB time.

        HTTP/auth demand handling is covered by OnlineIngestionTest. Avoid
        1,440 separate connections making fixture creation slower than traffic.
        """
        from data.ingestion import online_store as store
        from data.ingestion.online_models import Demand
        seeded = set()
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            with self._transaction() as cursor:
                locked = store.lock_run(cursor, run_id)
                cutoff = int((locked["now"] - meas_start).total_seconds())
                self.assertGreaterEqual(cutoff, lookback)
                for index in range(cutoff - lookback, cutoff):
                    if index in seeded:
                        continue
                    for j, fg in enumerate(self._topo.get_all_flow_groups()):
                        store.ingest_demand(cursor, locked, Demand(
                            run_id=run_id, flow_group_id=fg.flow_group_id,
                            bin_index=60 + index, event_id=f"net:{run_id}:{fg.flow_group_id}:{index}",
                            window_start_utc=_fmt_utc(meas_start + timedelta(seconds=index)),
                            window_end_utc=_fmt_utc(meas_start + timedelta(seconds=index + 1)),
                            requested_mbps=round(1.5 + j * .05 + index * .02, 4),
                            released_at_utc=_fmt_utc(meas_start + timedelta(seconds=index + 1))))
                    seeded.add(index)
            with self._transaction() as cursor:
                cursor.execute("SELECT clock_timestamp() AS now")
                as_of = cursor.fetchone()["now"]
            end = meas_start + timedelta(seconds=cutoff)
            if (as_of - end).total_seconds() < 1.5:
                return cutoff, end, as_of
        self.fail("Could not seed a fresh history within the 20-second fixture budget")

    def test_end_to_end_contracts_1_2_3_against_real_postgres(self) -> None:
        run_id = f"sdn-pg-v14-{uuid4().hex[:12]}"
        with self._transaction() as cursor:
            cursor.execute("SELECT clock_timestamp() AS now")
            db_now = cursor.fetchone()["now"]

        # Anchor origins in the past relative to DB clock_timestamp(); cutoff will be aligned
        # to the latest completed 1s bin after RoutingState v1 ACK so Snapshot is genuinely VALID.
        meas_start = (db_now - timedelta(seconds=10)).replace(microsecond=0)
        traffic_start = meas_start - timedelta(seconds=60)
        run_start = traffic_start - timedelta(seconds=5)

        reg = self._client.post(
            "/v1/runs",
            headers={"X-API-Key": "qa-runner"},
            json={
                "run_id": run_id,
                "topology_id": self._topology_id,
                "mode": "live",
                "method_id": "M0",
                "run_started_at_utc": _fmt_utc_sec(run_start),
                "traffic_started_at_utc": _fmt_utc_sec(traffic_start),
                "measurement_started_at_utc": _fmt_utc_sec(meas_start),
                "warmup_s": 60,
                "measurement_s": 360,
                "schema_version": "1.0",
                "config_version": "1.0",
                "config_hash": "sdn_config_v1_hash",
                "lookback_bins": 1,
                "git_commit": "cc202c9e6317cec448f33245ec76e27410810dc4",
                "manifest": {"suite": "sdn_pg_v14"},
            },
        )
        self.assertEqual(reg.status_code, 200, reg.text)

        # Bootstrap SDN RoutingState v1 -> publish RoutingState v1 to real Data API -> confirm ACK
        drv = InMemorySwitchDriver(self._topo)
        comp = RouteCompiler(self._topo, FlowMatcher(self._topo))
        inst = RuleInstaller(comp, drv, RuleVerifier(comp, drv))
        sm = StateManager(self._topo)
        boot_ts = _fmt_utc_sec(run_start + timedelta(seconds=1))
        evs = inst.bootstrap_initial_paths(run_id, boot_ts)
        sm.initialize_run(run_id, boot_ts, verification_evidences=evs)
        rs_v1 = sm.get_routing_state(run_id)

        with tempfile.TemporaryDirectory() as td:
            pub = DataEventPublisher(
                DataPublisherConfig(
                    endpoint_base_url="http://testserver",
                    outbox_dir=Path(td),
                    api_mode="data_v1",
                    api_key="qa-sdn",
                    max_retries=1,
                ),
                sender_fn=self._testclient_post,
            )
            r_rt1 = pub.publish_routing_state(rs_v1, deliver_now=True)
            self.assertEqual(r_rt1.status, DeliveryStatus.DELIVERED, r_rt1.last_error)

            cutoff, w_end, as_of_dt = self._seed_fresh_history(run_id, meas_start, 1)

            provider = DataAPIDemandHistoryProvider(
                DataDemandHistoryConfig(
                    endpoint_base_url="http://testserver",
                    api_key="qa-sdn",
                    timeout_s=5.0,
                ),
                http_get_fn=self._testclient_get,
            )
            fetched = provider.fetch_demand_history(
                run_id,
                history_cutoff_utc=_fmt_utc_sec(w_end),
                snapshot_as_of_utc=_fmt_utc(as_of_dt),
                lookback_L=1,
            )
            self.assertEqual(len(fetched), 48)

            # Contract 2: Build canonical Snapshot v1.4 -> assert VALID -> POST exact bytes + X-Content-SHA256 -> GET parity
            sc = SnapshotCreator(self._topo, sm, provider)
            origins = OriginRefs(
                run_started_at_utc=_fmt_utc_sec(run_start),
                traffic_started_at_utc=_fmt_utc_sec(traffic_start),
                measurement_started_at_utc=_fmt_utc_sec(meas_start),
                warmup_s=60,
                delta_s=1,
            )
            snap = provider.create_snapshot_via_data(
                sc,
                run_id=run_id,
                cutoff=cutoff,
                lookback_L=1,
                snapshot_as_of_utc=_fmt_utc(as_of_dt),
                origin_refs=origins,
            )
            self.assertEqual(snap.history_cutoff_utc, _fmt_utc_sec(w_end))
            self.assertEqual(snap.snapshot_as_of_utc, _fmt_utc(as_of_dt))
            self.assertEqual(
                snap.quality_status,
                SnapshotQualityStatus.VALID,
                f"Expected VALID snapshot, got {snap.quality_status} (reason={snap.reason}, "
                f"cutoff={snap.history_cutoff_utc}, as_of={snap.snapshot_as_of_utc})",
            )
            self.assertIsNone(snap.reason)
            _, expected_snap_bytes, expected_snap_sha = map_snapshot_to_data_v1(snap)

            r_sn = pub.publish_snapshot(snap, deliver_now=True)
            self.assertEqual(r_sn.status, DeliveryStatus.DELIVERED, r_sn.last_error)

            got = self._client.get(
                f"/v1/runs/{run_id}/snapshots/{snap.snapshot_id}",
                headers={"X-API-Key": "qa-reader"},
            )
            self.assertEqual(got.status_code, 200, got.text)
            self.assertEqual(got.content, expected_snap_bytes)
            self.assertEqual(got.headers["X-Content-SHA256"], expected_snap_sha)
            self.assertEqual(hashlib.sha256(got.content).hexdigest(), got.headers["X-Content-SHA256"])
            self.assertEqual(json.loads(got.content)["sdn"]["snapshot_id"], snap.snapshot_id)

            # Contract 3: 48-flow DecisionBatch -> ApplyResult queued BEFORE RoutingState v2 -> causal outbox order -> DB read-back
            with self._transaction() as cursor:
                cursor.execute("SELECT clock_timestamp() AS now")
                tick_dt = cursor.fetchone()["now"]

            dec_rows = [
                {
                    "flow_group_id": fg.flow_group_id,
                    "old_path_id": f"{fg.flow_group_id}_P1",
                    "proposed_path_id": "0x103_P2" if fg.flow_group_id == "0x103" else f"{fg.flow_group_id}_P1",
                }
                for fg in self._topo.get_all_flow_groups()
            ]
            batch_id = f"batch-{uuid4().hex[:8]}"
            r_dec = self._client.post(
                "/v1/optimization/decisions",
                headers={"X-API-Key": "qa-optimization"},
                json={
                    "run_id": run_id,
                    "decision_batch_id": batch_id,
                    "tick_id": f"tick-{uuid4().hex[:8]}",
                    "tick_at_utc": _fmt_utc(tick_dt),
                    "snapshot_id": snap.snapshot_id,
                    "forecast_batch_id": None,
                    "expected_state_version": rs_v1.state_version,
                    "apply_deadline_utc": _fmt_utc(tick_dt + timedelta(seconds=30)),
                    "optimizer_result": {"changed": 1},
                    "rows": dec_rows,
                },
            )
            self.assertEqual(r_dec.status_code, 200, r_dec.text)

            fetched_dec_rows = fetch_decision_batch_from_data_v1(
                "http://testserver",
                run_id,
                batch_id,
                api_key="qa-sdn",
                http_get_fn=self._testclient_get,
            )
            self.assertEqual(len(fetched_dec_rows), 48)

            with self._transaction() as cursor:
                cursor.execute("SELECT clock_timestamp() AS now")
                done_dt = cursor.fetchone()["now"]
            done_ts = _fmt_utc(done_dt)

            v2_entries = tuple(
                RoutingStateEntry(
                    run_id=e.run_id,
                    flow_group_id=e.flow_group_id,
                    path_id="0x103_P2" if e.flow_group_id == "0x103" else e.path_id,
                    state_version=rs_v1.state_version + 1,
                    confirmed_at=done_ts if e.flow_group_id == "0x103" else e.confirmed_at,
                    last_successful_change_at=done_ts if e.flow_group_id == "0x103" else e.last_successful_change_at,
                    verification_status=VerificationStatus.VERIFIED,
                    config_version=e.config_version,
                    evidence_mode=e.evidence_mode,
                    last_optimization_change_at=done_ts if e.flow_group_id == "0x103" else e.last_optimization_change_at,
                )
                for e in rs_v1.routing_state
            )
            rs_v2 = RoutingStateSnapshot(
                run_id=run_id,
                state_version=rs_v1.state_version + 1,
                config_version=rs_v1.config_version,
                topology_version=rs_v1.topology_version,
                reconcile_required=False,
                in_flight_batch_id=None,
                routing_state_complete=True,
                unknown_flow_group_ids=(),
                routing_state=v2_entries,
            )
            apply_resp = ApplyResponse(
                run_id=run_id,
                decision_batch_id=batch_id,
                status=BatchStatus.APPLIED,
                state_version=rs_v2.state_version,
                completed_at=done_ts,
                results=(
                    ApplyFlowResult(
                        flow_group_id="0x103",
                        old_path_id="0x103_P1",
                        proposed_path_id="0x103_P2",
                        actual_path_id="0x103_P2",
                        status=FlowDecisionStatus.APPLIED,
                        verification_status=VerificationStatus.VERIFIED,
                        rule_count=3,
                        reason=None,
                        applied_at=done_ts,
                    ),
                ),
                rule_count=3,
            )

            # Test outage + controller restart durability + runtime 48-flow ApplyResult via publish_apply_response_v1_runtime!
            outage = {"down": True}

            def _flaky_post(url: str, payload: dict[str, Any], headers: dict[str, str], timeout_s: float) -> tuple[int, str]:
                if outage["down"]:
                    return 503, '{"detail":"simulated pg outage"}'
                return self._testclient_post(url, payload, headers, timeout_s)

            def _flaky_lookup(rid: str, bid: str) -> list[dict[str, Any]]:
                if outage["down"]:
                    raise RuntimeError("simulated outage during decision lookup")
                return fetch_decision_batch_from_data_v1(
                    "http://testserver",
                    rid,
                    bid,
                    api_key="qa-sdn",
                    http_get_fn=self._testclient_get,
                )

            pub_outage = DataEventPublisher(
                DataPublisherConfig(
                    endpoint_base_url="http://testserver",
                    outbox_dir=Path(td),
                    api_mode="data_v1",
                    api_key="qa-sdn",
                    max_retries=1,
                ),
                sender_fn=_flaky_post,
                decision_lookup_fn=_flaky_lookup,
            )
            pub_outage.publish_routing_state(rs_v2, deliver_now=True)
            r_app = pub_outage.publish_apply_response_v1_runtime(
                apply_resp,
                post_apply_routing_state=rs_v2,
                deliver_now=True,
            )
            self.assertEqual(r_app.status, DeliveryStatus.PENDING_DELIVERY)
            self.assertEqual(r_app.attempts, 0)

            # Simulate controller restart on same outbox_dir while Data recovers!
            outage["down"] = False
            pub_recovered = DataEventPublisher(
                DataPublisherConfig(
                    endpoint_base_url="http://testserver",
                    outbox_dir=Path(td),
                    api_mode="data_v1",
                    api_key="qa-sdn",
                    max_retries=1,
                ),
                sender_fn=_flaky_post,
                decision_lookup_fn=_flaky_lookup,
            )
            gate_pre = pub_recovered.sync_gate_status(run_id, current_state_version=rs_v2.state_version, ovs_verified=True)
            self.assertFalse(gate_pre["optimization_tick_eligible"])

            flushed = pub_recovered.flush_pending()
            self.assertEqual(flushed, {"delivered": 2, "pending": 0})

            gate_post = pub_recovered.sync_gate_status(run_id, current_state_version=rs_v2.state_version, ovs_verified=True)
            self.assertTrue(gate_post["optimization_tick_eligible"])
            self.assertEqual(gate_post["data_acknowledged_state_version"], rs_v2.state_version)

            with self._transaction() as cursor:
                cursor.execute(
                    "SELECT status, count(*) AS n FROM public.routing_decisions "
                    "WHERE run_id=%s AND decision_batch_id=%s GROUP BY status",
                    (run_id, batch_id),
                )
                counts = {r["status"]: r["n"] for r in cursor.fetchall()}
                self.assertEqual(counts, {"APPLIED": 1, "KEPT": 47})

    def test_stale_snapshot_rejected_for_optimization_decision(self) -> None:
        """
        CF-28 Negative Test:
        Intentionally construct a Snapshot whose `history_cutoff_utc` is > T_stale (2.0s)
        older than `snapshot_as_of_utc` (`quality_status=STALE`, `reason=DATA_STALE`).
        Verify Data accepts and preserves exact bytes/SHA-256 on `/v1/sdn/snapshots`,
        and REJECTS (`409 Conflict`) any `/v1/optimization/decisions` referencing it.
        """
        run_id = f"sdn-pg-stale-{uuid4().hex[:12]}"
        with self._transaction() as cursor:
            cursor.execute("SELECT clock_timestamp() AS now")
            db_now = cursor.fetchone()["now"]

        meas_start = (db_now - timedelta(seconds=12)).replace(microsecond=0)
        traffic_start = meas_start - timedelta(seconds=60)
        run_start = traffic_start - timedelta(seconds=5)

        reg = self._client.post(
            "/v1/runs",
            headers={"X-API-Key": "qa-runner"},
            json={
                "run_id": run_id,
                "topology_id": self._topology_id,
                "mode": "live",
                "method_id": "M0",
                "run_started_at_utc": _fmt_utc_sec(run_start),
                "traffic_started_at_utc": _fmt_utc_sec(traffic_start),
                "measurement_started_at_utc": _fmt_utc_sec(meas_start),
                "warmup_s": 60,
                "measurement_s": 360,
                "schema_version": "1.0",
                "config_version": "1.0",
                "config_hash": "sdn_config_v1_hash",
                "lookback_bins": 1,
                "git_commit": "cc202c9e6317cec448f33245ec76e27410810dc4",
                "manifest": {"suite": "sdn_pg_v14_stale_negative"},
            },
        )
        self.assertEqual(reg.status_code, 200, reg.text)

        # Ingest bin 0 (meas_start..meas_start+1s -> ~11s old -> intentionally STALE!)
        w_start = meas_start
        w_end = meas_start + timedelta(seconds=1)
        for fg in self._topo.get_all_flow_groups():
            r_dem = self._client.post(
                "/v1/network/demand",
                headers={"X-API-Key": "qa-network"},
                json={
                    "run_id": run_id,
                    "flow_group_id": fg.flow_group_id,
                    "bin_index": 60,
                    "window_start_utc": _fmt_utc_sec(w_start),
                    "window_end_utc": _fmt_utc_sec(w_end),
                    "requested_mbps": 2.5,
                    "released_at_utc": _fmt_utc_sec(w_end),
                    "event_id": f"net:{run_id}:{fg.flow_group_id}:60",
                },
            )
            self.assertEqual(r_dem.status_code, 200, r_dem.text)

        drv = InMemorySwitchDriver(self._topo)
        comp = RouteCompiler(self._topo, FlowMatcher(self._topo))
        inst = RuleInstaller(comp, drv, RuleVerifier(comp, drv))
        sm = StateManager(self._topo)
        boot_ts = _fmt_utc_sec(run_start + timedelta(seconds=1))
        evs = inst.bootstrap_initial_paths(run_id, boot_ts)
        sm.initialize_run(run_id, boot_ts, verification_evidences=evs)
        rs_v1 = sm.get_routing_state(run_id)

        with tempfile.TemporaryDirectory() as td:
            pub = DataEventPublisher(
                DataPublisherConfig(
                    endpoint_base_url="http://testserver",
                    outbox_dir=Path(td),
                    api_mode="data_v1",
                    api_key="qa-sdn",
                    max_retries=1,
                ),
                sender_fn=self._testclient_post,
            )
            r_rt1 = pub.publish_routing_state(rs_v1, deliver_now=True)
            self.assertEqual(r_rt1.status, DeliveryStatus.DELIVERED, r_rt1.last_error)

            with self._transaction() as cursor:
                cursor.execute("SELECT clock_timestamp() AS now")
                as_of_dt = cursor.fetchone()["now"]

            provider = DataAPIDemandHistoryProvider(
                DataDemandHistoryConfig(
                    endpoint_base_url="http://testserver",
                    api_key="qa-sdn",
                    timeout_s=5.0,
                ),
                http_get_fn=self._testclient_get,
            )
            sc = SnapshotCreator(self._topo, sm, provider)
            origins = OriginRefs(
                run_started_at_utc=_fmt_utc_sec(run_start),
                traffic_started_at_utc=_fmt_utc_sec(traffic_start),
                measurement_started_at_utc=_fmt_utc_sec(meas_start),
                warmup_s=60,
                delta_s=1,
            )
            snap = provider.create_snapshot_via_data(
                sc,
                run_id=run_id,
                cutoff=1,
                lookback_L=1,
                snapshot_as_of_utc=_fmt_utc(as_of_dt),
                origin_refs=origins,
            )
            self.assertEqual(snap.quality_status, SnapshotQualityStatus.STALE)
            self.assertEqual(snap.reason, "DATA_STALE")

            _, expected_bytes, expected_sha = map_snapshot_to_data_v1(snap)
            r_sn = pub.publish_snapshot(snap, deliver_now=True)
            self.assertEqual(r_sn.status, DeliveryStatus.DELIVERED, r_sn.last_error)

            got = self._client.get(
                f"/v1/runs/{run_id}/snapshots/{snap.snapshot_id}",
                headers={"X-API-Key": "qa-reader"},
            )
            self.assertEqual(got.status_code, 200, got.text)
            self.assertEqual(got.content, expected_bytes)
            self.assertEqual(got.headers["X-Content-SHA256"], expected_sha)

            with self._transaction() as cursor:
                cursor.execute("SELECT clock_timestamp() AS now")
                tick_dt = cursor.fetchone()["now"]

            dec_rows = [
                {
                    "flow_group_id": fg.flow_group_id,
                    "old_path_id": f"{fg.flow_group_id}_P1",
                    "proposed_path_id": "0x103_P2" if fg.flow_group_id == "0x103" else f"{fg.flow_group_id}_P1",
                }
                for fg in self._topo.get_all_flow_groups()
            ]
            r_dec = self._client.post(
                "/v1/optimization/decisions",
                headers={"X-API-Key": "qa-optimization"},
                json={
                    "run_id": run_id,
                    "decision_batch_id": f"stale-batch-{uuid4().hex[:8]}",
                    "tick_id": f"tick-stale-{uuid4().hex[:8]}",
                    "tick_at_utc": _fmt_utc(tick_dt),
                    "snapshot_id": snap.snapshot_id,
                    "forecast_batch_id": None,
                    "expected_state_version": rs_v1.state_version,
                    "apply_deadline_utc": _fmt_utc(tick_dt + timedelta(seconds=30)),
                    "optimizer_result": {"changed": 1},
                    "rows": dec_rows,
                },
            )
            self.assertEqual(r_dec.status_code, 409, f"Expected 409 on STALE snapshot decision, got {r_dec.status_code}: {r_dec.text}")
            self.assertIn("VALID", r_dec.text)

    def test_cf21_ml_l30_snapshot_48x30_1440_observations(self) -> None:
        """
        CF-21 Real PostgreSQL Integration Test for ML L=30 Model:
        - lookback_L = 30, cutoff >= 30 measurement bins
        - 48 canonical flow groups x 30 bins = 1,440 genuine non-padded demand observations
        - 48 VERIFIED routing paths
        - Retrieved via real Data API `/v1/runs/{run_id}/demand`, published via `/v1/sdn/snapshots`,
          and verified for VALID status + exact canonical byte/X-Content-SHA256 parity.
        """
        run_id = f"sdn-pg-l30-{uuid4().hex[:12]}"
        with self._transaction() as cursor:
            cursor.execute("SELECT clock_timestamp() AS now")
            db_now = cursor.fetchone()["now"]

        meas_start = (db_now - timedelta(seconds=32)).replace(microsecond=0)
        traffic_start = meas_start - timedelta(seconds=60)
        run_start = traffic_start - timedelta(seconds=5)

        reg = self._client.post(
            "/v1/runs",
            headers={"X-API-Key": "qa-runner"},
            json={
                "run_id": run_id,
                "topology_id": self._topology_id,
                "mode": "live",
                "method_id": "M0",
                "run_started_at_utc": _fmt_utc_sec(run_start),
                "traffic_started_at_utc": _fmt_utc_sec(traffic_start),
                "measurement_started_at_utc": _fmt_utc_sec(meas_start),
                "warmup_s": 60,
                "measurement_s": 360,
                "schema_version": "1.0",
                "config_version": "1.0",
                "config_hash": "sdn_config_v1_hash",
                "lookback_bins": 30,
                "git_commit": "cc202c9e6317cec448f33245ec76e27410810dc4",
                "manifest": {"suite": "sdn_pg_v14_cf21_l30"},
            },
        )
        self.assertEqual(reg.status_code, 200, reg.text)

        drv = InMemorySwitchDriver(self._topo)
        comp = RouteCompiler(self._topo, FlowMatcher(self._topo))
        inst = RuleInstaller(comp, drv, RuleVerifier(comp, drv))
        sm = StateManager(self._topo)
        boot_ts = _fmt_utc_sec(run_start + timedelta(seconds=1))
        evs = inst.bootstrap_initial_paths(run_id, boot_ts)
        sm.initialize_run(run_id, boot_ts, verification_evidences=evs)
        rs_v1 = sm.get_routing_state(run_id)

        with tempfile.TemporaryDirectory() as td:
            pub = DataEventPublisher(
                DataPublisherConfig(
                    endpoint_base_url="http://testserver",
                    outbox_dir=Path(td),
                    api_mode="data_v1",
                    api_key="qa-sdn",
                    max_retries=1,
                ),
                sender_fn=self._testclient_post,
            )
            r_rt1 = pub.publish_routing_state(rs_v1, deliver_now=True)
            self.assertEqual(r_rt1.status, DeliveryStatus.DELIVERED, r_rt1.last_error)

            cutoff, w_end, as_of_dt = self._seed_fresh_history(run_id, meas_start, 30)

            self.assertGreaterEqual(cutoff, 30)
            provider = DataAPIDemandHistoryProvider(
                DataDemandHistoryConfig(
                    endpoint_base_url="http://testserver",
                    api_key="qa-sdn",
                    timeout_s=10.0,
                ),
                http_get_fn=self._testclient_get,
            )
            fetched = provider.fetch_demand_history(
                run_id,
                history_cutoff_utc=_fmt_utc_sec(w_end),
                snapshot_as_of_utc=_fmt_utc(as_of_dt),
                lookback_L=30,
            )
            self.assertEqual(len(fetched), 48 * 30)

            sc = SnapshotCreator(self._topo, sm, provider)
            origins = OriginRefs(
                run_started_at_utc=_fmt_utc_sec(run_start),
                traffic_started_at_utc=_fmt_utc_sec(traffic_start),
                measurement_started_at_utc=_fmt_utc_sec(meas_start),
                warmup_s=60,
                delta_s=1,
            )
            snap = provider.create_snapshot_via_data(
                sc,
                run_id=run_id,
                cutoff=cutoff,
                lookback_L=30,
                snapshot_as_of_utc=_fmt_utc(as_of_dt),
                origin_refs=origins,
            )
            self.assertEqual(snap.lookback_L, 30)
            self.assertGreaterEqual(snap.cutoff_offset_s, 30)
            self.assertEqual(snap.expected_bin_count, 1440)
            self.assertEqual(snap.valid_bin_count, 1440)
            self.assertEqual(len(snap.history), 1440)
            self.assertEqual(len(snap.routing_state), 48)
            self.assertTrue(all(item.valid and item.requested_mbps is not None and item.requested_mbps > 0.0 for item in snap.history))
            self.assertEqual(snap.quality_status, SnapshotQualityStatus.VALID, f"reason={snap.reason}")

            _, expected_bytes, expected_sha = map_snapshot_to_data_v1(snap)
            r_sn = pub.publish_snapshot(snap, deliver_now=True)
            self.assertEqual(r_sn.status, DeliveryStatus.DELIVERED, r_sn.last_error)

            got = self._client.get(
                f"/v1/runs/{run_id}/snapshots/{snap.snapshot_id}",
                headers={"X-API-Key": "qa-reader"},
            )
            self.assertEqual(got.status_code, 200, got.text)
            self.assertEqual(got.content, expected_bytes)
            self.assertEqual(got.headers["X-Content-SHA256"], expected_sha)


if __name__ == "__main__":
    unittest.main()
