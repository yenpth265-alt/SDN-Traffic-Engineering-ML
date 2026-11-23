"""Provisional run management and decision audit API. Does not control switches."""
import json
import psycopg2
from datetime import timedelta
from hashlib import sha256
from typing import Annotated, Literal

from fastapi import HTTPException
from psycopg2.extras import Json
from pydantic import Field, model_validator

from . import online_store as store
from .online_models import Message, ID, UTC, Count


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


class RunCreate(Message):
    run_id: ID
    topology_id: ID
    schedule_id: ID | None = None
    mode: Literal["calibration", "benchmark", "live", "oracle"]
    method_id: Literal["M0", "M1", "M2", "M3", "M4"]
    run_started_at_utc: UTC
    traffic_started_at_utc: UTC
    measurement_started_at_utc: UTC
    warmup_s: Annotated[int, Field(strict=True, ge=0)]
    measurement_s: Annotated[int, Field(strict=True, gt=0)]
    schema_version: ID
    config_version: ID
    config_hash: ID
    ml_config_hash: ID | None = None
    lookback_bins: Annotated[int, Field(strict=True, gt=0, le=3600)] | None = None
    horizon_bins: Annotated[int, Field(strict=True, gt=0)] | None = None
    model_version: ID | None = None
    calibration_version: ID | None = None
    git_commit: ID
    manifest: dict

    @model_validator(mode="after")
    def context(self):
        if self.run_started_at_utc > self.traffic_started_at_utc:
            raise ValueError("Run must start before traffic")
        if self.measurement_started_at_utc != self.traffic_started_at_utc + timedelta(seconds=self.warmup_s):
            raise ValueError("Measurement origin must equal traffic origin plus warmup_s")
        if (self.method_id == "M4") != (self.mode == "oracle"):
            raise ValueError("M4 is reserved for oracle")
        if self.mode in ("benchmark", "oracle") and not self.schedule_id:
            raise ValueError("This mode requires schedule_id")
        if self.method_id in ("M1", "M2", "M3") and not all((self.ml_config_hash, self.lookback_bins, self.horizon_bins)):
            raise ValueError("ML methods require frozen ML configuration, L and H")
        return self


class RunStatus(Message):
    status: Literal["ok", "failed", "discarded"]


class DecisionRow(Message):
    flow_group_id: ID
    old_path_id: ID
    proposed_path_id: ID


class Decision(Message):
    run_id: ID
    decision_batch_id: ID
    tick_id: ID
    tick_at_utc: UTC
    snapshot_id: ID
    forecast_batch_id: ID | None = None
    expected_state_version: Count
    apply_deadline_utc: UTC
    optimizer_result: dict
    rows: Annotated[list[DecisionRow], Field(min_length=1, max_length=4096)]


class ResultRow(Message):
    flow_group_id: ID
    actual_path_id: ID | None
    status: Literal["APPLIED", "KEPT", "FAILED", "ROLLED_BACK", "UNKNOWN"]
    verified_at_utc: UTC | None
    reason: ID | None = None

    @model_validator(mode="after")
    def verification(self):
        if ((self.actual_path_id is None) != (self.verified_at_utc is None)
                or (self.status == "UNKNOWN") != (self.actual_path_id is None)):
            raise ValueError("Known results require actual path and verification time; UNKNOWN requires nulls")
        return self


class ApplyResult(Message):
    run_id: ID
    decision_batch_id: ID
    response_status: Literal["APPLIED", "REJECTED", "FAILED", "ROLLED_BACK", "PARTIAL", "INCONSISTENT"]
    response_state_version: Count
    completed_at_utc: UTC
    rows: Annotated[list[ResultRow], Field(min_length=1, max_length=4096)]


class Event(Message):
    event_id: ID
    event_at_utc: UTC
    event_type: ID
    detail: dict


def create_run(cursor, _run, payload):
    row = payload.model_dump()
    row.update(status="running", manifest=encoded(payload.manifest))
    # Serialize identical concurrent registrations, including before the row exists.
    cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (payload.run_id,))
    return {"inserted": store.insert_once(cursor, "runs", row, ("run_id",))}


def set_status(cursor, run, payload):
    if run["status"] == payload.status:
        return {"updated": 0}
    if run["status"] != "running":
        raise store.Conflict("Terminal run status cannot be changed")
    cursor.execute("UPDATE public.runs SET status=%s WHERE run_id=%s", (payload.status, run["run_id"]))
    return {"updated": 1}


def record_decision(cursor, run, payload):
    content = payload.model_dump(mode="json")
    content["rows"] = sorted(content["rows"], key=lambda r: r["flow_group_id"])
    raw = encoded(content)
    cursor.execute("SELECT request_payload FROM public.decision_batches WHERE run_id=%s AND decision_batch_id=%s",
                   (payload.run_id, payload.decision_batch_id))
    existing = cursor.fetchone()
    if existing:
        if existing["request_payload"] is None or bytes(existing["request_payload"]) != raw:
            raise store.Conflict("Decision ID already has different content")
        return {"inserted": 0}
    if run["status"] != "running":
        raise store.Conflict("New decisions require a running run")
    store.check_time(run, payload.tick_at_utc)
    if payload.apply_deadline_utc <= run["now"]:
        raise ValueError("Decision deadline has expired")
    cursor.execute("SELECT * FROM public.snapshots WHERE run_id=%s AND snapshot_id=%s",
                   (payload.run_id, payload.snapshot_id))
    snapshot = cursor.fetchone()
    if (not snapshot or snapshot["quality_status"] != "VALID"
            or snapshot["state_version"] != payload.expected_state_version
            or snapshot["available_at_utc"] > payload.tick_at_utc):
        raise store.Conflict("Decision requires an available VALID snapshot with matching version")
    cursor.execute("SELECT max(state_version) AS version FROM public.routing_versions WHERE run_id=%s", (payload.run_id,))
    if cursor.fetchone()["version"] != payload.expected_state_version:
        raise store.Conflict("Routing version has changed")
    if run["method_id"] in ("M1", "M2", "M3") and payload.forecast_batch_id is None:
        raise ValueError("ML decision requires forecast_batch_id")
    if payload.forecast_batch_id:
        batch = store.get_ready_forecast(cursor, run, payload.forecast_batch_id)
        if (not batch or batch["snapshot_id"] != payload.snapshot_id
                or batch["committed_at_utc"] > payload.tick_at_utc):
            raise store.Conflict("Forecast must be complete and available for this snapshot")
    routes = store.route_rows(cursor, payload.run_id, payload.expected_state_version)
    ids = [r.flow_group_id for r in payload.rows]
    if len(set(ids)) != len(ids) or set(ids) != store.flow_ids(cursor, run):
        raise ValueError("Decision must cover every flow exactly once")
    for row in payload.rows:
        route = routes.get(row.flow_group_id)
        if not route or route["verification_status"] != "VERIFIED" or route["path_id"] != row.old_path_id:
            raise store.Conflict("old_path_id differs from verified snapshot routing")
    store.insert_once(cursor, "control_ticks", dict(run_id=payload.run_id, tick_id=payload.tick_id,
        tick_at_utc=payload.tick_at_utc, snapshot_id=payload.snapshot_id,
        forecast_batch_id=payload.forecast_batch_id, status="DECIDED", reason=None), ("run_id", "tick_id"))
    store.insert_once(cursor, "decision_batches", dict(run_id=payload.run_id,
        decision_batch_id=payload.decision_batch_id, tick_id=payload.tick_id, snapshot_id=payload.snapshot_id,
        expected_state_version=payload.expected_state_version, optimizer_result=encoded(payload.optimizer_result),
        request_payload=raw, request_hash=sha256(raw).hexdigest(), apply_deadline_utc=payload.apply_deadline_utc),
        ("run_id", "decision_batch_id"))
    for row in payload.rows:
        store.insert_once(cursor, "routing_decisions", dict(row.model_dump(), run_id=payload.run_id,
            topology_id=run["topology_id"], decision_batch_id=payload.decision_batch_id),
            ("run_id", "decision_batch_id", "flow_group_id"))
    return {"inserted": 1}


def record_result(cursor, run, payload):
    content = payload.model_dump(mode="json")
    content["rows"] = sorted(content["rows"], key=lambda r: r["flow_group_id"])
    raw = encoded(content)
    cursor.execute("SELECT * FROM public.decision_batches WHERE run_id=%s AND decision_batch_id=%s",
                   (payload.run_id, payload.decision_batch_id))
    decision = cursor.fetchone()
    if not decision:
        raise HTTPException(404, "Unknown decision_batch_id")
    if decision["response_payload"] is not None:
        if bytes(decision["response_payload"]) != raw:
            raise store.Conflict("Final result cannot be replaced")
        return {"updated": 0}
    store.check_time(run, payload.completed_at_utc)
    cursor.execute("SELECT * FROM public.control_ticks WHERE run_id=%s AND tick_id=%s", (payload.run_id, decision["tick_id"]))
    if payload.completed_at_utc < cursor.fetchone()["tick_at_utc"]:
        raise ValueError("Completion predates decision")
    cursor.execute("SELECT * FROM public.routing_decisions WHERE run_id=%s AND decision_batch_id=%s",
                   (payload.run_id, payload.decision_batch_id))
    expected = {r["flow_group_id"]: r for r in cursor}
    if len({r.flow_group_id for r in payload.rows}) != len(payload.rows) or {r.flow_group_id for r in payload.rows} != expected.keys():
        raise ValueError("Result must cover exactly the decision flows")
    routes = store.route_rows(cursor, payload.run_id, payload.response_state_version)
    if not routes:
        raise store.Conflict("Publish routing version before submitting apply result")
    for row in payload.rows:
        proposed = expected[row.flow_group_id]
        if row.verified_at_utc is not None and not run["run_started_at_utc"] <= row.verified_at_utc <= payload.completed_at_utc:
            raise ValueError("Verification time is outside run/completion bounds")
        route = routes.get(row.flow_group_id, {})
        if row.actual_path_id != route.get("path_id") or row.verified_at_utc != route.get("confirmed_at_utc"):
            raise store.Conflict("Result differs from published routing version")
        if row.status == "APPLIED" and row.actual_path_id != proposed["proposed_path_id"]:
            raise ValueError("APPLIED requires the proposed path")
        if row.status in ("KEPT", "ROLLED_BACK") and row.actual_path_id != proposed["old_path_id"]:
            raise ValueError("KEPT/ROLLED_BACK requires the old path")
        if payload.response_status == "APPLIED" and row.status not in ("APPLIED", "KEPT"):
            raise ValueError("APPLIED batch cannot contain failed or unknown rows")
        if payload.response_status == "APPLIED" and row.actual_path_id != proposed["proposed_path_id"]:
            raise ValueError("APPLIED batch requires every proposed path to be realized")
        cursor.execute("""UPDATE public.routing_decisions SET actual_path_id=%s, status=%s,
            verified_at_utc=%s, reason=%s WHERE run_id=%s AND decision_batch_id=%s AND flow_group_id=%s""",
            (row.actual_path_id, row.status, row.verified_at_utc, row.reason,
             payload.run_id, payload.decision_batch_id, row.flow_group_id))
    cursor.execute("""UPDATE public.decision_batches SET response_payload=%s, response_status=%s,
        response_state_version=%s, completed_at_utc=%s WHERE run_id=%s AND decision_batch_id=%s""",
        (raw, payload.response_status, payload.response_state_version, payload.completed_at_utc,
         payload.run_id, payload.decision_batch_id))
    return {"updated": 1}


def install_management(app, authorized, db_call):
    @app.get("/health/live")
    def live():
        return {"status": "alive"}

    @app.get("/health/ready")
    def ready():
        def check(cursor, _run):
            cursor.execute("SELECT version FROM public.schema_migrations")
            if not {1, 2, 3}.issubset({r["version"] for r in cursor}):
                raise HTTPException(503, "Apply schema revisions 1, 2 and 3")
            return {"status": "ready"}
        try:
            return db_call(check, None)
        except (HTTPException, psycopg2.Error) as error:
            raise HTTPException(503, "Database/schema is not ready") from error

    @app.post("/v1/runs", dependencies=[authorized("RUNNER")])
    def register(payload: RunCreate):
        return db_call(create_run, None, payload)

    @app.get("/v1/runs/{run_id}", dependencies=[authorized("RUNNER", "READER", "SDN", "ML", "OPTIMIZATION")])
    def read_run(run_id: str):
        def read(cursor, run):
            result = {k: v for k, v in run.items() if k not in ("now", "manifest")}
            result["manifest"] = json.loads(bytes(run["manifest"]))
            return result
        return db_call(read, run_id)

    @app.patch("/v1/runs/{run_id}/status", dependencies=[authorized("RUNNER")])
    def status(run_id: str, payload: RunStatus):
        return db_call(set_status, run_id, payload)

    @app.get("/v1/runs/{run_id}/topology", dependencies=[authorized("RUNNER", "NETWORK", "SDN", "ML", "OPTIMIZATION", "READER")])
    def topology(run_id: str):
        def read(cursor, run):
            cursor.execute("SELECT bundle FROM public.topologies WHERE topology_id=%s", (run["topology_id"],))
            return json.loads(bytes(cursor.fetchone()["bundle"]))
        return db_call(read, run_id)

    @app.post("/v1/optimization/decisions", dependencies=[authorized("OPTIMIZATION")])
    def decisions(payload: Decision):
        return db_call(record_decision, payload.run_id, payload)

    @app.get("/v1/runs/{run_id}/decisions/{decision_batch_id}", dependencies=[authorized("SDN", "OPTIMIZATION", "RUNNER", "READER")])
    def decision(run_id: str, decision_batch_id: str):
        def read(cursor, run):
            cursor.execute("SELECT request_payload,response_payload FROM public.decision_batches WHERE run_id=%s AND decision_batch_id=%s", (run_id, decision_batch_id))
            row = cursor.fetchone()
            if not row or row["request_payload"] is None:
                raise HTTPException(404, "Unknown decision_batch_id")
            return {k: json.loads(bytes(v)) if v is not None else None for k, v in row.items()}
        return db_call(read, run_id)

    @app.post("/v1/sdn/apply-results", dependencies=[authorized("SDN")])
    def results(payload: ApplyResult):
        return db_call(record_result, payload.run_id, payload)

    # Separate paths bind event ownership to the authenticated role, not a client field.
    for component in ("network", "sdn", "ml", "optimization", "runner"):
        def factory(component):
            def event(run_id: str, payload: Event):
                def write(cursor, run):
                    store.check_time(run, payload.event_at_utc)
                    if payload.event_at_utc < run["run_started_at_utc"]:
                        raise ValueError("Event predates run")
                    row = dict(payload.model_dump(), run_id=run_id, component=component,
                               available_at_utc=run["now"], detail=Json(payload.detail, dumps=lambda x: encoded(x).decode()))
                    # Compare JSON dictionaries on replay rather than adapter objects.
                    cursor.execute("SELECT * FROM public.events WHERE event_id=%s", (payload.event_id,))
                    old = cursor.fetchone()
                    if old:
                        expected = dict(row, detail=payload.detail)
                        if any(old[k] != v for k, v in expected.items() if k != "available_at_utc"):
                            raise store.Conflict("Event ID already has different content")
                        return {"inserted": 0}
                    return {"inserted": store.insert_once(cursor, "events", row, ("event_id",))}
                return db_call(write, run_id)
            return event
        app.post(f"/v1/runs/{{run_id}}/events/{component}", dependencies=[authorized(component.upper())],
                 name=f"event_{component}")(factory(component))
