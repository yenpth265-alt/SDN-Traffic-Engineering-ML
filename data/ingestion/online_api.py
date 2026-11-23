import hmac
import os
from datetime import timedelta
from hashlib import sha256
from typing import Annotated, Literal

import psycopg2
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import TypeAdapter, ValidationError
from starlette.concurrency import run_in_threadpool

from . import online_store as store
from .online_models import Demand, ForecastBatch, LinkBatch, Routing, Snapshot, TrafficBatch, UTC

app = FastAPI(title="SDN Online Data Ingestion", version="1")
PREFIX = "/v1"
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


@app.exception_handler(RequestValidationError)
async def invalid_request(_request: Request, error: RequestValidationError):
    details = [{key: item[key] for key in ("loc", "msg", "type")} for item in error.errors()]
    return JSONResponse({"detail": details}, status_code=422)


def authorized(*roles):
    def check(x_api_key: Annotated[str | None, Depends(api_key_header)]):
        keys = [key for role in roles if (key := os.getenv(f"SDN_{role}_KEY"))]
        if not keys:
            raise HTTPException(503, "Data API has no key configured for this endpoint")
        if not x_api_key:
            raise HTTPException(401, "Send the API key in the X-API-Key header")
        if not any(hmac.compare_digest(key.encode(), x_api_key.encode()) for key in keys):
            raise HTTPException(401, "X-API-Key is invalid for this endpoint")
    return Depends(check)


def db_call(operation, run_id, *args):
    try:
        with store.transaction() as cursor:
            run = store.lock_run(cursor, run_id) if run_id is not None else None
            return operation(cursor, run, *args)
    except store.MissingRun as error:
        raise HTTPException(404, str(error)) from error
    except store.Conflict as error:
        raise HTTPException(409, str(error)) from error
    except ValueError as error:
        raise HTTPException(422, str(error)) from error
    except psycopg2.errors.UniqueViolation as error:
        raise HTTPException(409, "A unique producer identifier is already in use") from error
    except (psycopg2.IntegrityError, psycopg2.DataError) as error:
        raise HTTPException(422, "Record violates the PostgreSQL data contract") from error
    except (KeyError, psycopg2.OperationalError) as error:
        raise HTTPException(503, "PostgreSQL connection is unavailable") from error


def ack(operation, payload, *args):
    return {"inserted": db_call(operation, payload.run_id, payload, *args)}


@app.post(PREFIX + "/network/demand", dependencies=[authorized("NETWORK")])
def demand(payload: Demand):
    return ack(store.ingest_demand, payload)


@app.post(PREFIX + "/network/traffic", dependencies=[authorized("NETWORK")])
def traffic(payload: TrafficBatch):
    return ack(store.ingest_traffic, payload)


@app.post(PREFIX + "/sdn/links", dependencies=[authorized("SDN", "NETWORK")])
def links(payload: LinkBatch):
    return ack(store.ingest_links, payload)


@app.post(PREFIX + "/sdn/routing", dependencies=[authorized("SDN")])
def routing(payload: Routing):
    return ack(store.ingest_routing, payload)


@app.post(PREFIX + "/ml/forecasts", dependencies=[authorized("ML")])
def forecast(payload: ForecastBatch):
    return ack(store.ingest_forecast, payload)


@app.get(PREFIX + "/runs/{run_id}/forecasts/{forecast_batch_id}", dependencies=[authorized("OPTIMIZATION")])
def read_forecast(run_id: str, forecast_batch_id: str):
    batch = db_call(store.get_ready_forecast, run_id, forecast_batch_id)
    if batch is None:
        raise HTTPException(404, "Forecast batch is unavailable or incomplete")
    return batch


@app.post(PREFIX + "/sdn/snapshots", dependencies=[authorized("SDN")])
async def snapshot(request: Request, x_content_sha256: Annotated[str, Header()]):
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
        raise HTTPException(415, "Expected application/json")
    raw = bytearray()
    async for chunk in request.stream():
        if len(raw) + len(chunk) > 16 * 1024 * 1024:
            raise HTTPException(413, "Snapshot exceeds 16 MiB")
        raw.extend(chunk)
    digest = sha256(raw).hexdigest()
    if not hmac.compare_digest(digest.encode(), x_content_sha256.encode()):
        raise HTTPException(422, "X-Content-SHA256 does not match the request bytes")
    try:
        payload = Snapshot.model_validate_json(raw)
    except ValidationError as error:
        raise HTTPException(422, error.errors(include_url=False, include_context=False, include_input=False)) from error
    return await run_in_threadpool(ack, store.ingest_snapshot, payload, bytes(raw), digest)


@app.get(PREFIX + "/runs/{run_id}/snapshots/{snapshot_id}", dependencies=[authorized("SDN", "READER")])
def get_snapshot(run_id: str, snapshot_id: str):
    def lookup(cursor, run, snapshot_id):
        cursor.execute("""
            SELECT payload, payload_hash FROM public.snapshots
            WHERE run_id = %s AND snapshot_id = %s
        """, (run["run_id"], snapshot_id))
        row = cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Unknown snapshot_id for this run")
        return Response(bytes(row["payload"]), media_type="application/json",
                        headers={"X-Content-SHA256": row["payload_hash"]})

    return db_call(lookup, run_id, snapshot_id)


@app.get(PREFIX + "/runs/{run_id}/demand-history", dependencies=[authorized("SDN")])
def history(run_id: str, cutoff: str, as_of: str, lookback: Annotated[int, Query(gt=0, le=3600)],
            format: Literal["compact", "sdn"] = "compact"):
    adapter = TypeAdapter(UTC)
    try:
        cutoff_at = adapter.validate_python(cutoff)
        as_of_at = adapter.validate_python(as_of)
    except ValidationError as error:
        raise HTTPException(422, "cutoff and as_of must be RFC3339 UTC timestamps") from error
    if cutoff_at > as_of_at:
        raise HTTPException(422, "cutoff must be at or before as_of")

    def lookup(cursor, run):
        store.traffic_bin(run, cutoff_at)
        store.check_time(run, as_of_at)
        measurement_origin = None
        if format == "sdn":
            measurement_origin = run["measurement_started_at_utc"]
            if measurement_origin is None:
                raise store.Conflict("Run is missing measurement_started_at_utc")
            offset = cutoff_at - measurement_origin
            if offset.microseconds or offset < timedelta(seconds=lookback):
                raise ValueError("SDN lookback must align with and stay within measurement bins")
        return store.demand_history(cursor, run_id, cutoff_at, as_of_at, lookback,
                                    measurement_origin=measurement_origin)

    return db_call(lookup, run_id)


# Provisional management and audit endpoints; contracts are documented separately.
from .management_api import install_management
install_management(app, authorized, db_call)
