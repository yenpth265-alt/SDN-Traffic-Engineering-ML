from contextlib import closing, contextmanager
from datetime import timedelta
from hashlib import sha256
import json
import os

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor


class Conflict(ValueError):
    pass


class MissingRun(LookupError):
    pass


@contextmanager
def transaction():
    with closing(psycopg2.connect(os.environ["SDN_DATABASE_URL"], connect_timeout=5)) as conn:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SET LOCAL TIME ZONE 'UTC'")
            yield cursor


def lock_run(cursor, run_id):
    # Every writer takes the same run lock before assigning availability or reading state.
    cursor.execute("SELECT * FROM public.runs WHERE run_id = %s FOR NO KEY UPDATE", (run_id,))
    run = cursor.fetchone()
    if run is None:
        raise MissingRun("Unknown run_id")
    cursor.execute("SELECT clock_timestamp() AS now")
    run["now"] = cursor.fetchone()["now"]
    return run


def insert_once(cursor, table, row, key, generated=(), mutable=()):
    query = sql.SQL("INSERT INTO public.{} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING")
    cursor.execute(query.format(sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, row)),
                                sql.SQL(", ").join(sql.Placeholder() for _ in row),
                                sql.SQL(", ").join(map(sql.Identifier, key))), list(row.values()))
    if cursor.rowcount:
        return 1
    query = sql.SQL("SELECT * FROM public.{} WHERE {}").format(
        sql.Identifier(table),
        sql.SQL(" AND ").join(sql.SQL("{} = %s").format(sql.Identifier(col)) for col in key),
    )
    cursor.execute(query, [row[col] for col in key])
    existing = cursor.fetchone()
    ignored = set(generated) | set(mutable)

    def same_value(existing_value, requested_value):
        byte_types = (bytes, bytearray, memoryview)
        if isinstance(existing_value, byte_types) and isinstance(requested_value, byte_types):
            return bytes(existing_value) == bytes(requested_value)
        return existing_value == requested_value

    if any(not same_value(existing[col], value)
           for col, value in row.items() if col not in ignored):
        raise Conflict(f"Existing {table} record has different content")
    return 0


def traffic_bin(run, timestamp):
    origin = run["traffic_started_at_utc"]
    if origin is None:
        raise Conflict("Run is missing traffic_started_at_utc")
    offset = timestamp - origin
    if offset < timedelta() or offset.microseconds:
        raise ValueError("Timestamp is not aligned to the traffic origin")
    return offset // timedelta(seconds=1)


def check_time(run, timestamp):
    if timestamp > run["now"]:
        raise ValueError("Producer timestamp is ahead of the database clock")


def ingest_demand(cursor, run, event):
    index = traffic_bin(run, event.window_start_utc)
    if index != event.bin_index or index >= run["warmup_s"] + run["measurement_s"]:
        raise ValueError("Demand bin does not match the run traffic window")
    check_time(run, event.released_at_utc)
    return insert_once(
        cursor, "demand_observations",
        dict(event.model_dump(), topology_id=run["topology_id"], available_at_utc=run["now"]),
        ("run_id", "flow_group_id", "bin_index"), ("available_at_utc",),
    )


def ingest_traffic(cursor, run, batch):
    inserted = 0
    for event in batch.rows:
        traffic_bin(run, event.window_start_utc)
        check_time(run, event.window_end_utc)
        inserted += insert_once(
            cursor, "traffic_actual",
            dict(event.model_dump(), run_id=batch.run_id, topology_id=run["topology_id"],
                 available_at_utc=run["now"]),
            ("run_id", "flow_group_id", "window_start_utc"), ("available_at_utc",),
        )
    return inserted


def ingest_links(cursor, run, batch):
    check_time(run, batch.sampled_at_utc)
    cursor.execute("SELECT * FROM public.links WHERE topology_id = %s", (run["topology_id"],))
    links = {row["link_id"]: row for row in cursor}
    # The Network collector emits one sample for every measurable directed
    # link. Ports numbered 0 are host-facing/unmeasured and are excluded.
    expected_ids = {link_id for link_id, link in links.items() if link["from_port"] > 0}
    supplied_ids = {row.link_id for row in batch.samples}
    if supplied_ids != expected_ids:
        missing = expected_ids - supplied_ids
        unexpected = supplied_ids - expected_ids
        raise ValueError(
            f"Link batch must cover every measurable topology link exactly once "
            f"(missing={len(missing)}, unexpected={len(unexpected)})"
        )
    columns = {"link_id", "measure_switch", "measure_port", "tx_bytes_total", "delta_bytes",
               "delta_t_s", "port_tx_mbps", "counter_status"}
    cursor.execute(
        "SELECT link_id FROM public.link_samples WHERE run_id = %s AND batch_id = %s",
        (batch.run_id, batch.batch_id),
    )
    existing_ids = {row["link_id"] for row in cursor}
    if existing_ids and existing_ids != {row.link_id for row in batch.samples}:
        raise Conflict("Existing link batch has different members")
    inserted = 0
    for event in batch.samples:
        link = links.get(event.link_id)
        if (link is None or (event.measure_switch, event.measure_port) != (link["from_node"], link["from_port"])
                or event.from_node not in (None, link["from_node"])
                or event.to_node not in (None, link["to_node"])
                or event.is_fabric not in (None, link["is_fabric"])):
            raise ValueError("Link sample does not match directed topology")
        inserted += insert_once(
            cursor, "link_samples",
            dict(event.model_dump(include=columns), run_id=batch.run_id, topology_id=run["topology_id"],
                 batch_id=batch.batch_id, sampled_at_utc=batch.sampled_at_utc, available_at_utc=run["now"]),
            ("run_id", "batch_id", "link_id"), ("available_at_utc",),
        )
    return inserted


def flow_ids(cursor, run):
    cursor.execute("SELECT flow_group_id FROM public.flow_groups WHERE topology_id = %s", (run["topology_id"],))
    return {row["flow_group_id"] for row in cursor}


def route_rows(cursor, run_id, version):
    cursor.execute("""
        SELECT flow_group_id, path_id, confirmed_at_utc, verification_status
        FROM public.routing_state WHERE run_id = %s AND state_version = %s
    """, (run_id, version))
    return {row["flow_group_id"]: dict(row) for row in cursor}


def ingest_routing(cursor, run, batch):
    check_time(run, batch.published_at_utc)
    by_id = {row.flow_group_id: row.model_dump() for row in batch.rows}
    if len(by_id) != len(batch.rows) or by_id.keys() != flow_ids(cursor, run):
        raise ValueError("Routing version must cover all flow groups exactly once")
    payload = batch.model_dump(mode="json", exclude={"rows"})
    payload["rows"] = [row.model_dump(mode="json") for row in sorted(batch.rows, key=lambda row: row.flow_group_id)]
    digest = sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    version = dict(run_id=batch.run_id, state_version=batch.state_version,
                   published_at_utc=batch.published_at_utc, payload_hash=digest,
                   available_at_utc=run["now"])
    cursor.execute("""
        SELECT state_version, published_at_utc FROM public.routing_versions
        WHERE run_id = %s ORDER BY state_version DESC LIMIT 1
    """, (batch.run_id,))
    previous = cursor.fetchone()
    inserted = insert_once(cursor, "routing_versions", version, ("run_id", "state_version"),
                           ("available_at_utc",))
    if not inserted:
        return 0
    old = {}
    if previous:
        if batch.state_version != previous["state_version"] + 1 or batch.published_at_utc <= previous["published_at_utc"]:
            raise Conflict("Expected the next routing version and a later publication time")
        old = route_rows(cursor, batch.run_id, previous["state_version"])
        if old == by_id:
            raise Conflict("Routing state did not change")
    elif batch.state_version not in (0, 1):
        raise Conflict("Initial routing version must be 0 or 1")
    for group, row in by_id.items():
        if row["confirmed_at_utc"] and not run["run_started_at_utc"] <= row["confirmed_at_utc"] <= batch.published_at_utc:
            raise ValueError("Confirmation time is outside the run or after publication")
        insert_once(cursor, "routing_state",
                    dict(row, run_id=batch.run_id, state_version=batch.state_version,
                         topology_id=run["topology_id"]),
                    ("run_id", "state_version", "flow_group_id"))
        prior = old.get(group, {})
        if (prior.get("path_id"), prior.get("confirmed_at_utc")) == (row["path_id"], row["confirmed_at_utc"]):
            continue
        boundary = row["confirmed_at_utc"] or batch.published_at_utc
        if previous and boundary <= previous["published_at_utc"]:
            raise Conflict("Route transition predates the previous publication")
        cursor.execute("""
            UPDATE public.routing_history SET valid_to_utc = %s
            WHERE run_id = %s AND flow_group_id = %s AND valid_to_utc IS NULL
        """, (boundary, batch.run_id, group))
        if row["path_id"] is not None:
            insert_once(cursor, "routing_history",
                        dict(run_id=batch.run_id, topology_id=run["topology_id"], flow_group_id=group,
                             path_id=row["path_id"], state_version=batch.state_version,
                             valid_from_utc=boundary, valid_to_utc=None),
                        ("run_id", "flow_group_id", "valid_from_utc"))
    return 1


def demand_history(cursor, run_id, cutoff, as_of, lookback, *, measurement_origin=None, include_reason=False):
    cursor.execute("""
        SELECT flow_group_id, bin_index, requested_mbps, available_at_utc,
               window_start_utc, window_end_utc, released_at_utc, event_id, reason
        FROM public.demand_observations
        WHERE run_id = %s AND window_start_utc >= %s AND window_end_utc <= %s
          AND released_at_utc <= %s AND available_at_utc <= %s
        ORDER BY bin_index, flow_group_id
    """, (run_id, cutoff - timedelta(seconds=lookback), cutoff, as_of, as_of))
    rows = []
    for row in cursor:
        record = {key: row[key] for key in ("flow_group_id", "bin_index", "requested_mbps", "available_at_utc")}
        if include_reason:
            record["reason"] = row["reason"]
        if measurement_origin is not None:
            record.update(
                run_id=run_id,
                bin_index=(row["window_start_utc"] - measurement_origin) // timedelta(seconds=1),
                window_start_utc=row["window_start_utc"],
                window_end_utc=row["window_end_utc"],
                released_at_utc=row["released_at_utc"],
                release_record_id=row["event_id"],
                source_kind="RELEASED_DEMAND",
                valid=row["requested_mbps"] is not None,
                reason=row["reason"],
            )
        rows.append(record)
    return rows


def snapshot_content_hash(snapshot_payload):
    """Hash canonical SDN v1.4 JSON with its content_hash field omitted."""
    content = dict(snapshot_payload)
    content.pop("content_hash", None)
    canonical = json.dumps(content, ensure_ascii=False, sort_keys=True,
                           separators=(",", ":"), allow_nan=False).encode("utf-8")
    return sha256(canonical).hexdigest()


def ingest_snapshot(cursor, run, message, raw, digest):
    snapshot = message.sdn
    wire = json.loads(raw)
    if snapshot_content_hash(wire["sdn"]) != snapshot.content_hash:
        raise ValueError("Snapshot content_hash does not match canonical SDN v1.4 content")
    if snapshot.run_id != run["run_id"]:
        raise Conflict("Snapshot run_id differs from the registered run")
    if (snapshot.config_version != run["config_version"]
            or snapshot.config_hash != run["config_hash"]):
        raise Conflict("Snapshot configuration metadata differs from the registered run")

    # Validate run context before the idempotent replay return too: previously
    # stored or replayed bytes must never bypass the run/configuration checks.
    cursor.execute("""
        SELECT payload, payload_hash FROM public.snapshots
        WHERE run_id = %s AND snapshot_id = %s
    """, (snapshot.run_id, snapshot.snapshot_id))
    existing = cursor.fetchone()
    if existing:
        if existing["payload_hash"] != digest or bytes(existing["payload"]) != raw:
            raise Conflict("Snapshot ID is already associated with different bytes")
        return 0

    origin = run["measurement_started_at_utc"]
    origins = snapshot.origin_refs
    if origin is None:
        raise Conflict("Run is missing measurement_started_at_utc")
    if (origins.run_started_at_utc != run["run_started_at_utc"]
            or origins.traffic_started_at_utc != run["traffic_started_at_utc"]
            or origins.measurement_started_at_utc != origin
            or origins.warmup_s != run["warmup_s"]
            or origins.measurement_started_at_utc != origins.traffic_started_at_utc + timedelta(seconds=origins.warmup_s)
            or origins.delta_s != snapshot.delta_s
            or snapshot.delta_s != 1):
        raise Conflict("Snapshot origins or bin duration differ from the registered run")
    check_time(run, snapshot.snapshot_as_of_utc)
    expected_cutoff = origin + timedelta(seconds=snapshot.cutoff_offset_s * snapshot.delta_s)
    if (snapshot.history_cutoff_utc != expected_cutoff
            or snapshot.history_cutoff_utc > snapshot.snapshot_as_of_utc
            or snapshot.cutoff_offset_s > run["measurement_s"]):
        raise ValueError("Snapshot cutoff does not match its measurement-relative offset")
    cutoff_bin = traffic_bin(run, snapshot.history_cutoff_utc)
    if run["lookback_bins"] is None or snapshot.lookback_L != run["lookback_bins"]:
        raise Conflict("Snapshot lookback_L differs from the registered run")

    cursor.execute("SELECT topology_hash, topology_version FROM public.topologies WHERE topology_id=%s",
                   (run["topology_id"],))
    topology = cursor.fetchone()
    if (not topology or topology["topology_hash"] != snapshot.topology_hash
            or topology["topology_version"] != snapshot.topology_version):
        raise Conflict("Snapshot topology metadata differs from the registered topology")

    cursor.execute("""
        SELECT state_version FROM public.routing_versions
        WHERE run_id = %s AND available_at_utc <= %s
        ORDER BY state_version DESC LIMIT 1
    """, (snapshot.run_id, snapshot.snapshot_as_of_utc))
    state = cursor.fetchone()
    if not state or state["state_version"] != snapshot.state_version:
        raise Conflict("Snapshot references an incorrect routing version for its as-of time")

    groups = flow_ids(cursor, run)
    listed_groups = snapshot.flow_group_ids
    if len(listed_groups) != len(set(listed_groups)) or set(listed_groups) != groups:
        raise Conflict("Snapshot flow_group_ids must match the topology's 48 flow groups")
    route_by_id = {row.flow_group_id: row for row in snapshot.routing_state}
    if len(route_by_id) != len(snapshot.routing_state) or route_by_id.keys() != groups:
        raise Conflict("Snapshot routing_state must contain every flow exactly once")
    if any(row.run_id != snapshot.run_id or row.state_version != snapshot.state_version
           for row in snapshot.routing_state):
        raise Conflict("Snapshot routing rows must match its run and state_version")
    routes = {
        row.flow_group_id: {
            "flow_group_id": row.flow_group_id,
            "path_id": row.path_id,
            "confirmed_at_utc": row.confirmed_at,
            "verification_status": row.verification_status,
        }
        for row in snapshot.routing_state
    }
    if routes != route_rows(cursor, snapshot.run_id, snapshot.state_version):
        raise Conflict("Snapshot routing state differs from the published version")

    unknown_groups = {group for group, row in routes.items() if row["verification_status"] != "VERIFIED"}
    if (len(snapshot.unknown_flow_group_ids) != len(set(snapshot.unknown_flow_group_ids))
            or set(snapshot.unknown_flow_group_ids) != unknown_groups
            or snapshot.routing_state_complete is not True):
        raise Conflict("Snapshot routing completeness metadata is inconsistent")

    supplied_route_history = set()
    for row in snapshot.routing_history:
        if row.run_id != snapshot.run_id or row.flow_group_id not in groups or row.valid_from > snapshot.snapshot_as_of_utc:
            raise Conflict("Snapshot routing_history contains an out-of-scope or future row")
        key = (row.flow_group_id, row.path_id, row.valid_from, row.valid_to)
        if key in supplied_route_history:
            raise Conflict("Snapshot routing_history contains duplicate intervals")
        supplied_route_history.add(key)
    cursor.execute("""
        SELECT h.flow_group_id, h.path_id, h.valid_from_utc, h.valid_to_utc,
               (SELECT v2.available_at_utc FROM public.routing_history h2
                JOIN public.routing_versions v2 USING(run_id,state_version)
                WHERE h2.run_id=h.run_id AND h2.flow_group_id=h.flow_group_id
                  AND h2.valid_from_utc=h.valid_to_utc LIMIT 1) AS closed_available_at_utc
        FROM public.routing_history h
        JOIN public.routing_versions v
          ON v.run_id = h.run_id AND v.state_version = h.state_version
        WHERE h.run_id = %s AND h.valid_from_utc <= %s AND v.available_at_utc <= %s
    """, (snapshot.run_id, snapshot.snapshot_as_of_utc, snapshot.snapshot_as_of_utc))
    known_route_history = set()
    for row in cursor:
        # A closure published after snapshot_as_of was not yet knowable then.
        end = row["valid_to_utc"]
        if end is not None and (end > snapshot.snapshot_as_of_utc
                or (row["closed_available_at_utc"] is not None
                    and row["closed_available_at_utc"] > snapshot.snapshot_as_of_utc)):
            end = None
        known_route_history.add((row["flow_group_id"], row["path_id"], row["valid_from_utc"], end))
    supplied_known_route_history = {row for row in supplied_route_history if row[1] is not None}
    if supplied_known_route_history != known_route_history:
        raise Conflict("Snapshot routing_history differs from Data's verified history as of snapshot time")

    expected_bins = set(range(snapshot.cutoff_offset_s - snapshot.lookback_L,
                              snapshot.cutoff_offset_s))
    observed = {(row["flow_group_id"], row["bin_index"]): row for row in demand_history(
        cursor, snapshot.run_id, snapshot.history_cutoff_utc, snapshot.snapshot_as_of_utc,
        snapshot.lookback_L, measurement_origin=origin)}
    supplied = {(row.flow_group_id, row.bin_index): row for row in snapshot.history}
    if len(supplied) != len(snapshot.history):
        raise Conflict("Snapshot history contains duplicate flow/bin rows")
    for key, row in supplied.items():
        if key[0] not in groups or key[1] not in expected_bins:
            raise Conflict("Snapshot history contains a flow/bin outside its declared window")
        evidence = observed.get(key)
        if evidence is None:
            # A missing observation remains explicitly missing; do not synthesize 0.
            if (row.valid or row.requested_mbps is not None or row.released_at_utc is not None
                    or row.available_at_utc is not None or row.reason is not None
                    or row.release_record_id is not None):
                raise Conflict("Snapshot claims demand evidence that Data does not have")
        elif any((getattr(row, field) != evidence[field]) for field in (
                "requested_mbps", "released_at_utc", "available_at_utc", "valid", "reason", "release_record_id")):
            raise Conflict("Snapshot modifies stored demand evidence")
        expected_window_start = origin + timedelta(seconds=row.bin_index)
        if (row.window_start_utc != expected_window_start
                or row.window_end_utc != expected_window_start + timedelta(seconds=snapshot.delta_s)):
            raise ValueError("Snapshot history windows do not match measurement-relative bins")

    expected_count = len(groups) * snapshot.lookback_L
    valid_count = sum(row.valid for row in snapshot.history)
    if snapshot.expected_bin_count != expected_count or snapshot.valid_bin_count != valid_count:
        raise ValueError("Snapshot history counts do not match its rows and lookback")
    if snapshot.quality_status == "VALID":
        if (len(supplied) != expected_count or set(supplied) != {
                (group, bin_index) for group in groups for bin_index in expected_bins}
                or valid_count != expected_count or snapshot.missing_groups
                or unknown_groups or any(row.verification_status != "VERIFIED"
                                         for row in snapshot.routing_state)):
            raise ValueError("VALID snapshot requires complete demand and verified routing")

    # Keep the canonical full payload in BYTEA; the relational columns are only
    # an index for Data's lookup and forecast/decision guards.
    metadata = dict(
        run_id=snapshot.run_id,
        snapshot_id=snapshot.snapshot_id,
        history_cutoff_utc=snapshot.history_cutoff_utc,
        cutoff_bin=cutoff_bin,
        snapshot_as_of_utc=snapshot.snapshot_as_of_utc,
        state_version=snapshot.state_version,
        schema_version=snapshot.provenance.schema_version,
        quality_status=snapshot.quality_status,
        payload=raw,
        payload_hash=digest,
        available_at_utc=run["now"],
    )
    return insert_once(cursor, "snapshots", metadata,
                       ("run_id", "snapshot_id"), ("available_at_utc",))

def ingest_forecast(cursor, run, batch):
    first = batch.rows[0]
    check_time(run, first.issued_at)
    cursor.execute("""
        SELECT history_cutoff_utc, quality_status, available_at_utc
        FROM public.snapshots WHERE run_id=%s AND snapshot_id=%s
    """, (batch.run_id, batch.snapshot_id))
    snapshot = cursor.fetchone()
    origin = run["measurement_started_at_utc"]
    horizon = run["horizon_bins"]
    if (not snapshot or snapshot["quality_status"] != "VALID" or origin is None or not horizon
            or run["method_id"] not in ("M1", "M2", "M3")):
        raise Conflict("Forecast requires a VALID snapshot and a run with frozen measurement and horizon")
    if first.issued_at < snapshot["available_at_utc"]:
        raise ValueError("Forecast issued_at precedes snapshot availability")
    offset = snapshot["history_cutoff_utc"] - origin
    start = offset // timedelta(seconds=1)
    if offset.microseconds or start < 0 or start + horizon > run["measurement_s"]:
        raise ValueError("Forecast target is outside the measurement window")
    groups = flow_ids(cursor, run)
    if len(groups) != 48 or len(batch.rows) != len(groups) * horizon:
        raise ValueError("Forecast must contain exactly 48 × H rows")
    seen = set()
    for row in batch.rows:
        key = (row.flow_group_id, row.forecast_start)
        if (key in seen or row.flow_group_id not in groups or not start <= row.forecast_start < start + horizon
                or row.forecast_end != row.forecast_start + 1 or row.run_id != batch.run_id
                or row.issued_at != first.issued_at or row.source != first.source):
            raise ValueError("Forecast rows must cover each flow and target bin exactly once with shared metadata")
        seen.add(key)
    ordered = sorted(batch.rows, key=lambda row: (row.flow_group_id, row.forecast_start))
    content = [row.model_dump(mode="json") for row in ordered]
    digest = sha256(json.dumps(content, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    inserted = insert_once(
        cursor, "forecast_batches",
        dict(run_id=batch.run_id, forecast_batch_id=batch.forecast_batch_id,
             snapshot_id=batch.snapshot_id, issued_at=first.issued_at, source=first.source,
             payload_hash=digest, committed_at_utc=run["now"]),
        ("run_id", "forecast_batch_id"), ("committed_at_utc",),
    )
    if inserted:
        cursor.executemany("""
            INSERT INTO public.predictions
                (run_id, flow_group_id, issued_at, forecast_start, forecast_end, predicted_mbps, source)
            VALUES (%(run_id)s, %(flow_group_id)s, %(issued_at)s, %(forecast_start)s, %(forecast_end)s,
                    %(predicted_mbps)s, %(source)s)
        """, [row.model_dump() for row in ordered])
    return len(ordered) if inserted else 0


def get_ready_forecast(cursor, run, batch_id):
    cursor.execute("""
        SELECT * FROM public.ready_forecast_batches WHERE run_id=%s AND forecast_batch_id=%s
    """, (run["run_id"], batch_id))
    batch = cursor.fetchone()
    if batch is None:
        return None
    cursor.execute("""
        SELECT * FROM public.predictions WHERE run_id=%s AND issued_at=%s
        ORDER BY flow_group_id, forecast_start
    """, (run["run_id"], batch["issued_at"]))
    # This endpoint only reads from ready_forecast_batches, so a returned
    # batch has passed Data's completeness checks and can be marked READY.
    return dict(batch, status="READY", rows=[dict(row) for row in cursor])
