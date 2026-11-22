-- PostgreSQL schema revision 1 aligned to integration guide v1.4.
BEGIN;

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY
);

CREATE TABLE topologies (
    topology_id TEXT PRIMARY KEY,
    topology_version TEXT NOT NULL,
    topology_hash TEXT NOT NULL UNIQUE,
    bundle BYTEA NOT NULL
);

CREATE TABLE topology_nodes (
    topology_id TEXT NOT NULL REFERENCES topologies,
    node_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('host', 'edge', 'core')),
    dpid BIGINT,
    PRIMARY KEY (topology_id, node_id),
    UNIQUE (topology_id, dpid),
    CHECK ((role = 'host') = (dpid IS NULL))
);

CREATE TABLE flow_groups (
    topology_id TEXT NOT NULL,
    flow_group_id TEXT NOT NULL,
    flow_cookie TEXT NOT NULL,
    src_host TEXT NOT NULL,
    dst_host TEXT NOT NULL,
    src_ip TEXT NOT NULL,
    dst_ip TEXT NOT NULL,
    src_edge TEXT NOT NULL,
    dst_edge TEXT NOT NULL,
    udp_port INTEGER NOT NULL CHECK (udp_port BETWEEN 1 AND 65535),
    PRIMARY KEY (topology_id, flow_group_id),
    UNIQUE (topology_id, src_host, dst_host),
    FOREIGN KEY (topology_id, src_host) REFERENCES topology_nodes,
    FOREIGN KEY (topology_id, dst_host) REFERENCES topology_nodes,
    FOREIGN KEY (topology_id, src_edge) REFERENCES topology_nodes,
    FOREIGN KEY (topology_id, dst_edge) REFERENCES topology_nodes,
    CHECK (src_edge <> dst_edge AND src_host <> dst_host),
    CHECK (flow_group_id = ('0x' || upper(to_hex(256 * CAST(substr(src_host, 2) AS INTEGER)
                                     + CAST(substr(dst_host, 2) AS INTEGER))))),
    CHECK (flow_cookie = flow_group_id)
);

CREATE TABLE links (
    topology_id TEXT NOT NULL,
    link_id TEXT NOT NULL,
    from_node TEXT NOT NULL,
    to_node TEXT NOT NULL,
    from_port INTEGER NOT NULL CHECK (from_port >= 0),
    to_port INTEGER NOT NULL CHECK (to_port >= 0),
    is_fabric BOOLEAN NOT NULL,
    capacity_mbps DOUBLE PRECISION NOT NULL CHECK (capacity_mbps > 0 AND capacity_mbps <= 1.7976931348623157e308),
    delay_ms DOUBLE PRECISION NOT NULL CHECK (delay_ms BETWEEN 0 AND 1.7976931348623157e308),
    PRIMARY KEY (topology_id, link_id),
    FOREIGN KEY (topology_id, from_node) REFERENCES topology_nodes,
    FOREIGN KEY (topology_id, to_node) REFERENCES topology_nodes,
    CHECK (from_node <> to_node AND link_id = from_node || '>' || to_node)
);

CREATE TABLE candidate_paths (
    topology_id TEXT NOT NULL,
    flow_group_id TEXT NOT NULL,
    path_id TEXT NOT NULL,
    path_label TEXT NOT NULL CHECK (path_label IN ('P1', 'P2')),
    PRIMARY KEY (topology_id, flow_group_id, path_id),
    FOREIGN KEY (topology_id, flow_group_id) REFERENCES flow_groups,
    CHECK (path_id = flow_group_id || '_' || path_label)
);

CREATE TABLE path_links (
    topology_id TEXT NOT NULL,
    flow_group_id TEXT NOT NULL,
    path_id TEXT NOT NULL,
    hop INTEGER NOT NULL CHECK (hop BETWEEN 1 AND 4),
    link_id TEXT NOT NULL,
    PRIMARY KEY (topology_id, flow_group_id, path_id, hop),
    FOREIGN KEY (topology_id, flow_group_id, path_id) REFERENCES candidate_paths,
    FOREIGN KEY (topology_id, link_id) REFERENCES links
);

CREATE TABLE split_assignments (
    split_manifest_id TEXT NOT NULL,
    seed_family TEXT NOT NULL,
    split TEXT NOT NULL CHECK (split IN ('train', 'validation', 'test')),
    PRIMARY KEY (split_manifest_id, seed_family)
);

CREATE TABLE schedules (
    schedule_id TEXT PRIMARY KEY,
    topology_id TEXT NOT NULL REFERENCES topologies,
    scenario_id TEXT NOT NULL,
    seed BIGINT NOT NULL,
    split_manifest_id TEXT NOT NULL,
    seed_family TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    load_level DOUBLE PRECISION,
    first_offset_s INTEGER NOT NULL CHECK (first_offset_s <= 0),
    duration_s INTEGER NOT NULL CHECK (duration_s > 0),
    UNIQUE (schedule_id, topology_id),
    FOREIGN KEY (split_manifest_id, seed_family) REFERENCES split_assignments
);

CREATE TABLE demand_plan (
    schedule_id TEXT NOT NULL,
    topology_id TEXT NOT NULL,
    flow_group_id TEXT NOT NULL,
    time_offset_s INTEGER NOT NULL,
    requested_mbps DOUBLE PRECISION NOT NULL CHECK (requested_mbps BETWEEN 0 AND 1.7976931348623157e308),
    PRIMARY KEY (schedule_id, flow_group_id, time_offset_s),
    FOREIGN KEY (schedule_id, topology_id) REFERENCES schedules (schedule_id, topology_id),
    FOREIGN KEY (topology_id, flow_group_id) REFERENCES flow_groups
);

CREATE FUNCTION check_demand_plan_within_schedule() RETURNS trigger
LANGUAGE plpgsql AS '
BEGIN
    IF NOT EXISTS (
    SELECT 1 FROM schedules AS s
    WHERE s.schedule_id = NEW.schedule_id
      AND NEW.time_offset_s >= s.first_offset_s
      AND NEW.time_offset_s < s.duration_s
    ) THEN
        RAISE EXCEPTION ''demand plan bin is outside schedule'' USING ERRCODE = ''23514'';
    END IF;
    RETURN NEW;
END;
';

CREATE TRIGGER demand_plan_within_schedule
BEFORE INSERT OR UPDATE ON demand_plan
FOR EACH ROW EXECUTE FUNCTION check_demand_plan_within_schedule();

CREATE TABLE runs (
    run_id TEXT PRIMARY KEY,
    topology_id TEXT NOT NULL REFERENCES topologies,
    schedule_id TEXT,
    mode TEXT NOT NULL CHECK (mode IN ('calibration', 'benchmark', 'live', 'oracle')),
    method_id TEXT NOT NULL CHECK (method_id IN ('M0', 'M1', 'M2', 'M3', 'M4')),
    run_started_at_utc TIMESTAMPTZ NOT NULL,
    traffic_started_at_utc TIMESTAMPTZ,
    measurement_started_at_utc TIMESTAMPTZ,
    warmup_s INTEGER NOT NULL CHECK (warmup_s >= 0),
    measurement_s INTEGER NOT NULL CHECK (measurement_s > 0),
    schema_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'ok', 'failed', 'discarded')),
    config_version TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    ml_config_hash TEXT,
    lookback_bins INTEGER CHECK (lookback_bins > 0),
    horizon_bins INTEGER CHECK (horizon_bins > 0),
    model_version TEXT,
    calibration_version TEXT,
    git_commit TEXT NOT NULL,
    manifest BYTEA NOT NULL,
    UNIQUE (run_id, topology_id),
    FOREIGN KEY (schedule_id, topology_id) REFERENCES schedules (schedule_id, topology_id),
    CHECK (mode NOT IN ('benchmark', 'oracle') OR schedule_id IS NOT NULL),
    CHECK ((method_id = 'M4') = (mode = 'oracle')),
    CHECK (method_id NOT IN ('M1', 'M2', 'M3') OR
           (ml_config_hash IS NOT NULL AND lookback_bins IS NOT NULL AND horizon_bins IS NOT NULL)),
    CHECK (run_started_at_utc <= traffic_started_at_utc),
    CHECK (measurement_started_at_utc IS NULL OR
           (traffic_started_at_utc IS NOT NULL AND traffic_started_at_utc <= measurement_started_at_utc))
);

CREATE TABLE demand_observations (
    run_id TEXT NOT NULL,
    topology_id TEXT NOT NULL,
    flow_group_id TEXT NOT NULL,
    bin_index INTEGER NOT NULL,
    event_id TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    window_start_utc TIMESTAMPTZ NOT NULL,
    window_end_utc TIMESTAMPTZ NOT NULL,
    requested_mbps DOUBLE PRECISION CHECK (requested_mbps BETWEEN 0 AND 1.7976931348623157e308),
    reason TEXT,
    released_at_utc TIMESTAMPTZ NOT NULL,
    available_at_utc TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, flow_group_id, bin_index),
    FOREIGN KEY (run_id, topology_id) REFERENCES runs (run_id, topology_id),
    FOREIGN KEY (topology_id, flow_group_id) REFERENCES flow_groups,
    CHECK (window_start_utc < window_end_utc AND window_end_utc <= released_at_utc
           AND released_at_utc <= available_at_utc),
    CHECK (requested_mbps IS NOT NULL OR reason IS NOT NULL)
);

CREATE TABLE traffic_actual (
    run_id TEXT NOT NULL,
    topology_id TEXT NOT NULL,
    flow_group_id TEXT NOT NULL,
    window_start_utc TIMESTAMPTZ NOT NULL,
    window_end_utc TIMESTAMPTZ NOT NULL,
    sent_bytes BIGINT CHECK (sent_bytes >= 0),
    received_bytes BIGINT CHECK (received_bytes >= 0),
    sent_packets BIGINT CHECK (sent_packets >= 0),
    received_packets BIGINT CHECK (received_packets >= 0),
    available_at_utc TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, flow_group_id, window_start_utc),
    FOREIGN KEY (run_id, topology_id) REFERENCES runs (run_id, topology_id),
    FOREIGN KEY (topology_id, flow_group_id) REFERENCES flow_groups,
    CHECK (window_start_utc < window_end_utc AND window_end_utc <= available_at_utc)
);

CREATE TABLE link_samples (
    run_id TEXT NOT NULL,
    topology_id TEXT NOT NULL,
    link_id TEXT NOT NULL,
    sampled_at_utc TIMESTAMPTZ NOT NULL,
    available_at_utc TIMESTAMPTZ NOT NULL,
    batch_id TEXT NOT NULL,
    measure_switch TEXT NOT NULL,
    measure_port INTEGER NOT NULL CHECK (measure_port > 0),
    tx_bytes_total NUMERIC(20,0) CHECK (tx_bytes_total BETWEEN 0 AND 18446744073709551615),
    delta_bytes NUMERIC(20,0) CHECK (delta_bytes BETWEEN 0 AND 18446744073709551615),
    delta_t_s DOUBLE PRECISION CHECK (delta_t_s > 0),
    port_tx_mbps DOUBLE PRECISION CHECK (port_tx_mbps BETWEEN 0 AND 1.7976931348623157e308),
    counter_status TEXT NOT NULL,
    PRIMARY KEY (run_id, batch_id, link_id),
    FOREIGN KEY (run_id, topology_id) REFERENCES runs (run_id, topology_id),
    FOREIGN KEY (topology_id, link_id) REFERENCES links,
    FOREIGN KEY (topology_id, measure_switch) REFERENCES topology_nodes,
    CHECK (sampled_at_utc <= available_at_utc),
    CHECK (port_tx_mbps IS NULL OR (delta_bytes IS NOT NULL AND delta_t_s IS NOT NULL
           AND counter_status IN ('OK', 'WRAPPED')))
);

CREATE TABLE snapshots (
    run_id TEXT NOT NULL REFERENCES runs,
    snapshot_id TEXT NOT NULL,
    history_cutoff_utc TIMESTAMPTZ NOT NULL,
    cutoff_bin INTEGER NOT NULL CHECK (cutoff_bin >= 0),
    snapshot_as_of_utc TIMESTAMPTZ NOT NULL,
    available_at_utc TIMESTAMPTZ NOT NULL,
    state_version BIGINT NOT NULL CHECK (state_version >= 0),
    schema_version TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    quality_status TEXT NOT NULL,
    payload BYTEA NOT NULL,
    PRIMARY KEY (run_id, snapshot_id),
    UNIQUE (run_id, snapshot_id, state_version),
    CHECK (history_cutoff_utc <= snapshot_as_of_utc AND snapshot_as_of_utc <= available_at_utc)
);

CREATE FUNCTION reject_immutable_row_change() RETURNS trigger
LANGUAGE plpgsql AS '
BEGIN
    RAISE EXCEPTION ''stored % row is immutable'', TG_TABLE_NAME USING ERRCODE = ''23514'';
END;
';

CREATE TRIGGER snapshots_immutable
BEFORE UPDATE OR DELETE ON snapshots
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE TABLE forecast_batches (
    run_id TEXT NOT NULL,
    forecast_batch_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    committed_at_utc TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, forecast_batch_id),
    UNIQUE (run_id, issued_at),
    FOREIGN KEY (run_id, snapshot_id) REFERENCES snapshots,
    CHECK (issued_at <= committed_at_utc)
);

-- Preserve the seven-field wire format and original key.
CREATE TABLE predictions (
    run_id TEXT NOT NULL,
    flow_group_id TEXT NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    forecast_start INTEGER NOT NULL CHECK (forecast_start >= 0),
    forecast_end INTEGER NOT NULL CHECK (forecast_end = forecast_start + 1),
    predicted_mbps DOUBLE PRECISION NOT NULL CHECK (predicted_mbps BETWEEN 0 AND 1.7976931348623157e308),
    source TEXT NOT NULL,
    PRIMARY KEY (run_id, flow_group_id, issued_at, forecast_start),
    FOREIGN KEY (run_id, issued_at) REFERENCES forecast_batches (run_id, issued_at)
);

CREATE FUNCTION check_prediction_matches_batch() RETURNS trigger
LANGUAGE plpgsql AS '
BEGIN
    IF NOT EXISTS (
    SELECT 1
    FROM forecast_batches AS b
    JOIN snapshots AS s USING (run_id, snapshot_id)
    JOIN runs AS r USING (run_id)
    JOIN flow_groups AS f USING (topology_id)
    WHERE b.run_id = NEW.run_id AND b.issued_at = NEW.issued_at
      AND b.source = NEW.source AND f.flow_group_id = NEW.flow_group_id
      AND r.method_id IN (''M1'', ''M2'', ''M3'') AND s.quality_status = ''VALID''
      AND NEW.issued_at >= s.available_at_utc
      AND NEW.forecast_start >= s.cutoff_bin
      AND NEW.forecast_end <= s.cutoff_bin + r.horizon_bins
      AND s.cutoff_bin + r.horizon_bins <= r.measurement_s
    ) THEN
        RAISE EXCEPTION ''prediction does not match frozen forecast context'' USING ERRCODE = ''23514'';
    END IF;
    RETURN NEW;
END;
';

CREATE TRIGGER prediction_matches_batch
BEFORE INSERT ON predictions
FOR EACH ROW EXECUTE FUNCTION check_prediction_matches_batch();

CREATE TRIGGER predictions_immutable
BEFORE UPDATE OR DELETE ON predictions
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE VIEW ready_forecast_batches AS
SELECT b.*, 48 * r.horizon_bins AS expected_rows, count(p.run_id) AS actual_rows
FROM forecast_batches AS b
JOIN runs AS r USING (run_id)
JOIN predictions AS p USING (run_id, issued_at)
GROUP BY b.run_id, b.forecast_batch_id, r.horizon_bins
HAVING count(p.run_id) = 48 * r.horizon_bins
   AND count(DISTINCT p.flow_group_id) = 48;

CREATE TABLE control_ticks (
    run_id TEXT NOT NULL REFERENCES runs,
    tick_id TEXT NOT NULL,
    tick_at_utc TIMESTAMPTZ NOT NULL,
    snapshot_id TEXT,
    forecast_batch_id TEXT,
    status TEXT NOT NULL,
    reason TEXT,
    PRIMARY KEY (run_id, tick_id),
    FOREIGN KEY (run_id, snapshot_id) REFERENCES snapshots,
    FOREIGN KEY (run_id, forecast_batch_id) REFERENCES forecast_batches
);

CREATE TABLE decision_batches (
    run_id TEXT NOT NULL,
    decision_batch_id TEXT NOT NULL,
    tick_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    expected_state_version BIGINT NOT NULL,
    optimizer_result BYTEA NOT NULL,
    request_payload BYTEA,
    request_hash TEXT,
    apply_deadline_utc TIMESTAMPTZ,
    response_payload BYTEA,
    response_status TEXT CHECK (response_status IN
        ('APPLIED', 'REJECTED', 'FAILED', 'ROLLED_BACK', 'PARTIAL', 'INCONSISTENT')),
    response_state_version BIGINT CHECK (response_state_version >= 0),
    completed_at_utc TIMESTAMPTZ,
    PRIMARY KEY (run_id, decision_batch_id),
    FOREIGN KEY (run_id, tick_id) REFERENCES control_ticks,
    FOREIGN KEY (run_id, snapshot_id, expected_state_version) REFERENCES snapshots (run_id, snapshot_id, state_version),
    CHECK ((request_payload IS NULL) = (request_hash IS NULL)),
    CHECK ((request_payload IS NULL) = (apply_deadline_utc IS NULL)),
    CHECK ((response_payload IS NULL) = (response_status IS NULL)),
    CHECK (response_payload IS NULL OR
           (request_payload IS NOT NULL AND response_state_version IS NOT NULL AND completed_at_utc IS NOT NULL))
);

CREATE TABLE routing_decisions (
    run_id TEXT NOT NULL,
    topology_id TEXT NOT NULL,
    decision_batch_id TEXT NOT NULL,
    flow_group_id TEXT NOT NULL,
    old_path_id TEXT NOT NULL,
    proposed_path_id TEXT NOT NULL,
    actual_path_id TEXT,
    status TEXT CHECK (status IN ('APPLIED', 'KEPT', 'FAILED', 'ROLLED_BACK', 'UNKNOWN')),
    verified_at_utc TIMESTAMPTZ,
    reason TEXT,
    PRIMARY KEY (run_id, decision_batch_id, flow_group_id),
    FOREIGN KEY (run_id, topology_id) REFERENCES runs (run_id, topology_id),
    FOREIGN KEY (run_id, decision_batch_id) REFERENCES decision_batches,
    FOREIGN KEY (topology_id, flow_group_id, old_path_id) REFERENCES candidate_paths,
    FOREIGN KEY (topology_id, flow_group_id, proposed_path_id) REFERENCES candidate_paths,
    FOREIGN KEY (topology_id, flow_group_id, actual_path_id) REFERENCES candidate_paths,
    CHECK ((actual_path_id IS NULL) = (verified_at_utc IS NULL)),
    CHECK (status IS NULL OR (status = 'UNKNOWN') = (actual_path_id IS NULL)),
    CHECK (status IS NULL OR status <> 'APPLIED' OR actual_path_id = proposed_path_id)
);

CREATE TABLE routing_versions (
    run_id TEXT NOT NULL REFERENCES runs,
    state_version BIGINT NOT NULL CHECK (state_version >= 0),
    published_at_utc TIMESTAMPTZ NOT NULL,
    payload_hash TEXT NOT NULL,
    PRIMARY KEY (run_id, state_version)
);

-- Each version contains all flows, including explicit unknowns. Publish atomically.
CREATE TABLE routing_state (
    run_id TEXT NOT NULL,
    topology_id TEXT NOT NULL,
    state_version BIGINT NOT NULL,
    flow_group_id TEXT NOT NULL,
    path_id TEXT,
    confirmed_at_utc TIMESTAMPTZ,
    verification_status TEXT NOT NULL CHECK (verification_status IN ('VERIFIED', 'UNKNOWN', 'INCONSISTENT')),
    PRIMARY KEY (run_id, state_version, flow_group_id),
    FOREIGN KEY (run_id, topology_id) REFERENCES runs (run_id, topology_id),
    FOREIGN KEY (run_id, state_version) REFERENCES routing_versions,
    FOREIGN KEY (topology_id, flow_group_id) REFERENCES flow_groups,
    FOREIGN KEY (topology_id, flow_group_id, path_id) REFERENCES candidate_paths,
    CHECK ((verification_status = 'VERIFIED') = (path_id IS NOT NULL)),
    CHECK ((path_id IS NULL) = (confirmed_at_utc IS NULL))
);

CREATE VIEW current_routing_state AS
SELECT s.*
FROM routing_state AS s
WHERE s.state_version = (SELECT max(v.state_version) FROM routing_versions AS v WHERE v.run_id = s.run_id);

CREATE TABLE routing_history (
    run_id TEXT NOT NULL,
    topology_id TEXT NOT NULL,
    flow_group_id TEXT NOT NULL,
    path_id TEXT NOT NULL,
    state_version BIGINT NOT NULL,
    valid_from_utc TIMESTAMPTZ NOT NULL,
    valid_to_utc TIMESTAMPTZ,
    PRIMARY KEY (run_id, flow_group_id, valid_from_utc),
    FOREIGN KEY (run_id, topology_id) REFERENCES runs (run_id, topology_id),
    FOREIGN KEY (run_id, state_version) REFERENCES routing_versions,
    FOREIGN KEY (topology_id, flow_group_id, path_id) REFERENCES candidate_paths,
    CHECK (valid_to_utc IS NULL OR valid_from_utc < valid_to_utc)
);

CREATE UNIQUE INDEX one_open_route_interval ON routing_history (run_id, flow_group_id)
WHERE valid_to_utc IS NULL;

CREATE TABLE events (
    event_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs,
    event_at_utc TIMESTAMPTZ NOT NULL,
    available_at_utc TIMESTAMPTZ NOT NULL,
    component TEXT NOT NULL,
    event_type TEXT NOT NULL,
    detail JSONB NOT NULL
);

INSERT INTO schema_migrations (version) VALUES (1);
COMMIT;
