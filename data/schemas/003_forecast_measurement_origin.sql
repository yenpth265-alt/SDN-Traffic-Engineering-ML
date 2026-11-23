BEGIN;

CREATE OR REPLACE FUNCTION public.check_prediction_matches_batch() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.forecast_batches AS b
        JOIN public.snapshots AS s USING (run_id, snapshot_id)
        JOIN public.runs AS r USING (run_id)
        JOIN public.flow_groups AS f USING (topology_id)
        CROSS JOIN LATERAL (
            SELECT EXTRACT(EPOCH FROM s.history_cutoff_utc - r.measurement_started_at_utc) AS start_s
        ) AS target
        WHERE b.run_id = NEW.run_id AND b.issued_at = NEW.issued_at
          AND b.source = NEW.source AND f.flow_group_id = NEW.flow_group_id
          AND r.method_id IN ('M1', 'M2', 'M3') AND s.quality_status = 'VALID'
          AND NEW.issued_at >= s.available_at_utc
          AND target.start_s >= 0 AND target.start_s = trunc(target.start_s)
          AND NEW.forecast_start >= target.start_s
          AND NEW.forecast_end <= target.start_s + r.horizon_bins
          AND target.start_s + r.horizon_bins <= r.measurement_s
    ) THEN
        RAISE EXCEPTION 'prediction does not match frozen forecast context' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

INSERT INTO public.schema_migrations (version) VALUES (3);

COMMIT;
