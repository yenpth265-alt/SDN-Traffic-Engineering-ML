BEGIN;

-- Existing rows keep an unknown arrival time. New ingestion supplies the DB clock.
ALTER TABLE public.routing_versions
    ADD COLUMN available_at_utc timestamptz,
    ADD CONSTRAINT routing_version_available_after_publish
        CHECK (published_at_utc <= available_at_utc);

ALTER TABLE public.snapshots
    ADD CONSTRAINT snapshot_routing_version_fk
        FOREIGN KEY (run_id, state_version)
        REFERENCES public.routing_versions (run_id, state_version);

INSERT INTO public.schema_migrations (version) VALUES (2);

COMMIT;
