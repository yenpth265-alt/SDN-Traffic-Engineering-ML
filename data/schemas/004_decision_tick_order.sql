BEGIN;

-- A control tick may produce at most one immutable DecisionBatch.
-- Replaying the same batch remains idempotent; a different batch for the
-- same tick is rejected by the unique index and returned as HTTP 409.
CREATE UNIQUE INDEX decision_batches_one_per_tick
    ON public.decision_batches (run_id, tick_id);

INSERT INTO public.schema_migrations (version) VALUES (4);

COMMIT;
