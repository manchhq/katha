-- Postgres only, and opt-in: applied by ensure_payload_validation(), never by
-- ensure_events_table().
--
-- data is TEXT holding serde_json output, so Postgres will store a truncated
-- or corrupted payload without complaint and the failure surfaces later, at
-- deserialize time, during a replay. The text-to-jsonb cast is immutable, so a
-- CHECK constraint can reject it at write time instead.
--
-- This is the cheap half of payload integrity. It costs about 10 percent of
-- append throughput and no disk, where the GIN index in 0003 costs roughly 55
-- percent and an index. Take this one if you want the guarantee, and 0003 only
-- if you also query payloads in SQL.
--
-- Adding the constraint FAILS LOUDLY if any existing row is not valid JSON.
-- Postgres has no ADD CONSTRAINT IF NOT EXISTS, so the caller tolerates the
-- already-exists error to stay idempotent.
ALTER TABLE "{{name}}_events"
    ADD CONSTRAINT "{{name}}_events_data_json" CHECK ((data::jsonb) IS NOT NULL);
