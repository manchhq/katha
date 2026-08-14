-- Postgres only, and opt-in: applied by `ensure_payload_index()`, never by
-- `ensure_events_table()`.
--
-- `data` is TEXT holding serde_json output. Postgres can still index it by
-- casting: the cast is immutable, so a GIN index over `(data::jsonb)` serves
-- containment queries without changing the column type.
--
-- jsonb_path_ops rather than the default jsonb_ops: it indexes whole paths
-- instead of every key and value separately, which is smaller and faster for
-- the containment queries event payloads actually get asked. The trade is that
-- it supports containment only, not the key-existence operators.
--
-- Two consequences worth knowing before enabling this. Creating it FAILS
-- LOUDLY if any existing row is not valid JSON, and once it exists an insert
-- of malformed JSON is rejected rather than stored. It also costs write
-- throughput and disk on a table that only ever grows.
CREATE INDEX IF NOT EXISTS "{{name}}_events_data_gin"
    ON "{{name}}_events" USING gin ((data::jsonb) jsonb_path_ops);
