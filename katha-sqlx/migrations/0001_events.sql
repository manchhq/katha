-- Version columns are BIGINT, not INTEGER. Versions are bound and decoded as
-- i64 in Rust. On Postgres INTEGER is i32 and would reject an i64 bind, while
-- SQLite gives both INTEGER affinity, so BIGINT is portable across backends.
CREATE TABLE IF NOT EXISTS "{{name}}_streams" (
    id TEXT PRIMARY KEY,
    last_version BIGINT NOT NULL,
    last_updated_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "{{name}}_events" (
    id TEXT PRIMARY KEY,
    correlation_id TEXT,
    causation_id TEXT,
    stream_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    name TEXT NOT NULL,
    data TEXT NOT NULL,
    metadata TEXT,
    created_utc TEXT NOT NULL,
    UNIQUE(stream_id, version)
);

CREATE INDEX IF NOT EXISTS "{{name}}_events_correlation_id"
    ON "{{name}}_events" (correlation_id);

CREATE INDEX IF NOT EXISTS "{{name}}_events_causation_id"
    ON "{{name}}_events" (causation_id);
