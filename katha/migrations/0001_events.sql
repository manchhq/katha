CREATE TABLE IF NOT EXISTS "{{name}}_streams" (
    id TEXT PRIMARY KEY,
    last_version INTEGER NOT NULL,
    last_updated_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "{{name}}_events" (
    id TEXT PRIMARY KEY,
    correlation_id TEXT,
    causation_id TEXT,
    stream_id TEXT NOT NULL,
    version INTEGER NOT NULL,
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
