CREATE TABLE IF NOT EXISTS katha_events (
    id TEXT PRIMARY KEY, namespace TEXT NOT NULL,
    correlation_id TEXT, causation_id TEXT, stream_id TEXT NOT NULL,
    version BIGINT NOT NULL, name TEXT NOT NULL, data TEXT NOT NULL,
    metadata TEXT, created_utc TEXT NOT NULL,
    UNIQUE(namespace, stream_id, version)
);
CREATE INDEX IF NOT EXISTS katha_events_correlation ON katha_events(namespace, correlation_id);
CREATE INDEX IF NOT EXISTS katha_events_causation ON katha_events(namespace, causation_id);
