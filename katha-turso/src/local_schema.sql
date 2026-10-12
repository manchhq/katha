CREATE TABLE IF NOT EXISTS device (id INTEGER PRIMARY KEY, uuid TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection TEXT NOT NULL, namespace TEXT NOT NULL, stream_id TEXT NOT NULL,
    version BIGINT NOT NULL, updated_utc TEXT NOT NULL,
    PRIMARY KEY(projection, namespace, stream_id)
);
CREATE TABLE IF NOT EXISTS sync_status (id INTEGER PRIMARY KEY, in_flight TEXT, last_error TEXT);
INSERT INTO sync_status (id) VALUES (1) ON CONFLICT(id) DO NOTHING;
