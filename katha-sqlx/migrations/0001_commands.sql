CREATE TABLE IF NOT EXISTS "{{name}}_commands" (
    id TEXT PRIMARY KEY,
    correlation_id TEXT NOT NULL,
    data TEXT NOT NULL,
    name TEXT NOT NULL,
    created_utc TEXT NOT NULL
);
