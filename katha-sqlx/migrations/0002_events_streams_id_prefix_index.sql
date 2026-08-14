-- Postgres only. `StreamsReadFilter::IdPrefix` compiles to `id LIKE 'prefix%'`,
-- and Postgres will only use a plain btree for a prefix LIKE when the database
-- collation is C/POSIX or the index uses text_pattern_ops. Under en_US.UTF-8 --
-- the default nearly everywhere -- the primary key index cannot serve it, so
-- every per-prefix stream lookup degrades to a sequential scan.
--
-- SQLite has no opclasses and never runs this file.
CREATE INDEX IF NOT EXISTS "{{name}}_streams_id_prefix"
    ON "{{name}}_streams" (id text_pattern_ops);
