# katha-sqlx

[![crates.io](https://img.shields.io/crates/v/katha-sqlx.svg)](https://crates.io/crates/katha-sqlx)
[![docs.rs](https://img.shields.io/docsrs/katha-sqlx)](https://docs.rs/katha-sqlx)
[![CI](https://github.com/manchhq/katha/actions/workflows/ci.yml/badge.svg)](https://github.com/manchhq/katha/actions/workflows/ci.yml)

SQLite/Postgres event-sourcing backend for [`katha`](https://github.com/manchhq/katha) — provides `SqlxEventStore` and `SqlxCommandStore` over [`sqlx`](https://github.com/launchbadge/sqlx).

## Install

```toml
[dependencies]
katha = "0.2"
katha-sqlx = "0.2"
```

## Quick start

```rust
use katha::traits::event_store::EventStore;
use katha_sqlx::SqlxEventStore;

# async fn demo() -> anyhow::Result<()> {
let store = SqlxEventStore::new_memory("demo").await?;
EventStore::<String, String>::ensure_events_table(&store).await?;
# Ok(())
# }
```

## Payload integrity and querying (Postgres)

`data` and `metadata` are `TEXT` holding `serde_json` output. The text-to-`jsonb`
cast is immutable, so Postgres can validate and index those columns without
changing their type. Both helpers are **opt-in** and are no-ops on SQLite.

```rust,ignore
store.ensure_payload_validation().await?;  // reject malformed payloads at append
store.ensure_payload_index().await?;       // make payload queries indexable
```

Measured on 50k events with nested payloads:

| | selective payload lookup | scan-shaped query | append cost | disk |
|---|---|---|---|---|
| neither (default) | 59.8 ms | 59.0 ms | baseline | — |
| `ensure_payload_validation` | — | — | +10% | none |
| `ensure_payload_index` | **0.065 ms** | 29.7 ms | +55% | 3.1 MB |
| hand-written narrow index | — | **5.3 ms** | +54% | 360 kB |

### When the GIN index is worth it

Take it when payload questions are **selective and their shape is not known
ahead of time** — the diagnostic case: *which event produced this state*, *find
every event carrying this id*. There it turns a 59.8 ms scan into a 0.065 ms
lookup, because nothing has to re-parse 50k rows of JSON.

Do not take it by reflex. A query matching a large fraction of the table only
improves about 2x, which is ordinary GIN behaviour. And in a well-built
event-sourced system the application does not query payloads at all — it
projects them into a read model and queries that. If that describes you, this
index costs 55% of your append throughput and buys nothing.

When one payload field is queried repeatedly and you know which, a narrow
expression index is smaller and faster than the GIN index for that one
question, and composes with it:

```sql
CREATE INDEX ON "demo_events" (((data::jsonb)->'payload'->>'type'));
```

The index uses `jsonb_path_ops`, chosen by measurement over the default
opclass: half the size and half the append overhead at the same query
performance. It supports containment (`@>`), not the key-existence operators.

Applying either helper **fails loudly** if any existing row is not valid JSON,
which is the intended outcome rather than something to smooth over.

## Backends

Both the SQLite and Postgres `sqlx` drivers are enabled together (SQLite via the
bundled `libsqlite3-sys`). Splitting them into per-driver Cargo features is
planned for a future release.

## License

Dual-licensed under MIT OR Apache-2.0.
