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

## Backends

Both the SQLite and Postgres `sqlx` drivers are enabled together (SQLite via the
bundled `libsqlite3-sys`). Splitting them into per-driver Cargo features is
planned for a future release.

## License

Dual-licensed under MIT OR Apache-2.0.
