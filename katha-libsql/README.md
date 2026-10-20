# katha-libsql

Turso/libSQL storage for [katha](https://crates.io/crates/katha). Implements the
core `EventStore` and `CommandStore` traits without depending on SQLx or macros.
Uses the official [libSQL Rust client](https://docs.turso.tech/sdk/rust/quickstart).

```toml
[dependencies]
katha = "0.3"
katha-libsql = "0.3"
```

```rust,no_run
use katha::{CommandStore, EventStore, EventWrite, ExpectedVersion};
use katha_libsql::{LibsqlCommandStore, LibsqlDatabase, LibsqlEventStore};
use serde_json::Value;

# async fn example() -> anyhow::Result<()> {
let database = LibsqlDatabase::new_remote(
    &std::env::var("TURSO_DATABASE_URL")?,
    &std::env::var("TURSO_AUTH_TOKEN")?,
).await?;
// For offline use: LibsqlDatabase::new_local("events.db").await?
// For tests: LibsqlDatabase::new_memory().await?
let events = LibsqlEventStore::new_from_database("app", database.clone())?;
let commands = LibsqlCommandStore::new_from_database("app", database)?;
<LibsqlEventStore as EventStore<Value, Value>>::ensure_events_table(&events).await?;
<LibsqlCommandStore as CommandStore<Value>>::ensure_commands_table(&commands).await?;
let event = EventWrite {
    id: uuid::Uuid::new_v4(),
    correlation_id: None,
    causation_id: None,
    name: "Created".into(),
    data: serde_json::json!({"title": "Hello"}),
    metadata: None::<Value>,
};
events.append_event("item-1", &ExpectedVersion::NoStream, &event).await?;
# Ok(())
# }
```

Each store also offers `new_memory(name)`, `new_local(name, path)` and
`new_remote(name, url, token)` constructors. Share a `LibsqlDatabase` when event
and command stores should use the same in-memory database. Local paths use the
client's filesystem API directly; parent directories must already exist.
`LibsqlDatabase::from_database` accepts a caller-configured `libsql::Database`.
Tables are created explicitly with `ensure_events_table` / `ensure_commands_table`.
Use distinct store names to isolate table namespaces.

Appends use `BEGIN IMMEDIATE`: stream lookup, expected-version check, event
inserts, and stream update run in one transaction. Failed batches roll back.
Versions start at zero; `ExpectedVersion::Exact(n)` expects the **next** available
version, matching `katha-sqlx`. Empty batches return no events without modifying
or checking a stream. Serialization and version overflow return errors.

Reads include ordered stream replay, inclusive version ranges, correlation and
causation lookups, all core stream filters, and command pagination (newest first, matching SQLx). Event push
notifications and SQLx-specific projection helpers are not provided. For remote
writers, fetch authoritative stream state and retry lock/transport errors as
appropriate for your application; an uncertain commit response requires checking
the caller-supplied event IDs before retrying.

Local libSQL integration tests run without credentials. An opt-in remote test
uses `KATHA_TEST_TURSO_URL` and `KATHA_TEST_TURSO_TOKEN`, with a unique namespace
per run. It creates tables in the configured database, so use a dedicated test
database. The test cleans up its tables after running.

```sh
cargo test -p katha-libsql remote_turso_contract -- --ignored
```
