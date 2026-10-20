# katha-rusqlite

A typed rusqlite implementation of `katha::EventStore`. SQLite work runs on
Tokio blocking workers. Appends use immediate transactions, preserve supplied
UUIDs and metadata, and enforce optimistic concurrency and immutable event IDs.
Versions begin at zero; `ExpectedVersion::Exact(n)` expects the next version.

```rust
use katha_rusqlite::RusqliteEventStore;
use serde_json::Value;
# async fn example() -> anyhow::Result<()> {
let store = RusqliteEventStore::<Value, Value>::open(":memory:", "orders").await?;
let backlog = store.pending_events("order-summary").await?;
for event in backlog {
    // Commit your projection effects before acknowledging.
    store.acknowledge("order-summary", event.id).await?;
}
# Ok(()) }
```

Clone handles to share local append notifications. `pending_events` uses durable
per-projection event-ID receipts, so restart recovery and late events do not
depend on timestamps. Projection effects must be idempotent: a crash between
effects and acknowledgement may deliver the event again. `apply_projection_once`
accepts a SQLite transaction callback and commits read-model effects and the receipt
atomically. Backlogs preserve version order within each stream.

Enable the optional `powersync` feature for `from_powersync`, `sync::event_table`
and fallible `watch_projection` streams. The separate `katha-powersync` crate is
a convenience entry point for the same implementation. Core katha and SQLx do
not depend on either adapter. Requires Rust 1.94+.
