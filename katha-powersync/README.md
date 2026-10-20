# katha-powersync

Managed PowerSync event storage for katha. This is a convenience entry point for
`katha-rusqlite` with its optional `powersync` feature enabled. Rust 1.94+.

Add `event_table()` to your PowerSync `Schema`, construct a `PowerSyncDatabase`
with your application's environment, then use
`PowerSyncEventStore::<Payload, Metadata>::from_powersync(database.clone(), "orders")`.

```rust,no_run
use katha_powersync::{PowerSyncEventStore, event_table, powersync};
use futures_lite::StreamExt;
use serde_json::Value;

// Include event_table() in the schema used to construct database.
# async fn project(database: powersync::PowerSyncDatabase) -> anyhow::Result<()> {
let store = PowerSyncEventStore::<Value, Value>::from_powersync(database, "orders").await?;
let mut batches = Box::pin(store.watch_projection("order-summary")?);
while let Some(batch) = batches.next().await {
    for event in batch? {
        store.apply_projection_once("order-summary", event.id, |tx, event| {
            tx.execute_batch("CREATE TABLE IF NOT EXISTS projected_orders (event_id TEXT PRIMARY KEY)")?;
            tx.execute("INSERT INTO projected_orders VALUES (?1)", [event.id.to_string()])?;
            Ok(())
        }).await?;
    }
}
# Ok(()) }
```

The application owns the SDK connection, authentication, sync streams, and backend
connector. No credentials or remote endpoint are required for offline appends.

Local batches use the managed SDK writer and a single SQLite transaction. Event
rows and the SDK CRUD queue commit or roll back together. Supplied event UUIDs,
correlation/causation IDs, names, JSON metadata, and payloads are preserved.
Repeated UUIDs are rejected rather than replaced.

Use `watch_projection("order-summary")` with `futures_lite::StreamExt` to receive
fallible backlog batches after initial subscription and local or remote changes.
After committing each event's projection effects, call `acknowledge` with its UUID.
Receipts are ordinary local tables, excluded from synchronization. Restart,
notification lag, and late events with old timestamps are recovered by scanning
`pending_events`, using event-ID receipts. Processing is at least once: projection
effects must be idempotent. For local SQLite read models,
`apply_projection_once` applies effects and the receipt in the same managed
transaction. `EventStore::event_appended` also provides
best-effort notifications (including initial replay); errors close that receiver.
Use the fallible projection stream to handle errors explicitly.

Remote arrival only reads the SDK's managed tables; it never re-appends events or
adds them to the upload queue. Local version mismatches return katha's typed
`ConcurrencyConflict`. Multiple UUIDs claiming the same stream/version return
`StreamVersionConflict` on reads, appends, and projection scans, preserving both
rows for reconciliation.

The upload connector must enforce **insert-only** event writes, a unique
`(namespace, stream_id, version)` constraint, globally unique event IDs, and an
atomic batch on the server. Acknowledge the SDK CRUD transaction only after server
acceptance. On server conflicts, return an error and retain the queued batch for
explicit application reconciliation. Offline checks cannot prevent two devices
from choosing the same next version. PowerSync's default last-write-wins upload
policies are unsuitable for immutable event histories. Never upload with upsert.

PowerSync SDK 0.1.1 is experimental. End-to-end authentication and server conflict
resolution remain application responsibilities; this adapter does not select a
remote conflict winner.
