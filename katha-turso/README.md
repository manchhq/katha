# katha-turso

Persistent local-first event storage with explicit Turso push/pull. Uses
[`turso::sync`](https://docs.turso.tech/sdk/rust/reference), rather than libSQL
embedded replicas. Requires **Rust 1.88 or newer**. The existing Katha event and
actor APIs remain unchanged. `katha-libsql` continues to provide ordinary local
SQLite and remote libSQL access.

```toml
[dependencies]
katha = "0.2"
katha-turso = "0.2"
```

```rust,no_run
use katha::{EventStore, EventWrite, ExpectedVersion};
use katha_turso::{SyncConfig, SyncDatabase};
use serde_json::Value;

# async fn example() -> anyhow::Result<()> {
let mut config = SyncConfig::new("data/todos.db")
    .with_remote_url(std::env::var("TURSO_DATABASE_URL")?);
if let Ok(token) = std::env::var("TURSO_AUTH_TOKEN") {
    config = config.with_auth_token(token);
}
let database = SyncDatabase::open(config).await?;
let events = database.event_store("todo")?;
let stream = database.device_stream("todos");
let event = EventWrite {
    id: uuid::Uuid::new_v4(), correlation_id: None, causation_id: None,
    name: "TodoAdded".into(), data: serde_json::json!({"title": "Buy milk"}),
    metadata: None::<Value>,
};
// This transaction is local, including when the device is offline.
events.append_event(&stream, &ExpectedVersion::Any, &event).await?;
database.push().await?;
let changed = database.pull().await?;
let status = database.status().await?;
println!("changed={changed}, pending operations={}", status.pending_operations);
# Ok(())
# }
```

## Opening and credentials

`SyncDatabase::open` always disables network bootstrap. It creates parent
directories and initializes the event schema locally. First use and reopening an
existing replica both work offline; no authentication request occurs. To reopen
without credentials or a URL, use `SyncConfig::new(path)`; the sync SDK retains
the previously configured remote URL. Supply credentials before synchronizing.

Credentials stay in memory and are omitted from debug output and persisted error
messages. Rotate them without reopening:

```rust,ignore
database.set_auth_token(Some(refreshed_token));
database.push().await?;
```

Expired/missing credentials stop synchronization while local reads and appends
remain available. `SyncError` exposes the failed operation, a kind
(`Authentication`, `Unavailable`, `Protocol`, `Configuration`, `Interrupted`, or
`LocalStorage`), and an actionable message. `status()` works offline and reports
pending CDC operations, last successful push/pull times, remote revision, and the
last error. Pending operations include schema changes as well as
events; they are not an event count. Error and interrupted-attempt state survives
restart. Success clears the last error. The configurable sync timeout defaults
to 30 seconds.

## Delivery and device streams

Every event append is a local atomic transaction. Push/pull and event operations
are serialized through the shared database handle, so a pull cannot rebase an
active append. A sync can hold this gate until completion or timeout; choose an
appropriate timeout for your app. Clone the handle to share it; a file lock
rejects opening the same replica twice.

Use `database.device_stream(logical_id)` for every append. It returns a physical
stream ID prefixed with the device's persistent UUID. Appends to another device's
stream are rejected; reads can replay all devices' streams. Each device has its
own `(stream_id, version)` sequence. Stream heads are derived from the immutable
event log; no mutable stream-head rows are synchronized. UUIDs, payloads, metadata, and timestamps
survive import unchanged. `ExpectedVersion::Exact(n)` expects the next version,
starting at zero, as in the other Katha backends.

Retry push/pull using the **same replica and sync metadata** after lost responses,
timeouts, or interruptions. The official SDK retains pending CDC changes and
remote revision/client watermarks; Katha does not clear an independent outbox or
reinsert downloaded events. Repeated delivery preserves event IDs. The live
integration suite exercises an upstream commit with its response dropped, a
cancelled push, restarting before retry, and two-device convergence.

Business conflicts remain an application concern. Device streams preserve
separate histories; the app decides how concurrent todo events affect its model.
No CRDT or cross-device stream concurrency protocol is added.

## Local projections

After `pull()` returns, existing `EventStore` reads see the imported events.
Enumerate streams with `get_streams(AllStreams)` and replay each one from its
local projection checkpoint. Do not use timestamps as a delivery cursor: a newly
imported event can have an older creation time.

```rust,ignore
for stream in events.get_streams(&StreamsReadFilter::AllStreams).await? {
    let range = match database.projection_checkpoint("todos", "todo", &stream.id).await? {
        Some(u32::MAX) => continue,
        Some(version) => EventsReadRange::FromVersion(version + 1),
        None => EventsReadRange::AllEvents,
    };
    for event in events.get_events(&stream.id, &range).await? {
        apply_to_local_projection(&event).await?;
        database.set_projection_checkpoint("todos", "todo", &stream.id, event.version).await?;
    }
}
```

Device identity, projection checkpoints, and error status live in `path.local.db`,
which is never pushed or pulled. The checkpoint helper commits separately from
application projection writes. For crash-safe projection effects, make processing
idempotent by event ID, or store projection effects and checkpoints atomically in
your application's own local projection database. Imported events do not emit
push notifications; explicitly catch up after a successful pull or on startup.

Keep the replica, its WAL/revert/metadata files, and `path.local.db` together when
backing it up. Never open the synced replica with SQLx, libSQL, plain SQLite, or
the non-sync Turso builder. Compact it through `database.checkpoint()` only; the
sync SDK owns its WAL invariants. Each device must create its own replica rather
than clone another device's identity and sync metadata.

## Remote compatibility and tests

The remote must implement **Turso Sync**, not merely expose a libSQL SQL endpoint.
`database.verify_remote().await?` performs a real pull, proving the endpoint can
speak the chosen protocol and durably applying any available remote changes.
It does not upload local events. An ordinary SQL connection or a URL scheme alone
is insufficient verification. Configure a dedicated test database before app
integration.

Offline tests need no credentials. The ignored live tests use
`KATHA_TEST_TURSO_URL` and `KATHA_TEST_TURSO_TOKEN` from the environment. They leave
uniquely namespaced test events in that dedicated database. The retry test also
requires Python 3 for a local fault proxy, which forwards auth without logging it.

```sh
cargo test -p katha-turso
cargo test -p katha-turso -- --ignored
```
