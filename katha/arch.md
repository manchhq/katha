# katha Architecture

This document describes the architecture and design intent of `katha` and its storage backends (for example `katha-sqlx`). It is intentionally pragmatic and Rust-native while preserving the DNA of the original F# CosmoStore.

The guiding ideas are: simple, composable building blocks; domain ownership; and a bias for correctness and clarity over framework magic. This aligns with the event-sourcing and DDD perspective from Jessica Kerr, Andrea (Roundcrisis), and Gien Verschatse, without being prescriptive.

## Goals

- Provide a small, stable core for event sourcing in Rust.
- Keep domain logic pure and explicit.
- Avoid unnecessary infrastructure assumptions.
- Support local-first storage (LibSQL/SQLite) as a first-class experience.
- Make it easy to add projections and messaging with `tokio`, without forcing a full actor framework.

## Non-Goals

- A full CQRS framework.
- Built-in snapshotting, upcasting, or advanced multi-tenant routing.
- Opinionated service architecture.

## Conceptual Model

- **Event**: A fact that happened in the domain.
- **Stream**: An ordered sequence of events for a single aggregate identity (or a bounded time slice if you intentionally keep streams short).
- **ExpectedVersion**: Optimistic concurrency guard.
- **Aggregate**: Domain logic that decides which events to emit given a command and current state.
- **Event Store**: Persistence and retrieval of events.
- **Projection**: A read model derived from events.

## Core Crate (`katha`)

The core crate provides small, explicit traits and types:

- `EventStore` trait
- `Aggregate` trait + `make_handler` helper
- Event and stream types
- Expected version types

The core crate does not assume any specific storage or messaging implementation. This keeps the surface area clean and makes testing straightforward.

## Storage Backend (`katha-sqlx`)

The SQLx backend implements:

- Table-per-store naming (`{name}_events`, `{name}_streams`)
- Stream metadata for fast access
- Transactional append semantics
- Unique constraint on `(stream_id, version)`

This backend is aligned with a local-first Tauri architecture and avoids network assumptions.

## Stream Size and Versioning Strategy

We intentionally keep streams small (for example, time-sliced streams such as "per day" or "per shift") and avoid snapshotting by design. This is a deliberate strategy based on the guidance to keep streams short and focused, so replay cost stays reasonable and cognitive load stays low.

Implications:

- Version numbers use `u32` (4,294,967,295 events per stream). While streams are intentionally kept small via time-slicing, u32 removes the 65k surprise for general consumers and future-proofs the API before crates.io publication.
- Streams can represent a bounded time window rather than a lifetime aggregate.
- Long-lived aggregates can be represented by a sequence of streams, each with a clear boundary.

If this design ever changes, the version type and stream id strategy can evolve without rewriting the core.

## Command Handling

Commands are a boundary. They are not domain facts. You can store them for traceability, but they are not the source of truth.

We keep command storage separate and optional:

- The domain validates commands.
- The aggregate emits events.
- The event store persists events.

If you want to log commands, do it as an explicit side-effect, not as a substitute for events.

## Projections

Projections are kept out of the core. They are intentionally pluggable to avoid coupling to any specific runtime. The recommended approach in Rust is:

- Use `tokio` tasks for projection runners.
- Use `broadcast` or `mpsc` channels for event delivery.
- Keep projection state updates idempotent.

Minimal recommended interface (example only):

```rust
pub trait Projection<E> {
    fn name(&self) -> &'static str;
    fn apply(&self, event: &E) -> anyhow::Result<()>;
}
```

Projections can be wired into the event store by publishing `EventRead` records after append, or by replaying from storage at startup.

## Messaging and Async Runtime

Rust favors explicit async wiring. We lean into that with `tokio` rather than building a custom actor runtime.

Suggested patterns:

- Event bus: `tokio::sync::broadcast` for fanout or `mpsc` for work queues.
- Projection workers: spawned tasks that consume event streams.
- Idempotency: use unique constraints or de-dup tables if needed.

Actors are optional. If you choose to use them, use them at the boundaries (UI -> command handler, or event -> projection), not inside domain logic.

## DDD Alignment

- Domain logic is pure and testable (`Aggregate` trait).
- Infrastructure concerns live in separate crates.
- Ubiquitous language stays in domain crates, not in the store.

The event store is deliberately generic to avoid leaking infrastructure into the domain.

## Error Handling

- The core uses `anyhow::Result` for ergonomic errors at boundaries.
- Concurrency errors are surfaced explicitly (e.g., `ExpectedVersion` mismatch).

## Extensibility Points

- Alternative backends (Postgres, Dynamo, in-memory).
- Optional derive macros (for example `#[derive(EventName)]` in `katha-macros`) for explicit ergonomics.

### Macros Feature

`EventWrite::from_payload` requires `Payload: EventName`. For production use, enable the `macros` feature:

```toml
katha = { version = "...", features = ["macros"] }
```

Then derive `EventName` on your event types:

```rust
use katha_macros::EventName;

#[derive(Clone, Debug, EventName)]
enum MyEvent { ... }
```

Without the feature, implement `EventName` manually or add `katha-macros` as a direct dependency.
- Event upcasters (if you add schema evolution).
- Snapshot store (if stream size strategy changes).
- Background workers for projections or sync.

## Open Questions / Future Work

- Formalizing a projection runner crate.
- Optional command de-dup or inbox/outbox patterns.
- Schema evolution strategy for long-lived deployments.

---

The core commitment is to keep the event store small, explicit, and Rust-native while remaining compatible with event-sourcing principles and DDD practices.
