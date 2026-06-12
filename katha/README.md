# katha

**कथा** — Hindi for "story" or "narrative." Event sourcing is the story of everything that happened in your system.

Companion to [`kathputli`](https://github.com/manchhq) (कठपुतली — the actor framework in the Manch family).

---

A small, explicit event sourcing core for Rust. Inspired by the F# [CosmoStore](https://github.com/Dzoukr/CosmoStore) by Roman Provazník.

## What it provides

- `EventStore` trait — append and read event streams
- `CommandStore` trait — append-only command log
- `Aggregate` trait + `make_handler` — pure domain command handling
- `ExpectedVersion` — optimistic concurrency guard
- Event and stream types
- Optional `#[derive(EventName)]` macro via the `macros` feature
- Optional SQLite/Postgres backend via the `sqlx` feature

## What it does not do

- No CQRS framework or batteries-included runtime
- No snapshotting or upcasting
- No opinionated service architecture

## Feature flags

| Feature | Description |
|---------|-------------|
| `macros` | Enables `#[derive(EventName)]` proc macro for `EventWrite::from_payload`. Brings in `katha-macros`. |
| `sqlx` | Enables `SqlxEventStore` and `SqlxCommandStore` — SQLite and Postgres backends. Backends are Cargo features, not sibling crates (the same approach sqlx itself uses for postgres/sqlite/mysql). |

## Usage

```toml
[dependencies]
# Core only (traits and types):
katha = { version = "0.1", features = ["macros"] }

# With SQLite/Postgres backend:
katha = { version = "0.1", features = ["macros", "sqlx"] }
```

## Why `sqlx` is a feature, not a separate crate

`katha-sqlx` was originally a sibling crate. We folded it behind `katha`'s `sqlx` feature for the same reason sqlx itself gates postgres/sqlite/mysql as features: it avoids lockstep version maintenance across N crates and keeps the dependency graph simpler for consumers who only need the core traits.

`katha-macros` remains a separate crate because proc-macro crates must be compiled separately by the Rust toolchain. It cannot be inlined.

## Notes on version type

Event versions are `u32`. Streams are intentionally time-sliced (one stream per entity-day or similar) so version numbers stay small. No snapshotting needed.

## Typed error roadmap

The public API currently uses `anyhow::Result`. Typed errors at the `EventStore` / `CommandStore` trait boundary are planned — tracked in pi-health-apps/pi_dx#408.

See `arch.md` for design rationale.
