# katha

[![crates.io](https://img.shields.io/crates/v/katha.svg)](https://crates.io/crates/katha)
[![docs.rs](https://img.shields.io/docsrs/katha)](https://docs.rs/katha)
[![CI](https://github.com/manchhq/katha/actions/workflows/ci.yml/badge.svg)](https://github.com/manchhq/katha/actions/workflows/ci.yml)

**कथा** — Hindi for "story" or "narrative." Event sourcing is the story of everything that happened in your system.

Companion to [`kathputli`](https://github.com/manchhq/kathputli) (कठपुतली — the actor framework in the Manch family).

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
The SQLite/Postgres backend now lives in the separate [`katha-sqlx`](https://crates.io/crates/katha-sqlx) crate.

## Usage

```toml
[dependencies]
# Core (traits + types, includes the macros feature by default):
katha = "0.2"

# SQLite/Postgres backend:
katha-sqlx = "0.2"
```

## Notes on version type

Event versions are `u32`. Streams are intentionally time-sliced (one stream per entity-day or similar) so version numbers stay small. No snapshotting needed.

## Typed error roadmap

The public API currently uses `anyhow::Result`. Typed errors at the `EventStore` / `CommandStore` trait boundary are planned — tracked in pi-health-apps/pi_dx#408.

See [arch.md](https://github.com/manchhq/katha/blob/main/katha/arch.md) for design rationale.
