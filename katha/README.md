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
- SQLite/Postgres backend via the separate [`katha-sqlx`](https://crates.io/crates/katha-sqlx) crate

## What it does not do

- No CQRS framework or batteries-included runtime
- No snapshotting or upcasting
- No opinionated service architecture

## Feature flags

| Feature | Description |
|---------|-------------|
| `macros` | Enables `#[derive(EventName)]` proc macro for `EventWrite::from_payload`. Brings in `katha-macros`. Enabled by default. |

The SQLite/Postgres backend now lives in the separate [`katha-sqlx`](https://crates.io/crates/katha-sqlx) crate.

## Usage

```toml
[dependencies]
# Core (traits + types, includes the macros feature by default):
katha = "0.2"

# SQLite/Postgres backend:
katha-sqlx = "0.2"
```

## The functional core

`katha` is functional-first: your domain is **pure functions over events**, and
all I/O lives at the edges. An `Aggregate` is three pure functions — `init` (the
empty state), `apply` (fold one event into state), and `execute` (decide which
events a command produces, or reject it). No mutation, no framework, no I/O:

```rust
use katha::Aggregate;
use anyhow::Result;

#[derive(Clone)]
struct Account {
    balance: i64,
}

enum Command {
    Deposit(i64),
    Withdraw(i64),
}

#[derive(Clone)]
enum Event {
    Deposited(i64),
    Withdrawn(i64),
}

struct Bank;

impl Aggregate<Account, Command, Event> for Bank {
    // the empty state
    fn init(&self) -> Account {
        Account { balance: 0 }
    }

    // fold one event into state — pure
    fn apply(&self, state: Account, event: &Event) -> Account {
        match event {
            Event::Deposited(n) => Account { balance: state.balance + n },
            Event::Withdrawn(n) => Account { balance: state.balance - n },
        }
    }

    // decide events from a command — pure; return Err to reject
    fn execute(&self, state: &Account, command: &Command) -> Result<Vec<Event>> {
        match command {
            Command::Deposit(n) => Ok(vec![Event::Deposited(*n)]),
            Command::Withdraw(n) if state.balance >= *n => Ok(vec![Event::Withdrawn(*n)]),
            Command::Withdraw(_) => Err(anyhow::anyhow!("insufficient funds")),
        }
    }
}
```

The imperative shell is just glue: `make_handler` and
`load_state_and_expected_version` rehydrate state by folding stored events, run
`execute`, and append the result under an `ExpectedVersion` optimistic-concurrency
guard — backed by an `EventStore` such as the one in
[`katha-sqlx`](https://crates.io/crates/katha-sqlx).

## Notes on version type

Event versions are `u32`. Streams are intentionally time-sliced (one stream per entity-day or similar) so version numbers stay small. No snapshotting needed.

## Typed error roadmap

The public API currently uses `anyhow::Result`. Typed errors at the `EventStore` / `CommandStore` trait boundary are planned — tracked in pi-health-apps/pi_dx#408.

See [arch.md](https://github.com/manchhq/katha/blob/main/katha/arch.md) for design rationale.
