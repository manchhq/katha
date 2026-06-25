# katha-macros

[![crates.io](https://img.shields.io/crates/v/katha-macros.svg)](https://crates.io/crates/katha-macros)
[![docs.rs](https://img.shields.io/docsrs/katha-macros)](https://docs.rs/katha-macros)

Proc-macro helpers for [`katha`](https://github.com/manchhq/katha) — the event sourcing crate.

## `#[derive(EventName)]`

Implements `katha::traits::event_name::EventName` for a struct or enum, which
is required to use `EventWrite::from_payload`.

```rust
use katha::types::event_write::EventWrite;
use katha_macros::EventName;
use uuid::Uuid;

#[derive(Clone, Debug, EventName)]
#[event_name = "Patient.Created"]
struct PatientCreated {
    id: String,
}

let event = EventWrite::from_payload(
    Uuid::new_v4(),
    Some(Uuid::new_v4()),
    None,
    PatientCreated { id: "p-1".to_string() },
    None::<()>,
);
assert_eq!(event.name, "Patient.Created");
```

Without the `#[event_name = "..."]` attribute the struct name is used as-is.

## Why a separate crate?

Proc-macro crates must be compiled as a separate compiler artifact by the Rust
toolchain — they cannot be inlined into a regular crate. Everything else in
katha is in the main `katha` crate behind Cargo feature flags. This is the
single exception forced by the language.

Enable via `katha`'s `macros` feature:

```toml
katha = "0.2"
```
