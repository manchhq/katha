# katha

[![CI](https://github.com/manchhq/katha/actions/workflows/ci.yml/badge.svg)](https://github.com/manchhq/katha/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/katha.svg)](https://crates.io/crates/katha)
[![docs.rs](https://docs.rs/katha/badge.svg)](https://docs.rs/katha)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

**कथा** — Hindi for "story" or "narrative." Event sourcing is the story of everything that happened in your system.

A small, explicit event sourcing core for Rust. Inspired by the F# [CosmoStore](https://github.com/Dzoukr/CosmoStore) by Roman Provazník. Part of the [Manch](https://github.com/manchhq) (मंच, "the stage") family: *Katha records, Kathputli performs, Manch presents.*

## Crates

| Crate | Description |
|-------|-------------|
| [`katha`](katha/) | Core traits (`EventStore`, `CommandStore`, `Aggregate`), event/stream types, optimistic concurrency, and an optional SQLite/Postgres backend behind the `sqlx` feature |
| [`katha-macros`](katha-macros/) | Proc-macro helpers — `#[derive(EventName)]`; enabled via katha's `macros` feature |

Backends and helpers are Cargo **features**, not sibling crates — the same approach sqlx uses for its database drivers. `katha-macros` is the single exception, because proc-macro crates must be compiled separately by the toolchain.

## Usage

```toml
[dependencies]
# Core only (traits and types):
katha = { version = "0.1", features = ["macros"] }

# With SQLite/Postgres backend:
katha = { version = "0.1", features = ["macros", "sqlx"] }
```

See the [katha README](katha/README.md) for the full feature list and design notes, and [arch.md](katha/arch.md) for design rationale.

## Development

Uses [just](https://github.com/casey/just) and [cargo-release](https://github.com/crate-ci/cargo-release):

```sh
just            # list recipes
just check      # fmt + clippy + tests — must pass clean
just test       # run the test suite (all features)
just release patch   # bump version, commit, tag, push — CI publishes to crates.io
```

Releases are tag-driven: `cargo release` bumps the shared workspace version, commits, tags `vX.Y.Z`, and pushes; the [release workflow](.github/workflows/release.yml) then builds, tests, and publishes both crates to crates.io.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
