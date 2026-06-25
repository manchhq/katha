# katha

[![CI](https://github.com/manchhq/katha/actions/workflows/ci.yml/badge.svg)](https://github.com/manchhq/katha/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/katha.svg)](https://crates.io/crates/katha)
[![docs.rs](https://img.shields.io/docsrs/katha)](https://docs.rs/katha)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

**कथा** — Hindi for "story" or "narrative." Event sourcing is the story of everything that happened in your system.

A small, explicit event sourcing core for Rust. Inspired by the F# [CosmoStore](https://github.com/Dzoukr/CosmoStore) by Roman Provazník. Part of the [Manch](https://github.com/manchhq) (मंच, "the stage") family: *Katha records, Kathputli performs, Manch presents.*

## Crates

| Crate | Description |
|-------|-------------|
| [`katha`](https://crates.io/crates/katha) | Core traits (`EventStore`, `CommandStore`, `Aggregate`), event/stream types, and optimistic concurrency |
| [`katha-macros`](https://crates.io/crates/katha-macros) | Proc-macro helpers — `#[derive(EventName)]`; re-exported via katha's `macros` feature |
| [`katha-sqlx`](https://crates.io/crates/katha-sqlx) | SQLite/Postgres backend — `SqlxEventStore` and `SqlxCommandStore` over sqlx |

## Usage

```toml
[dependencies]
# Core (traits + types, includes the macros feature by default):
katha = "0.2"

# SQLite/Postgres backend:
katha-sqlx = "0.2"
```

See the [katha README](https://github.com/manchhq/katha/blob/main/katha/README.md) for the full feature list and design notes, and [arch.md](https://github.com/manchhq/katha/blob/main/katha/arch.md) for design rationale.

## Development

Uses [just](https://github.com/casey/just) and [cargo-release](https://github.com/crate-ci/cargo-release):

```sh
just            # list recipes
just check      # fmt + clippy + tests — must pass clean
just test       # run the test suite (all features)
just release patch   # bump version, commit, tag, push — CI publishes to crates.io
```

Releases are tag-driven: `cargo release` bumps the shared workspace version, commits, tags `vX.Y.Z`, and pushes; the [release workflow](.github/workflows/release.yml) then builds, tests, and publishes all three crates to crates.io.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
