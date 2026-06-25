# katha 0.2.0 — split `katha-sqlx`, airtight release

**Date:** 2026-06-25
**Status:** Approved (design)
**Version target:** 0.1.1 → 0.2.0

## Goal

Make the `katha` workspace airtight for a polished crates.io release, aligning
publish practices with the sibling project `manchhq/kathputli`, and split the
SQLite/Postgres backend into its own `katha-sqlx` crate.

## Core invariant

**Macros are coupled to the core, never to a backend.** `katha-macros`
(`#[derive(EventName)]`) is reachable through the core `katha` crate via its
`macros` feature. Every backend crate (`katha-sqlx` today, a possible
`katha-diesel` tomorrow) depends only on `katha` and therefore gets the same
derive macro for free — without depending on `sqlx` or any other backend.
Verified precondition: `katha/src/sqlx_store/` uses **no** macros today.

## 1. Workspace layout — three crates

| Crate          | Role                                   | Depends on            |
|----------------|----------------------------------------|-----------------------|
| `katha`        | core traits + types (+ optional macros)| `katha-macros` (opt)  |
| `katha-macros` | `#[derive(EventName)]` proc-macro      | —                     |
| `katha-sqlx`   | **new** — SQLite/Postgres backend      | `katha`               |

No crate depends on a backend; `katha` never depends on `katha-sqlx`. No cycles.

### Move list
- `katha/src/sqlx_store/*` (12 files) → `katha-sqlx/src/`. The module becomes the
  crate root:
  - `crate::sqlx_store::error` → `crate::error` (and siblings)
  - `crate::types::*` → `katha::types::*`
  - `crate::traits::event_store` / `command_store` → `katha::traits::*`
- `katha/src/sqlx_store/mod.rs` content → `katha-sqlx/src/lib.rs` (carrying the
  re-exports currently gated in `katha/src/lib.rs`: `SqlxEventStore`,
  `SqlxCommandStore`, `CommandCursor`, `CommandCursorPage`, `EventCursorPage`,
  `EventNotification`, `ProjectionRunStats`, `DEFAULT_NOTIFICATION_BUFFER`).
- Test files → `katha-sqlx/tests/`: `sqlx_event_store_tests.rs`,
  `sqlx_command_store_tests.rs`, `sqlx_property_tests.rs`,
  `sqlx_behavioral_projection_flow_tests.rs`, and `tests/common/` (used only by
  these four). Update `common/mod.rs` imports from `katha::{SqlxCommandStore,
  SqlxEventStore}` to `katha_sqlx::{...}`.
- `event_write_helpers_tests.rs` (uses macros, not sqlx) **stays** in `katha`.

### New files for `katha-sqlx`
- `Cargo.toml` with full crates.io metadata (see §4).
- `README.md` with badges (see §5).
- `LICENSE-MIT` + `LICENSE-APACHE` (copies of the root files).

## 2. Features

### `katha`
```toml
[features]
default = ["macros"]
macros  = ["dep:katha-macros"]
```
Removed: the `sqlx` feature, deps `sqlx` / `libsqlite3-sys` / `tokio-util`, and
the four `[[test]] required-features = ["sqlx"]` entries. `katha/src/lib.rs`
loses the `#[cfg(feature = "sqlx")]` module and its re-exports.

### `katha-sqlx`
Backend drivers exposed as features so the C-built SQLite dep is opt-out:
```toml
[features]
default  = ["sqlite"]
sqlite   = ["sqlx/sqlite", "dep:libsqlite3-sys"]
postgres = ["sqlx/postgres"]
```
Open implementation detail: current code enables `sqlite`, `postgres`, and `any`
together. During implementation, confirm whether the code compiles cleanly under
each driver alone; if separating is non-trivial, fall back to a single feature
set that enables both (documented in the README). Either way `libsqlite3-sys`
(bundled) must only be pulled when `sqlite` is on.

## 3. Tests → nextest

`ci.yml` (push to main + PRs):
- `actions/checkout@v5`, `dtolnay/rust-toolchain@stable` (rustfmt, clippy),
  `Swatinem/rust-cache@v2`, `taiki-e/install-action@nextest`.
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-features --all-targets -- -D warnings`
- `cargo nextest run --workspace` (default features)
- `cargo nextest run --workspace --all-features`
- `cargo test --doc --all-features` (nextest does not run doctests)

`justfile`: `test`, `ci`, and a new `doc` recipe switch to nextest; keep
`build`, `fmt`, `lint`, `check`, `release`, `release-dry`.

## 4. docs.rs metadata

Add to `katha` **and** `katha-sqlx` `Cargo.toml`:
```toml
[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
```
Rationale: feature-gated APIs (macros; sqlx backends) otherwise do not render on
docs.rs, whose default build uses default features only.

## 5. READMEs (per-crate — crates.io renders the crate README, not the root)

Each crate `README.md` gets shields badges: crates.io version, docs.rs, CI.
- `katha/README.md`: add badges; fix the relative `kathputli` link; reference
  `katha-sqlx` for the backend; bump install snippets to `0.2`.
- `katha-macros/README.md`: add badges; change relative links (`../katha`) to
  absolute GitHub URLs so they don't break on crates.io; install snippet `0.2`.
- `katha-sqlx/README.md` (new): badges, quick start (install + minimal store
  setup), feature-flag table (`sqlite`/`postgres`), link back to `katha`.
- root `README.md`: list all three crates, the install matrix (core /
  macros / backend), bump snippets to `0.2`.

## 6. Release workflow

`release.yml` (`on: push: tags: ['v*']`):
- Add tag↔version guard before publishing:
  ```sh
  VERSION=$(cargo pkgid -p katha | cut -d'#' -f2)
  [ "v$VERSION" = "$GITHUB_REF_NAME" ] || { echo "tag/version mismatch"; exit 1; }
  ```
- Build + test (nextest, all-features) as a gate.
- Publish in dependency order, each waiting for the index:
  1. `cargo publish -p katha-macros`
  2. `cargo publish -p katha`
  3. `cargo publish -p katha-sqlx`
- Secret: `CARGO_REGISTRY_TOKEN` (already configured).

## 7. Version → 0.2.0

Shared workspace version 0.1.1 → 0.2.0 (removing the `sqlx` feature + new crate
layout is breaking pre-1.0). Cut via `just release minor`. `katha-sqlx` joins
`[workspace.metadata.release]` shared-version; the existing `tag = false`
per-package override (on `katha-macros`) is applied to `katha-sqlx` too so a
single `vX.Y.Z` tag is cut from `katha`.

## 8. Deferred (tracked, not in 0.2.0)

`diesel` backend → future `katha-diesel` crate (e.g. 0.3.0), depending only on
`katha`, reusing the same `EventName` macro from core per the core invariant.

## Acceptance criteria

- `cargo build --workspace --all-features` and `cargo nextest run --workspace
  --all-features` pass; `cargo test --doc --all-features` passes.
- `katha` no longer references `sqlx`; `katha-sqlx` compiles against published
  `katha` API (path dep in workspace).
- `cargo publish --dry-run` succeeds for all three crates.
- docs.rs metadata present on `katha` and `katha-sqlx`.
- Each crate README renders correct badges/links on crates.io (no relative
  links).
- `release.yml` guards tag↔version and publishes in `macros → katha → sqlx`
  order.
