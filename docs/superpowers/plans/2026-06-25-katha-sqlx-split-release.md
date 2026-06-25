# katha 0.2.0 — sqlx split + airtight release — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the SQLite/Postgres backend into a new `katha-sqlx` crate and make the workspace airtight for a polished crates.io 0.2.0 release (nextest CI, docs.rs metadata, per-crate README badges, tag-guarded publish).

**Architecture:** Three-crate workspace — `katha` (core traits + types, optional `macros`), `katha-macros` (proc-macro), and new `katha-sqlx` (backend, depends only on `katha`). Backends never depend on macros; macros live in core and reach every backend through `katha`.

**Tech Stack:** Rust edition 2024, MSRV 1.85, sqlx 0.8, cargo-nextest, cargo-release, GitHub Actions.

## Global Constraints

- Edition `2024`, `rust-version = "1.85"`, `resolver = "3"`.
- License `MIT OR Apache-2.0`; dual `LICENSE-MIT` + `LICENSE-APACHE` in every published crate dir.
- Repository: `https://github.com/manchhq/katha`.
- Shared workspace version (`shared-version = true`); target bump `0.1.1` → `0.2.0`.
- **Core invariant:** `katha-sqlx` (and any future backend) must NOT depend on `katha-macros`. It depends on `katha` with `default-features = false`. Verified: `sqlx_store` uses no macros.
- All crates.io metadata fields populated: `description`, `license`, `repository`, `readme`, `keywords`, `categories`, `rust-version`.
- CI/tests run via `cargo nextest`; doctests via `cargo test --doc`.
- Publish order is `katha-macros` → `katha` → `katha-sqlx`.

---

### Task 1: Split `sqlx_store` into the `katha-sqlx` crate

This is one atomic refactor: the workspace does not compile in intermediate states (core still references `sqlx_store` while files move), so the whole move + import rewrite + core strip lands together.

**Files:**
- Create: `katha-sqlx/Cargo.toml`
- Create: `katha-sqlx/src/lib.rs`
- Create: `katha-sqlx/LICENSE-MIT`, `katha-sqlx/LICENSE-APACHE` (copies of root)
- Create: `katha-sqlx/README.md` (placeholder; filled in Task 4)
- Move: `katha/src/sqlx_store/{command_db,command_store,error,event_db,event_store,notifications,pagination,projection_runner,types,validate}.rs` → `katha-sqlx/src/`
- Move: `katha/tests/{sqlx_event_store_tests,sqlx_command_store_tests,sqlx_property_tests,sqlx_behavioral_projection_flow_tests}.rs` and `katha/tests/common/` → `katha-sqlx/tests/`
- Modify: root `Cargo.toml` (add member + `katha` workspace dep)
- Modify: `katha/Cargo.toml` (remove sqlx feature/deps/tests; set `default = ["macros"]`)
- Modify: `katha/src/lib.rs` (drop sqlx module + re-exports)

**Interfaces:**
- Produces crate `katha_sqlx` re-exporting at its root: `SqlxEventStore`, `SqlxCommandStore`, `CommandCursor`, `CommandCursorPage`, `EventCursorPage`, `EventNotification` (+ `DEFAULT_NOTIFICATION_BUFFER`), `ProjectionRunStats`.
- Consumes from `katha`: `katha::types::*`, `katha::traits::event_store::EventStore`, `katha::traits::command_store::CommandStore`.

- [ ] **Step 1: Create the crate dir and move source files (preserve history)**

```bash
cd /home/kunjee/Workspace/manchhq/katha
mkdir -p katha-sqlx/src katha-sqlx/tests
git mv katha/src/sqlx_store/command_db.rs        katha-sqlx/src/command_db.rs
git mv katha/src/sqlx_store/command_store.rs      katha-sqlx/src/command_store.rs
git mv katha/src/sqlx_store/error.rs              katha-sqlx/src/error.rs
git mv katha/src/sqlx_store/event_db.rs           katha-sqlx/src/event_db.rs
git mv katha/src/sqlx_store/event_store.rs        katha-sqlx/src/event_store.rs
git mv katha/src/sqlx_store/notifications.rs      katha-sqlx/src/notifications.rs
git mv katha/src/sqlx_store/pagination.rs         katha-sqlx/src/pagination.rs
git mv katha/src/sqlx_store/projection_runner.rs  katha-sqlx/src/projection_runner.rs
git mv katha/src/sqlx_store/types.rs              katha-sqlx/src/types.rs
git mv katha/src/sqlx_store/validate.rs           katha-sqlx/src/validate.rs
git mv katha/src/sqlx_store/mod.rs                katha-sqlx/src/lib.rs
git mv katha/tests/sqlx_event_store_tests.rs               katha-sqlx/tests/sqlx_event_store_tests.rs
git mv katha/tests/sqlx_command_store_tests.rs            katha-sqlx/tests/sqlx_command_store_tests.rs
git mv katha/tests/sqlx_property_tests.rs                 katha-sqlx/tests/sqlx_property_tests.rs
git mv katha/tests/sqlx_behavioral_projection_flow_tests.rs katha-sqlx/tests/sqlx_behavioral_projection_flow_tests.rs
git mv katha/tests/common                                 katha-sqlx/tests/common
cp LICENSE-MIT LICENSE-APACHE katha-sqlx/
printf '# katha-sqlx\n\nSQLite/Postgres backend for [`katha`](https://github.com/manchhq/katha). Filled in Task 4.\n' > katha-sqlx/README.md
```

- [ ] **Step 2: Rewrite intra-crate paths in the moved `src/` files**

The module is now the crate root. Apply (order-independent; prefixes are disjoint):

```bash
cd /home/kunjee/Workspace/manchhq/katha
for f in katha-sqlx/src/*.rs; do
  sed -i \
    -e 's|crate::sqlx_store::|crate::|g' \
    -e 's|crate::types::|katha::types::|g' \
    -e 's|crate::traits::|katha::traits::|g' \
    "$f"
done
```

`katha-sqlx/src/lib.rs` (the old `mod.rs`) keeps its `mod ...;` + `pub use ...;` lines unchanged — it is now the crate root and the `pub use` re-exports become the crate's public API. Verify it reads:

```rust
mod command_db;
mod command_store;
mod error;
mod event_db;
mod event_store;
mod notifications;
mod pagination;
mod projection_runner;
mod types;
mod validate;

pub use notifications::{DEFAULT_NOTIFICATION_BUFFER, EventNotification};
pub use pagination::{CommandCursor, CommandCursorPage, EventCursorPage};
pub use projection_runner::ProjectionRunStats;
pub use types::{SqlxCommandStore, SqlxEventStore};
```

- [ ] **Step 3: Write `katha-sqlx/Cargo.toml`**

```toml
[package]
name = "katha-sqlx"
version = { workspace = true }
edition = { workspace = true }
description = "SQLite/Postgres event-sourcing backend for katha (SqlxEventStore, SqlxCommandStore)"
license = { workspace = true }
repository = { workspace = true }
authors = { workspace = true }
readme = "README.md"
rust-version = { workspace = true }
keywords = ["event-sourcing", "cqrs", "sqlx", "sqlite", "postgres"]
categories = ["database", "asynchronous"]

[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]

[dependencies]
# Backend depends ONLY on core katha (no macros) — preserves the core invariant.
katha = { workspace = true, default-features = false }
anyhow = { workspace = true }
async-trait = { workspace = true }
chrono = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
uuid = { workspace = true }
tokio = { workspace = true, features = ["sync"] }
tokio-util = { workspace = true }
sqlx = { workspace = true, features = ["chrono", "uuid", "migrate", "sqlite", "postgres", "any"] }
libsqlite3-sys = { workspace = true }

[dev-dependencies]
katha = { workspace = true }
proptest = "1"
tempfile = { workspace = true }
tokio = { workspace = true }
```

Note: both SQLite and Postgres drivers ship together (matches the crate's prior single-`sqlx`-feature behaviour). Per-driver feature split is deferred — call it out in the README (Task 4).

- [ ] **Step 4: Update root `Cargo.toml` — add member and the `katha` workspace dep**

In `[workspace] members`, change to:

```toml
members = ["katha", "katha-macros", "katha-sqlx"]
```

In `[workspace.dependencies]`, add (next to the existing `katha-macros` line):

```toml
katha = { version = "0.1.1", path = "katha" }
```

(The version is bumped to `0.2.0` automatically by `cargo release` in Task 6.)

- [ ] **Step 5: Strip sqlx from `katha/Cargo.toml`**

Set `default = ["macros"]` and remove the `sqlx` feature, its three deps, and the four `[[test]]` blocks. The features + deps sections become:

```toml
[features]
default = ["macros"]
# Enable #[derive(EventName)] for EventWrite::from_payload. Required for production use.
macros = ["dep:katha-macros"]

[dependencies]
chrono = { workspace = true }
uuid = { workspace = true }
anyhow = { workspace = true }
async-trait = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
tokio = { workspace = true, features = ["sync"] }
katha-macros = { workspace = true, optional = true }

[dev-dependencies]
katha-macros = { workspace = true }
proptest = "1"
tokio = { workspace = true }
tempfile = { workspace = true }
```

Delete these blocks entirely from `katha/Cargo.toml`:

```toml
# sqlx feature dependencies
sqlx           = { ... optional = true }
libsqlite3-sys = { workspace = true, optional = true }
tokio-util     = { workspace = true, optional = true }

[[test]]
name = "sqlx_event_store_tests"
required-features = ["sqlx"]
# ... and the other three [[test]] blocks
```

- [ ] **Step 6: Strip sqlx from `katha/src/lib.rs`**

Remove the `#[cfg(feature = "sqlx")] pub mod sqlx_store;` line and the entire `// sqlx feature re-exports` block at the bottom. Final `katha/src/lib.rs`:

```rust
pub mod traits;
pub mod types;

// Re-exports for ergonomic imports
pub use traits::aggregate::{
    Aggregate, load_state_and_expected_version, make_handler, next_expected_version, rehydrate,
};
pub use traits::command_store::CommandStore;
pub use traits::event_store::EventStore;
pub use traits::version::Version;
pub use types::command_write::{CommandRead, CommandWrite};
pub use types::event_read::EventRead;
pub use types::event_read_range::EventsReadRange;
pub use types::event_write::EventWrite;
pub use types::expected_version::ExpectedVersion;
```

- [ ] **Step 7: Repoint imports in the moved test files**

Backend symbols now come from `katha_sqlx`, core symbols stay on `katha`. Symbols owned by `katha_sqlx`: `SqlxEventStore`, `SqlxCommandStore`, `CommandCursor`, `CommandCursorPage`, `EventCursorPage`, `EventNotification`, `DEFAULT_NOTIFICATION_BUFFER`, `ProjectionRunStats`. Everything else (`EventStore`, `CommandStore`, `EventWrite`, `EventRead`, types, `ExpectedVersion`, …) stays on `katha`.

`katha-sqlx/tests/common/mod.rs` header changes from `use katha::{SqlxCommandStore, SqlxEventStore};` to:

```rust
use katha_sqlx::{SqlxCommandStore, SqlxEventStore};
```

For each test file, split any `use katha::{...}` that mixes core + backend symbols into two `use` lines (one `katha::`, one `katha_sqlx::`). Then let the compiler confirm:

```bash
cargo build -p katha-sqlx --tests 2>&1 | grep -E "error\[|unresolved import" | head
```
Resolve any remaining `unresolved import` by moving the named symbol to the correct crate per the ownership list above. Expected end state: no errors.

- [ ] **Step 8: Build the whole workspace**

Run: `cargo build --workspace --all-features`
Expected: PASS (clean build, no `sqlx_store` references remain in `katha`).

Also confirm core no longer mentions sqlx:
```bash
grep -rn "sqlx" katha/src katha/Cargo.toml || echo "clean: no sqlx in core"
```
Expected: `clean: no sqlx in core`.

- [ ] **Step 9: Run the full test suite**

Run: `cargo test --workspace --all-features`
Expected: PASS — the four sqlx integration tests now run under `katha-sqlx`, `event_write_helpers_tests` still runs under `katha`.

- [ ] **Step 10: Clippy + fmt gate**

```bash
cargo fmt --all
cargo clippy --workspace --all-features --all-targets -- -D warnings
```
Expected: no warnings.

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -m "refactor: split sqlx backend into katha-sqlx crate

Core katha drops the sqlx feature; backend lives in katha-sqlx, depending
on katha with default-features=false so macros stay decoupled from backends.
default = [\"macros\"] for katha.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Migrate CI and justfile to nextest

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `justfile`

- [ ] **Step 1: Rewrite `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

env:
  CARGO_TERM_COLOR: always

jobs:
  test:
    name: Build & test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy
      - uses: Swatinem/rust-cache@v2
      - uses: taiki-e/install-action@nextest
      - name: Format check
        run: cargo fmt --all --check
      - name: Clippy
        run: cargo clippy --workspace --all-features --all-targets -- -D warnings
      - name: Test (default features)
        run: cargo nextest run --workspace
      - name: Test (all features)
        run: cargo nextest run --workspace --all-features
      - name: Doctests
        run: cargo test --doc --all-features
```

- [ ] **Step 2: Update `justfile` test/doc/ci recipes**

Replace the `test` recipe and add a `doc` recipe:

```make
# Run the test suite with all features (nextest) + doctests
test:
    cargo nextest run --workspace --all-features
    cargo test --doc --all-features

# Build docs the way docs.rs does (all features)
doc:
    cargo doc --workspace --all-features --no-deps
```

Keep `lint`, `check`, `build`, `fmt`, `release`, `release-dry` as-is.

- [ ] **Step 3: Verify locally (nextest must be installed)**

```bash
cargo nextest run --workspace --all-features
cargo test --doc --all-features
```
Expected: PASS. (If nextest is missing: `cargo install cargo-nextest --locked`.)

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml justfile
git commit -m "ci: run tests via cargo-nextest; bump checkout to v5

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Add docs.rs metadata to `katha`

(`katha-sqlx` already got its block in Task 1 Step 3.)

**Files:**
- Modify: `katha/Cargo.toml`

- [ ] **Step 1: Add the docs.rs block after `categories`**

```toml
[package.metadata.docs.rs]
all-features = true
rustdoc-args = ["--cfg", "docsrs"]
```

- [ ] **Step 2: Verify metadata builds**

Run: `cargo doc -p katha --all-features --no-deps`
Expected: PASS, docs generated for the `macros`-gated re-exports.

- [ ] **Step 3: Commit**

```bash
git add katha/Cargo.toml
git commit -m "docs: add docs.rs all-features metadata to katha

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Per-crate READMEs with badges + fix relative links

crates.io renders the crate-level README, so badges and absolute links must live in each crate's README. Use shields.io badges matching kathputli's style.

**Files:**
- Modify: `katha/README.md`, `katha-macros/README.md`, `README.md` (root)
- Create/replace: `katha-sqlx/README.md`

- [ ] **Step 1: Add a badge block to the top of `katha/README.md`**

Insert immediately under the `# katha` title:

```markdown
[![crates.io](https://img.shields.io/crates/v/katha.svg)](https://crates.io/crates/katha)
[![docs.rs](https://img.shields.io/docsrs/katha)](https://docs.rs/katha)
[![CI](https://github.com/manchhq/katha/actions/workflows/ci.yml/badge.svg)](https://github.com/manchhq/katha/actions/workflows/ci.yml)
```

Fix the companion link (line ~5) to absolute: `[`kathputli`](https://github.com/manchhq/kathputli)`. In the Usage block, point the backend install at the new crate and bump to `0.2`:

```toml
[dependencies]
# Core (traits + types, includes the macros feature by default):
katha = "0.2"

# SQLite/Postgres backend:
katha-sqlx = "0.2"
```

Replace the `sqlx` row of the feature-flags table with a sentence: "The SQLite/Postgres backend now lives in the separate [`katha-sqlx`](https://crates.io/crates/katha-sqlx) crate." Change the `arch.md` reference to an absolute URL: `https://github.com/manchhq/katha/blob/main/katha/arch.md`.

- [ ] **Step 2: Add badges + fix links in `katha-macros/README.md`**

Insert under the title:

```markdown
[![crates.io](https://img.shields.io/crates/v/katha-macros.svg)](https://crates.io/crates/katha-macros)
[![docs.rs](https://img.shields.io/docsrs/katha-macros)](https://docs.rs/katha-macros)
```

Change `[`katha`](../katha)` → `[`katha`](https://github.com/manchhq/katha)`, and bump the install snippet to `katha = "0.2"`.

- [ ] **Step 3: Write `katha-sqlx/README.md`**

```markdown
# katha-sqlx

[![crates.io](https://img.shields.io/crates/v/katha-sqlx.svg)](https://crates.io/crates/katha-sqlx)
[![docs.rs](https://img.shields.io/docsrs/katha-sqlx)](https://docs.rs/katha-sqlx)
[![CI](https://github.com/manchhq/katha/actions/workflows/ci.yml/badge.svg)](https://github.com/manchhq/katha/actions/workflows/ci.yml)

SQLite/Postgres event-sourcing backend for [`katha`](https://github.com/manchhq/katha) — provides `SqlxEventStore` and `SqlxCommandStore` over [`sqlx`](https://github.com/launchbadge/sqlx).

## Install

```toml
[dependencies]
katha = "0.2"
katha-sqlx = "0.2"
```

## Quick start

```rust
use katha::traits::event_store::EventStore;
use katha_sqlx::SqlxEventStore;

# async fn demo() -> anyhow::Result<()> {
let store = SqlxEventStore::new_memory("demo").await?;
EventStore::<String, String>::ensure_events_table(&store).await?;
# Ok(())
# }
```

## Backends

Both the SQLite and Postgres `sqlx` drivers are enabled together (SQLite via the
bundled `libsqlite3-sys`). Splitting them into per-driver Cargo features is
planned for a future release.

## License

Dual-licensed under MIT OR Apache-2.0.
```

- [ ] **Step 4: Update root `README.md`**

List all three crates (katha / katha-macros / katha-sqlx) with one-line roles, update the install matrix to the `0.2` snippets above, and ensure the workspace badges point at `ci.yml`. Keep the existing development/`just release` section.

- [ ] **Step 5: Sanity-check no relative links remain in published READMEs**

```bash
grep -rnE '\]\(\.\./|\]\(arch\.md' katha/README.md katha-macros/README.md katha-sqlx/README.md || echo "clean: no relative links"
```
Expected: `clean: no relative links`.

- [ ] **Step 6: Commit**

```bash
git add README.md katha/README.md katha-macros/README.md katha-sqlx/README.md
git commit -m "docs: per-crate README badges, katha-sqlx README, absolute links

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Tag-guarded 3-crate release workflow

**Files:**
- Modify: `.github/workflows/release.yml`

- [ ] **Step 1: Rewrite `.github/workflows/release.yml`**

```yaml
name: Release

on:
  push:
    tags: ["v*"]

env:
  CARGO_TERM_COLOR: always

permissions:
  contents: read

jobs:
  publish:
    name: Build, test & publish to crates.io
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - uses: taiki-e/install-action@nextest
      - name: Verify tag matches crate version
        run: |
          VERSION=$(cargo pkgid -p katha | cut -d'#' -f2)
          if [ "v$VERSION" != "$GITHUB_REF_NAME" ]; then
            echo "Tag $GITHUB_REF_NAME does not match crate version $VERSION" >&2
            exit 1
          fi
      - name: Test
        run: cargo nextest run --workspace --all-features
      - name: Doctests
        run: cargo test --doc --all-features
      # Publish in dependency order; cargo waits for each to appear in the index.
      - name: Publish katha-macros
        run: cargo publish -p katha-macros
        env:
          CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}
      - name: Publish katha
        run: cargo publish -p katha
        env:
          CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}
      - name: Publish katha-sqlx
        run: cargo publish -p katha-sqlx
        env:
          CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}
```

Note: `cargo publish -p katha` now publishes default features (`macros`); `--all-features` is no longer needed since sqlx left the crate.

- [ ] **Step 2: Lint the YAML**

```bash
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/release.yml')); print('valid yaml')"
```
Expected: `valid yaml`.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci: tag-guarded release publishing macros -> katha -> katha-sqlx

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Release config + dry-run verification, then cut 0.2.0

**Files:**
- Modify: root `Cargo.toml` (`[workspace.metadata.release]` already present; add per-package `tag = false` for `katha-sqlx`)
- Modify: `katha-sqlx/Cargo.toml` (add release override)

- [ ] **Step 1: Add the single-tag override to `katha-sqlx/Cargo.toml`**

Append (mirrors the existing override on `katha-macros`):

```toml
[package.metadata.release]
tag = false
```

Confirm `katha-macros/Cargo.toml` has the same `tag = false`; the single `vX.Y.Z` tag is cut from `katha`.

- [ ] **Step 2: Verify each crate publishes cleanly (dry run)**

```bash
cargo publish -p katha-macros --dry-run
cargo publish -p katha --dry-run
cargo publish -p katha-sqlx --dry-run
```
Expected: all three package and verify without error. (`katha-sqlx --dry-run` resolves `katha` by path; the version requirement must be satisfiable.)

- [ ] **Step 3: Dry-run the version bump**

```bash
just release-dry minor
```
Expected: cargo-release reports bumping all members `0.1.1` → `0.2.0`, one commit `chore: release v0.2.0`, one tag `v0.2.0`, no publish.

- [ ] **Step 4: Final full gate**

```bash
cargo fmt --all --check
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo nextest run --workspace --all-features
cargo test --doc --all-features
```
Expected: all PASS.

- [ ] **Step 5: Cut the release (publishes via CI on the pushed tag)**

> Confirm with the user before running — this pushes a tag that triggers crates.io publish.

```bash
just release minor
```
Expected: version bumped to 0.2.0, committed, tagged `v0.2.0`, pushed; the `release.yml` workflow then validates the tag, tests, and publishes `katha-macros` → `katha` → `katha-sqlx`.

---

## Self-Review

**Spec coverage:**
- §1 three-crate split → Task 1. ✓
- Core invariant (no macro/backend coupling) → Task 1 Step 3 (`default-features = false`), Global Constraints. ✓
- §2 features (`default = ["macros"]`, sqlx removed; katha-sqlx drivers) → Task 1 Steps 3,5. ✓
- §3 nextest CI + justfile → Task 2. ✓
- §4 docs.rs metadata (both crates) → Task 1 Step 3 + Task 3. ✓
- §5 READMEs/badges/links → Task 4. ✓
- §6 tag-guarded ordered release → Task 5. ✓
- §7 version 0.2.0 / shared-version / tag=false → Task 6. ✓
- §8 diesel deferred → out of scope, noted in plan header. ✓
- Acceptance criteria (build/test/dry-run/docs/links/order) → Tasks 1 Steps 8-10, 4 Step 5, 6 Steps 2-4. ✓

**Placeholder scan:** README content, CI YAML, Cargo.toml, and import rules are all concrete. The only deferred decision (per-driver feature split) is explicitly out of scope per spec §2 and documented in the katha-sqlx README. No TBDs.

**Type/symbol consistency:** `katha_sqlx` public exports in Task 1 Step 2 match the ownership list in Step 7 and the README quick-start import in Task 4. Publish order identical across Tasks 5 and 6 and Global Constraints.
