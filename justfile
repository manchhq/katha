# List available recipes
default:
    @just --list

# Build the workspace with all features
build:
    cargo build --workspace --all-features

# Run the test suite with all features
test:
    cargo test --workspace --all-features

# Format all crates
fmt:
    cargo fmt --all

# Format check + clippy with warnings denied
lint:
    cargo fmt --all --check
    cargo clippy --workspace --all-features --all-targets -- -D warnings

# Full gate: lint + tests — must pass clean before pushing
check: lint test

# Dry-run a release (level: patch | minor | major | x.y.z)
release-dry level:
    cargo release {{level}} --workspace

# Cut a release: bump shared version, commit, tag vX.Y.Z, push. CI publishes to crates.io.
release level:
    cargo release {{level}} --workspace --execute
