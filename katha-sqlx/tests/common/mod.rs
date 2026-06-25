#![allow(dead_code)]
use katha::traits::command_store::CommandStore;
use katha::traits::event_store::EventStore;
use katha_sqlx::{SqlxCommandStore, SqlxEventStore};
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

/// Postgres connection URL for integration tests, read from `KATHA_TEST_PG_URL`.
///
/// When unset (the common case for local SQLite-only runs and forks without a
/// database), Postgres-backed cases are skipped rather than failing.
pub fn pg_test_url() -> Option<String> {
    std::env::var("KATHA_TEST_PG_URL").ok()
}

/// Generates a unique, SQL-safe store name.
///
/// SQLite tests each get an isolated in-memory database, but Postgres tests
/// share one database, so table names must not collide across the
/// process-per-test parallelism nextest uses. The name stays within Postgres's
/// 63-byte identifier limit even after the `_events`/`_commands` suffixes.
pub fn unique_store_name(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{n}_{}", Uuid::new_v4().simple())
}

/// Returns the event-store backends to exercise: always SQLite in-memory, plus
/// Postgres when `KATHA_TEST_PG_URL` is set. Each store has its event and
/// projection-idempotency tables ensured and a unique namespace.
pub async fn event_store_backends() -> Vec<(&'static str, SqlxEventStore)> {
    let mut stores = Vec::new();
    stores.push((
        "sqlite",
        SqlxEventStore::new_memory(&unique_store_name("evt"))
            .await
            .unwrap(),
    ));
    if let Some(url) = pg_test_url() {
        stores.push((
            "postgres",
            SqlxEventStore::new_from_url(&unique_store_name("evt"), &url)
                .await
                .unwrap(),
        ));
    }
    for (backend, store) in &stores {
        EventStore::<String, String>::ensure_events_table(store)
            .await
            .unwrap_or_else(|e| panic!("ensure_events_table failed on {backend}: {e:?}"));
        store
            .ensure_projection_idempotency_table()
            .await
            .unwrap_or_else(|e| {
                panic!("ensure_projection_idempotency_table failed on {backend}: {e:?}")
            });
    }
    stores
}

/// Returns the command-store backends to exercise: always SQLite in-memory,
/// plus Postgres when `KATHA_TEST_PG_URL` is set.
pub async fn command_store_backends() -> Vec<(&'static str, SqlxCommandStore)> {
    let mut stores = Vec::new();
    stores.push((
        "sqlite",
        SqlxCommandStore::new_memory(&unique_store_name("cmd"))
            .await
            .unwrap(),
    ));
    if let Some(url) = pg_test_url() {
        stores.push((
            "postgres",
            SqlxCommandStore::new_from_url(&unique_store_name("cmd"), &url)
                .await
                .unwrap(),
        ));
    }
    for (backend, store) in &stores {
        CommandStore::<String>::ensure_commands_table(store)
            .await
            .unwrap_or_else(|e| panic!("ensure_commands_table failed on {backend}: {e:?}"));
    }
    stores
}

pub async fn create_and_setup_memory_store() -> SqlxEventStore {
    let store = SqlxEventStore::new_memory("test_store").await.unwrap();
    EventStore::<String, String>::ensure_events_table(&store)
        .await
        .unwrap();
    store
}

pub async fn create_and_setup_local_store(db_path: &str) -> SqlxEventStore {
    let store = SqlxEventStore::new_local("test_store", db_path)
        .await
        .unwrap();
    EventStore::<String, String>::ensure_events_table(&store)
        .await
        .unwrap();
    store
}

pub async fn create_and_setup_memory_command_store() -> SqlxCommandStore {
    let store = SqlxCommandStore::new_memory("test_store").await.unwrap();
    CommandStore::<String>::ensure_commands_table(&store)
        .await
        .unwrap();
    store
}

pub async fn create_and_setup_local_command_store(db_path: &str) -> SqlxCommandStore {
    let store = SqlxCommandStore::new_local("test_store", db_path)
        .await
        .unwrap();
    CommandStore::<String>::ensure_commands_table(&store)
        .await
        .unwrap();
    store
}
