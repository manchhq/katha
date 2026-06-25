#![allow(dead_code)]
use katha::traits::command_store::CommandStore;
use katha::traits::event_store::EventStore;
use katha_sqlx::{SqlxCommandStore, SqlxEventStore};

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
