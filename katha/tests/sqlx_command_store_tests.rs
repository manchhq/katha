#![cfg(feature = "sqlx")]

mod common;

use chrono::{TimeZone, Utc};
use common::create_and_setup_memory_command_store;
use katha::{
    CommandCursor, SqlxCommandStore,
    traits::command_store::CommandStore,
    types::command_write::{CommandRead, CommandWrite},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TestCommand {
    action: String,
    amount: Option<i32>,
}

#[tokio::test]
async fn test_append_and_get_command() {
    let store: SqlxCommandStore = create_and_setup_memory_command_store().await;

    let command = CommandWrite {
        id: Uuid::new_v4(),
        correlation_id: Uuid::new_v4(),
        causation_id: None,
        data: TestCommand {
            action: "create".to_string(),
            amount: Some(10),
        },
        name: "CreateCommand".to_string(),
    };

    CommandStore::<TestCommand>::append_command(&store, &command)
        .await
        .unwrap();

    let loaded = CommandStore::<TestCommand>::get_command(&store, &command.id)
        .await
        .unwrap()
        .expect("expected command to exist");

    assert_eq!(loaded.id, command.id);
    assert_eq!(loaded.correlation_id, command.correlation_id);
    assert_eq!(loaded.name, command.name);
    assert_eq!(loaded.data.action, "create");
}

#[tokio::test]
async fn test_get_commands_pagination() {
    let store: SqlxCommandStore = create_and_setup_memory_command_store().await;

    for i in 0..3 {
        let command = CommandWrite {
            id: Uuid::new_v4(),
            correlation_id: Uuid::new_v4(),
            causation_id: None,
            data: TestCommand {
                action: format!("cmd_{i}"),
                amount: Some(i),
            },
            name: format!("Command{i}"),
        };

        CommandStore::<TestCommand>::append_command(&store, &command)
            .await
            .unwrap();
    }

    let page_one: Vec<CommandRead<TestCommand>> =
        CommandStore::<TestCommand>::get_commands(&store, Some(2), 0)
            .await
            .unwrap();
    let page_two: Vec<CommandRead<TestCommand>> =
        CommandStore::<TestCommand>::get_commands(&store, Some(2), 2)
            .await
            .unwrap();

    assert_eq!(page_one.len(), 2);
    assert_eq!(page_two.len(), 1);
}

#[tokio::test]
async fn test_get_nonexistent_command_returns_none() {
    let store: SqlxCommandStore = create_and_setup_memory_command_store().await;
    let result = CommandStore::<TestCommand>::get_command(&store, &Uuid::new_v4())
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_duplicate_command_id_fails() {
    let store: SqlxCommandStore = create_and_setup_memory_command_store().await;
    let id = Uuid::new_v4();

    let first = CommandWrite {
        id,
        correlation_id: Uuid::new_v4(),
        causation_id: None,
        data: TestCommand {
            action: "first".to_string(),
            amount: Some(1),
        },
        name: "Cmd".to_string(),
    };
    let second = CommandWrite {
        id,
        correlation_id: Uuid::new_v4(),
        causation_id: None,
        data: TestCommand {
            action: "second".to_string(),
            amount: Some(2),
        },
        name: "Cmd".to_string(),
    };

    CommandStore::<TestCommand>::append_command(&store, &first)
        .await
        .unwrap();
    let result = CommandStore::<TestCommand>::append_command(&store, &second).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_commands_are_ordered_by_latest_first() {
    let store: SqlxCommandStore = create_and_setup_memory_command_store().await;
    let first_id = Uuid::new_v4();
    let second_id = Uuid::new_v4();

    CommandStore::<TestCommand>::append_command(
        &store,
        &CommandWrite {
            id: first_id,
            correlation_id: Uuid::new_v4(),
            causation_id: None,
            data: TestCommand {
                action: "first".to_string(),
                amount: Some(1),
            },
            name: "Cmd".to_string(),
        },
    )
    .await
    .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    CommandStore::<TestCommand>::append_command(
        &store,
        &CommandWrite {
            id: second_id,
            correlation_id: Uuid::new_v4(),
            causation_id: None,
            data: TestCommand {
                action: "second".to_string(),
                amount: Some(2),
            },
            name: "Cmd".to_string(),
        },
    )
    .await
    .unwrap();

    let commands = CommandStore::<TestCommand>::get_commands(&store, None, 0)
        .await
        .unwrap();
    assert_eq!(commands.len(), 2);
    assert_eq!(commands[0].id, second_id);
    assert_eq!(commands[1].id, first_id);
}

#[tokio::test]
async fn test_command_store_persistence_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("commands_persist.db");
    let id = Uuid::new_v4();

    let store1 = common::create_and_setup_local_command_store(db_path.to_str().unwrap()).await;
    CommandStore::<TestCommand>::append_command(
        &store1,
        &CommandWrite {
            id,
            correlation_id: Uuid::new_v4(),
            causation_id: None,
            data: TestCommand {
                action: "persist".to_string(),
                amount: Some(99),
            },
            name: "PersistCmd".to_string(),
        },
    )
    .await
    .unwrap();
    drop(store1);

    let store2 = common::create_and_setup_local_command_store(db_path.to_str().unwrap()).await;
    let loaded = CommandStore::<TestCommand>::get_command(&store2, &id)
        .await
        .unwrap();
    assert!(loaded.is_some());
    assert_eq!(loaded.expect("command should exist").data.action, "persist");
}

#[tokio::test]
async fn test_concurrent_command_appends() {
    let store: SqlxCommandStore = create_and_setup_memory_command_store().await;

    let mut handles = Vec::new();
    for i in 0..10 {
        let store_clone = store.clone();
        handles.push(tokio::spawn(async move {
            CommandStore::<TestCommand>::append_command(
                &store_clone,
                &CommandWrite {
                    id: Uuid::new_v4(),
                    correlation_id: Uuid::new_v4(),
                    causation_id: None,
                    data: TestCommand {
                        action: format!("cmd_{i}"),
                        amount: Some(i),
                    },
                    name: "ConcurrentCmd".to_string(),
                },
            )
            .await
        }));
    }

    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    let all = CommandStore::<TestCommand>::get_commands(&store, None, 0)
        .await
        .unwrap();
    assert_eq!(all.len(), 10);
}

// ── get_commands_page (cursor-based pagination) ──────────────────────────────

/// Helper: append `n` commands with a small sleep between each so
/// `created_utc` timestamps are strictly ordered.
async fn append_n_commands(store: &SqlxCommandStore, n: usize) -> Vec<Uuid> {
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let id = Uuid::new_v4();
        ids.push(id);
        CommandStore::<TestCommand>::append_command(
            store,
            &CommandWrite {
                id,
                correlation_id: Uuid::new_v4(),
                causation_id: None,
                data: TestCommand {
                    action: format!("action_{i}"),
                    amount: Some(i as i32),
                },
                name: format!("Cmd{i}"),
            },
        )
        .await
        .unwrap();
        // Ensure distinct created_utc values (SQLite timestamp precision is 1 ms).
        tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
    }
    ids
}

/// First page (no cursor) returns `limit` items and a non-None `next_cursor`
/// when more records exist.
#[tokio::test]
async fn test_get_commands_page_first_page() {
    let store: SqlxCommandStore = create_and_setup_memory_command_store().await;
    append_n_commands(&store, 5).await;

    let page = store
        .get_commands_page::<TestCommand>(None, 3)
        .await
        .unwrap();

    assert_eq!(
        page.items.len(),
        3,
        "first page should contain exactly 3 items"
    );
    assert!(
        page.next_cursor.is_some(),
        "next_cursor should be Some when more items exist"
    );
}

/// Second page (using the cursor from the first page) returns the remaining
/// items and a None `next_cursor` when exhausted.
#[tokio::test]
async fn test_get_commands_page_second_page() {
    let store: SqlxCommandStore = create_and_setup_memory_command_store().await;
    append_n_commands(&store, 5).await;

    let first = store
        .get_commands_page::<TestCommand>(None, 3)
        .await
        .unwrap();
    assert_eq!(first.items.len(), 3);
    let cursor = first
        .next_cursor
        .expect("expected next_cursor from first page");

    let second = store
        .get_commands_page::<TestCommand>(Some(&cursor), 3)
        .await
        .unwrap();

    assert_eq!(
        second.items.len(),
        2,
        "second page should contain the remaining 2 items"
    );
    assert!(
        second.next_cursor.is_none(),
        "next_cursor should be None on the last page"
    );
}

/// Items are returned in descending `created_utc` order (newest first) across
/// cursor pages — matches the ordering tested in
/// `test_commands_are_ordered_by_latest_first`.
#[tokio::test]
async fn test_get_commands_page_ordering_newest_first() {
    let store: SqlxCommandStore = create_and_setup_memory_command_store().await;
    // append 4 commands; `append_n_commands` sleeps 2 ms between each.
    append_n_commands(&store, 4).await;

    // Collect all items across two pages (limit = 2).
    let first = store
        .get_commands_page::<TestCommand>(None, 2)
        .await
        .unwrap();
    assert_eq!(first.items.len(), 2);

    let cursor = first.next_cursor.as_ref().expect("expected a cursor");
    let second = store
        .get_commands_page::<TestCommand>(Some(cursor), 2)
        .await
        .unwrap();
    assert_eq!(second.items.len(), 2);

    let all_items: Vec<_> = first.items.iter().chain(second.items.iter()).collect();

    // Verify strictly descending created_utc.
    for window in all_items.windows(2) {
        assert!(
            window[0].created_utc >= window[1].created_utc,
            "items should be ordered newest-first: {:?} >= {:?}",
            window[0].created_utc,
            window[1].created_utc
        );
    }
}

/// A cursor pointing past the last record returns an empty page and no
/// `next_cursor`.
#[tokio::test]
async fn test_get_commands_page_out_of_range_cursor_returns_empty() {
    let store: SqlxCommandStore = create_and_setup_memory_command_store().await;
    append_n_commands(&store, 3).await;

    // Build a cursor with epoch (the oldest possible timestamp + nil UUID).
    // Every real command was appended after the epoch, so this cursor is
    // older than everything in the store — the WHERE clause filters all rows.
    let stale_cursor = CommandCursor {
        created_utc: Utc.timestamp_opt(0, 0).unwrap(),
        id: Uuid::nil(),
    };

    let page = store
        .get_commands_page::<TestCommand>(Some(&stale_cursor), 10)
        .await
        .unwrap();

    assert!(
        page.items.is_empty(),
        "expected empty page for out-of-range cursor, got {} items",
        page.items.len()
    );
    assert!(page.next_cursor.is_none());
}

/// When `limit = 1` the store should return exactly 1 item per page and
/// produce a valid `next_cursor` until exhausted.
#[tokio::test]
async fn test_get_commands_page_limit_one() {
    let store: SqlxCommandStore = create_and_setup_memory_command_store().await;
    append_n_commands(&store, 3).await;

    let mut cursor: Option<CommandCursor> = None;
    let mut collected = 0usize;

    loop {
        let page = store
            .get_commands_page::<TestCommand>(cursor.as_ref(), 1)
            .await
            .unwrap();

        assert!(
            page.items.len() <= 1,
            "limit=1 page should never have more than 1 item"
        );
        collected += page.items.len();
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }

    assert_eq!(
        collected, 3,
        "should have visited all 3 commands one by one"
    );
}

/// No-data scenario: first page on an empty store returns empty items and no cursor.
#[tokio::test]
async fn test_get_commands_page_empty_store() {
    let store: SqlxCommandStore = create_and_setup_memory_command_store().await;

    let page = store
        .get_commands_page::<TestCommand>(None, 10)
        .await
        .unwrap();

    assert!(page.items.is_empty(), "empty store should return no items");
    assert!(
        page.next_cursor.is_none(),
        "empty store should return no cursor"
    );
}
