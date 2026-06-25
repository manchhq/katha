mod common;

use common::{create_and_setup_memory_command_store, create_and_setup_memory_store};
use katha::{
    traits::command_store::CommandStore,
    traits::event_store::EventStore,
    types::{
        command_write::{CommandRead, CommandWrite},
        event_read_range::EventsReadRange,
        event_write::EventWrite,
        expected_version::ExpectedVersion,
    },
};
use katha_sqlx::{SqlxCommandStore, SqlxEventStore};
use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct PropEvent {
    seq: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct PropMeta;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PropCommand {
    value: i32,
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime for proptest")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
    fn prop_cursor_pagination_returns_all_versions_without_duplicates(
        total_events in 1usize..40,
        page_size in 1usize..10
    ) {
        rt().block_on(async move {
            let store: SqlxEventStore = create_and_setup_memory_store().await;
            let stream_id = "prop_cursor_stream";

            for i in 0..total_events {
                let event = EventWrite::<PropEvent, PropMeta> {
                    id: Uuid::new_v4(),
                    correlation_id: None,
                    causation_id: None,
                    data: PropEvent { seq: i as i32 },
                    metadata: None,
                    name: "PropEvent".to_string(),
                };
                let expected = if i == 0 { ExpectedVersion::NoStream } else { ExpectedVersion::Any };
                EventStore::<PropEvent, PropMeta>::append_event(&store, stream_id, &expected, &event).await.unwrap();
            }

            let mut cursor = None;
            let mut versions = Vec::new();
            loop {
                let page = store
                    .get_events_page::<PropEvent, PropMeta>(stream_id, cursor, page_size)
                    .await
                    .unwrap();
                versions.extend(page.items.into_iter().map(|e| e.version));
                cursor = page.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }

            assert_eq!(versions.len(), total_events);
            for (idx, version) in versions.iter().enumerate() {
                assert_eq!(*version, idx as u32);
            }
        });
    }

    #[test]
    fn prop_event_append_read_roundtrip_preserves_sequence(
        values in proptest::collection::vec(any::<i32>(), 1..30)
    ) {
        rt().block_on(async move {
            let store: SqlxEventStore = create_and_setup_memory_store().await;
            let stream_id = "prop_roundtrip_stream";

            for (idx, value) in values.iter().enumerate() {
                let event = EventWrite::<PropEvent, PropMeta> {
                    id: Uuid::new_v4(),
                    correlation_id: None,
                    causation_id: None,
                    data: PropEvent { seq: *value },
                    metadata: None,
                    name: "PropEvent".to_string(),
                };
                let expected = if idx == 0 { ExpectedVersion::NoStream } else { ExpectedVersion::Any };
                EventStore::<PropEvent, PropMeta>::append_event(&store, stream_id, &expected, &event)
                    .await
                    .unwrap();
            }

            let events = EventStore::<PropEvent, PropMeta>::get_events(
                &store,
                stream_id,
                &EventsReadRange::AllEvents,
            )
            .await
            .unwrap();

            assert_eq!(events.len(), values.len());
            for (idx, event) in events.iter().enumerate() {
                assert_eq!(event.version, idx as u32);
                assert_eq!(event.data.seq, values[idx]);
            }
        });
    }

    #[test]
    fn prop_apply_projection_once_runs_at_most_once_for_retries(
        retries in 1usize..30
    ) {
        rt().block_on(async move {
            let store: SqlxEventStore = create_and_setup_memory_store().await;
            store.ensure_projection_idempotency_table().await.unwrap();

            let event = EventWrite::<PropEvent, PropMeta> {
                id: Uuid::new_v4(),
                correlation_id: None,
                causation_id: None,
                data: PropEvent { seq: 1 },
                metadata: None,
                name: "PropEvent".to_string(),
            };

            let persisted = EventStore::<PropEvent, PropMeta>::append_event(
                &store,
                "prop_projection_stream",
                &ExpectedVersion::NoStream,
                &event,
            )
            .await
            .unwrap();

            let apply_count = Arc::new(AtomicUsize::new(0));
            let mut true_count = 0usize;

            for _ in 0..retries {
                let count = apply_count.clone();
                let applied = store
                    .apply_projection_once("prop_projection", &persisted, move |_| {
                        let count = count.clone();
                        async move {
                            count.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }
                    })
                    .await
                    .unwrap();

                if applied {
                    true_count += 1;
                }
            }

            assert_eq!(true_count, 1);
            assert_eq!(apply_count.load(Ordering::SeqCst), 1);
        });
    }

    #[test]
    fn prop_command_pagination_reconstructs_full_desc_order(
        values in proptest::collection::vec(any::<i32>(), 1..35),
        page_size in 1usize..10
    ) {
        rt().block_on(async move {
            let store: SqlxCommandStore = create_and_setup_memory_command_store().await;

            for value in &values {
                let cmd = CommandWrite {
                    id: Uuid::new_v4(),
                    correlation_id: Uuid::new_v4(),
                    causation_id: None,
                    data: PropCommand { value: *value },
                    name: "PropCommand".to_string(),
                };
                CommandStore::<PropCommand>::append_command(&store, &cmd)
                    .await
                    .unwrap();
            }

            let full = CommandStore::<PropCommand>::get_commands(&store, None, 0)
                .await
                .unwrap();

            let mut rebuilt: Vec<CommandRead<PropCommand>> = Vec::new();
            let mut offset = 0usize;
            loop {
                let page = CommandStore::<PropCommand>::get_commands(&store, Some(page_size), offset)
                    .await
                    .unwrap();
                let len = page.len();
                rebuilt.extend(page);
                if len < page_size {
                    break;
                }
                offset += page_size;
            }

            assert_eq!(rebuilt.len(), full.len());
            for (a, b) in rebuilt.iter().zip(full.iter()) {
                assert_eq!(a.id, b.id);
            }
        });
    }
}
