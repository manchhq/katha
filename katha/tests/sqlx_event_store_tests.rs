#![cfg(feature = "sqlx")]

mod common;
use chrono::Utc;
use common::{create_and_setup_local_store, create_and_setup_memory_store};
use katha::{
    SqlxEventStore,
    traits::event_store::EventStore,
    types::{
        event_read::EventRead, event_read_range::EventsReadRange, event_stream::EventStream,
        event_write::EventWrite, expected_version::ExpectedVersion,
        stream_read_filter::StreamsReadFilter,
    },
};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::time::{Duration, timeout};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestEvent {
    user_id: String,
    action: String,
    amount: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestMetadata {
    source: String,
    timestamp: String,
}

#[tokio::test]
async fn test_basic_event_store() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;

    let event_id = Uuid::new_v4();
    let correlation_id = Uuid::new_v4();
    let causation_id = Uuid::new_v4();

    let test_event = TestEvent {
        user_id: "user123".to_string(),
        action: "deposit".to_string(),
        amount: Some(100),
    };

    let test_metadata = TestMetadata {
        source: "test".to_string(),
        timestamp: Utc::now().to_rfc3339(),
    };

    let event_write = EventWrite::<TestEvent, TestMetadata> {
        id: event_id,
        correlation_id: Some(correlation_id),
        causation_id: Some(causation_id),
        data: test_event.clone(),
        metadata: Some(test_metadata.clone()),
        name: "TestEvent".to_string(),
    };

    let result = store
        .append_event("test_stream", &ExpectedVersion::NoStream, &event_write)
        .await;
    assert!(result.is_ok(), "Failed to append event: {:?}", result.err());

    let retrieved = EventStore::<TestEvent, TestMetadata>::get_event(&store, "test_stream", 0)
        .await
        .unwrap();
    assert_eq!(retrieved.data, test_event);
    assert_eq!(retrieved.metadata, Some(test_metadata));
    assert_eq!(retrieved.id, event_id);
}

#[tokio::test]
async fn test_event_stream_versioning() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    let stream_id = "version_test_stream";

    let event1 = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: Some(Uuid::new_v4()),
        data: TestEvent {
            user_id: "user1".to_string(),
            action: "create".to_string(),
            amount: None,
        },
        metadata: None,
        name: "CreateEvent".to_string(),
    };

    let result1 = store
        .append_event(stream_id, &ExpectedVersion::NoStream, &event1)
        .await;
    assert!(result1.is_ok());

    let event2 = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: Some(Uuid::new_v4()),
        data: TestEvent {
            user_id: "user1".to_string(),
            action: "update".to_string(),
            amount: Some(50),
        },
        metadata: None,
        name: "UpdateEvent".to_string(),
    };

    let result2 = store
        .append_event(stream_id, &ExpectedVersion::Exact(1), &event2)
        .await;
    assert!(result2.is_ok());

    let event0: EventRead<TestEvent, TestMetadata> = store.get_event(stream_id, 0).await.unwrap();
    let event1_retrieved: EventRead<TestEvent, TestMetadata> =
        store.get_event(stream_id, 1).await.unwrap();

    assert_eq!(event0.data.user_id, "user1");
    assert_eq!(event0.data.action, "create");
    assert_eq!(event1_retrieved.data.user_id, "user1");
    assert_eq!(event1_retrieved.data.action, "update");
}

#[tokio::test]
async fn test_expected_version_any() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    let stream_id = "any_version_stream";

    let event1 = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: Some(Uuid::new_v4()),
        data: TestEvent {
            user_id: "user1".to_string(),
            action: "create".to_string(),
            amount: None,
        },
        metadata: None,
        name: "CreateEvent".to_string(),
    };

    store
        .append_event(stream_id, &ExpectedVersion::NoStream, &event1)
        .await
        .unwrap();

    let event2 = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: Some(Uuid::new_v4()),
        data: TestEvent {
            user_id: "user1".to_string(),
            action: "update".to_string(),
            amount: Some(100),
        },
        metadata: None,
        name: "UpdateEvent".to_string(),
    };

    let result = store
        .append_event(stream_id, &ExpectedVersion::Any, &event2)
        .await;
    assert!(result.is_ok());

    let retrieved: EventRead<TestEvent, TestMetadata> =
        store.get_event(stream_id, 1).await.unwrap();
    assert_eq!(retrieved.data.action, "update");
    assert_eq!(retrieved.version, 1);
}

#[tokio::test]
async fn test_expected_version_exact_conflicts() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    let stream_id = "exact_version_stream";

    let event1 = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: Some(Uuid::new_v4()),
        data: TestEvent {
            user_id: "user1".to_string(),
            action: "create".to_string(),
            amount: None,
        },
        metadata: None,
        name: "CreateEvent".to_string(),
    };

    store
        .append_event(stream_id, &ExpectedVersion::NoStream, &event1)
        .await
        .unwrap();

    let event2 = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: Some(Uuid::new_v4()),
        data: TestEvent {
            user_id: "user1".to_string(),
            action: "update".to_string(),
            amount: Some(50),
        },
        metadata: None,
        name: "UpdateEvent".to_string(),
    };

    let result = store
        .append_event(stream_id, &ExpectedVersion::Exact(0), &event2)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_get_events_and_streams() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    let stream_id = "range_test_stream";

    for i in 0..3 {
        let event = EventWrite::<TestEvent, TestMetadata> {
            id: Uuid::new_v4(),
            correlation_id: Some(Uuid::new_v4()),
            causation_id: Some(Uuid::new_v4()),
            data: TestEvent {
                user_id: "user1".to_string(),
                action: format!("action_{i}"),
                amount: Some(i),
            },
            metadata: None,
            name: "RangeEvent".to_string(),
        };

        let expected = if i == 0 {
            ExpectedVersion::NoStream
        } else {
            ExpectedVersion::Any
        };

        store
            .append_event(stream_id, &expected, &event)
            .await
            .unwrap();
    }

    let events = EventStore::<TestEvent, TestMetadata>::get_events(
        &store,
        stream_id,
        &EventsReadRange::VersionRange {
            from_version: 1,
            to_version: 2,
        },
    )
    .await
    .unwrap();
    assert_eq!(events.len(), 2);

    let streams =
        EventStore::<TestEvent, TestMetadata>::get_streams(&store, &StreamsReadFilter::AllStreams)
            .await
            .unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].id, stream_id);
}

#[tokio::test]
async fn test_get_events_with_range_queries() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    let stream_id = "range_queries_stream";

    for i in 0..5 {
        let event = EventWrite::<TestEvent, TestMetadata> {
            id: Uuid::new_v4(),
            correlation_id: Some(Uuid::new_v4()),
            causation_id: Some(Uuid::new_v4()),
            data: TestEvent {
                user_id: format!("user{i}"),
                action: format!("action{i}"),
                amount: Some(i),
            },
            metadata: None,
            name: "RangeQueryEvent".to_string(),
        };

        let expected = if i == 0 {
            ExpectedVersion::NoStream
        } else {
            ExpectedVersion::Any
        };
        store
            .append_event(stream_id, &expected, &event)
            .await
            .unwrap();
    }

    let all = EventStore::<TestEvent, TestMetadata>::get_events(
        &store,
        stream_id,
        &EventsReadRange::AllEvents,
    )
    .await
    .unwrap();
    assert_eq!(all.len(), 5);

    let from_v2 = EventStore::<TestEvent, TestMetadata>::get_events(
        &store,
        stream_id,
        &EventsReadRange::FromVersion(2),
    )
    .await
    .unwrap();
    assert_eq!(from_v2.len(), 3);

    let to_v2 = EventStore::<TestEvent, TestMetadata>::get_events(
        &store,
        stream_id,
        &EventsReadRange::ToVersion(2),
    )
    .await
    .unwrap();
    assert_eq!(to_v2.len(), 3);
}

#[tokio::test]
async fn test_correlation_and_causation_queries() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    let stream_id = "correlation_stream";
    let correlation_id = Uuid::new_v4();

    let event1 = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: Some(correlation_id),
        causation_id: Some(Uuid::new_v4()),
        data: TestEvent {
            user_id: "u1".to_string(),
            action: "start".to_string(),
            amount: None,
        },
        metadata: None,
        name: "StartEvent".to_string(),
    };
    let event2 = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: Some(correlation_id),
        causation_id: Some(event1.id),
        data: TestEvent {
            user_id: "u1".to_string(),
            action: "followup".to_string(),
            amount: Some(10),
        },
        metadata: None,
        name: "FollowupEvent".to_string(),
    };

    store
        .append_event(stream_id, &ExpectedVersion::NoStream, &event1)
        .await
        .unwrap();
    store
        .append_event(stream_id, &ExpectedVersion::Any, &event2)
        .await
        .unwrap();

    let correlated = EventStore::<TestEvent, TestMetadata>::get_events_by_correlation_id(
        &store,
        &correlation_id,
    )
    .await
    .unwrap();
    assert_eq!(correlated.len(), 2);

    let caused =
        EventStore::<TestEvent, TestMetadata>::get_events_by_causation_id(&store, &event1.id)
            .await
            .unwrap();
    assert_eq!(caused.len(), 1);
    assert_eq!(caused[0].data.action, "followup");
}

#[tokio::test]
async fn test_append_multiple_events_versions_sequential() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    let stream_id = "batch_stream";

    let events = vec![
        EventWrite::<TestEvent, TestMetadata> {
            id: Uuid::new_v4(),
            correlation_id: Some(Uuid::new_v4()),
            causation_id: Some(Uuid::new_v4()),
            data: TestEvent {
                user_id: "user1".to_string(),
                action: "step1".to_string(),
                amount: None,
            },
            metadata: None,
            name: "Step1Event".to_string(),
        },
        EventWrite::<TestEvent, TestMetadata> {
            id: Uuid::new_v4(),
            correlation_id: Some(Uuid::new_v4()),
            causation_id: Some(Uuid::new_v4()),
            data: TestEvent {
                user_id: "user1".to_string(),
                action: "step2".to_string(),
                amount: Some(2),
            },
            metadata: None,
            name: "Step2Event".to_string(),
        },
        EventWrite::<TestEvent, TestMetadata> {
            id: Uuid::new_v4(),
            correlation_id: Some(Uuid::new_v4()),
            causation_id: Some(Uuid::new_v4()),
            data: TestEvent {
                user_id: "user1".to_string(),
                action: "step3".to_string(),
                amount: Some(3),
            },
            metadata: None,
            name: "Step3Event".to_string(),
        },
    ];

    let appended = EventStore::<TestEvent, TestMetadata>::append_events(
        &store,
        stream_id,
        &ExpectedVersion::NoStream,
        events,
    )
    .await
    .unwrap();

    assert_eq!(appended.len(), 3);
    for (idx, event) in appended.iter().enumerate() {
        assert_eq!(event.version, idx as u32);
    }
}

#[tokio::test]
async fn test_event_store_persistence_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("persist_events.db");
    let stream_id = "persistent_stream";

    let store1: SqlxEventStore = create_and_setup_local_store(db_path.to_str().unwrap()).await;
    let event = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: Some(Uuid::new_v4()),
        data: TestEvent {
            user_id: "persist_user".to_string(),
            action: "persist".to_string(),
            amount: Some(99),
        },
        metadata: None,
        name: "PersistentEvent".to_string(),
    };
    store1
        .append_event(stream_id, &ExpectedVersion::NoStream, &event)
        .await
        .unwrap();
    drop(store1);

    let store2: SqlxEventStore = create_and_setup_local_store(db_path.to_str().unwrap()).await;
    let retrieved = EventStore::<TestEvent, TestMetadata>::get_event(&store2, stream_id, 0)
        .await
        .unwrap();
    assert_eq!(retrieved.data.user_id, "persist_user");
}

#[tokio::test]
async fn test_nonexistent_stream_and_event_errors() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;

    let stream_result =
        EventStore::<TestEvent, TestMetadata>::get_stream(&store, "missing_stream").await;
    assert!(stream_result.is_err());
    assert_eq!(stream_result.unwrap_err().to_string(), "Stream not found");

    let events = EventStore::<TestEvent, TestMetadata>::get_events(
        &store,
        "missing_stream",
        &EventsReadRange::AllEvents,
    )
    .await
    .unwrap();
    assert!(events.is_empty());

    let seed = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: Some(Uuid::new_v4()),
        data: TestEvent {
            user_id: "u1".to_string(),
            action: "seed".to_string(),
            amount: None,
        },
        metadata: None,
        name: "SeedEvent".to_string(),
    };
    store
        .append_event("existing_stream", &ExpectedVersion::NoStream, &seed)
        .await
        .unwrap();

    let missing_event =
        EventStore::<TestEvent, TestMetadata>::get_event(&store, "existing_stream", 99).await;
    assert!(missing_event.is_err());
    assert_eq!(missing_event.unwrap_err().to_string(), "Event not found");
}

#[tokio::test]
async fn test_empty_append_events_noop() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    let stream_id = "empty_append_stream";

    let appended = EventStore::<TestEvent, TestMetadata>::append_events(
        &store,
        stream_id,
        &ExpectedVersion::NoStream,
        vec![],
    )
    .await
    .unwrap();
    assert!(appended.is_empty());

    let stream_result = EventStore::<TestEvent, TestMetadata>::get_stream(&store, stream_id).await;
    assert!(stream_result.is_err());
}

#[tokio::test]
async fn test_many_streams_creation() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;

    for i in 0..30 {
        let stream_id = format!("perf_stream_{i}");
        let event = EventWrite::<TestEvent, TestMetadata> {
            id: Uuid::new_v4(),
            correlation_id: Some(Uuid::new_v4()),
            causation_id: Some(Uuid::new_v4()),
            data: TestEvent {
                user_id: format!("user{i}"),
                action: "create".to_string(),
                amount: Some(i),
            },
            metadata: None,
            name: "PerfEvent".to_string(),
        };
        store
            .append_event(&stream_id, &ExpectedVersion::NoStream, &event)
            .await
            .unwrap();
    }

    let streams =
        EventStore::<TestEvent, TestMetadata>::get_streams(&store, &StreamsReadFilter::AllStreams)
            .await
            .unwrap();
    assert_eq!(streams.len(), 30);
}

#[tokio::test]
async fn test_concurrent_event_appends_same_stream_integrity() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    let stream_id = "concurrent_stream";

    let mut handles = Vec::new();
    for i in 0..10 {
        let store_clone = store.clone();
        let stream_id = stream_id.to_string();
        handles.push(tokio::spawn(async move {
            let event = EventWrite::<TestEvent, TestMetadata> {
                id: Uuid::new_v4(),
                correlation_id: Some(Uuid::new_v4()),
                causation_id: Some(Uuid::new_v4()),
                data: TestEvent {
                    user_id: format!("user{i}"),
                    action: format!("action{i}"),
                    amount: Some(i),
                },
                metadata: None,
                name: "ConcurrentEvent".to_string(),
            };
            store_clone
                .append_event(&stream_id, &ExpectedVersion::Any, &event)
                .await
        }));
    }

    let mut success_count = 0usize;
    for handle in handles {
        if handle.await.unwrap().is_ok() {
            success_count += 1;
        }
    }
    assert!(success_count >= 1);

    let all = EventStore::<TestEvent, TestMetadata>::get_events(
        &store,
        stream_id,
        &EventsReadRange::AllEvents,
    )
    .await
    .unwrap();
    assert_eq!(all.len(), success_count);

    for (i, event) in all.iter().enumerate() {
        assert_eq!(event.version, i as u32);
    }
}

#[tokio::test]
async fn test_local_store() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let store: SqlxEventStore = create_and_setup_local_store(db_path.to_str().unwrap()).await;

    let event = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: None,
        causation_id: None,
        data: TestEvent {
            user_id: "user1".to_string(),
            action: "local".to_string(),
            amount: None,
        },
        metadata: None,
        name: "LocalEvent".to_string(),
    };

    store
        .append_event("local_stream", &ExpectedVersion::NoStream, &event)
        .await
        .unwrap();

    let stream: EventStream =
        EventStore::<TestEvent, TestMetadata>::get_stream(&store, "local_stream")
            .await
            .unwrap();
    assert_eq!(stream.id, "local_stream");
}

#[tokio::test]
async fn test_notifications_emitted_after_append() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    let mut rx = store.subscribe();

    let event = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: None,
        causation_id: None,
        data: TestEvent {
            user_id: "user1".to_string(),
            action: "create".to_string(),
            amount: Some(1),
        },
        metadata: None,
        name: "Created".to_string(),
    };

    store
        .append_event("notifications_stream", &ExpectedVersion::NoStream, &event)
        .await
        .unwrap();

    let notification = timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("timed out waiting for event notification")
        .expect("notification channel unexpectedly closed");

    assert_eq!(notification.store_name, "test_store");
    assert_eq!(notification.stream_id, "notifications_stream");
    assert_eq!(notification.from_version, 0);
    assert_eq!(notification.to_version, 0);
    assert_eq!(notification.event_ids, vec![event.id]);
    assert_eq!(notification.event_names, vec!["Created".to_string()]);
}

#[tokio::test]
async fn test_notifications_fanout_to_multiple_subscribers() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    let mut rx_a = store.subscribe();
    let mut rx_b = store.subscribe();

    let payload = (0..2)
        .map(|i| EventWrite::<TestEvent, TestMetadata> {
            id: Uuid::new_v4(),
            correlation_id: None,
            causation_id: None,
            data: TestEvent {
                user_id: "user1".to_string(),
                action: format!("step_{i}"),
                amount: Some(i),
            },
            metadata: None,
            name: format!("Evt{i}"),
        })
        .collect::<Vec<_>>();

    store
        .append_events("fanout_stream", &ExpectedVersion::NoStream, payload.clone())
        .await
        .unwrap();

    let notification_a = timeout(Duration::from_millis(500), rx_a.recv())
        .await
        .expect("timed out waiting for subscriber A")
        .expect("subscriber A channel unexpectedly closed");
    let notification_b = timeout(Duration::from_millis(500), rx_b.recv())
        .await
        .expect("timed out waiting for subscriber B")
        .expect("subscriber B channel unexpectedly closed");

    assert_eq!(notification_a, notification_b);
    assert_eq!(notification_a.from_version, 0);
    assert_eq!(notification_a.to_version, 1);
    assert_eq!(
        notification_a.event_ids,
        payload.iter().map(|event| event.id).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn test_failed_append_does_not_emit_notification() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    let mut rx = store.subscribe();
    let stream_id = "conflict_stream";

    let first = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: None,
        causation_id: None,
        data: TestEvent {
            user_id: "user1".to_string(),
            action: "create".to_string(),
            amount: Some(1),
        },
        metadata: None,
        name: "Created".to_string(),
    };

    store
        .append_event(stream_id, &ExpectedVersion::NoStream, &first)
        .await
        .unwrap();

    timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("timed out waiting for initial notification")
        .expect("notification channel unexpectedly closed");

    let conflicting = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: None,
        causation_id: None,
        data: TestEvent {
            user_id: "user1".to_string(),
            action: "conflicting".to_string(),
            amount: Some(2),
        },
        metadata: None,
        name: "Conflicting".to_string(),
    };

    let result = store
        .append_event(stream_id, &ExpectedVersion::Exact(0), &conflicting)
        .await;
    assert!(result.is_err());

    let no_message = timeout(Duration::from_millis(150), rx.recv()).await;
    assert!(
        no_message.is_err(),
        "unexpected notification for failed append"
    );
}

#[tokio::test]
async fn test_event_appended_observable_returns_typed_event() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    let mut rx = EventStore::<TestEvent, TestMetadata>::event_appended(&store)
        .expect("sqlx store should expose event_appended observable");

    let event = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: Some(Uuid::new_v4()),
        data: TestEvent {
            user_id: "user-observable".to_string(),
            action: "typed".to_string(),
            amount: Some(42),
        },
        metadata: Some(TestMetadata {
            source: "observable-test".to_string(),
            timestamp: Utc::now().to_rfc3339(),
        }),
        name: "TypedEvent".to_string(),
    };

    store
        .append_event("observable_stream", &ExpectedVersion::NoStream, &event)
        .await
        .unwrap();

    let appended = timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("timed out waiting for typed appended event")
        .expect("typed appended channel unexpectedly closed");

    assert_eq!(appended.id, event.id);
    assert_eq!(appended.stream_id, "observable_stream");
    assert_eq!(appended.version, 0);
    assert_eq!(appended.name, "TypedEvent");
    assert_eq!(appended.data.action, "typed");
    assert_eq!(
        appended.metadata.expect("metadata expected").source,
        "observable-test"
    );
}

#[tokio::test]
async fn test_projection_idempotency_mark_and_check() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    store.ensure_projection_idempotency_table().await.unwrap();

    let projection = "patient_read_model";
    let event_id = Uuid::new_v4();

    assert!(
        !store
            .is_event_processed(projection, &event_id)
            .await
            .unwrap()
    );

    let first = store
        .try_mark_event_processed(projection, &event_id)
        .await
        .unwrap();
    let second = store
        .try_mark_event_processed(projection, &event_id)
        .await
        .unwrap();

    assert!(first);
    assert!(!second);
    assert!(
        store
            .is_event_processed(projection, &event_id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_apply_projection_once_runs_only_once_for_same_event() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    store.ensure_projection_idempotency_table().await.unwrap();

    let event = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: Some(Uuid::new_v4()),
        data: TestEvent {
            user_id: "u1".to_string(),
            action: "project".to_string(),
            amount: Some(1),
        },
        metadata: None,
        name: "ProjectEvent".to_string(),
    };

    let persisted = store
        .append_event("projection_stream", &ExpectedVersion::NoStream, &event)
        .await
        .unwrap();

    let apply_count = Arc::new(AtomicUsize::new(0));
    let count_a = apply_count.clone();
    let count_b = apply_count.clone();

    let first = store
        .apply_projection_once("p1", &persisted, move |_| {
            let count = count_a.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();
    let second = store
        .apply_projection_once("p1", &persisted, move |_| {
            let count = count_b.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();

    assert!(first);
    assert!(!second);
    assert_eq!(apply_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_apply_projection_once_rolls_back_marker_on_failure() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    store.ensure_projection_idempotency_table().await.unwrap();

    let event = EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: Some(Uuid::new_v4()),
        data: TestEvent {
            user_id: "u2".to_string(),
            action: "project-fail".to_string(),
            amount: Some(1),
        },
        metadata: None,
        name: "ProjectFailEvent".to_string(),
    };

    let persisted = store
        .append_event("projection_stream_2", &ExpectedVersion::NoStream, &event)
        .await
        .unwrap();

    let fail_result = store
        .apply_projection_once("p2", &persisted, |_| async {
            Err(anyhow::anyhow!("projection failed"))
        })
        .await;
    assert!(fail_result.is_err());
    assert!(!store.is_event_processed("p2", &persisted.id).await.unwrap());

    let retry = store
        .apply_projection_once("p2", &persisted, |_| async { Ok(()) })
        .await
        .unwrap();
    assert!(retry);
    assert!(store.is_event_processed("p2", &persisted.id).await.unwrap());
}

#[tokio::test]
async fn test_get_events_page_cursor_pagination() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    let stream_id = "cursor_stream";

    for i in 0..7 {
        let event = EventWrite::<TestEvent, TestMetadata> {
            id: Uuid::new_v4(),
            correlation_id: Some(Uuid::new_v4()),
            causation_id: Some(Uuid::new_v4()),
            data: TestEvent {
                user_id: "cursor-user".to_string(),
                action: format!("action_{i}"),
                amount: Some(i),
            },
            metadata: None,
            name: "CursorEvent".to_string(),
        };
        let expected = if i == 0 {
            ExpectedVersion::NoStream
        } else {
            ExpectedVersion::Any
        };
        store
            .append_event(stream_id, &expected, &event)
            .await
            .unwrap();
    }

    let page1 = store
        .get_events_page::<TestEvent, TestMetadata>(stream_id, None, 3)
        .await
        .unwrap();
    assert_eq!(page1.items.len(), 3);
    assert_eq!(page1.items[0].version, 0);
    assert_eq!(page1.items[2].version, 2);
    assert_eq!(page1.next_cursor, Some(2));

    let page2 = store
        .get_events_page::<TestEvent, TestMetadata>(stream_id, page1.next_cursor, 3)
        .await
        .unwrap();
    assert_eq!(page2.items.len(), 3);
    assert_eq!(page2.items[0].version, 3);
    assert_eq!(page2.items[2].version, 5);
    assert_eq!(page2.next_cursor, Some(5));

    let page3 = store
        .get_events_page::<TestEvent, TestMetadata>(stream_id, page2.next_cursor, 3)
        .await
        .unwrap();
    assert_eq!(page3.items.len(), 1);
    assert_eq!(page3.items[0].version, 6);
    assert_eq!(page3.next_cursor, None);
}

#[tokio::test]
async fn test_get_streams_time_filters() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;

    let event = |action: &str| EventWrite::<TestEvent, TestMetadata> {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: Some(Uuid::new_v4()),
        data: TestEvent {
            user_id: "u1".to_string(),
            action: action.to_string(),
            amount: None,
        },
        metadata: None,
        name: "TimeEvent".to_string(),
    };

    store
        .append_event("time_stream_1", &ExpectedVersion::NoStream, &event("first"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    store
        .append_event(
            "time_stream_2",
            &ExpectedVersion::NoStream,
            &event("second"),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    store
        .append_event("time_stream_3", &ExpectedVersion::NoStream, &event("third"))
        .await
        .unwrap();

    let streams =
        EventStore::<TestEvent, TestMetadata>::get_streams(&store, &StreamsReadFilter::AllStreams)
            .await
            .unwrap();
    assert_eq!(streams.len(), 3);

    let t1 = streams
        .iter()
        .find(|s| s.id == "time_stream_1")
        .unwrap()
        .last_updated_utc;
    let t2 = streams
        .iter()
        .find(|s| s.id == "time_stream_2")
        .unwrap()
        .last_updated_utc;
    let t3 = streams
        .iter()
        .find(|s| s.id == "time_stream_3")
        .unwrap()
        .last_updated_utc;

    let before_t2 = EventStore::<TestEvent, TestMetadata>::get_streams(
        &store,
        &StreamsReadFilter::BeforeTime(t2),
    )
    .await
    .unwrap();
    assert!(
        before_t2.iter().all(|s| s.last_updated_utc < t2),
        "BeforeTime(t2) should only return streams with last_updated_utc < t2"
    );

    let after_t2 = EventStore::<TestEvent, TestMetadata>::get_streams(
        &store,
        &StreamsReadFilter::AfterTime(t2),
    )
    .await
    .unwrap();
    assert!(
        after_t2.iter().all(|s| s.last_updated_utc > t2),
        "AfterTime(t2) should only return streams with last_updated_utc > t2"
    );

    let between = EventStore::<TestEvent, TestMetadata>::get_streams(
        &store,
        &StreamsReadFilter::BetweenTimes(t1, t3),
    )
    .await
    .unwrap();
    assert!(
        between
            .iter()
            .all(|s| s.last_updated_utc >= t1 && s.last_updated_utc <= t3),
        "BetweenTimes should return streams in range [t1, t3]"
    );
    assert_eq!(between.len(), 3);
}

#[tokio::test]
async fn test_get_streams_version_filters() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;

    for i in 0..5 {
        let stream_id = format!("version_stream_{i}");
        let event = EventWrite::<TestEvent, TestMetadata> {
            id: Uuid::new_v4(),
            correlation_id: Some(Uuid::new_v4()),
            causation_id: Some(Uuid::new_v4()),
            data: TestEvent {
                user_id: format!("user{i}"),
                action: "create".to_string(),
                amount: Some(i),
            },
            metadata: None,
            name: "VersionEvent".to_string(),
        };
        store
            .append_event(&stream_id, &ExpectedVersion::NoStream, &event)
            .await
            .unwrap();
    }

    let all =
        EventStore::<TestEvent, TestMetadata>::get_streams(&store, &StreamsReadFilter::AllStreams)
            .await
            .unwrap();
    assert_eq!(all.len(), 5);

    let before_v1 = EventStore::<TestEvent, TestMetadata>::get_streams(
        &store,
        &StreamsReadFilter::BeforeVersion(1),
    )
    .await
    .unwrap();
    assert!(before_v1.iter().all(|s| s.last_version < 1));

    let after_v0 = EventStore::<TestEvent, TestMetadata>::get_streams(
        &store,
        &StreamsReadFilter::AfterVersion(0),
    )
    .await
    .unwrap();
    assert!(after_v0.iter().all(|s| s.last_version > 0));

    let between = EventStore::<TestEvent, TestMetadata>::get_streams(
        &store,
        &StreamsReadFilter::BetweenVersions(0, 1),
    )
    .await
    .unwrap();
    assert!(between.iter().all(|s| s.last_version <= 1));
}

#[tokio::test]
async fn test_get_events_page_empty_stream() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    let page = store
        .get_events_page::<TestEvent, TestMetadata>("missing_cursor_stream", None, 5)
        .await
        .unwrap();
    assert!(page.items.is_empty());
    assert!(page.next_cursor.is_none());
}
