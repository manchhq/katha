#![cfg(feature = "sqlx")]

mod common;

use common::create_and_setup_memory_store;
use katha::{
    SqlxEventStore,
    traits::event_store::EventStore,
    types::{event_write::EventWrite, expected_version::ExpectedVersion},
};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::time::{Duration, timeout};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ScenarioEvent {
    action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ScenarioMeta;

#[tokio::test]
async fn behavior_projection_subscriber_applies_once_even_if_notified_twice() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    store.ensure_projection_idempotency_table().await.unwrap();
    let mut rx = store.subscribe();

    let event = EventWrite::<ScenarioEvent, ScenarioMeta> {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: None,
        data: ScenarioEvent {
            action: "patient-created".to_string(),
        },
        metadata: None,
        name: "ScenarioEvent".to_string(),
    };

    let persisted = EventStore::<ScenarioEvent, ScenarioMeta>::append_event(
        &store,
        "scenario-stream",
        &ExpectedVersion::NoStream,
        &event,
    )
    .await
    .unwrap();

    let notification = timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("timed out waiting for first notification")
        .expect("notification channel closed");
    assert_eq!(notification.event_ids, vec![persisted.id]);

    let applied_count = Arc::new(AtomicUsize::new(0));
    let count_a = applied_count.clone();
    let count_b = applied_count.clone();

    let first = store
        .apply_projection_once("scenario-projection", &persisted, move |_| {
            let count = count_a.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();

    let second = store
        .apply_projection_once("scenario-projection", &persisted, move |_| {
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
    assert_eq!(applied_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn behavior_projection_runner_processes_message_batch() {
    let store: SqlxEventStore = create_and_setup_memory_store().await;
    store.ensure_projection_idempotency_table().await.unwrap();

    let mut rx = EventStore::<ScenarioEvent, ScenarioMeta>::event_appended(&store)
        .expect("event_appended receiver expected");

    let event = EventWrite::<ScenarioEvent, ScenarioMeta> {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: None,
        data: ScenarioEvent {
            action: "runner".to_string(),
        },
        metadata: None,
        name: "ScenarioEvent".to_string(),
    };

    EventStore::<ScenarioEvent, ScenarioMeta>::append_event(
        &store,
        "runner-stream",
        &ExpectedVersion::NoStream,
        &event,
    )
    .await
    .unwrap();

    let apply_count = Arc::new(AtomicUsize::new(0));
    let count = apply_count.clone();

    let stats = timeout(
        Duration::from_millis(1000),
        store.process_projection_messages("runner-projection", &mut rx, 1, move |_| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }),
    )
    .await
    .expect("runner timed out")
    .unwrap();

    assert_eq!(stats.received, 1);
    assert_eq!(stats.applied, 1);
    assert_eq!(stats.skipped, 0);
    assert_eq!(apply_count.load(Ordering::SeqCst), 1);
}
