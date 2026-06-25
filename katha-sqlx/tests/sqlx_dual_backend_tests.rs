//! Backend-parity tests: every test runs against SQLite (in-memory, always)
//! and Postgres (when `KATHA_TEST_PG_URL` is set).
//!
//! These exercise the full query surface — every placeholder site and the
//! i64/BIGINT version columns — against a real Postgres so the `?` → `$N`
//! rewrite and the `BIGINT` migration stay correct on both dialects. Without a
//! Postgres URL the suite still runs the SQLite leg, so it is never skipped
//! entirely.

mod common;

use chrono::Utc;
use common::{command_store_backends, event_store_backends};
use katha::{
    traits::command_store::CommandStore,
    traits::event_store::EventStore,
    types::{
        command_write::{CommandRead, CommandWrite},
        event_read::EventRead,
        event_read_range::EventsReadRange,
        event_write::EventWrite,
        expected_version::ExpectedVersion,
        stream_read_filter::StreamsReadFilter,
    },
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestEvent {
    user_id: String,
    action: String,
    amount: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestMeta {
    source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TestCommand {
    action: String,
    amount: Option<i64>,
}

fn event(action: &str, correlation_id: Option<Uuid>) -> EventWrite<TestEvent, TestMeta> {
    EventWrite {
        id: Uuid::new_v4(),
        correlation_id,
        causation_id: None,
        data: TestEvent {
            user_id: "u1".to_string(),
            action: action.to_string(),
            amount: Some(42),
        },
        metadata: Some(TestMeta {
            source: "dual-backend".to_string(),
        }),
        name: format!("{action}Event"),
    }
}

/// Append + read round trip: exercises the streams/events INSERTs (9-column
/// event insert, ON CONFLICT stream upsert) and the get_event SELECT, plus the
/// i64 ↔ BIGINT version path.
#[tokio::test]
async fn append_and_get_event_roundtrip() {
    for (backend, store) in event_store_backends().await {
        let ev = event("deposit", Some(Uuid::new_v4()));
        store
            .append_event("acct-1", &ExpectedVersion::NoStream, &ev)
            .await
            .unwrap_or_else(|e| panic!("[{backend}] append failed: {e:?}"));

        let got: EventRead<TestEvent, TestMeta> =
            EventStore::<TestEvent, TestMeta>::get_event(&store, "acct-1", 0)
                .await
                .unwrap_or_else(|e| panic!("[{backend}] get_event failed: {e:?}"));

        assert_eq!(got.data, ev.data, "[{backend}] payload round-trips");
        assert_eq!(
            got.metadata, ev.metadata,
            "[{backend}] metadata round-trips"
        );
        assert_eq!(got.id, ev.id, "[{backend}] id round-trips");
        assert_eq!(got.version, 0, "[{backend}] version is 0");
    }
}

/// Optimistic concurrency: exercises the stream SELECT-for-version and both the
/// `Exact` and `Any` expected-version paths, plus a conflict.
#[tokio::test]
async fn expected_version_paths_and_conflict() {
    for (backend, store) in event_store_backends().await {
        store
            .append_event("s", &ExpectedVersion::NoStream, &event("a", None))
            .await
            .unwrap_or_else(|e| panic!("[{backend}] initial append: {e:?}"));

        // Exact(1) succeeds: next version after v0.
        store
            .append_event("s", &ExpectedVersion::Exact(1), &event("b", None))
            .await
            .unwrap_or_else(|e| panic!("[{backend}] Exact(1) append: {e:?}"));

        // Any appends at the tail.
        let third = store
            .append_event("s", &ExpectedVersion::Any, &event("c", None))
            .await
            .unwrap_or_else(|e| panic!("[{backend}] Any append: {e:?}"));
        assert_eq!(third.version, 2, "[{backend}] Any lands at v2");

        // Stale Exact(0) conflicts.
        let conflict = store
            .append_event("s", &ExpectedVersion::Exact(0), &event("d", None))
            .await;
        assert!(
            conflict.is_err(),
            "[{backend}] stale Exact(0) must conflict"
        );
    }
}

/// Range reads: exercises the version-comparison placeholders in get_events.
#[tokio::test]
async fn get_events_ranges() {
    for (backend, store) in event_store_backends().await {
        for i in 0..5 {
            let expected = if i == 0 {
                ExpectedVersion::NoStream
            } else {
                ExpectedVersion::Any
            };
            store
                .append_event("r", &expected, &event(&format!("e{i}"), None))
                .await
                .unwrap_or_else(|e| panic!("[{backend}] seed append {i}: {e:?}"));
        }

        let all =
            EventStore::<TestEvent, TestMeta>::get_events(&store, "r", &EventsReadRange::AllEvents)
                .await
                .unwrap();
        assert_eq!(all.len(), 5, "[{backend}] AllEvents");

        let from2 = EventStore::<TestEvent, TestMeta>::get_events(
            &store,
            "r",
            &EventsReadRange::FromVersion(2),
        )
        .await
        .unwrap();
        assert_eq!(from2.len(), 3, "[{backend}] FromVersion(2)");

        let range = EventStore::<TestEvent, TestMeta>::get_events(
            &store,
            "r",
            &EventsReadRange::VersionRange {
                from_version: 1,
                to_version: 3,
            },
        )
        .await
        .unwrap();
        assert_eq!(range.len(), 3, "[{backend}] VersionRange(1..=3)");
    }
}

/// Correlation / causation lookups: exercises the `correlation_id = ?` and
/// `causation_id = ?` placeholders.
#[tokio::test]
async fn correlation_and_causation_queries() {
    for (backend, store) in event_store_backends().await {
        let correlation_id = Uuid::new_v4();
        let first = event("start", Some(correlation_id));
        let mut second = event("followup", Some(correlation_id));
        second.causation_id = Some(first.id);

        store
            .append_event("c", &ExpectedVersion::NoStream, &first)
            .await
            .unwrap();
        store
            .append_event("c", &ExpectedVersion::Any, &second)
            .await
            .unwrap();

        let correlated = EventStore::<TestEvent, TestMeta>::get_events_by_correlation_id(
            &store,
            &correlation_id,
        )
        .await
        .unwrap();
        assert_eq!(correlated.len(), 2, "[{backend}] by correlation_id");

        let caused =
            EventStore::<TestEvent, TestMeta>::get_events_by_causation_id(&store, &first.id)
                .await
                .unwrap();
        assert_eq!(caused.len(), 1, "[{backend}] by causation_id");
        assert_eq!(caused[0].data.action, "followup");
    }
}

/// Stream filters: exercises every version and time filter placeholder plus the
/// placeholder-free AllStreams query and get_stream.
#[tokio::test]
async fn stream_version_and_time_filters() {
    for (backend, store) in event_store_backends().await {
        for i in 0..3 {
            store
                .append_event(
                    &format!("st{i}"),
                    &ExpectedVersion::NoStream,
                    &event(&format!("e{i}"), None),
                )
                .await
                .unwrap();
            // Distinct created_utc so time filters are meaningful.
            tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
        }

        let all =
            EventStore::<TestEvent, TestMeta>::get_streams(&store, &StreamsReadFilter::AllStreams)
                .await
                .unwrap();
        assert_eq!(all.len(), 3, "[{backend}] AllStreams");

        let after_v_minus = EventStore::<TestEvent, TestMeta>::get_streams(
            &store,
            &StreamsReadFilter::AfterVersion(0),
        )
        .await
        .unwrap();
        assert!(
            after_v_minus.is_empty(),
            "[{backend}] AfterVersion(0): all single-event streams are at v0"
        );

        let before_v1 = EventStore::<TestEvent, TestMeta>::get_streams(
            &store,
            &StreamsReadFilter::BeforeVersion(1),
        )
        .await
        .unwrap();
        assert_eq!(before_v1.len(), 3, "[{backend}] BeforeVersion(1)");

        let between_v = EventStore::<TestEvent, TestMeta>::get_streams(
            &store,
            &StreamsReadFilter::BetweenVersions(0, 0),
        )
        .await
        .unwrap();
        assert_eq!(between_v.len(), 3, "[{backend}] BetweenVersions(0,0)");

        // Time filters: pick the middle stream's timestamp as the pivot.
        let mid = all
            .iter()
            .find(|s| s.id == "st1")
            .expect("st1 present")
            .last_updated_utc;

        let before_mid = EventStore::<TestEvent, TestMeta>::get_streams(
            &store,
            &StreamsReadFilter::BeforeTime(mid),
        )
        .await
        .unwrap();
        assert!(
            before_mid.iter().all(|s| s.last_updated_utc < mid),
            "[{backend}] BeforeTime"
        );

        let after_mid = EventStore::<TestEvent, TestMeta>::get_streams(
            &store,
            &StreamsReadFilter::AfterTime(mid),
        )
        .await
        .unwrap();
        assert!(
            after_mid.iter().all(|s| s.last_updated_utc > mid),
            "[{backend}] AfterTime"
        );

        let between_t = EventStore::<TestEvent, TestMeta>::get_streams(
            &store,
            &StreamsReadFilter::BetweenTimes(
                all.iter().map(|s| s.last_updated_utc).min().unwrap(),
                all.iter().map(|s| s.last_updated_utc).max().unwrap(),
            ),
        )
        .await
        .unwrap();
        assert_eq!(between_t.len(), 3, "[{backend}] BetweenTimes");

        let one = EventStore::<TestEvent, TestMeta>::get_stream(&store, "st1")
            .await
            .unwrap();
        assert_eq!(one.id, "st1", "[{backend}] get_stream");
    }
}

/// Cursor pagination over events: exercises the `version > ?` + `LIMIT ?`
/// placeholders and BIGINT cursor math.
#[tokio::test]
async fn event_cursor_pagination() {
    for (backend, store) in event_store_backends().await {
        for i in 0..7 {
            let expected = if i == 0 {
                ExpectedVersion::NoStream
            } else {
                ExpectedVersion::Any
            };
            store
                .append_event("p", &expected, &event(&format!("e{i}"), None))
                .await
                .unwrap();
        }

        let page1 = store
            .get_events_page::<TestEvent, TestMeta>("p", None, 3)
            .await
            .unwrap();
        assert_eq!(page1.items.len(), 3, "[{backend}] page1 size");
        assert_eq!(page1.next_cursor, Some(2), "[{backend}] page1 cursor");

        let page3 = store
            .get_events_page::<TestEvent, TestMeta>("p", Some(5), 3)
            .await
            .unwrap();
        assert_eq!(page3.items.len(), 1, "[{backend}] page3 size");
        assert_eq!(page3.next_cursor, None, "[{backend}] page3 no cursor");
    }
}

/// reset_all: exercises the DELETE statements + re-migration.
#[tokio::test]
async fn reset_all_clears_store() {
    for (backend, store) in event_store_backends().await {
        store
            .append_event("z", &ExpectedVersion::NoStream, &event("x", None))
            .await
            .unwrap();
        store
            .reset_all()
            .await
            .unwrap_or_else(|e| panic!("[{backend}] reset_all: {e:?}"));

        let all =
            EventStore::<TestEvent, TestMeta>::get_streams(&store, &StreamsReadFilter::AllStreams)
                .await
                .unwrap();
        assert!(all.is_empty(), "[{backend}] store empty after reset");
    }
}

/// Projection idempotency: exercises the projection_processed INSERT/SELECT/
/// DELETE placeholders, including the rollback-on-failure path.
#[tokio::test]
async fn projection_idempotency_and_rollback() {
    for (backend, store) in event_store_backends().await {
        let persisted = store
            .append_event("pj", &ExpectedVersion::NoStream, &event("proj", None))
            .await
            .unwrap();

        assert!(
            !store.is_event_processed("p", &persisted.id).await.unwrap(),
            "[{backend}] not processed initially"
        );

        let first = store
            .try_mark_event_processed("p", &persisted.id)
            .await
            .unwrap();
        let second = store
            .try_mark_event_processed("p", &persisted.id)
            .await
            .unwrap();
        assert!(first, "[{backend}] first mark inserts");
        assert!(!second, "[{backend}] second mark is a no-op");

        // Rollback path on a different projection: failure must un-mark.
        let fail = store
            .apply_projection_once("p2", &persisted, |_| async { Err(anyhow::anyhow!("boom")) })
            .await;
        assert!(fail.is_err(), "[{backend}] apply propagates error");
        assert!(
            !store.is_event_processed("p2", &persisted.id).await.unwrap(),
            "[{backend}] marker rolled back after failure"
        );

        let retry = store
            .apply_projection_once("p2", &persisted, |_| async { Ok(()) })
            .await
            .unwrap();
        assert!(retry, "[{backend}] retry succeeds after rollback");
    }
}

// ── Command store ────────────────────────────────────────────────────────────

fn command(action: &str) -> CommandWrite<TestCommand> {
    CommandWrite {
        id: Uuid::new_v4(),
        correlation_id: Uuid::new_v4(),
        causation_id: Some(Uuid::new_v4()),
        data: TestCommand {
            action: action.to_string(),
            amount: Some(7),
        },
        name: format!("{action}Cmd"),
    }
}

/// Command append + get: exercises the 6-column command INSERT and the
/// `WHERE id = ?` SELECT.
#[tokio::test]
async fn command_append_and_get() {
    for (backend, store) in command_store_backends().await {
        let cmd = command("create");
        CommandStore::<TestCommand>::append_command(&store, &cmd)
            .await
            .unwrap_or_else(|e| panic!("[{backend}] append_command: {e:?}"));

        let got = CommandStore::<TestCommand>::get_command(&store, &cmd.id)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("[{backend}] command should exist"));
        assert_eq!(got.id, cmd.id, "[{backend}] id");
        assert_eq!(got.data.action, "create", "[{backend}] payload");
        assert_eq!(
            got.causation_id, cmd.causation_id,
            "[{backend}] causation_id"
        );

        let missing = CommandStore::<TestCommand>::get_command(&store, &Uuid::new_v4())
            .await
            .unwrap();
        assert!(missing.is_none(), "[{backend}] missing command is None");
    }
}

/// Offset pagination: exercises `LIMIT ? OFFSET ?` and the None-limit branch
/// (`LIMIT 9223372036854775807 OFFSET ?`).
#[tokio::test]
async fn command_offset_pagination() {
    for (backend, store) in command_store_backends().await {
        for i in 0..3 {
            CommandStore::<TestCommand>::append_command(&store, &command(&format!("c{i}")))
                .await
                .unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
        }

        let page = CommandStore::<TestCommand>::get_commands(&store, Some(2), 0)
            .await
            .unwrap();
        assert_eq!(page.len(), 2, "[{backend}] limit=2 offset=0");

        let tail = CommandStore::<TestCommand>::get_commands(&store, Some(2), 2)
            .await
            .unwrap();
        assert_eq!(tail.len(), 1, "[{backend}] limit=2 offset=2");

        let all: Vec<CommandRead<TestCommand>> =
            CommandStore::<TestCommand>::get_commands(&store, None, 0)
                .await
                .unwrap();
        assert_eq!(all.len(), 3, "[{backend}] None-limit returns all");
    }
}

/// Cursor pagination: exercises the row-value `(created_utc, id) < (?, ?)`
/// comparison and `LIMIT ?` on both dialects.
#[tokio::test]
async fn command_cursor_pagination() {
    for (backend, store) in command_store_backends().await {
        for i in 0..5 {
            CommandStore::<TestCommand>::append_command(&store, &command(&format!("c{i}")))
                .await
                .unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
        }

        let first = store
            .get_commands_page::<TestCommand>(None, 3)
            .await
            .unwrap_or_else(|e| panic!("[{backend}] page1: {e:?}"));
        assert_eq!(first.items.len(), 3, "[{backend}] page1 size");
        let cursor = first.next_cursor.expect("[cursor] expected from page1");

        let second = store
            .get_commands_page::<TestCommand>(Some(&cursor), 3)
            .await
            .unwrap();
        assert_eq!(second.items.len(), 2, "[{backend}] page2 size");
        assert!(second.next_cursor.is_none(), "[{backend}] page2 last");

        // Newest-first ordering holds across the page boundary.
        let combined: Vec<_> = first.items.iter().chain(second.items.iter()).collect();
        for w in combined.windows(2) {
            assert!(
                w[0].created_utc >= w[1].created_utc,
                "[{backend}] newest-first ordering"
            );
        }

        // Out-of-range cursor (epoch) returns nothing.
        let stale = katha_sqlx::CommandCursor {
            created_utc: Utc::now() - chrono::Duration::days(3650),
            id: Uuid::nil(),
        };
        let empty = store
            .get_commands_page::<TestCommand>(Some(&stale), 10)
            .await
            .unwrap();
        assert!(empty.items.is_empty(), "[{backend}] stale cursor empty");
    }
}
