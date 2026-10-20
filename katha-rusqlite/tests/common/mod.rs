use anyhow::Result;
use chrono::{Duration, Utc};
use katha::types::{event_stream::EventStream, stream_read_filter::StreamsReadFilter};
use katha::{
    ConcurrencyConflict, EventRead, EventStore, EventWrite, EventsReadRange, ExpectedVersion,
};
use katha_rusqlite::RusqliteEventStore;
use serde_json::{Value, json};
use uuid::Uuid;
pub type Store = RusqliteEventStore<Value, Value>;

type Event = EventWrite<Value, Value>;

pub fn event() -> Event {
    EventWrite {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: Some(Uuid::new_v4()),
        name: "Created".into(),
        data: json!({"hello": "नमस्ते"}),
        metadata: Some(json!({"source": "test"})),
    }
}

async fn ensure(store: &Store) -> Result<()> {
    <Store as EventStore<Value, Value>>::ensure_events_table(store).await
}
async fn read(
    store: &Store,
    stream: &str,
    range: EventsReadRange,
) -> Result<Vec<EventRead<Value, Value>>> {
    store.get_events(stream, &range).await
}
async fn streams(store: &Store, filter: StreamsReadFilter) -> Result<Vec<EventStream>> {
    <Store as EventStore<Value, Value>>::get_streams(store, &filter).await
}

pub async fn exercise(store: &Store) -> Result<()> {
    ensure(store).await?;
    ensure(store).await?;
    let first = event();
    let second = EventWrite {
        metadata: None,
        correlation_id: None,
        causation_id: None,
        ..event()
    };
    let written = store
        .append_events(
            "account_%α",
            &ExpectedVersion::NoStream,
            vec![first.clone(), second.clone()],
        )
        .await?;
    assert_eq!(written[0].id, first.id);
    assert_eq!(written[0].correlation_id, first.correlation_id);
    assert_eq!(written[0].causation_id, first.causation_id);
    assert_eq!(written[0].metadata, first.metadata);
    assert_eq!(written[0].data, first.data);
    assert_eq!(written[0].name, first.name);
    assert_eq!(
        written.iter().map(|e| e.version).collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(
        read(store, "account_%α", EventsReadRange::AllEvents).await?,
        written
    );
    assert_eq!(
        read(store, "account_%α", EventsReadRange::FromVersion(1)).await?,
        vec![written[1].clone()]
    );
    assert_eq!(
        read(store, "account_%α", EventsReadRange::ToVersion(0)).await?,
        vec![written[0].clone()]
    );
    assert_eq!(
        read(
            store,
            "account_%α",
            EventsReadRange::VersionRange {
                from_version: 0,
                to_version: 0
            }
        )
        .await?,
        vec![written[0].clone()]
    );
    assert!(
        read(
            store,
            "account_%α",
            EventsReadRange::VersionRange {
                from_version: 2,
                to_version: 1
            }
        )
        .await?
        .is_empty()
    );
    let fetched: EventRead<Value, Value> = store.get_event("account_%α", 0).await?;
    assert_eq!(fetched, written[0]);
    let correlated: Vec<EventRead<Value, Value>> = store
        .get_events_by_correlation_id(&first.correlation_id.unwrap())
        .await?;
    let caused: Vec<EventRead<Value, Value>> = store
        .get_events_by_causation_id(&first.causation_id.unwrap())
        .await?;
    assert_eq!(correlated, vec![written[0].clone()]);
    assert_eq!(caused, correlated);
    assert!(
        store
            .append_event("account_%α", &ExpectedVersion::NoStream, &event())
            .await
            .is_err()
    );
    let error = store
        .append_event("account_%α", &ExpectedVersion::Exact(1), &event())
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<ConcurrencyConflict>().unwrap().actual,
        2
    );
    let error = store
        .append_event("missing", &ExpectedVersion::Exact(1), &event())
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<ConcurrencyConflict>().unwrap().actual,
        0
    );
    assert_eq!(
        store
            .append_event("account_%α", &ExpectedVersion::Exact(2), &event())
            .await?
            .version,
        2
    );
    assert_eq!(
        store
            .append_event("account_%α", &ExpectedVersion::Any, &event())
            .await?
            .version,
        3
    );
    assert_eq!(
        store
            .append_event("ACCOUNT_other", &ExpectedVersion::Exact(0), &event())
            .await?
            .version,
        0
    );
    assert_eq!(
        streams(store, StreamsReadFilter::AllStreams).await?.len(),
        2
    );
    assert_eq!(
        streams(store, StreamsReadFilter::IdPrefix("account_%".into()))
            .await?
            .len(),
        1
    );
    assert!(
        streams(store, StreamsReadFilter::IdPrefix("account_\\".into()))
            .await?
            .is_empty()
    );
    assert_eq!(
        streams(store, StreamsReadFilter::BeforeVersion(1))
            .await?
            .len(),
        1
    );
    assert_eq!(
        streams(store, StreamsReadFilter::AfterVersion(0))
            .await?
            .len(),
        1
    );
    assert_eq!(
        streams(store, StreamsReadFilter::BetweenVersions(3, 3))
            .await?
            .len(),
        1
    );
    let before = Utc::now() - Duration::days(1);
    let after = Utc::now() + Duration::days(1);
    assert_eq!(
        streams(store, StreamsReadFilter::BeforeTime(after))
            .await?
            .len(),
        2
    );
    assert_eq!(
        streams(store, StreamsReadFilter::AfterTime(before))
            .await?
            .len(),
        2
    );
    assert_eq!(
        streams(store, StreamsReadFilter::BetweenTimes(before, after))
            .await?
            .len(),
        2
    );
    let stream = <Store as EventStore<Value, Value>>::get_stream(store, "account_%α").await?;
    assert_eq!(stream.last_version, 3);
    assert!(
        <Store as EventStore<Value, Value>>::get_stream(store, "missing")
            .await
            .is_err()
    );
    assert!(
        <Store as EventStore<Value, Value>>::get_event(store, "missing", 0)
            .await
            .is_err()
    );
    assert!(
        store
            .append_events("missing", &ExpectedVersion::Any, Vec::<Event>::new())
            .await?
            .is_empty()
    );
    assert!(
        read(store, "missing", EventsReadRange::AllEvents)
            .await?
            .is_empty()
    );
    // Duplicate IDs fail after the first insert: neither the batch nor stream head survives.
    let duplicate = event();
    assert!(
        store
            .append_events(
                "rollback",
                &ExpectedVersion::NoStream,
                vec![duplicate.clone(), duplicate]
            )
            .await
            .is_err()
    );
    assert!(
        read(store, "rollback", EventsReadRange::AllEvents)
            .await?
            .is_empty()
    );
    assert!(
        <Store as EventStore<Value, Value>>::get_stream(store, "rollback")
            .await
            .is_err()
    );
    let duplicate = written[0].id;
    assert!(
        store
            .append_events(
                "account_%α",
                &ExpectedVersion::Exact(4),
                vec![
                    event(),
                    EventWrite {
                        id: duplicate,
                        ..event()
                    }
                ]
            )
            .await
            .is_err()
    );
    assert_eq!(
        read(store, "account_%α", EventsReadRange::AllEvents)
            .await?
            .len(),
        4
    );
    assert_eq!(
        store
            .append_event("account_%α", &ExpectedVersion::Exact(4), &event())
            .await?
            .version,
        4
    );
    Ok(())
}

/// Projection effects and receipts either both commit or both roll back.
pub async fn projection_contract(store: &Store) -> Result<()> {
    let first = store
        .append_event("projection-source", &ExpectedVersion::Any, &event())
        .await?;
    assert!(
        store
            .apply_projection_once("atomic", first.id, |tx, _| {
                tx.execute_batch(
                    "CREATE TABLE counts (n INTEGER NOT NULL); INSERT INTO counts VALUES (0);",
                )?;
                Ok(())
            })
            .await?
    );
    assert!(
        !store
            .apply_projection_once("atomic", first.id, |_, _| anyhow::bail!(
                "must not run twice"
            ))
            .await?
    );
    let second = store
        .append_event("projection-source", &ExpectedVersion::Any, &event())
        .await?;
    assert!(
        store
            .apply_projection_once("atomic", second.id, |tx, _| {
                tx.execute("UPDATE counts SET n = n + 1", [])?;
                anyhow::bail!("simulate failed projection")
            })
            .await
            .is_err()
    );
    assert_eq!(
        store
            .pending_events("atomic")
            .await?
            .iter()
            .filter(|e| e.stream_id == "projection-source")
            .count(),
        1
    );
    assert!(
        store
            .apply_projection_once("atomic", second.id, |tx, e| {
                assert_eq!(e.stream_id, "projection-source");
                let count: i64 = tx.query_row("SELECT n FROM counts", [], |r| r.get(0))?;
                assert_eq!(count, 0, "failed callback rolled back effects");
                tx.execute("UPDATE counts SET n = n + 1", [])?;
                Ok(())
            })
            .await?
    );
    assert!(
        !store
            .apply_projection_once("atomic", second.id, |_, _| anyhow::bail!(
                "must not run twice"
            ))
            .await?
    );
    Ok(())
}
