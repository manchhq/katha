use anyhow::Result;
use chrono::{Duration, Utc};
use katha::types::{event_stream::EventStream, stream_read_filter::StreamsReadFilter};
use katha::{
    CommandRead, CommandStore, CommandWrite, ConcurrencyConflict, EventRead, EventStore,
    EventWrite, EventsReadRange, ExpectedVersion,
};
use katha_libsql::{LibsqlCommandStore, LibsqlDatabase, LibsqlEventStore};
use serde_json::{Value, json};
use uuid::Uuid;

type Event = EventWrite<Value, Value>;

fn event() -> Event {
    EventWrite {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: Some(Uuid::new_v4()),
        name: "Created".into(),
        data: json!({"hello": "नमस्ते"}),
        metadata: Some(json!({"source": "test"})),
    }
}

async fn ensure(store: &LibsqlEventStore) -> Result<()> {
    <LibsqlEventStore as EventStore<Value, Value>>::ensure_events_table(store).await
}
async fn read(
    store: &LibsqlEventStore,
    stream: &str,
    range: EventsReadRange,
) -> Result<Vec<EventRead<Value, Value>>> {
    store.get_events(stream, &range).await
}
async fn streams(store: &LibsqlEventStore, filter: StreamsReadFilter) -> Result<Vec<EventStream>> {
    <LibsqlEventStore as EventStore<Value, Value>>::get_streams(store, &filter).await
}

async fn exercise(store: &LibsqlEventStore) -> Result<()> {
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
    let stream =
        <LibsqlEventStore as EventStore<Value, Value>>::get_stream(store, "account_%α").await?;
    assert_eq!(stream.last_version, 3);
    assert!(
        <LibsqlEventStore as EventStore<Value, Value>>::get_stream(store, "missing")
            .await
            .is_err()
    );
    assert!(
        <LibsqlEventStore as EventStore<Value, Value>>::get_event(store, "missing", 0)
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
        <LibsqlEventStore as EventStore<Value, Value>>::get_stream(store, "rollback")
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

#[tokio::test]
async fn event_contract_in_memory() -> Result<()> {
    exercise(&LibsqlEventStore::new_memory("test").await?).await
}

#[tokio::test]
async fn file_persistence_and_independent_writers() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("events ?# नमस्ते.db");
    let store = LibsqlEventStore::new_local("test", &path).await?;
    exercise(&store).await?;
    drop(store);
    let a = LibsqlEventStore::new_local("test", &path).await?;
    let b = LibsqlEventStore::new_local("test", &path).await?;
    assert_eq!(
        read(&a, "account_%α", EventsReadRange::AllEvents)
            .await?
            .len(),
        5
    );
    let first = event();
    let second = event();
    let (left, right) = tokio::join!(
        a.append_event("race", &ExpectedVersion::NoStream, &first),
        b.append_event("race", &ExpectedVersion::NoStream, &second),
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert_eq!(read(&a, "race", EventsReadRange::AllEvents).await?.len(), 1);
    let third = event();
    let fourth = event();
    let (left, right) = tokio::join!(
        a.append_event("race", &ExpectedVersion::Exact(1), &third),
        b.append_event("race", &ExpectedVersion::Exact(1), &fourth),
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert_eq!(read(&b, "race", EventsReadRange::AllEvents).await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn commands_share_database_and_persist() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("commands.db");
    let database = LibsqlDatabase::new_local(&path).await?;
    let events = LibsqlEventStore::new_from_database("test", database.clone())?;
    let commands = LibsqlCommandStore::new_from_database("test", database)?;
    ensure(&events).await?;
    <LibsqlCommandStore as CommandStore<Value>>::ensure_commands_table(&commands).await?;
    <LibsqlCommandStore as CommandStore<Value>>::ensure_commands_table(&commands).await?;
    let command = CommandWrite {
        id: Uuid::new_v4(),
        correlation_id: Uuid::new_v4(),
        causation_id: Some(Uuid::new_v4()),
        name: "Create".into(),
        data: json!({"amount": 42}),
    };
    commands.append_command(&command).await?;
    assert!(commands.append_command(&command).await.is_err());
    let fetched: CommandRead<Value> = commands.get_command(&command.id).await?.unwrap();
    assert_eq!(fetched.id, command.id);
    assert_eq!(fetched.data, command.data);
    assert_eq!(fetched.correlation_id, command.correlation_id);
    assert_eq!(fetched.causation_id, command.causation_id);
    let all: Vec<CommandRead<Value>> = commands.get_commands(None, 0).await?;
    assert_eq!(all, vec![fetched.clone()]);
    assert!(
        <LibsqlCommandStore as CommandStore<Value>>::get_commands(&commands, Some(0), 0)
            .await?
            .is_empty()
    );
    assert!(
        <LibsqlCommandStore as CommandStore<Value>>::get_commands(&commands, None, 1)
            .await?
            .is_empty()
    );
    assert!(
        <LibsqlCommandStore as CommandStore<Value>>::get_command(&commands, &Uuid::new_v4())
            .await?
            .is_none()
    );
    commands
        .append_command(&CommandWrite {
            id: Uuid::new_v4(),
            causation_id: None,
            ..command.clone()
        })
        .await?;
    let page: Vec<CommandRead<Value>> = commands.get_commands(Some(1), 1).await?;
    assert_eq!(page.len(), 1);
    assert_eq!(page[0], fetched);
    events
        .append_event(
            "account",
            &ExpectedVersion::NoStream,
            &EventWrite {
                causation_id: Some(command.id),
                ..event()
            },
        )
        .await?;
    drop(commands);
    let reopened = LibsqlCommandStore::new_local("test", &path).await?;
    let persisted: CommandRead<Value> = reopened.get_command(&command.id).await?.unwrap();
    assert_eq!(persisted, fetched);
    Ok(())
}

#[tokio::test]
async fn shared_memory_and_namespaces() -> Result<()> {
    let database = LibsqlDatabase::new_memory().await?;
    let first = LibsqlEventStore::new_from_database("one", database.clone())?;
    let second = LibsqlEventStore::new_from_database("two", database.clone())?;
    let commands = LibsqlCommandStore::new_from_database("one", database.clone())?;
    ensure(&first).await?;
    ensure(&second).await?;
    <LibsqlCommandStore as CommandStore<Value>>::ensure_commands_table(&commands).await?;
    first
        .append_event("stream", &ExpectedVersion::Any, &event())
        .await?;
    let clone = first.clone();
    assert_eq!(
        read(&clone, "stream", EventsReadRange::AllEvents)
            .await?
            .len(),
        1
    );
    assert!(
        read(&second, "stream", EventsReadRange::AllEvents)
            .await?
            .is_empty()
    );
    for name in ["", "bad-name", "bad\";DROP TABLE one_events;--", "日本語"] {
        assert!(LibsqlEventStore::new_from_database(name, database.clone()).is_err());
        assert!(LibsqlCommandStore::new_from_database(name, database.clone()).is_err());
    }
    assert!(!format!("{first:?}").contains("token"));
    Ok(())
}

#[tokio::test]
async fn version_overflow_rolls_back() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("overflow.db");
    let store = LibsqlEventStore::new_local("test", &path).await?;
    ensure(&store).await?;
    let raw = libsql::Builder::new_local(&path).build().await?;
    raw.connect()?
        .execute(
            "INSERT INTO test_streams VALUES ('full', ?, ?)",
            libsql::params![i64::from(u32::MAX), Utc::now().to_rfc3339()],
        )
        .await?;
    assert!(
        store
            .append_event("full", &ExpectedVersion::Any, &event())
            .await
            .is_err()
    );
    raw.connect()?
        .execute(
            "UPDATE test_streams SET last_version = ? WHERE id = 'full'",
            [i64::from(u32::MAX) - 1],
        )
        .await?;
    assert!(
        store
            .append_events("full", &ExpectedVersion::Any, vec![event(), event()])
            .await
            .is_err()
    );
    assert!(
        read(&store, "full", EventsReadRange::AllEvents)
            .await?
            .is_empty()
    );
    assert_eq!(
        store
            .append_event("full", &ExpectedVersion::Any, &event())
            .await?
            .version,
        u32::MAX
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a dedicated Turso test database and credentials"]
async fn remote_turso_contract() -> Result<()> {
    let url = std::env::var("KATHA_TEST_TURSO_URL")?;
    let token = std::env::var("KATHA_TEST_TURSO_TOKEN")?;
    let name = format!("test_{}", Uuid::new_v4().simple());
    let store = LibsqlEventStore::new_remote(&name, &url, &token).await?;
    let commands = LibsqlCommandStore::new_remote(&name, &url, &token).await?;
    let result = async {
        exercise(&store).await?;
        <LibsqlCommandStore as CommandStore<Value>>::ensure_commands_table(&commands).await?;
        let command = CommandWrite {
            id: Uuid::new_v4(),
            correlation_id: Uuid::new_v4(),
            causation_id: Some(Uuid::new_v4()),
            name: "Remote".into(),
            data: json!({"remote": true}),
        };
        commands.append_command(&command).await?;
        let fetched: CommandRead<Value> = commands.get_command(&command.id).await?.unwrap();
        assert_eq!(fetched.data, command.data);
        assert_eq!(fetched.causation_id, command.causation_id);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let raw = libsql::Builder::new_remote(url, token).build().await?;
    raw.connect()?.execute_batch(&format!(
        "DROP TABLE IF EXISTS \"{name}_events\"; DROP TABLE IF EXISTS \"{name}_streams\"; DROP TABLE IF EXISTS \"{name}_commands\";"
    )).await?;
    result?;
    Ok(())
}

#[derive(Clone, serde::Deserialize)]
struct FalliblePayload {
    fail: bool,
}

impl serde::Serialize for FalliblePayload {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        if self.fail {
            return Err(serde::ser::Error::custom(
                "intentional serialization failure",
            ));
        }
        serializer.serialize_bool(false)
    }
}

#[tokio::test]
async fn serialization_failure_is_atomic() -> Result<()> {
    let database = LibsqlDatabase::new_memory().await?;
    let store = LibsqlEventStore::new_from_database("test", database.clone())?;
    ensure(&store).await?;
    let good = EventWrite {
        id: Uuid::new_v4(),
        correlation_id: None,
        causation_id: None,
        name: "Test".into(),
        data: FalliblePayload { fail: false },
        metadata: None::<Value>,
    };
    let bad = EventWrite {
        id: Uuid::new_v4(),
        data: FalliblePayload { fail: true },
        ..good.clone()
    };
    assert!(
        store
            .append_events("stream", &ExpectedVersion::Any, vec![good, bad])
            .await
            .is_err()
    );
    assert!(
        read(&store, "stream", EventsReadRange::AllEvents)
            .await?
            .is_empty()
    );
    assert!(
        <LibsqlEventStore as EventStore<Value, Value>>::get_stream(&store, "stream")
            .await
            .is_err()
    );
    let commands = LibsqlCommandStore::new_from_database("test", database)?;
    <LibsqlCommandStore as CommandStore<Value>>::ensure_commands_table(&commands).await?;
    let bad = CommandWrite {
        id: Uuid::new_v4(),
        correlation_id: Uuid::new_v4(),
        causation_id: None,
        name: "Test".into(),
        data: FalliblePayload { fail: true },
    };
    assert!(commands.append_command(&bad).await.is_err());
    assert!(
        <LibsqlCommandStore as CommandStore<Value>>::get_commands(&commands, None, 0)
            .await?
            .is_empty()
    );
    // A failed serialization must leave the connection usable.
    store
        .append_event("stream", &ExpectedVersion::NoStream, &event())
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simultaneous_independent_writers() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("race.db");
    let a = LibsqlEventStore::new_local("test", &path).await?;
    let b = LibsqlEventStore::new_local("test", &path).await?;
    ensure(&a).await?;
    for (stream, expected) in [
        ("new", ExpectedVersion::NoStream),
        ("existing", ExpectedVersion::Exact(1)),
    ] {
        if stream == "existing" {
            a.append_event(stream, &ExpectedVersion::Any, &event())
                .await?;
        }
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let mut tasks = tokio::task::JoinSet::new();
        for store in [a.clone(), b.clone()] {
            let barrier = barrier.clone();
            let expected = expected.clone();
            tasks.spawn(async move {
                barrier.wait().await;
                store.append_event(stream, &expected, &event()).await
            });
        }
        let results = [
            tasks.join_next().await.unwrap()?,
            tasks.join_next().await.unwrap()?,
        ];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            read(&a, stream, EventsReadRange::AllEvents).await?.len(),
            if stream == "new" { 1 } else { 2 }
        );
    }
    // Clones serialize Any appends without dropping successful writes.
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let store = a.clone();
        tasks.spawn(async move {
            store
                .append_event("any", &ExpectedVersion::Any, &event())
                .await
        });
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    let events = read(&a, "any", EventsReadRange::AllEvents).await?;
    assert_eq!(
        events.iter().map(|e| e.version).collect::<Vec<_>>(),
        (0..20).collect::<Vec<_>>()
    );
    Ok(())
}

#[tokio::test]
async fn malformed_stored_values_return_errors() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("corrupt.db");
    let store = LibsqlEventStore::new_local("test", &path).await?;
    ensure(&store).await?;
    let original = event();
    store
        .append_event("stream", &ExpectedVersion::Any, &original)
        .await?;
    let raw = libsql::Builder::new_local(&path).build().await?;
    let connection = raw.connect()?;
    connection
        .execute("UPDATE test_events SET data = 'invalid json'", ())
        .await?;
    assert!(
        read(&store, "stream", EventsReadRange::AllEvents)
            .await
            .is_err()
    );
    connection
        .execute("UPDATE test_events SET data = '{}', version = -1", ())
        .await?;
    assert!(
        <LibsqlEventStore as EventStore<Value, Value>>::get_events_by_correlation_id(
            &store,
            &original.correlation_id.unwrap()
        )
        .await
        .is_err()
    );
    // Stream reads reject out-of-range versions instead of truncating to u32.
    connection
        .execute("UPDATE test_streams SET last_version = -1", ())
        .await?;
    assert!(
        <LibsqlEventStore as EventStore<Value, Value>>::get_stream(&store, "stream")
            .await
            .is_err()
    );
    assert!(
        store
            .append_event("stream", &ExpectedVersion::Any, &event())
            .await
            .is_err()
    );
    Ok(())
}
