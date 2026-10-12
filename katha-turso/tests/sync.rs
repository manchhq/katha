use anyhow::{Context, Result};
use katha::types::stream_read_filter::StreamsReadFilter;
use katha::{EventRead, EventStore, EventWrite, EventsReadRange, ExpectedVersion};
use katha_turso::{SyncConfig, SyncDatabase, SyncErrorKind, TursoEventStore};
use serde_json::{Value, json};
use uuid::Uuid;

fn event() -> EventWrite<Value, Value> {
    EventWrite {
        id: Uuid::new_v4(),
        correlation_id: Some(Uuid::new_v4()),
        causation_id: Some(Uuid::new_v4()),
        name: "TodoAdded".into(),
        data: json!({"title": "offline todo"}),
        metadata: Some(json!({"origin": "test"})),
    }
}
async fn read(store: &TursoEventStore, stream: &str) -> Result<Vec<EventRead<Value, Value>>> {
    store
        .get_events(stream, &EventsReadRange::AllEvents)
        .await
        .context("read local event stream")
}

#[tokio::test]
async fn offline_restart_failed_sync_and_local_checkpoints() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("offline ? नमस्ते.db");
    let db = SyncDatabase::open(
        SyncConfig::new(&path)
            .with_remote_url("http://127.0.0.1:9")
            .with_auth_token("expired"),
    )
    .await?;
    assert!(SyncDatabase::open(SyncConfig::new(&path)).await.is_err());
    let device = db.device_id();
    let stream = db.device_stream("todos");
    let store = db.event_store("todo")?;
    let first = event();
    let stored = store
        .append_event(&stream, &ExpectedVersion::NoStream, &first)
        .await?;
    assert_eq!(stored.version, 0);
    assert_eq!(stored.id, first.id);
    let pending = db.status().await?.pending_operations;
    assert!(pending > 0);
    db.set_projection_checkpoint("list", "todo", &stream, 0)
        .await?;
    assert_eq!(
        db.projection_checkpoint("list", "todo", &stream).await?,
        Some(0)
    );
    assert_eq!(db.status().await?.pending_operations, pending);
    assert!(db.push().await.is_err());
    assert_eq!(db.status().await?.pending_operations, pending);
    assert!(db.status().await?.last_error.is_some());
    assert!(db.pull().await.is_err());
    assert_eq!(read(&store, &stream).await?, vec![stored.clone()]);
    let second = store
        .append_event(&stream, &ExpectedVersion::Exact(1), &event())
        .await?;
    assert_eq!(second.version, 1);
    assert!(
        store
            .append_event("shared-stream", &ExpectedVersion::Any, &event())
            .await
            .is_err()
    );
    assert!(
        store
            .append_event(&stream, &ExpectedVersion::Exact(0), &event())
            .await
            .is_err()
    );
    let duplicate = event();
    assert!(
        store
            .append_events(
                &stream,
                &ExpectedVersion::Exact(2),
                vec![duplicate.clone(), duplicate]
            )
            .await
            .is_err()
    );
    assert_eq!(read(&store, &stream).await?.len(), 2);
    db.checkpoint().await?;
    drop(store);
    drop(db);
    // No URL, credentials, or working network are needed to reopen the replica.
    let db = SyncDatabase::open(SyncConfig::new(&path)).await?;
    assert_eq!(db.device_id(), device);
    assert_eq!(
        db.projection_checkpoint("list", "todo", &stream).await?,
        Some(0)
    );
    assert!(db.status().await?.pending_operations > 0);
    let store = db.event_store("todo")?;
    let imported: Vec<EventRead<Value, Value>> = store
        .get_events(&stream, &EventsReadRange::FromVersion(1))
        .await?;
    assert_eq!(imported, vec![second]);
    assert_eq!(
        store
            .append_event(&stream, &ExpectedVersion::Exact(2), &event())
            .await?
            .version,
        2
    );
    assert_eq!(read(&store, &stream).await?.len(), 3);
    Ok(())
}

#[tokio::test]
async fn namespaces_and_local_only_open() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = SyncDatabase::open(SyncConfig::new(dir.path().join("local.db"))).await?;
    let stream = db.device_stream("todos");
    let store = db.event_store("todo")?;
    let first = event();
    store
        .append_event(&stream, &ExpectedVersion::Any, &first)
        .await?;
    assert!(read(&db.event_store("another")?, &stream).await?.is_empty());
    let events: Vec<EventRead<Value, Value>> = store
        .get_events_by_correlation_id(&first.correlation_id.unwrap())
        .await?;
    assert_eq!(events[0].id, first.id);
    let caused: Vec<EventRead<Value, Value>> = store
        .get_events_by_causation_id(&first.causation_id.unwrap())
        .await?;
    assert_eq!(caused, events);
    let streams = <TursoEventStore as EventStore<Value, Value>>::get_streams(
        &store,
        &StreamsReadFilter::AllStreams,
    )
    .await?;
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].id, stream);
    let before = chrono::Utc::now() - chrono::Duration::days(1);
    let after = chrono::Utc::now() + chrono::Duration::days(1);
    for (filter, count) in [
        (
            StreamsReadFilter::IdPrefix(format!("{}:to", db.device_id())),
            1,
        ),
        (StreamsReadFilter::BeforeVersion(1), 1),
        (StreamsReadFilter::AfterVersion(0), 0),
        (StreamsReadFilter::BetweenVersions(0, 0), 1),
        (StreamsReadFilter::BeforeTime(after), 1),
        (StreamsReadFilter::AfterTime(before), 1),
        (StreamsReadFilter::BetweenTimes(before, after), 1),
    ] {
        let streams =
            <TursoEventStore as EventStore<Value, Value>>::get_streams(&store, &filter).await?;
        assert_eq!(streams.len(), count);
    }
    assert!(db.event_store("").is_err());
    assert!(
        SyncDatabase::open(SyncConfig::new(":memory:"))
            .await
            .is_err()
    );
    assert!(
        !format!(
            "{:?}",
            SyncConfig::new("local.db").with_auth_token("never-print-this")
        )
        .contains("never-print-this")
    );
    Ok(())
}

fn remote_config(path: &std::path::Path) -> Result<SyncConfig> {
    Ok(SyncConfig::new(path)
        .with_remote_url(std::env::var("KATHA_TEST_TURSO_URL")?)
        .with_auth_token(std::env::var("KATHA_TEST_TURSO_TOKEN")?))
}

#[tokio::test]
#[ignore = "requires a dedicated Turso Sync database"]
async fn remote_protocol_and_two_device_convergence() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let a_path = dir.path().join("a.db");
    let b_path = dir.path().join("b.db");
    let a = SyncDatabase::open(remote_config(&a_path)?).await?;
    // An actual pull verifies sync protocol support, rather than ordinary SQL connectivity.
    a.verify_remote().await.context("A initial protocol pull")?;
    let namespace = format!("test_{}", Uuid::new_v4().simple());
    let a_stream = a.device_stream("todos");
    let a_store = a.event_store(&namespace)?;
    let a_event = event();
    a_store
        .append_event(&a_stream, &ExpectedVersion::NoStream, &a_event)
        .await?;
    let pending = a.status().await?.pending_operations;
    a.set_auth_token(Some("expired-test-token".into()));
    let error = a.push().await.unwrap_err();
    assert_eq!(error.kind, SyncErrorKind::Authentication, "{error}");
    assert_eq!(a.status().await?.pending_operations, pending);
    assert_eq!(read(&a_store, &a_stream).await?[0].id, a_event.id);
    a.set_auth_token(Some(std::env::var("KATHA_TEST_TURSO_TOKEN")?));
    a.push().await?;
    a.push().await?;
    assert_eq!(a.status().await?.pending_operations, 0);
    let b = SyncDatabase::open(remote_config(&b_path)?).await?;
    b.pull().await?;
    let b_store = b.event_store(&namespace)?;
    assert_eq!(read(&b_store, &a_stream).await?[0].id, a_event.id);
    assert_eq!(
        b.projection_checkpoint("list", &namespace, &a_stream)
            .await?,
        None
    );
    let b_stream = b.device_stream("todos");
    assert_ne!(a.device_id(), b.device_id());
    let b_event = event();
    b_store
        .append_event(&b_stream, &ExpectedVersion::NoStream, &b_event)
        .await?;
    b.push().await?;
    a.pull().await?;
    a_store
        .append_event(&a_stream, &ExpectedVersion::Exact(1), &event())
        .await?;
    a.push().await?;
    b.pull().await?;
    // Both devices append offline. Pull must preserve B's unsent changes
    // while importing A's newly pushed event.
    let a_read: EventRead<Value, Value> = a_store
        .get_event(&a_stream, 1)
        .await
        .context("read A event before third append")?;
    assert_eq!(a_read.version, 1);
    <TursoEventStore as EventStore<Value, Value>>::get_stream(&a_store, &a_stream)
        .await
        .context("read A stream head before third append")?;
    let a_offline = event();
    let b_offline = event();
    a_store
        .append_event(&a_stream, &ExpectedVersion::Exact(2), &a_offline)
        .await?;
    b_store
        .append_event(&b_stream, &ExpectedVersion::Exact(1), &b_offline)
        .await?;
    a.push().await?;
    b.pull().await?;
    assert!(b.status().await?.pending_operations > 0);
    assert_eq!(read(&b_store, &b_stream).await?[1].id, b_offline.id);
    b.push().await?;
    a.pull().await?;
    for db in [&a, &b] {
        db.push().await?;
        db.pull().await?;
        db.pull().await?;
        let store = db.event_store(&namespace)?;
        assert_eq!(read(&store, &a_stream).await?.len(), 3);
        assert_eq!(read(&store, &b_stream).await?.len(), 2);
        assert_eq!(read(&store, &b_stream).await?[0].id, b_event.id);
    }
    let b_device = b.device_id();
    drop(b_store);
    drop(b);
    let b = SyncDatabase::open(SyncConfig::new(&b_path)).await?;
    assert_eq!(b.device_id(), b_device);
    assert_eq!(b.status().await?.pending_operations, 0);
    assert_eq!(read(&b.event_store(&namespace)?, &a_stream).await?.len(), 3);
    assert!(a.status().await?.last_push.is_some());
    assert!(a.status().await?.last_pull.is_some());
    Ok(())
}

struct FaultProxy(std::process::Child);
impl Drop for FaultProxy {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_proxy(
    mode: &str,
) -> Result<(
    FaultProxy,
    std::io::BufReader<std::process::ChildStdout>,
    String,
)> {
    use std::io::BufRead;
    let mut child = std::process::Command::new("python3")
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fault_proxy.py"))
        .env("KATHA_TEST_PROXY_FAULT", mode)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
    let mut url = String::new();
    output.read_line(&mut url)?;
    anyhow::ensure!(
        url.starts_with("http://127.0.0.1:"),
        "Fault proxy failed to start"
    );
    Ok((FaultProxy(child), output, url.trim().to_owned()))
}

#[tokio::test]
#[ignore = "requires a dedicated Turso Sync database and Python 3"]
async fn remote_lost_response_and_interruption_retries() -> Result<()> {
    use std::{io::BufRead, time::Duration};
    for mode in ["drop", "delay", "timeout"] {
        let (_proxy, output, url) = start_proxy(mode)?;
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("replica.db");
        let config = || {
            remote_config(&path).map(|c| {
                let c = c.with_remote_url(&url);
                if mode == "timeout" {
                    c.with_sync_timeout(Duration::from_secs(1))
                } else {
                    c
                }
            })
        };
        let db = SyncDatabase::open(config()?).await?;
        db.verify_remote().await?;
        let namespace = format!("retry_{}", Uuid::new_v4().simple());
        let stream = db.device_stream("todos");
        let store = db.event_store(&namespace)?;
        let first = event();
        store
            .append_event(&stream, &ExpectedVersion::NoStream, &first)
            .await?;
        let pending = db.status().await?.pending_operations;
        let pushing = db.clone();
        let task = tokio::spawn(async move { pushing.push().await });
        // The proxy confirms the upstream commit before withholding its reply.
        let committed = tokio::task::spawn_blocking(move || {
            let mut output = output;
            let mut line = String::new();
            output.read_line(&mut line)?;
            anyhow::ensure!(
                line.trim() == "COMMITTED",
                "Proxy did not observe an upstream commit"
            );
            Ok::<_, anyhow::Error>(())
        });
        tokio::time::timeout(Duration::from_secs(30), committed).await???;
        if mode == "delay" {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert_eq!(
                db.status().await?.last_error.unwrap().kind,
                SyncErrorKind::Interrupted
            );
            tokio::time::sleep(Duration::from_millis(2200)).await;
        } else if mode == "timeout" {
            assert_eq!(task.await?.unwrap_err().kind, SyncErrorKind::Interrupted);
            tokio::time::sleep(Duration::from_millis(2200)).await;
        } else {
            assert!(task.await?.is_err());
        }
        assert_eq!(db.status().await?.pending_operations, pending);
        assert_eq!(read(&store, &stream).await?[0].id, first.id);
        // Restart before retry, retaining the SDK's durable commit/revision metadata.
        drop(store);
        drop(db);
        let db = SyncDatabase::open(config()?).await?;
        assert_eq!(db.status().await?.pending_operations, pending);
        db.push().await?;
        db.push().await?;
        db.pull().await?;
        db.pull().await?;
        assert_eq!(db.status().await?.pending_operations, 0);
        assert!(db.status().await?.last_error.is_none());
        assert_eq!(read(&db.event_store(&namespace)?, &stream).await?.len(), 1);
        // A fresh device verifies that the upstream log also contains exactly one event.
        let peer = SyncDatabase::open(remote_config(&dir.path().join("peer.db"))?).await?;
        peer.pull().await?;
        let imported = read(&peer.event_store(&namespace)?, &stream).await?;
        assert_eq!(imported.len(), 1);
        assert_eq!(imported[0].id, first.id);
    }
    Ok(())
}
