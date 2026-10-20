#![cfg(feature = "powersync")]

mod common;
use anyhow::Result;
use async_trait::async_trait;
use common::*;
use futures_lite::StreamExt;
use katha::{ConcurrencyConflict, EventRead, EventStore, ExpectedVersion};
use katha_rusqlite::{StreamVersionConflict, sync::event_table};
use powersync::{
    ConnectionPool, PowerSyncDatabase, env::PowerSyncEnvironment, error::PowerSyncError,
    schema::Schema,
};
use serde_json::{Value, json};
use std::path::Path;
use uuid::Uuid;

struct OfflineHttp;
#[async_trait]
impl powersync::http::HttpClient for OfflineHttp {
    async fn send(
        &self,
        _: powersync::http::Request,
    ) -> std::result::Result<powersync::http::Response, PowerSyncError> {
        panic!("Test attempted network access")
    }
}

fn database(path: &Path) -> Result<PowerSyncDatabase> {
    PowerSyncEnvironment::powersync_auto_extension()?;
    let env = PowerSyncEnvironment::custom(
        OfflineHttp,
        ConnectionPool::open(path)?,
        PowerSyncEnvironment::tokio(),
    );
    Ok(PowerSyncDatabase::new(
        env,
        Schema {
            tables: vec![event_table()],
            ..Schema::default()
        },
    ))
}

async fn queue_count(db: &PowerSyncDatabase) -> Result<i64> {
    Ok(db
        .reader()
        .await?
        .query_row("SELECT COUNT(*) FROM ps_crud", [], |r| r.get(0))?)
}

// Test fixture at the SDK storage boundary: emulate an already-downloaded row,
// bypassing the view's local-write triggers exactly as remote materialization does.
// Production code never writes SDK internal tables.
async fn remote_row(db: &PowerSyncDatabase, id: Uuid, stream: &str, version: u32) -> Result<()> {
    db.writer().await?.execute(
        "INSERT INTO ps_data__katha_events(id, data) VALUES (?, ?)",
        rusqlite::params![
            id.to_string(),
            json!({
                "namespace": "test", "stream_id": stream, "version": version,
                "name": "RemoteCreated", "data": json!({"remote": true}).to_string(),
                "metadata": json!({"origin": "server"}).to_string(),
                "created_utc": "2001-01-01T00:00:00+00:00"
            })
            .to_string()
        ],
    )?;
    Ok(())
}

#[tokio::test]
async fn managed_contract_and_atomic_upload_queue() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = database(&dir.path().join("managed.db"))?;
    let store = Store::from_powersync(db.clone(), "test").await?;
    exercise(&store).await?;
    let before = queue_count(&db).await?;
    let duplicate = event();
    assert!(
        store
            .append_events(
                "bad",
                &ExpectedVersion::Any,
                vec![event(), duplicate.clone(), duplicate]
            )
            .await
            .is_err()
    );
    assert_eq!(queue_count(&db).await?, before);
    assert!(store.get_stream("bad").await.is_err());
    let batch = db.next_crud_transaction().await?.unwrap();
    assert_eq!(batch.crud.len(), 2, "first batch has one SDK transaction");
    assert!(batch.id.is_some());
    let first = &batch.crud[0];
    let persisted: EventRead<Value, Value> = store.get_event("account_%α", 0).await?;
    assert_eq!(first.id, persisted.id.to_string());
    assert_eq!(
        first.data.as_ref().unwrap()["metadata"],
        persisted.metadata.as_ref().unwrap().to_string()
    );
    let original_count = store.pending_events("view").await?.len();
    store.acknowledge("view", persisted.id).await?;
    assert_eq!(
        queue_count(&db).await?,
        before,
        "projection receipts never upload"
    );
    assert_eq!(
        store.pending_events("view").await?.len(),
        original_count - 1
    );
    drop(batch);
    projection_contract(&store).await?;
    assert_eq!(queue_count(&db).await?, before + 2);
    Ok(())
}

#[tokio::test]
async fn remote_notifications_restart_and_old_timestamps() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("remote.db");
    let local_id;
    let late_id = Uuid::new_v4();
    {
        let db = database(&path)?;
        let store = Store::from_powersync(db.clone(), "test").await?;
        let local = store
            .append_event("local", &ExpectedVersion::Any, &event())
            .await?;
        local_id = local.id;
        let mut watch = Box::pin(store.watch_projection("view")?);
        assert_eq!(watch.next().await.unwrap()?.len(), 1);
        store.acknowledge("view", local.id).await?;
        let count = queue_count(&db).await?;
        let mut notifications = store.event_appended().unwrap();
        assert_eq!(notifications.recv().await?.id, local_id);
        remote_row(&db, late_id, "remote", 0).await?;
        let pending = tokio::time::timeout(std::time::Duration::from_secs(3), watch.next())
            .await?
            .unwrap()?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, late_id);
        assert_eq!(pending[0].metadata, Some(json!({"origin": "server"})));
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(3), notifications.recv())
                .await??
                .id,
            late_id
        );
        assert_eq!(queue_count(&db).await?, count);
        drop(watch);
        drop(notifications);
    }
    // A notification task may still be releasing its lease; opening another SDK
    // handle is safe and the durable receipts are already committed.
    let db = database(&path)?;
    let store = Store::from_powersync(db.clone(), "test").await?;
    let pending = store.pending_events("view").await?;
    assert_eq!(
        pending.iter().map(|e| e.id).collect::<Vec<_>>(),
        vec![late_id]
    );
    assert!(!pending.iter().any(|e| e.id == local_id));
    let mut watch = Box::pin(store.watch_projection("view")?);
    assert_eq!(watch.next().await.unwrap()?[0].id, late_id);
    let count = queue_count(&db).await?;
    // A late next version has an older wall clock than its predecessor. Replay
    // still follows stream versions instead of timestamps.
    remote_row(&db, Uuid::new_v4(), "local", 1).await?;
    let all = store.pending_events("new-view").await?;
    let versions = all
        .iter()
        .filter(|e| e.stream_id == "local")
        .map(|e| e.version)
        .collect::<Vec<_>>();
    assert_eq!(versions, vec![0, 1]);
    // Consume the update before introducing a conflicting stream/version.
    let _ = watch.next().await.unwrap()?;
    remote_row(&db, Uuid::new_v4(), "remote", 0).await?;
    let err = tokio::time::timeout(std::time::Duration::from_secs(3), watch.next())
        .await?
        .unwrap()
        .unwrap_err();
    assert!(err.downcast_ref::<StreamVersionConflict>().is_some());
    assert!(
        store
            .get_event("remote", 0)
            .await
            .unwrap_err()
            .downcast_ref::<StreamVersionConflict>()
            .is_some()
    );
    assert!(
        store
            .append_event("remote", &ExpectedVersion::Any, &event())
            .await
            .unwrap_err()
            .downcast_ref::<StreamVersionConflict>()
            .is_some()
    );
    assert_eq!(queue_count(&db).await?, count);
    let rows: i64 = db.reader().await?.query_row(
        "SELECT COUNT(*) FROM katha_events WHERE stream_id = 'remote'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(
        rows, 2,
        "both conflicting events remain available for reconciliation"
    );
    Ok(())
}

#[tokio::test]
async fn managed_writers_surface_optimistic_conflicts() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = database(&dir.path().join("race.db"))?;
    let a = Store::from_powersync(db.clone(), "test").await?;
    let b = Store::from_powersync(db.clone(), "test").await?;
    a.append_event("race", &ExpectedVersion::NoStream, &event())
        .await?;
    let first = event();
    let second = event();
    let (left, right) = tokio::join!(
        a.append_event("race", &ExpectedVersion::Exact(1), &first),
        b.append_event("race", &ExpectedVersion::Exact(1), &second)
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    assert!(
        left.err()
            .or_else(|| right.err())
            .unwrap()
            .downcast_ref::<ConcurrencyConflict>()
            .is_some()
    );
    assert_eq!(queue_count(&db).await?, 2);
    Ok(())
}
