mod common;
use anyhow::Result;
use common::*;
use katha::{ConcurrencyConflict, EventStore, ExpectedVersion};
use uuid::Uuid;
#[tokio::test]
async fn memory_contract_and_notifications() -> Result<()> {
    let store = Store::open(":memory:", "test").await?;
    let mut receiver = store.event_appended().unwrap();
    exercise(&store).await?;
    projection_contract(&store).await?;
    assert!(receiver.try_recv().is_ok());
    Ok(())
}

#[tokio::test]
async fn restart_receipts_and_independent_writers() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("नमस्ते ?#.db");
    let first_id;
    {
        let store = Store::open(&path, "test").await?;
        exercise(&store).await?;
        let pending = store.pending_events("view").await?;
        first_id = pending[0].id;
        store.acknowledge("view", first_id).await?;
        store.acknowledge("view", first_id).await?;
        assert!(store.acknowledge("view", Uuid::new_v4()).await.is_err());
    }
    let a = Store::open(&path, "test").await?;
    let b = Store::open(&path, "test").await?;
    let backlog = a.pending_events("view").await?;
    assert!(!backlog.iter().any(|e| e.id == first_id));
    assert_eq!(
        a.pending_events("another-view").await?.len(),
        backlog.len() + 1
    );
    let first = event();
    let second = event();
    let (left, right) = tokio::join!(
        a.append_event("race", &ExpectedVersion::NoStream, &first),
        b.append_event("race", &ExpectedVersion::NoStream, &second)
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let third = event();
    let fourth = event();
    let (left, right) = tokio::join!(
        a.append_event("race", &ExpectedVersion::Exact(1), &third),
        b.append_event("race", &ExpectedVersion::Exact(1), &fourth)
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let error = left.err().or_else(|| right.err()).unwrap();
    assert!(error.downcast_ref::<ConcurrencyConflict>().is_some());
    let isolated = Store::open(&path, "other").await?;
    assert!(isolated.pending_events("view").await?.is_empty());
    assert!(Store::open(&path, "").await.is_err());
    Ok(())
}
