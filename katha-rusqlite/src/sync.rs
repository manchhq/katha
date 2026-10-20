//! Managed PowerSync integration. The application owns credentials and upload policy.

use crate::{Database, RusqliteEventStore};
use anyhow::{Result, ensure};
use futures_lite::{Stream, StreamExt};
use katha::EventRead;
pub use powersync;
use powersync::{
    PowerSyncDatabase,
    schema::{Column, Table},
};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashSet;

/// Add this table to the application's PowerSync schema before opening it.
/// The SDK provides the `id` column automatically. Event UUIDs are global.
pub fn event_table() -> Table {
    Table::create(
        "katha_events",
        vec![
            Column::text("namespace"),
            Column::text("correlation_id"),
            Column::text("causation_id"),
            Column::text("stream_id"),
            Column::integer("version"),
            Column::text("name"),
            Column::text("data"),
            Column::text("metadata"),
            Column::text("created_utc"),
        ],
        |_| {},
    )
}

impl<P, M> RusqliteEventStore<P, M>
where
    P: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    M: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Uses only the SDK's managed writer. Never pass an independently opened
    /// connection to a PowerSync file to `from_connection`.
    pub async fn from_powersync(database: PowerSyncDatabase, namespace: &str) -> Result<Self> {
        let store = Self::new(Database::PowerSync(database), namespace)?;
        store.query("1 = 0".into(), vec![]).await?;
        store.ensure_receipts().await?;
        Ok(store)
    }

    /// Emits a durable projection backlog initially and whenever synced or local
    /// events change. Reading and acknowledging never inserts into synced tables.
    /// Errors (including stream/version collisions) are returned to the consumer.
    pub fn watch_projection(
        &self,
        projection: &str,
    ) -> Result<impl Stream<Item = Result<Vec<EventRead<P, M>>>> + Send + 'static> {
        let Database::PowerSync(database) = &self.database else {
            anyhow::bail!("watch_projection requires a managed PowerSync database")
        };
        ensure!(!projection.is_empty(), "Projection name cannot be empty");
        // Subscribe before the initial read to avoid a read/subscribe race.
        let updates = Box::pin(database.watch_tables(true, ["katha_events"]));
        let store = self.clone();
        let projection = projection.to_owned();
        Ok(futures_lite::stream::unfold(
            (updates, store, projection),
            |(mut updates, store, projection)| async move {
                updates.next().await?;
                let events = store.pending_events(&projection).await;
                Some((events, (updates, store, projection)))
            },
        ))
    }

    pub(crate) fn sync_notifications(&self) -> tokio::sync::broadcast::Receiver<EventRead<P, M>> {
        let (sender, receiver) = tokio::sync::broadcast::channel(512);
        let store = self.clone();
        let Database::PowerSync(database) = &self.database else {
            unreachable!()
        };
        let mut updates = Box::pin(database.watch_tables(true, ["katha_events"]));
        tokio::spawn(async move {
            let mut seen = HashSet::new();
            // A timer releases this task if subscribers disappear without another update.
            loop {
                tokio::select! {
                    update = updates.next() => {
                        if update.is_none() || sender.receiver_count() == 0 { break; }
                        let Ok(events) = store.query("1 = 1".into(), vec![]).await else { break; };
                        for event in events {
                            if seen.insert(event.id) { let _ = sender.send(event); }
                        }
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                        if sender.receiver_count() == 0 { break; }
                    }
                }
            }
        });
        receiver
    }
}
