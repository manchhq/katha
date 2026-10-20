#![doc = include_str!("../README.md")]

mod events;
#[cfg(feature = "powersync")]
pub mod sync;

use anyhow::{Context, Result, ensure};
use katha::EventRead;
use rusqlite::Connection;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast;

const COLUMNS: &str =
    "id, correlation_id, causation_id, stream_id, version, name, data, metadata, created_utc";

#[derive(Clone)]
enum Database {
    Local(Arc<Mutex<Connection>>),
    #[cfg(feature = "powersync")]
    PowerSync(powersync::PowerSyncDatabase),
}

impl Database {
    async fn run<T: Send + 'static>(
        &self,
        work: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        match self {
            Self::Local(connection) => {
                let connection = connection.clone();
                tokio::task::spawn_blocking(move || {
                    let mut guard = connection
                        .lock()
                        .map_err(|_| anyhow::anyhow!("Database mutex poisoned"))?;
                    work(&mut guard)
                })
                .await?
            }
            #[cfg(feature = "powersync")]
            Self::PowerSync(database) => {
                // The managed writer owns the SDK's lock and update hooks. Never
                // open the underlying file through an independent connection.
                let mut writer = database.writer().await?;
                tokio::task::spawn_blocking(move || work(&mut writer)).await?
            }
        }
    }
}

/// Two immutable events claim the same stream version. Both rows are retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamVersionConflict {
    pub stream_id: String,
    pub version: u32,
}
impl std::fmt::Display for StreamVersionConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "multiple events claim version {} of stream '{}'",
            self.version, self.stream_id
        )
    }
}
impl std::error::Error for StreamVersionConflict {}

/// Typed event store. Clone this handle to share local notifications and connections.
#[derive(Clone)]
pub struct RusqliteEventStore<P, M> {
    database: Database,
    namespace: String,
    notifications: broadcast::Sender<EventRead<P, M>>,
}

impl<P, M> RusqliteEventStore<P, M>
where
    P: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    M: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub async fn open(path: impl AsRef<Path>, namespace: &str) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let connection = tokio::task::spawn_blocking(move || -> Result<_> {
            let connection = Connection::open(path)?;
            connection.busy_timeout(std::time::Duration::from_secs(5))?;
            Ok(connection)
        })
        .await??;
        Self::from_connection(connection, namespace).await
    }

    pub async fn from_connection(connection: Connection, namespace: &str) -> Result<Self> {
        let store = Self::new(Database::Local(Arc::new(Mutex::new(connection))), namespace)?;
        store.database.run(|c| {
            c.execute_batch("CREATE TABLE IF NOT EXISTS katha_events (
                id TEXT PRIMARY KEY, namespace TEXT NOT NULL,
                correlation_id TEXT, causation_id TEXT, stream_id TEXT NOT NULL,
                version INTEGER NOT NULL CHECK(version >= 0), name TEXT NOT NULL,
                data TEXT NOT NULL, metadata TEXT, created_utc TEXT NOT NULL,
                UNIQUE(namespace, stream_id, version));
                CREATE INDEX IF NOT EXISTS katha_events_stream ON katha_events(namespace, stream_id, version);")?;
            Ok(())
        }).await?;
        store.ensure_receipts().await?;
        Ok(store)
    }

    fn new(database: Database, namespace: &str) -> Result<Self> {
        ensure!(!namespace.is_empty(), "Namespace cannot be empty");
        Ok(Self {
            database,
            namespace: namespace.to_owned(),
            notifications: broadcast::channel(512).0,
        })
    }

    async fn ensure_receipts(&self) -> Result<()> {
        self.database
            .run(|c| {
                // Ordinary local table: deliberately absent from the sync schema.
                c.execute_batch(
                    "CREATE TABLE IF NOT EXISTS katha_projection_receipts (
                namespace TEXT NOT NULL, projection TEXT NOT NULL, event_id TEXT NOT NULL,
                PRIMARY KEY(namespace, projection, event_id));",
                )?;
                Ok(())
            })
            .await
    }

    /// Replays every event not acknowledged by this projection, including late
    /// remote events with old timestamps or versions. Delivery is at least once.
    pub async fn pending_events(&self, projection: &str) -> Result<Vec<EventRead<P, M>>> {
        ensure!(!projection.is_empty(), "Projection name cannot be empty");
        let projection = projection.to_owned();
        let namespace = self.namespace.clone();
        self.database.run(move |c| {
            events::check_conflicts(c, &namespace)?;
            events::query(c, &namespace,
                "NOT EXISTS (SELECT 1 FROM katha_projection_receipts r WHERE r.namespace = katha_events.namespace AND r.projection = ? AND r.event_id = katha_events.id)",
                vec![projection.into()])
        }).await
    }

    /// Call only after projection effects commit. For atomic local SQLite effects
    /// and receipts, use `apply_projection_once`.
    pub async fn acknowledge(&self, projection: &str, id: uuid::Uuid) -> Result<()> {
        ensure!(!projection.is_empty(), "Projection name cannot be empty");
        let namespace = self.namespace.clone();
        let projection = projection.to_owned();
        self.database
            .run(move |c| {
                let exists: bool = c.query_row(
                    "SELECT EXISTS(SELECT 1 FROM katha_events WHERE namespace = ? AND id = ?)",
                    rusqlite::params![namespace, id.to_string()],
                    |r| r.get(0),
                )?;
                ensure!(exists, "Cannot acknowledge a missing event");
                c.execute(
                    "INSERT INTO katha_projection_receipts VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
                    rusqlite::params![namespace, projection, id.to_string()],
                )?;
                Ok(())
            })
            .await
    }

    /// Applies synchronous SQLite projection effects and records the event ID in
    /// the same managed/local transaction. Returns false if already processed.
    /// The callback must only mutate local read-model tables, never synced events.
    pub async fn apply_projection_once(
        &self,
        projection: &str,
        id: uuid::Uuid,
        apply: impl FnOnce(&rusqlite::Transaction<'_>, &EventRead<P, M>) -> Result<()> + Send + 'static,
    ) -> Result<bool> {
        ensure!(!projection.is_empty(), "Projection name cannot be empty");
        let namespace = self.namespace.clone();
        let projection = projection.to_owned();
        self.database.run(move |c| {
            let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            events::check_conflicts(&tx, &namespace)?;
            let done: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM katha_projection_receipts WHERE namespace = ? AND projection = ? AND event_id = ?)",
                rusqlite::params![namespace, projection, id.to_string()], |r| r.get(0))?;
            if done { return Ok(false); }
            let event = events::query(&tx, &namespace, "id = ?", vec![id.to_string().into()])?
                .into_iter().next().context("Cannot project a missing event")?;
            apply(&tx, &event)?;
            tx.execute("INSERT INTO katha_projection_receipts VALUES (?, ?, ?)",
                rusqlite::params![namespace, projection, id.to_string()])?;
            tx.commit()?;
            Ok(true)
        }).await
    }

    async fn query(
        &self,
        condition: String,
        values: Vec<rusqlite::types::Value>,
    ) -> Result<Vec<EventRead<P, M>>> {
        let namespace = self.namespace.clone();
        self.database
            .run(move |c| {
                events::check_conflicts(c, &namespace)?;
                events::query(c, &namespace, &condition, values)
            })
            .await
            .context("Read immutable events")
    }
}
