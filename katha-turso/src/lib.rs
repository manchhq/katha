#![doc = include_str!("../README.md")]

mod events;
mod status;

pub use status::{SyncError, SyncErrorKind, SyncOperation, SyncStatus};

use anyhow::{Context, Result, ensure};
use chrono::Utc;
use fs2::FileExt;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::sync::{Mutex, MutexGuard};
use turso::{Connection, params};
use uuid::Uuid;

/// Configuration for a persistent local replica. Credentials are held in memory.
#[derive(Clone)]
pub struct SyncConfig {
    path: PathBuf,
    remote_url: Option<String>,
    auth_token: Option<String>,
    timeout: Duration,
}

impl SyncConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            remote_url: None,
            auth_token: None,
            timeout: Duration::from_secs(30),
        }
    }
    pub fn with_remote_url(mut self, url: impl Into<String>) -> Self {
        self.remote_url = Some(url.into());
        self
    }
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }
    pub fn with_sync_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

impl std::fmt::Debug for SyncConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncConfig")
            .field("path", &self.path)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// Persistent local event storage with explicit Turso synchronization.
///
/// Opening always disables network bootstrap. Device identity, checkpoints and
/// error status live in a separate local database and are never synchronized.
#[derive(Clone)]
pub struct SyncDatabase(Arc<Inner>);

struct Inner {
    sync: turso::sync::Database,
    connection: Mutex<Connection>,
    _local_database: turso::Database,
    local: Mutex<Connection>,
    token: Arc<RwLock<Option<String>>>,
    device_id: Uuid,
    timeout: Duration,
    // Drop last, after the SDK and both connections release their files.
    _file_lock: std::fs::File,
}

impl std::fmt::Debug for SyncDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncDatabase")
            .field("device_id", &self.0.device_id)
            .finish_non_exhaustive()
    }
}

impl SyncDatabase {
    /// Opens local storage without making network requests or validating auth.
    pub async fn open(config: SyncConfig) -> Result<Self> {
        ensure!(
            !config.path.as_os_str().is_empty(),
            "SyncDatabase requires a nonempty file path"
        );
        ensure!(
            config.path != Path::new(":memory:"),
            "SyncDatabase requires a persistent file path"
        );
        ensure!(!config.timeout.is_zero(), "Sync timeout must be positive");
        if let Some(parent) = config.path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let path = config
            .path
            .to_str()
            .context("Database path must be UTF-8")?;
        let file_lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(format!("{path}.katha.lock"))?;
        file_lock.try_lock_exclusive().context(
            "Replica is already open; clone SyncDatabase instead of reopening the same path",
        )?;
        let token = Arc::new(RwLock::new(config.auth_token));
        let provider = token.clone();
        let mut builder = turso::sync::Builder::new_remote(path)
            .bootstrap_if_empty(false)
            .with_auth_token_fn(move || {
                let value = provider
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone()
                    .unwrap_or_default();
                async move { Ok(value) }
            });
        if let Some(url) = config.remote_url {
            builder = builder.with_remote_url(url);
        }
        let sync = builder.build().await?;
        let connection = sync.connect().await?;
        connection.execute_batch(include_str!("schema.sql")).await?;
        let local_database = turso::Builder::new_local(&format!("{path}.local.db"))
            .build()
            .await?;
        let local = local_database.connect()?;
        local
            .execute_batch(include_str!("local_schema.sql"))
            .await?;
        local
            .execute(
                "INSERT INTO device (id, uuid) VALUES (1, ?) ON CONFLICT(id) DO NOTHING",
                [Uuid::new_v4().to_string()],
            )
            .await?;
        let mut rows = local
            .query("SELECT uuid FROM device WHERE id = 1", ())
            .await?;
        let device_id = Uuid::parse_str(
            &rows
                .next()
                .await?
                .context("Device identity missing")?
                .get::<String>(0)?,
        )?;
        drop(rows);
        Ok(Self(Arc::new(Inner {
            _file_lock: file_lock,
            sync,
            connection: Mutex::new(connection),
            _local_database: local_database,
            local: Mutex::new(local),
            token,
            device_id,
            timeout: config.timeout,
        })))
    }

    pub fn event_store(&self, name: &str) -> Result<TursoEventStore> {
        ensure!(!name.is_empty(), "Store name cannot be empty");
        Ok(TursoEventStore {
            database: self.clone(),
            name: name.to_owned(),
        })
    }

    pub fn device_id(&self) -> Uuid {
        self.0.device_id
    }

    /// Builds a stream owned by this device; use it for local event appends.
    pub fn device_stream(&self, logical_id: &str) -> String {
        format!("{}:{logical_id}", self.device_id())
    }

    /// Replaces credentials in memory without reopening or touching local data.
    pub fn set_auth_token(&self, token: Option<String>) {
        *self.0.token.write().unwrap_or_else(|p| p.into_inner()) = token;
    }

    /// Uploads locally committed changes. Failure leaves them pending in the SDK.
    pub async fn push(&self) -> std::result::Result<(), SyncError> {
        self.synchronize(SyncOperation::Push).await.map(|_| ())
    }

    /// Durably imports remote changes. Returns whether local data changed.
    pub async fn pull(&self) -> std::result::Result<bool, SyncError> {
        self.synchronize(SyncOperation::Pull).await
    }

    /// Probes the actual sync protocol by performing a pull; no remote writes.
    pub async fn verify_remote(&self) -> std::result::Result<bool, SyncError> {
        self.pull().await
    }

    async fn synchronize(&self, operation: SyncOperation) -> std::result::Result<bool, SyncError> {
        // Prevent a pull from rebasing the database during an event transaction.
        let connection = self.session().await;
        // Access the SQL connection so the driver rolls back any dangling
        // transaction left by a cancelled local append before SDK sync begins.
        let mut probe = connection
            .query("SELECT 1", ())
            .await
            .map_err(|e| SyncError::local(operation, e.into()))?;
        while probe
            .next()
            .await
            .map_err(|e| SyncError::local(operation, e.into()))?
            .is_some()
        {}
        drop(probe);
        self.save_attempt(operation)
            .await
            .map_err(|e| SyncError::local(operation, e))?;
        let result = tokio::time::timeout(self.0.timeout, async {
            match operation {
                SyncOperation::Push => self.0.sync.push().await.map(|_| false),
                SyncOperation::Pull => self.0.sync.pull().await,
            }
        })
        .await;
        let result = match result {
            Ok(Ok(changed)) => Ok(changed),
            Ok(Err(error)) => Err(SyncError::driver(
                operation,
                error,
                self.current_token().as_deref(),
            )),
            Err(_) => Err(SyncError::timeout(operation)),
        };
        self.save_result(result.as_ref().err())
            .await
            .map_err(|e| SyncError::local(operation, e))?;
        result
    }

    fn current_token(&self) -> Option<String> {
        self.0
            .token
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    async fn save_attempt(&self, operation: SyncOperation) -> Result<()> {
        self.0
            .local
            .lock()
            .await
            .execute(
                "UPDATE sync_status SET in_flight = ?, last_error = NULL WHERE id = 1",
                [serde_json::to_string(&operation)?],
            )
            .await?;
        Ok(())
    }

    async fn save_result(&self, error: Option<&SyncError>) -> Result<()> {
        self.0
            .local
            .lock()
            .await
            .execute(
                "UPDATE sync_status SET in_flight = NULL, last_error = ? WHERE id = 1",
                params![error.map(serde_json::to_string).transpose()?],
            )
            .await?;
        Ok(())
    }

    /// Reads durable SDK statistics and local error status without network access.
    /// Pending operations include schema changes, not just events.
    pub async fn status(&self) -> Result<SyncStatus> {
        let _connection = self.session().await;
        let stats = self.0.sync.stats().await?;
        let local = self.0.local.lock().await;
        let mut rows = local
            .query(
                "SELECT in_flight, last_error FROM sync_status WHERE id = 1",
                (),
            )
            .await?;
        let row = rows.next().await?.context("Sync status missing")?;
        let interrupted = row
            .get::<Option<String>>(0)?
            .map(|s| serde_json::from_str(&s))
            .transpose()?;
        let last_error = row
            .get::<Option<String>>(1)?
            .map(|s| serde_json::from_str(&s))
            .transpose()?
            .or_else(|| interrupted.map(SyncError::interrupted));
        Ok(SyncStatus {
            pending_operations: stats.cdc_operations.try_into()?,
            last_push: stats
                .last_push_unix_time
                .and_then(|t| chrono::DateTime::from_timestamp(t, 0)),
            last_pull: stats
                .last_pull_unix_time
                .and_then(|t| chrono::DateTime::from_timestamp(t, 0)),
            revision: stats.revision,
            last_error,
        })
    }

    /// Reads a projection's per-stream checkpoint from the local sidecar.
    pub async fn projection_checkpoint(
        &self,
        projection: &str,
        namespace: &str,
        stream_id: &str,
    ) -> Result<Option<u32>> {
        let local = self.0.local.lock().await;
        let mut rows = local.query("SELECT version FROM projection_checkpoints WHERE projection = ? AND namespace = ? AND stream_id = ?",
            params![projection, namespace, stream_id]).await?;
        rows.next()
            .await?
            .map(|r| Ok(r.get::<i64>(0)?.try_into()?))
            .transpose()
    }

    /// Saves a local checkpoint after projection effects have committed.
    /// For atomic read-model/checkpoint writes, the app should own both in its
    /// own local projection database instead of using this convenience method.
    pub async fn set_projection_checkpoint(
        &self,
        projection: &str,
        namespace: &str,
        stream_id: &str,
        version: u32,
    ) -> Result<()> {
        self.0.local.lock().await.execute(
            "INSERT INTO projection_checkpoints (projection, namespace, stream_id, version, updated_utc) VALUES (?, ?, ?, ?, ?) ON CONFLICT(projection, namespace, stream_id) DO UPDATE SET version = excluded.version, updated_utc = excluded.updated_utc",
            params![projection, namespace, stream_id, i64::from(version), Utc::now().to_rfc3339()]).await?;
        Ok(())
    }

    /// Compacts local WAL through the sync SDK, preserving pending writes.
    pub async fn checkpoint(&self) -> Result<()> {
        let _connection = self.session().await;
        self.0.sync.checkpoint().await?;
        Ok(())
    }

    async fn session(&self) -> MutexGuard<'_, Connection> {
        self.0.connection.lock().await
    }
}

#[derive(Debug, Clone)]
pub struct TursoEventStore {
    database: SyncDatabase,
    name: String,
}
