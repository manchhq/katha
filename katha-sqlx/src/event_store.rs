use crate::{
    DEFAULT_NOTIFICATION_BUFFER, EventNotification, SqlxEventStore,
    backend::Backend,
    error::DbConversionError,
    event_db::{EventReadDb, StreamsDb},
    pagination::EventCursorPage,
    validate::validate_store_name,
};
use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use katha::traits::event_store::EventStore;
use katha::types::event_read::EventRead;
use katha::types::event_read_range::EventsReadRange;
use katha::types::event_stream::EventStream;
use katha::types::event_write::EventWrite;
use katha::types::expected_version::ExpectedVersion;
use katha::types::stream_read_filter::StreamsReadFilter;
use serde::{Deserialize, Serialize};
use sqlx::{
    AnyPool, Executor, Transaction,
    any::{Any, AnyPoolOptions},
};
use std::future::Future;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

impl SqlxEventStore {
    /// Creates an event store backed by an in-memory SQLite database.
    pub async fn new_memory(name: &str) -> Result<Self> {
        Self::new_memory_with_buffer(name, DEFAULT_NOTIFICATION_BUFFER).await
    }

    /// Creates an in-memory event store with a custom notification buffer size.
    pub async fn new_memory_with_buffer(name: &str, notification_buffer: usize) -> Result<Self> {
        sqlx::any::install_default_drivers();
        validate_store_name(name)?;
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        let (notifier, _) = broadcast::channel(notification_buffer.max(1));
        Ok(Self {
            name: name.to_string(),
            pool,
            backend: Backend::Sqlite,
            notifier,
            cancel_token: CancellationToken::new(),
        })
    }

    /// Creates an event store backed by a local SQLite file.
    pub async fn new_local(name: &str, path: &str) -> Result<Self> {
        Self::new_local_with_buffer(name, path, DEFAULT_NOTIFICATION_BUFFER).await
    }

    /// Creates a local-file event store with a custom notification buffer size.
    pub async fn new_local_with_buffer(
        name: &str,
        path: &str,
        notification_buffer: usize,
    ) -> Result<Self> {
        sqlx::any::install_default_drivers();
        validate_store_name(name)?;
        let url = format!("sqlite://{path}?mode=rwc");
        let pool = AnyPoolOptions::new().connect(&url).await?;
        let (notifier, _) = broadcast::channel(notification_buffer.max(1));
        Ok(Self {
            name: name.to_string(),
            pool,
            backend: Backend::Sqlite,
            notifier,
            cancel_token: CancellationToken::new(),
        })
    }

    /// Creates an event store from a database URL.
    ///
    /// URL format:
    /// - SQLite in-memory: `"sqlite::memory:"`
    /// - SQLite file: `"sqlite:///absolute/path/to/file.db"`
    /// - Postgres: `"postgres://user:pass@host/dbname"`
    pub async fn new_from_url(name: &str, url: &str) -> Result<Self> {
        Self::new_from_url_with_buffer(name, url, DEFAULT_NOTIFICATION_BUFFER).await
    }

    pub async fn new_from_url_with_buffer(
        name: &str,
        url: &str,
        notification_buffer: usize,
    ) -> Result<Self> {
        sqlx::any::install_default_drivers();
        validate_store_name(name)?;
        let pool = AnyPoolOptions::new().connect(url).await?;
        let (notifier, _) = broadcast::channel(notification_buffer.max(1));
        Ok(Self {
            name: name.to_string(),
            pool,
            backend: Backend::from_url(url),
            notifier,
            cancel_token: CancellationToken::new(),
        })
    }

    /// Creates an event store from a pre-built `AnyPool`.
    ///
    /// The caller is responsible for calling `sqlx::any::install_default_drivers()` before
    /// creating the pool.
    pub async fn new_from_pool(name: &str, pool: AnyPool) -> Result<Self> {
        validate_store_name(name)?;
        let backend = Backend::from_url(pool.connect_options().database_url.as_str());
        let (notifier, _) = broadcast::channel(DEFAULT_NOTIFICATION_BUFFER);
        Ok(Self {
            name: name.to_string(),
            pool,
            backend,
            notifier,
            cancel_token: CancellationToken::new(),
        })
    }

    /// Signals the `event_appended` spawned task to shut down.
    ///
    /// Call this before dropping the store when clean shutdown is desired,
    /// e.g. when switching databases in a Tauri app.
    pub fn shutdown(&self) {
        self.cancel_token.cancel();
    }

    /// Subscribes to event append notifications for projection runners.
    pub fn subscribe(&self) -> broadcast::Receiver<EventNotification> {
        self.notifier.subscribe()
    }

    /// Ensures stream and event tables exist by running the event migration file.
    async fn ensure_event_tables(&self) -> Result<()> {
        let template = include_str!("../migrations/0001_events.sql");
        let sql = template.replace("{{name}}", &self.name);

        let mut tx = self.pool.begin().await?;
        for statement in sql.split(';') {
            let stmt = statement.trim();
            if stmt.is_empty() {
                continue;
            }
            tx.execute(stmt).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Deletes all events and streams, then re-runs the schema migration so the
    /// store is in the same state as a fresh install. Used by the admin reset endpoint.
    pub async fn reset_all(&self) -> Result<()> {
        sqlx::query(&format!(r#"DELETE FROM "{}_events""#, self.name))
            .execute(&self.pool)
            .await?;
        sqlx::query(&format!(r#"DELETE FROM "{}_streams""#, self.name))
            .execute(&self.pool)
            .await?;
        self.ensure_event_tables().await
    }

    /// Ensures projection idempotency tracking table exists.
    ///
    /// Projections can use this table to deduplicate event processing by
    /// `(projection_name, event_id)`.
    pub async fn ensure_projection_idempotency_table(&self) -> Result<()> {
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS "{}_projection_processed" (
                projection_name TEXT NOT NULL,
                event_id TEXT NOT NULL,
                processed_utc TEXT NOT NULL,
                PRIMARY KEY(projection_name, event_id)
            )
            "#,
            self.name
        ))
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Attempts to mark an event as processed for a projection.
    ///
    /// Returns `true` if a new marker was inserted and `false` if the event was
    /// already marked.
    pub async fn try_mark_event_processed(
        &self,
        projection_name: &str,
        event_id: &Uuid,
    ) -> Result<bool> {
        let result = sqlx::query(&self.backend.bind(&format!(
            r#"
            INSERT INTO "{}_projection_processed" (projection_name, event_id, processed_utc)
            VALUES (?, ?, ?)
            ON CONFLICT DO NOTHING
            "#,
            self.name
        )))
        .bind(projection_name)
        .bind(event_id.to_string())
        .bind(Utc::now().to_rfc3339())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Returns whether a projection has already processed a given event.
    pub async fn is_event_processed(&self, projection_name: &str, event_id: &Uuid) -> Result<bool> {
        let row = sqlx::query_scalar::<_, i64>(&self.backend.bind(&format!(
            r#"
            SELECT COUNT(1) FROM "{}_projection_processed"
            WHERE projection_name = ? AND event_id = ?
            "#,
            self.name
        )))
        .bind(projection_name)
        .bind(event_id.to_string())
        .fetch_one(&self.pool)
        .await?;

        Ok(row > 0)
    }

    /// Applies a projection operation exactly-once per `(projection_name, event_id)`.
    ///
    /// The event is reserved in the idempotency table before `apply` runs.
    /// If `apply` fails, the reservation is removed so retries can run again.
    ///
    /// **Crash window:** If the process dies after the marker insert but before
    /// `apply` completes, the event is marked processed but never applied. Use
    /// [`apply_projection_once_in_tx`](Self::apply_projection_once_in_tx) for
    /// atomicity when the projection writes must be in the same transaction.
    ///
    /// Returns:
    /// - `Ok(true)` when `apply` executed and succeeded.
    /// - `Ok(false)` when the event had already been processed.
    pub async fn apply_projection_once<Payload, Meta, F, Fut>(
        &self,
        projection_name: &str,
        event: &EventRead<Payload, Meta>,
        apply: F,
    ) -> Result<bool>
    where
        F: FnOnce(&EventRead<Payload, Meta>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let inserted = self
            .try_mark_event_processed(projection_name, &event.id)
            .await?;
        if !inserted {
            return Ok(false);
        }

        if let Err(error) = apply(event).await {
            sqlx::query(&self.backend.bind(&format!(
                r#"
                DELETE FROM "{}_projection_processed"
                WHERE projection_name = ? AND event_id = ?
                "#,
                self.name
            )))
            .bind(projection_name)
            .bind(event.id.to_string())
            .execute(&self.pool)
            .await?;
            return Err(error);
        }

        Ok(true)
    }

    /// Applies a projection operation exactly-once within a transaction.
    ///
    /// Both the idempotency marker and the `apply` closure run in the same
    /// transaction. If `apply` fails or the transaction is rolled back, the
    /// marker is never committed. Use this when projection writes must be
    /// atomic with the processed marker.
    ///
    /// The `apply` closure receives the event and the transaction so it can
    /// perform its writes using the same connection.
    ///
    /// Returns:
    /// - `Ok(true)` when a new marker was inserted and `apply` succeeded.
    /// - `Ok(false)` when the event had already been processed.
    pub async fn apply_projection_once_in_tx<'c, Payload, Meta, F, Fut>(
        &self,
        tx: &mut Transaction<'c, Any>,
        projection_name: &str,
        event: &EventRead<Payload, Meta>,
        apply: F,
    ) -> Result<bool>
    where
        F: FnOnce(&EventRead<Payload, Meta>, &mut Transaction<'c, Any>) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let result = sqlx::query(&self.backend.bind(&format!(
            r#"
            INSERT INTO "{}_projection_processed" (projection_name, event_id, processed_utc)
            VALUES (?, ?, ?)
            ON CONFLICT DO NOTHING
            "#,
            self.name
        )))
        .bind(projection_name)
        .bind(event.id.to_string())
        .bind(Utc::now().to_rfc3339())
        .execute(tx.as_mut())
        .await?;

        if result.rows_affected() == 0 {
            return Ok(false);
        }

        apply(event, tx).await?;
        Ok(true)
    }

    /// Persists events with optimistic concurrency checks and returns stored events.
    async fn process_event<Payload, Meta>(
        &self,
        stream_id: &str,
        version: &ExpectedVersion,
        events: Vec<EventWrite<Payload, Meta>>,
    ) -> Result<Vec<EventRead<Payload, Meta>>>
    where
        Payload: Send + Sync + 'static + Clone + Serialize + for<'de> Deserialize<'de>,
        Meta: Send + Sync + 'static + Clone + Serialize + for<'de> Deserialize<'de>,
    {
        if events.is_empty() {
            return Ok(vec![]);
        }

        let events_len_u32: u32 = events.len().try_into().map_err(|_| {
            anyhow::anyhow!("Stream version overflow: batch size would exceed u32::MAX")
        })?;

        let stream_opt = sqlx::query_as::<_, StreamsDb>(&self.backend.bind(&format!(
            "SELECT id, last_version, last_updated_utc FROM \"{}_streams\" WHERE id = ? LIMIT 1",
            self.name
        )))
        .bind(stream_id)
        .fetch_optional(&self.pool)
        .await?;

        let stream_opt = stream_opt
            .map(EventStream::try_from)
            .transpose()
            .map_err(|e: DbConversionError| anyhow::anyhow!(e))?;

        if matches!(version, ExpectedVersion::NoStream) && stream_opt.is_some() {
            return Err(anyhow::anyhow!("Expected no stream but stream exists"));
        }

        let updated_stream = stream_opt
            .as_ref()
            .map(|stream| {
                let new_last_version =
                    stream
                        .last_version
                        .checked_add(events_len_u32)
                        .ok_or_else(|| {
                            anyhow::anyhow!("Stream version overflow: would exceed u32::MAX")
                        })?;
                Ok::<_, anyhow::Error>(EventStream {
                    id: stream.id.clone(),
                    last_version: new_last_version,
                    last_updated_utc: Utc::now(),
                })
            })
            .transpose()?
            .unwrap_or_else(|| EventStream {
                id: stream_id.to_string(),
                last_version: events_len_u32
                    .checked_sub(1)
                    .expect("events_len_u32 >= 1 when events non-empty"),
                last_updated_utc: Utc::now(),
            });

        let updated_stream_db = StreamsDb::from(updated_stream.clone());

        let starting_version = match version {
            ExpectedVersion::Any => match stream_opt.as_ref() {
                Some(stream) => stream.last_version.checked_add(1).ok_or_else(|| {
                    anyhow::anyhow!("Stream version overflow: would exceed u32::MAX")
                })?,
                None => 0,
            },
            ExpectedVersion::NoStream => 0,
            ExpectedVersion::Exact(v) => {
                if let Some(stream) = stream_opt.as_ref() {
                    let next_version = stream.last_version.checked_add(1).ok_or_else(|| {
                        anyhow::anyhow!("Stream version overflow: would exceed u32::MAX")
                    })?;
                    if *v != next_version {
                        return Err(anyhow::anyhow!(
                            "Expected version {} but next available version is {}",
                            v,
                            next_version
                        ));
                    }
                } else if *v != 0 {
                    return Err(anyhow::anyhow!(
                        "Expected version {} but stream doesn't exist",
                        v
                    ));
                }
                *v
            }
        };

        let events_reads: Vec<EventRead<Payload, Meta>> = events
            .iter()
            .enumerate()
            .map(
                |(index, event)| -> Result<EventRead<Payload, Meta>, anyhow::Error> {
                    let version = starting_version.checked_add(index as u32).ok_or_else(|| {
                        anyhow::anyhow!("Stream version overflow: would exceed u32::MAX")
                    })?;
                    Ok(EventRead {
                        id: event.id,
                        correlation_id: event.correlation_id,
                        causation_id: event.causation_id,
                        stream_id: stream_id.to_string(),
                        version,
                        name: event.name.clone(),
                        data: event.data.clone(),
                        metadata: event.metadata.clone(),
                        created_utc: Utc::now(),
                    })
                },
            )
            .collect::<Result<Vec<_>, _>>()?;

        let event_reads_db = events_reads
            .iter()
            .map(EventReadDb::from)
            .collect::<Vec<EventReadDb>>();

        let mut tx = self.pool.begin().await?;

        sqlx::query(&self.backend.bind(&format!(
            r#"INSERT INTO "{}_streams" (id, last_version, last_updated_utc)
            VALUES (?, ?, ?) ON CONFLICT(id)
            DO UPDATE SET last_version = excluded.last_version, last_updated_utc = excluded.last_updated_utc"#,
            self.name
        )))
        .bind(&updated_stream_db.id)
        .bind(updated_stream_db.last_version)
        .bind(&updated_stream_db.last_updated_utc)
        .execute(&mut *tx)
        .await?;

        for event in &event_reads_db {
            sqlx::query(&self.backend.bind(&format!(
                r#"INSERT INTO "{}_events" (id, correlation_id, causation_id, stream_id, version, name, data, metadata, created_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
                self.name
            )))
            .bind(&event.id)
            .bind(&event.correlation_id)
            .bind(&event.causation_id)
            .bind(&event.stream_id)
            .bind(event.version)
            .bind(&event.name)
            .bind(&event.data)
            .bind(&event.metadata)
            .bind(&event.created_utc)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        let _ = self.notifier.send(Self::build_notification(
            &self.name,
            stream_id,
            &events_reads,
        ));
        Ok(events_reads)
    }

    /// Reads events from a stream using a version cursor.
    ///
    /// Cursor semantics:
    /// - `None` starts at the beginning of the stream.
    /// - `Some(v)` returns events with `version > v`.
    /// - `next_cursor` is the last returned version when more items are available.
    pub async fn get_events_page<Payload, Meta>(
        &self,
        stream_id: &str,
        cursor: Option<u32>,
        limit: usize,
    ) -> Result<EventCursorPage<Payload, Meta>>
    where
        Payload: Send + Sync + 'static + Clone + Serialize + for<'de> Deserialize<'de>,
        Meta: Send + Sync + 'static + Clone + Serialize + for<'de> Deserialize<'de>,
    {
        let fetch_limit = (limit.max(1) + 1) as i64;
        let start_after = cursor.map_or(-1_i64, i64::from);

        let rows = sqlx::query_as::<_, EventReadDb>(&self.backend.bind(&format!(
            r#"SELECT id, correlation_id, causation_id, stream_id,
            version, name, data, metadata, created_utc FROM "{}_events"
            WHERE stream_id = ? AND version > ?
            ORDER BY version ASC
            LIMIT ?"#,
            self.name
        )))
        .bind(stream_id)
        .bind(start_after)
        .bind(fetch_limit)
        .fetch_all(&self.pool)
        .await?;

        let has_more = rows.len() > limit.max(1);
        let mut page_items: Vec<EventRead<Payload, Meta>> = rows
            .into_iter()
            .take(limit.max(1))
            .map(EventRead::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e: DbConversionError| anyhow::anyhow!(e))?;

        let next_cursor = if has_more {
            page_items.last().map(|event| event.version)
        } else {
            None
        };

        Ok(EventCursorPage {
            items: std::mem::take(&mut page_items),
            next_cursor,
        })
    }

    /// Builds a compact append notification for projection subscribers.
    fn build_notification<Payload, Meta>(
        store_name: &str,
        stream_id: &str,
        events: &[EventRead<Payload, Meta>],
    ) -> EventNotification {
        let from_version = events.first().map(|event| event.version).unwrap_or(0);
        let to_version = events.last().map(|event| event.version).unwrap_or(0);

        EventNotification {
            store_name: store_name.to_string(),
            stream_id: stream_id.to_string(),
            from_version,
            to_version,
            event_ids: events.iter().map(|event| event.id).collect(),
            event_names: events.iter().map(|event| event.name.clone()).collect(),
        }
    }
}

#[async_trait]
impl<Payload, Meta> EventStore<Payload, Meta> for SqlxEventStore
where
    Payload: Send + Sync + 'static + Clone + Serialize + for<'de> Deserialize<'de>,
    Meta: Send + Sync + 'static + Clone + Serialize + for<'de> Deserialize<'de>,
{
    /// Subscribes to typed `EventRead` notifications after successful appends.
    fn event_appended(&self) -> Option<broadcast::Receiver<EventRead<Payload, Meta>>> {
        let mut source = self.subscribe();
        let pool = self.pool.clone();
        let store_name = self.name.clone();
        let backend = self.backend;
        let cancel_token = self.cancel_token.clone();
        let (tx, rx) = broadcast::channel(DEFAULT_NOTIFICATION_BUFFER);

        tokio::spawn(async move {
            loop {
                let notification = tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => break,
                    result = source.recv() => match result {
                        Ok(notification) => notification,
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    },
                };

                for event_id in notification.event_ids {
                    let row = sqlx::query_as::<_, EventReadDb>(&backend.bind(&format!(
                        r#"SELECT id, correlation_id, causation_id, stream_id,
                        version, name, data, metadata, created_utc FROM "{}_events"
                        WHERE id = ? LIMIT 1"#,
                        store_name
                    )))
                    .bind(event_id.to_string())
                    .fetch_optional(&pool)
                    .await;

                    if let Ok(Some(event_db)) = row
                        && let Ok(event_read) = EventRead::try_from(event_db)
                    {
                        let _ = tx.send(event_read);
                    }
                }
            }
        });

        Some(rx)
    }

    async fn ensure_events_table(&self) -> Result<()> {
        self.ensure_event_tables().await
    }

    async fn append_event(
        &self,
        stream_id: &str,
        version: &ExpectedVersion,
        payload: &EventWrite<Payload, Meta>,
    ) -> Result<EventRead<Payload, Meta>> {
        let res = self
            .append_events(stream_id, version, vec![payload.clone()])
            .await?;
        Ok(res[0].clone())
    }

    async fn append_events(
        &self,
        stream_id: &str,
        version: &ExpectedVersion,
        payload: Vec<EventWrite<Payload, Meta>>,
    ) -> Result<Vec<EventRead<Payload, Meta>>> {
        self.process_event(stream_id, version, payload).await
    }

    async fn get_event(&self, stream_id: &str, version: u32) -> Result<EventRead<Payload, Meta>> {
        let row = sqlx::query_as::<_, EventReadDb>(&self.backend.bind(&format!(
            r#"SELECT id, correlation_id, causation_id, stream_id,
            version, name, data, metadata, created_utc FROM "{}_events"
            WHERE stream_id = ? AND version = ?"#,
            self.name
        )))
        .bind(stream_id)
        .bind(version as i64)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(event_read_db) => EventRead::try_from(event_read_db)
                .map_err(|e: DbConversionError| anyhow::anyhow!(e)),
            None => Err(anyhow::anyhow!("Event not found")),
        }
    }

    async fn get_events(
        &self,
        stream_id: &str,
        range: &EventsReadRange,
    ) -> Result<Vec<EventRead<Payload, Meta>>> {
        let (start_version, end_version) = match range {
            EventsReadRange::AllEvents => (0, u32::MAX),
            EventsReadRange::FromVersion(start) => (*start, u32::MAX),
            EventsReadRange::ToVersion(end) => (0, *end),
            EventsReadRange::VersionRange {
                from_version,
                to_version,
            } => (*from_version, *to_version),
        };

        let rows = sqlx::query_as::<_, EventReadDb>(&self.backend.bind(&format!(
            r#"SELECT id, correlation_id, causation_id, stream_id,
            version, name, data, metadata, created_utc FROM "{}_events"
            WHERE stream_id = ? AND version >= ? AND version <= ?
            ORDER BY version ASC"#,
            self.name
        )))
        .bind(stream_id)
        .bind(start_version as i64)
        .bind(end_version as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(EventRead::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e: DbConversionError| anyhow::anyhow!(e))
    }

    async fn get_events_by_correlation_id(
        &self,
        correlation_id: &Uuid,
    ) -> Result<Vec<EventRead<Payload, Meta>>> {
        let rows = sqlx::query_as::<_, EventReadDb>(&self.backend.bind(&format!(
            r#"SELECT id, correlation_id, causation_id, stream_id,
            version, name, data, metadata, created_utc FROM "{}_events"
            WHERE correlation_id = ? ORDER BY created_utc ASC"#,
            self.name
        )))
        .bind(correlation_id.to_string())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(EventRead::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e: DbConversionError| anyhow::anyhow!(e))
    }

    async fn get_events_by_causation_id(
        &self,
        causation_id: &Uuid,
    ) -> Result<Vec<EventRead<Payload, Meta>>> {
        let rows = sqlx::query_as::<_, EventReadDb>(&self.backend.bind(&format!(
            r#"SELECT id, correlation_id, causation_id, stream_id,
            version, name, data, metadata, created_utc FROM "{}_events"
            WHERE causation_id = ? ORDER BY created_utc ASC"#,
            self.name
        )))
        .bind(causation_id.to_string())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(EventRead::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e: DbConversionError| anyhow::anyhow!(e))
    }

    async fn get_streams(&self, filter: &StreamsReadFilter) -> Result<Vec<EventStream>> {
        let rows = match filter {
            StreamsReadFilter::AllStreams => sqlx::query_as::<_, StreamsDb>(&format!(
                r#"SELECT id, last_version, last_updated_utc FROM "{}_streams""#,
                self.name
            ))
            .fetch_all(&self.pool)
            .await?,
            StreamsReadFilter::BeforeVersion(version) => sqlx::query_as::<_, StreamsDb>(&self.backend.bind(&format!(
                r#"SELECT id, last_version, last_updated_utc FROM "{}_streams" WHERE last_version < ?"#,
                self.name
            )))
            .bind(*version as i64)
            .fetch_all(&self.pool)
            .await?,
            StreamsReadFilter::AfterVersion(version) => sqlx::query_as::<_, StreamsDb>(&self.backend.bind(&format!(
                r#"SELECT id, last_version, last_updated_utc FROM "{}_streams" WHERE last_version > ?"#,
                self.name
            )))
            .bind(*version as i64)
            .fetch_all(&self.pool)
            .await?,
            StreamsReadFilter::BetweenVersions(start, end) => sqlx::query_as::<_, StreamsDb>(&self.backend.bind(&format!(
                r#"SELECT id, last_version, last_updated_utc FROM "{}_streams" WHERE last_version >= ? AND last_version <= ?"#,
                self.name
            )))
            .bind(*start as i64)
            .bind(*end as i64)
            .fetch_all(&self.pool)
            .await?,
            StreamsReadFilter::BeforeTime(time) => sqlx::query_as::<_, StreamsDb>(&self.backend.bind(&format!(
                r#"SELECT id, last_version, last_updated_utc FROM "{}_streams" WHERE last_updated_utc < ?"#,
                self.name
            )))
            .bind(time.to_rfc3339())
            .fetch_all(&self.pool)
            .await?,
            StreamsReadFilter::AfterTime(time) => sqlx::query_as::<_, StreamsDb>(&self.backend.bind(&format!(
                r#"SELECT id, last_version, last_updated_utc FROM "{}_streams" WHERE last_updated_utc > ?"#,
                self.name
            )))
            .bind(time.to_rfc3339())
            .fetch_all(&self.pool)
            .await?,
            StreamsReadFilter::BetweenTimes(start, end) => sqlx::query_as::<_, StreamsDb>(&self.backend.bind(&format!(
                r#"SELECT id, last_version, last_updated_utc FROM "{}_streams" WHERE last_updated_utc >= ? AND last_updated_utc <= ?"#,
                self.name
            )))
            .bind(start.to_rfc3339())
            .bind(end.to_rfc3339())
            .fetch_all(&self.pool)
            .await?,
        };

        rows.into_iter()
            .map(EventStream::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e: DbConversionError| anyhow::anyhow!(e))
    }

    async fn get_stream(&self, stream_id: &str) -> Result<EventStream> {
        let row = sqlx::query_as::<_, StreamsDb>(&self.backend.bind(&format!(
            r#"SELECT id, last_version, last_updated_utc FROM "{}_streams" WHERE id = ?"#,
            self.name
        )))
        .bind(stream_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(stream_db) => {
                EventStream::try_from(stream_db).map_err(|e: DbConversionError| anyhow::anyhow!(e))
            }
            None => Err(anyhow::anyhow!("Stream not found")),
        }
    }
}
