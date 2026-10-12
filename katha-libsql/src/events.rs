use crate::LibsqlEventStore;
use anyhow::{Result, anyhow, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use katha::types::{event_stream::EventStream, stream_read_filter::StreamsReadFilter};
use katha::{
    ConcurrencyConflict, EventRead, EventStore, EventWrite, EventsReadRange, ExpectedVersion,
};
use libsql::{Connection, Row, TransactionBehavior, Value, params};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

const EVENT_COLUMNS: &str =
    "id, correlation_id, causation_id, stream_id, version, name, data, metadata, created_utc";

fn optional_uuid(row: &Row, column: i32) -> Result<Option<Uuid>> {
    Ok(row
        .get::<Option<String>>(column)?
        .map(|s| Uuid::parse_str(&s))
        .transpose()?)
}

fn read_event<P: DeserializeOwned, M: DeserializeOwned>(row: Row) -> Result<EventRead<P, M>> {
    Ok(EventRead {
        id: Uuid::parse_str(&row.get::<String>(0)?)?,
        correlation_id: optional_uuid(&row, 1)?,
        causation_id: optional_uuid(&row, 2)?,
        stream_id: row.get(3)?,
        version: row.get::<i64>(4)?.try_into()?,
        name: row.get(5)?,
        data: serde_json::from_str(&row.get::<String>(6)?)?,
        metadata: row
            .get::<Option<String>>(7)?
            .map(|s| serde_json::from_str(&s))
            .transpose()?,
        created_utc: DateTime::parse_from_rfc3339(&row.get::<String>(8)?)?.with_timezone(&Utc),
    })
}

fn read_stream(row: Row) -> Result<EventStream> {
    Ok(EventStream {
        id: row.get(0)?,
        last_version: row.get::<i64>(1)?.try_into()?,
        last_updated_utc: DateTime::parse_from_rfc3339(&row.get::<String>(2)?)?.with_timezone(&Utc),
    })
}

impl LibsqlEventStore {
    async fn query_events<P: DeserializeOwned, M: DeserializeOwned>(
        &self,
        condition: &str,
        values: Vec<Value>,
    ) -> Result<Vec<EventRead<P, M>>> {
        let connection = self.database.session().await;
        let mut rows = connection.query(&format!(
            "SELECT {EVENT_COLUMNS} FROM \"{}_events\" WHERE {condition} ORDER BY created_utc, stream_id, version, id", self.name
        ), values).await?;
        let mut events = Vec::new();
        while let Some(row) = rows.next().await? {
            events.push(read_event(row)?);
        }
        Ok(events)
    }

    async fn write_batch<P: Clone + Serialize, M: Clone + Serialize>(
        &self,
        connection: &Connection,
        stream_id: &str,
        expected: &ExpectedVersion,
        events: Vec<EventWrite<P, M>>,
    ) -> Result<Vec<EventRead<P, M>>> {
        let mut rows = connection
            .query(
                &format!(
                    "SELECT last_version FROM \"{}_streams\" WHERE id = ?",
                    self.name
                ),
                [stream_id],
            )
            .await?;
        let current = rows
            .next()
            .await?
            .map(|row| row.get::<i64>(0))
            .transpose()?
            .map(u32::try_from)
            .transpose()?;
        drop(rows);
        let next: u32 = current
            .map(|v| {
                v.checked_add(1)
                    .ok_or_else(|| anyhow!("Stream version overflow"))
            })
            .transpose()?
            .unwrap_or(0);
        match expected {
            ExpectedVersion::NoStream => {
                ensure!(current.is_none(), "Expected no stream but stream exists")
            }
            ExpectedVersion::Exact(expected) if *expected != next => {
                return Err(ConcurrencyConflict {
                    stream_id: stream_id.to_owned(),
                    expected: *expected,
                    actual: next,
                }
                .into());
            }
            _ => {}
        }
        let last = next
            .checked_add(u32::try_from(events.len() - 1)?)
            .ok_or_else(|| anyhow!("Stream version overflow"))?;
        let now = Utc::now();
        let mut written = Vec::with_capacity(events.len());
        for (offset, event) in events.into_iter().enumerate() {
            let version = next + u32::try_from(offset)?;
            connection.execute(&format!(
                "INSERT INTO \"{}_events\" ({EVENT_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", self.name
            ), params![event.id.to_string(), event.correlation_id.map(|id| id.to_string()),
                event.causation_id.map(|id| id.to_string()), stream_id, i64::from(version),
                event.name.clone(), serde_json::to_string(&event.data)?,
                event.metadata.as_ref().map(serde_json::to_string).transpose()?, now.to_rfc3339()
            ]).await?;
            written.push(EventRead {
                id: event.id,
                correlation_id: event.correlation_id,
                causation_id: event.causation_id,
                stream_id: stream_id.to_owned(),
                version,
                name: event.name,
                data: event.data,
                metadata: event.metadata,
                created_utc: now,
            });
        }
        connection.execute(&format!(
            "INSERT INTO \"{}_streams\" (id, last_version, last_updated_utc) VALUES (?, ?, ?) ON CONFLICT(id) DO UPDATE SET last_version = excluded.last_version, last_updated_utc = excluded.last_updated_utc", self.name
        ), params![stream_id, i64::from(last), now.to_rfc3339()]).await?;
        Ok(written)
    }
}

#[async_trait]
impl<P, M> EventStore<P, M> for LibsqlEventStore
where
    P: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    M: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn ensure_events_table(&self) -> Result<()> {
        let connection = self.database.session().await;
        connection.execute_transactional_batch(&format!(r#"
            CREATE TABLE IF NOT EXISTS "{n}_streams" (
                id TEXT PRIMARY KEY, last_version BIGINT NOT NULL, last_updated_utc TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS "{n}_events" (
                id TEXT PRIMARY KEY, correlation_id TEXT, causation_id TEXT, stream_id TEXT NOT NULL,
                version BIGINT NOT NULL, name TEXT NOT NULL, data TEXT NOT NULL, metadata TEXT,
                created_utc TEXT NOT NULL, UNIQUE(stream_id, version));
            CREATE INDEX IF NOT EXISTS "{n}_events_correlation_id" ON "{n}_events" (correlation_id);
            CREATE INDEX IF NOT EXISTS "{n}_events_causation_id" ON "{n}_events" (causation_id);
        "#, n = self.name)).await?;
        Ok(())
    }

    async fn append_event(
        &self,
        stream_id: &str,
        version: &ExpectedVersion,
        event: &EventWrite<P, M>,
    ) -> Result<EventRead<P, M>> {
        Ok(self
            .append_events(stream_id, version, vec![event.clone()])
            .await?
            .remove(0))
    }

    async fn append_events(
        &self,
        stream_id: &str,
        version: &ExpectedVersion,
        events: Vec<EventWrite<P, M>>,
    ) -> Result<Vec<EventRead<P, M>>> {
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let connection = self.database.session().await;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;
        match self
            .write_batch(&transaction, stream_id, version, events)
            .await
        {
            Ok(written) => {
                transaction.commit().await?;
                Ok(written)
            }
            Err(error) => {
                transaction.rollback().await?;
                Err(error)
            }
        }
    }

    async fn get_event(&self, stream_id: &str, version: u32) -> Result<EventRead<P, M>> {
        self.query_events(
            "stream_id = ? AND version = ?",
            vec![stream_id.to_owned().into(), i64::from(version).into()],
        )
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("Event not found"))
    }

    async fn get_events(
        &self,
        stream_id: &str,
        range: &EventsReadRange,
    ) -> Result<Vec<EventRead<P, M>>> {
        let (from, to) = match range {
            EventsReadRange::AllEvents => (0, u32::MAX),
            EventsReadRange::FromVersion(from) => (*from, u32::MAX),
            EventsReadRange::ToVersion(to) => (0, *to),
            EventsReadRange::VersionRange {
                from_version,
                to_version,
            } => (*from_version, *to_version),
        };
        // Per-stream versions, rather than timestamps, define replay order.
        self.query_events(
            "stream_id = ? AND version BETWEEN ? AND ?",
            vec![
                stream_id.to_owned().into(),
                i64::from(from).into(),
                i64::from(to).into(),
            ],
        )
        .await
        .map(|mut events: Vec<EventRead<P, M>>| {
            events.sort_by_key(|e| e.version);
            events
        })
    }

    async fn get_events_by_correlation_id(&self, id: &Uuid) -> Result<Vec<EventRead<P, M>>> {
        self.query_events("correlation_id = ?", vec![id.to_string().into()])
            .await
    }

    async fn get_events_by_causation_id(&self, id: &Uuid) -> Result<Vec<EventRead<P, M>>> {
        self.query_events("causation_id = ?", vec![id.to_string().into()])
            .await
    }

    async fn get_streams(&self, filter: &StreamsReadFilter) -> Result<Vec<EventStream>> {
        let (condition, values) = stream_filter(filter);
        let connection = self.database.session().await;
        let mut rows = connection.query(&format!(
            "SELECT id, last_version, last_updated_utc FROM \"{}_streams\" WHERE {condition} ORDER BY id", self.name
        ), values).await?;
        let mut streams = Vec::new();
        while let Some(row) = rows.next().await? {
            streams.push(read_stream(row)?);
        }
        Ok(streams)
    }

    async fn get_stream(&self, stream_id: &str) -> Result<EventStream> {
        let connection = self.database.session().await;
        let mut rows = connection
            .query(
                &format!(
                    "SELECT id, last_version, last_updated_utc FROM \"{}_streams\" WHERE id = ?",
                    self.name
                ),
                [stream_id],
            )
            .await?;
        read_stream(
            rows.next()
                .await?
                .ok_or_else(|| anyhow!("Stream not found"))?,
        )
    }
}

fn stream_filter(filter: &StreamsReadFilter) -> (&'static str, Vec<Value>) {
    match filter {
        StreamsReadFilter::AllStreams => ("1 = 1", vec![]),
        // Literal prefix comparison: %, _ and backslashes are not wildcards.
        StreamsReadFilter::IdPrefix(prefix) => (
            "substr(id, 1, length(?)) = ?",
            vec![prefix.clone().into(), prefix.clone().into()],
        ),
        StreamsReadFilter::BeforeVersion(v) => ("last_version < ?", vec![i64::from(*v).into()]),
        StreamsReadFilter::AfterVersion(v) => ("last_version > ?", vec![i64::from(*v).into()]),
        StreamsReadFilter::BetweenVersions(a, b) => (
            "last_version BETWEEN ? AND ?",
            vec![i64::from(*a).into(), i64::from(*b).into()],
        ),
        StreamsReadFilter::BeforeTime(t) => ("last_updated_utc < ?", vec![t.to_rfc3339().into()]),
        StreamsReadFilter::AfterTime(t) => ("last_updated_utc > ?", vec![t.to_rfc3339().into()]),
        StreamsReadFilter::BetweenTimes(a, b) => (
            "last_updated_utc BETWEEN ? AND ?",
            vec![a.to_rfc3339().into(), b.to_rfc3339().into()],
        ),
    }
}
