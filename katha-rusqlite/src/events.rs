use super::{COLUMNS, RusqliteEventStore, StreamVersionConflict};
use anyhow::{Context, Result, anyhow, ensure};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use katha::types::{event_stream::EventStream, stream_read_filter::StreamsReadFilter};
use katha::{
    ConcurrencyConflict, EventRead, EventStore, EventWrite, EventsReadRange, ExpectedVersion,
};
use rusqlite::{Connection, Row, TransactionBehavior, params, params_from_iter, types::Value};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

pub(super) fn check_conflicts(c: &Connection, namespace: &str) -> Result<()> {
    let mut statement = c.prepare("SELECT stream_id, version FROM katha_events WHERE namespace = ? GROUP BY stream_id, version HAVING COUNT(*) > 1 LIMIT 1")?;
    let mut rows = statement.query([namespace])?;
    if let Some(row) = rows.next()? {
        return Err(StreamVersionConflict {
            stream_id: row.get(0)?,
            version: row.get::<_, u32>(1)?,
        }
        .into());
    }
    Ok(())
}

fn decode<P: DeserializeOwned, M: DeserializeOwned>(row: &Row<'_>) -> Result<EventRead<P, M>> {
    let optional_uuid = |column| -> Result<Option<Uuid>> {
        row.get::<_, Option<String>>(column)?
            .map(|id| Ok(Uuid::parse_str(&id)?))
            .transpose()
    };
    Ok(EventRead {
        id: Uuid::parse_str(&row.get::<_, String>(0)?)?,
        correlation_id: optional_uuid(1)?,
        causation_id: optional_uuid(2)?,
        stream_id: row.get(3)?,
        version: row.get(4)?,
        name: row.get(5)?,
        data: serde_json::from_str(&row.get::<_, String>(6)?)?,
        metadata: row
            .get::<_, Option<String>>(7)?
            .map(|s| serde_json::from_str(&s))
            .transpose()?,
        created_utc: DateTime::parse_from_rfc3339(&row.get::<_, String>(8)?)?.with_timezone(&Utc),
    })
}

pub(super) fn query<P: DeserializeOwned, M: DeserializeOwned>(
    c: &Connection,
    namespace: &str,
    condition: &str,
    mut values: Vec<Value>,
) -> Result<Vec<EventRead<P, M>>> {
    values.insert(0, namespace.to_owned().into());
    let mut statement = c.prepare(&format!("SELECT {COLUMNS} FROM katha_events WHERE namespace = ? AND ({condition}) ORDER BY stream_id, version, id"))?;
    let mut rows = statement.query(params_from_iter(values))?;
    let mut events = Vec::new();
    while let Some(row) = rows.next()? {
        events.push(decode(row)?);
    }
    Ok(events)
}

#[async_trait]
impl<P, M> EventStore<P, M> for RusqliteEventStore<P, M>
where
    P: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    M: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    async fn ensure_events_table(&self) -> Result<()> {
        // Constructors ensure local schema or verify the managed schema.
        self.query("1 = 0".into(), vec![]).await.map(|_| ())
    }

    fn event_appended(&self) -> Option<tokio::sync::broadcast::Receiver<EventRead<P, M>>> {
        #[cfg(feature = "powersync")]
        if matches!(self.database, super::Database::PowerSync(_)) {
            // Fallible watch_projection is the preferred PowerSync interface:
            // it also reports conflicts and supports durable recovery.
            return Some(self.sync_notifications());
        }
        Some(self.notifications.subscribe())
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
        expected: &ExpectedVersion,
        events: Vec<EventWrite<P, M>>,
    ) -> Result<Vec<EventRead<P, M>>> {
        if events.is_empty() {
            return Ok(vec![]);
        }
        let namespace = self.namespace.clone();
        let stream_id = stream_id.to_owned();
        let expected = expected.clone();
        let notifications = self.notifications.clone();
        self.database.run(move |c| {
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            check_conflicts(&tx, &namespace)?;
            let current: Option<u32> = tx.query_row("SELECT MAX(version) FROM katha_events WHERE namespace = ? AND stream_id = ?",
                params![namespace, stream_id], |row| row.get(0))?;
            let next = current.map(|v| v.checked_add(1).context("Stream version overflow")).transpose()?.unwrap_or(0);
            match expected {
                ExpectedVersion::NoStream => ensure!(current.is_none(), "Expected no stream but stream exists"),
                ExpectedVersion::Exact(version) if version != next => return Err(ConcurrencyConflict { stream_id, expected: version, actual: next }.into()),
                _ => {}
            }
            next.checked_add(u32::try_from(events.len() - 1)?).context("Stream version overflow")?;
            let now = Utc::now();
            let mut written = Vec::with_capacity(events.len());
            for (offset, event) in events.into_iter().enumerate() {
                // Managed views may implement INSERT as replacement. Reject IDs
                // explicitly under the writer lock to preserve immutable facts.
                let exists: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM katha_events WHERE id = ?)", [event.id.to_string()], |r| r.get(0))?;
                ensure!(!exists, "Event ID already exists: {}", event.id);
                let version = next + u32::try_from(offset)?;
                tx.execute(&format!("INSERT INTO katha_events (namespace, {COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
                    params![namespace, event.id.to_string(), event.correlation_id.map(|v| v.to_string()), event.causation_id.map(|v| v.to_string()),
                        stream_id, i64::from(version), event.name, serde_json::to_string(&event.data)?,
                        event.metadata.as_ref().map(serde_json::to_string).transpose()?, now.to_rfc3339()])?;
                written.push(EventRead { id: event.id, correlation_id: event.correlation_id, causation_id: event.causation_id,
                    stream_id: stream_id.clone(), version, name: event.name, data: event.data, metadata: event.metadata, created_utc: now });
            }
            tx.commit()?;
            for event in &written { let _ = notifications.send(event.clone()); }
            Ok(written)
        }).await
    }

    async fn get_event(&self, stream_id: &str, version: u32) -> Result<EventRead<P, M>> {
        self.query(
            "stream_id = ? AND version = ?".into(),
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
        let mut events = self
            .query(
                "stream_id = ? AND version >= ? AND version <= ?".into(),
                vec![
                    stream_id.to_owned().into(),
                    i64::from(from).into(),
                    i64::from(to).into(),
                ],
            )
            .await?;
        events.sort_by_key(|event| event.version);
        Ok(events)
    }

    async fn get_events_by_correlation_id(&self, id: &Uuid) -> Result<Vec<EventRead<P, M>>> {
        self.query("correlation_id = ?".into(), vec![id.to_string().into()])
            .await
    }
    async fn get_events_by_causation_id(&self, id: &Uuid) -> Result<Vec<EventRead<P, M>>> {
        self.query("causation_id = ?".into(), vec![id.to_string().into()])
            .await
    }

    async fn get_streams(&self, filter: &StreamsReadFilter) -> Result<Vec<EventStream>> {
        let namespace = self.namespace.clone();
        let filter = filter.clone();
        self.database.run(move |c| {
            check_conflicts(c, &namespace)?;
            let mut statement = c.prepare("SELECT stream_id, MAX(version), MAX(created_utc) FROM katha_events WHERE namespace = ? GROUP BY stream_id ORDER BY stream_id")?;
            let mut rows = statement.query([namespace])?;
            let mut streams = Vec::new();
            while let Some(row) = rows.next()? {
                let stream = EventStream { id: row.get(0)?, last_version: row.get(1)?, last_updated_utc: DateTime::parse_from_rfc3339(&row.get::<_, String>(2)?)?.with_timezone(&Utc) };
                let matches = match &filter {
                    StreamsReadFilter::AllStreams => true,
                    StreamsReadFilter::IdPrefix(prefix) => stream.id.starts_with(prefix),
                    StreamsReadFilter::BeforeVersion(v) => stream.last_version < *v,
                    StreamsReadFilter::AfterVersion(v) => stream.last_version > *v,
                    StreamsReadFilter::BetweenVersions(a, b) => stream.last_version >= *a && stream.last_version <= *b,
                    StreamsReadFilter::BeforeTime(t) => stream.last_updated_utc < *t,
                    StreamsReadFilter::AfterTime(t) => stream.last_updated_utc > *t,
                    StreamsReadFilter::BetweenTimes(a, b) => stream.last_updated_utc >= *a && stream.last_updated_utc <= *b,
                };
                if matches { streams.push(stream); }
            }
            Ok(streams)
        }).await
    }
    async fn get_stream(&self, stream_id: &str) -> Result<EventStream> {
        self.get_streams(&StreamsReadFilter::AllStreams)
            .await?
            .into_iter()
            .find(|s| s.id == stream_id)
            .context("Stream not found")
    }
}
