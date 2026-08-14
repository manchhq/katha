use crate::error::DbConversionError;
use chrono::{DateTime, Utc};
use katha::types::event_read::EventRead;
use katha::types::event_stream::EventStream;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::convert::TryFrom;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize, FromRow)]
pub struct EventReadDb {
    pub id: String,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub stream_id: String,
    pub version: i64,
    pub name: String,
    pub data: String,
    pub metadata: Option<String>,
    pub created_utc: String,
}

impl<Payload, Meta> TryFrom<EventReadDb> for EventRead<Payload, Meta>
where
    Payload: for<'de> Deserialize<'de>,
    Meta: for<'de> Deserialize<'de>,
{
    type Error = DbConversionError;

    fn try_from(event_read_db: EventReadDb) -> Result<Self, Self::Error> {
        Self::try_from(&event_read_db)
    }
}

impl<Payload, Meta> TryFrom<&EventReadDb> for EventRead<Payload, Meta>
where
    Payload: for<'de> Deserialize<'de>,
    Meta: for<'de> Deserialize<'de>,
{
    type Error = DbConversionError;

    fn try_from(event_read_db: &EventReadDb) -> Result<Self, Self::Error> {
        let id = Uuid::parse_str(&event_read_db.id).map_err(|e| DbConversionError::UuidParse {
            field: "id".to_string(),
            value: event_read_db.id.clone(),
            source: e,
        })?;
        let correlation_id = event_read_db
            .correlation_id
            .as_ref()
            .map(|cid| {
                Uuid::parse_str(cid).map_err(|e| DbConversionError::UuidParse {
                    field: "correlation_id".to_string(),
                    value: cid.clone(),
                    source: e,
                })
            })
            .transpose()?;
        let causation_id = event_read_db
            .causation_id
            .as_ref()
            .map(|cid| {
                Uuid::parse_str(cid).map_err(|e| DbConversionError::UuidParse {
                    field: "causation_id".to_string(),
                    value: cid.clone(),
                    source: e,
                })
            })
            .transpose()?;
        let data = serde_json::from_str(&event_read_db.data).map_err(|e| {
            DbConversionError::JsonDeserialize {
                field: "data".to_string(),
                source: e,
            }
        })?;
        let metadata = event_read_db
            .metadata
            .as_ref()
            .map(|md| {
                serde_json::from_str(md).map_err(|e| DbConversionError::JsonDeserialize {
                    field: "metadata".to_string(),
                    source: e,
                })
            })
            .transpose()?;
        let created_utc = DateTime::parse_from_rfc3339(&event_read_db.created_utc)
            .map_err(|e| DbConversionError::DateTimeParse {
                field: "created_utc".to_string(),
                value: event_read_db.created_utc.clone(),
                source: e,
            })?
            .with_timezone(&Utc);

        Ok(EventRead {
            id,
            correlation_id,
            causation_id,
            stream_id: event_read_db.stream_id.clone(),
            version: event_read_db.version as u32,
            name: event_read_db.name.clone(),
            data,
            metadata,
            created_utc,
        })
    }
}

impl<Payload, Meta> From<&EventRead<Payload, Meta>> for EventReadDb
where
    Payload: Serialize,
    Meta: Serialize,
{
    fn from(event_read: &EventRead<Payload, Meta>) -> Self {
        EventReadDb {
            id: event_read.id.to_string(),
            correlation_id: event_read.correlation_id.map(|cid| cid.to_string()),
            causation_id: event_read.causation_id.map(|cid| cid.to_string()),
            stream_id: event_read.stream_id.clone(),
            version: event_read.version as i64,
            name: event_read.name.clone(),
            data: serde_json::to_string(&event_read.data)
                .expect("Failed to serialize event read data"),
            metadata: event_read.metadata.as_ref().map(|md| {
                serde_json::to_string(md).expect("Failed to serialize event read metadata")
            }),
            created_utc: event_read.created_utc.to_rfc3339(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, FromRow)]
pub struct StreamsDb {
    pub id: String,
    pub last_version: i64,
    pub last_updated_utc: String,
}

impl From<EventStream> for StreamsDb {
    fn from(event_stream: EventStream) -> Self {
        StreamsDb {
            id: event_stream.id,
            last_version: event_stream.last_version as i64,
            last_updated_utc: event_stream.last_updated_utc.to_rfc3339(),
        }
    }
}

impl TryFrom<StreamsDb> for EventStream {
    type Error = DbConversionError;

    fn try_from(streams_db: StreamsDb) -> Result<Self, Self::Error> {
        let last_updated_utc = DateTime::parse_from_rfc3339(&streams_db.last_updated_utc)
            .map_err(|e| DbConversionError::DateTimeParse {
                field: "last_updated_utc".to_string(),
                value: streams_db.last_updated_utc.clone(),
                source: e,
            })?
            .with_timezone(&Utc);

        Ok(EventStream {
            id: streams_db.id,
            last_version: streams_db.last_version as u32,
            last_updated_utc,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::EventReadDb;
    use katha::types::event_read::EventRead;
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Payload {
        action: String,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Meta {
        source: String,
    }

    fn sample_row(id: Uuid, correlation_id: Uuid) -> EventReadDb {
        EventReadDb {
            id: id.to_string(),
            correlation_id: Some(correlation_id.to_string()),
            causation_id: None,
            stream_id: "stream-1".to_string(),
            version: 7,
            name: "Sampled".to_string(),
            data: r#"{"action":"deposit"}"#.to_string(),
            metadata: Some(r#"{"source":"unit-test"}"#.to_string()),
            created_utc: "2026-08-14T10:11:12+00:00".to_string(),
        }
    }

    #[test]
    fn converts_borrowed_row_into_typed_event() {
        let id = Uuid::new_v4();
        let correlation_id = Uuid::new_v4();
        let row = sample_row(id, correlation_id);

        let event: EventRead<Payload, Meta> = EventRead::try_from(&row).expect("conversion failed");

        assert_eq!(event.id, id);
        assert_eq!(event.correlation_id, Some(correlation_id));
        assert_eq!(event.causation_id, None);
        assert_eq!(event.stream_id, "stream-1");
        assert_eq!(event.version, 7);
        assert_eq!(event.name, "Sampled");
        assert_eq!(event.data.action, "deposit");
        assert_eq!(
            event.metadata.expect("metadata expected").source,
            "unit-test"
        );
        assert_eq!(event.created_utc.to_rfc3339(), "2026-08-14T10:11:12+00:00");
    }

    #[test]
    fn borrowed_row_conversion_leaves_row_reusable() {
        let row = sample_row(Uuid::new_v4(), Uuid::new_v4());

        let first: EventRead<Payload, Meta> = EventRead::try_from(&row).expect("first conversion");
        let second: EventRead<Payload, Meta> =
            EventRead::try_from(&row).expect("second conversion");

        assert_eq!(first.id, second.id);
        assert_eq!(first.data, second.data);
    }
}
