use crate::sqlx_store::error::DbConversionError;
use crate::types::event_read::EventRead;
use crate::types::event_stream::EventStream;
use chrono::{DateTime, Utc};
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
            stream_id: event_read_db.stream_id,
            version: event_read_db.version as u32,
            name: event_read_db.name,
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
