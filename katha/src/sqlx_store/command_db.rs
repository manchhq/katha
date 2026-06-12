use crate::sqlx_store::error::DbConversionError;
use crate::types::command_write::{CommandRead, CommandWrite};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize, sqlx::FromRow)]
pub struct CommandReadDb {
    pub id: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub data: String,
    pub name: String,
    pub created_utc: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CommandWriteDb {
    pub id: String,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub data: String,
    pub name: String,
    pub created_utc: String,
}

impl<Payload> From<&CommandWrite<Payload>> for CommandWriteDb
where
    Payload: Serialize,
{
    fn from(command_write: &CommandWrite<Payload>) -> Self {
        CommandWriteDb {
            id: command_write.id.to_string(),
            correlation_id: command_write.correlation_id.to_string(),
            causation_id: command_write.causation_id.as_ref().map(|u| u.to_string()),
            data: serde_json::to_string(&command_write.data)
                .expect("Failed to serialize command payload"),
            name: command_write.name.clone(),
            created_utc: Utc::now().to_rfc3339(),
        }
    }
}

impl<Payload> TryFrom<CommandReadDb> for CommandRead<Payload>
where
    Payload: for<'de> Deserialize<'de>,
{
    type Error = DbConversionError;

    fn try_from(command_read_db: CommandReadDb) -> Result<Self, Self::Error> {
        let id =
            Uuid::parse_str(&command_read_db.id).map_err(|e| DbConversionError::UuidParse {
                field: "id".to_string(),
                value: command_read_db.id.clone(),
                source: e,
            })?;
        let correlation_id = Uuid::parse_str(&command_read_db.correlation_id).map_err(|e| {
            DbConversionError::UuidParse {
                field: "correlation_id".to_string(),
                value: command_read_db.correlation_id.clone(),
                source: e,
            }
        })?;
        let causation_id = command_read_db
            .causation_id
            .as_ref()
            .map(|s| {
                Uuid::parse_str(s).map_err(|e| DbConversionError::UuidParse {
                    field: "causation_id".to_string(),
                    value: s.clone(),
                    source: e,
                })
            })
            .transpose()?;
        let data = serde_json::from_str(&command_read_db.data).map_err(|e| {
            DbConversionError::JsonDeserialize {
                field: "data".to_string(),
                source: e,
            }
        })?;
        let created_utc = DateTime::parse_from_rfc3339(&command_read_db.created_utc)
            .map_err(|e| DbConversionError::DateTimeParse {
                field: "created_utc".to_string(),
                value: command_read_db.created_utc.clone(),
                source: e,
            })?
            .with_timezone(&Utc);

        Ok(CommandRead {
            id,
            correlation_id,
            causation_id,
            data,
            name: command_read_db.name,
            created_utc,
        })
    }
}
