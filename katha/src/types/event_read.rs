use crate::types::event_write::EventWrite;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
/// Represents an event that has been read from the event store.
///
/// This type contains all the information about an event that has been persisted,
/// including its stream information, version, and timing details.
///
/// # Type Parameters
/// * `Payload` - The type of the event data
/// * `Meta` - The type of the event metadata
///
/// # Fields
/// * `id` - Unique identifier for this event
/// * `correlation_id` - Optional identifier linking related events
/// * `causation_id` - Optional identifier of the event that caused this one
/// * `stream_id` - Identifier of the stream this event belongs to
/// * `version` - Version number of this event in its stream
/// * `name` - Name/type of the event
/// * `data` - The actual event payload
/// * `metadata` - Optional metadata associated with the event
/// * `created_utc` - When the event was created
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventRead<Payload, Meta> {
    pub id: Uuid,
    pub correlation_id: Option<Uuid>,
    pub causation_id: Option<Uuid>,
    pub stream_id: String,
    pub version: u32,
    pub name: String,
    pub data: Payload,
    pub metadata: Option<Meta>,
    pub created_utc: DateTime<Utc>,
}
impl<Payload: Clone, Meta: Clone> EventRead<Payload, Meta> {
    /// Creates a new `EventRead` instance from an `EventWrite` instance.
    ///
    /// This is typically used when persisting an event, converting from the write
    /// form to the read form with additional stream and version information.
    ///
    /// # Arguments
    /// * `stream_id` - The ID of the stream this event belongs to
    /// * `version` - The version number of this event in its stream
    /// * `created_utc` - When the event was created
    /// * `event_write` - The event write instance to convert from
    ///
    /// # Returns
    /// A new `EventRead` instance with all the information from the write instance
    /// plus the additional stream and version information.
    pub fn from_event_write(
        stream_id: &str,
        version: u32,
        created_utc: DateTime<Utc>,
        event_write: &EventWrite<Payload, Meta>,
    ) -> EventRead<Payload, Meta> {
        EventRead {
            id: event_write.id,
            name: event_write.name.to_string(),
            correlation_id: event_write.correlation_id,
            causation_id: event_write.causation_id,
            stream_id: stream_id.to_string(),
            data: event_write.data.clone(),
            metadata: event_write.metadata.clone(),
            created_utc,
            version,
        }
    }
}
