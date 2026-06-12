use crate::traits::event_name::EventName;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
/// Represents an event that is ready to be written to the event store.
///
/// This type encapsulates all the information needed to write an event,
/// including its payload, metadata, and correlation information.
///
/// # Type Parameters
/// * `Payload` - The type of the event data
/// * `Meta` - The type of the event metadata
///
/// # Fields
/// * `id` - Unique identifier for this event
/// * `correlation_id` - Optional identifier linking related events
/// * `causation_id` - Optional identifier of the event that caused this one
/// * `name` - Name/type of the event
/// * `data` - The actual event payload
/// * `metadata` - Optional metadata associated with the event
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventWrite<Payload, Meta> {
    pub id: Uuid,
    pub correlation_id: Option<Uuid>,
    pub causation_id: Option<Uuid>,
    pub name: String,
    pub data: Payload,
    pub metadata: Option<Meta>,
}

impl<Payload, Meta> EventWrite<Payload, Meta>
where
    Payload: EventName,
{
    /// Builds an `EventWrite` using `Payload::NAME` as the event name.
    ///
    /// This removes manual string literals for event names while keeping IDs
    /// explicit and caller-controlled.
    pub fn from_payload(
        id: Uuid,
        correlation_id: Option<Uuid>,
        causation_id: Option<Uuid>,
        data: Payload,
        metadata: Option<Meta>,
    ) -> Self {
        Self {
            id,
            correlation_id,
            causation_id,
            name: Payload::NAME.to_string(),
            data,
            metadata,
        }
    }
}
