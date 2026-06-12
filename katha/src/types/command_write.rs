use chrono::{DateTime, Utc};
use uuid::Uuid;
/// Types for handling commands in the event sourcing system.
///
/// Commands have identifiers for traceability:
/// - `id`: Unique identifier for the command
/// - `correlation_id`: Links related commands in a conversation
/// - `causation_id`: Optional identifier of the command that caused this one
///
/// A command that is ready to be written to the command store.
///
/// # Type Parameters
/// * `Payload` - The type of the command data
///
/// # Fields
/// * `id` - Unique identifier for this command
/// * `correlation_id` - Identifier linking related commands
/// * `causation_id` - Optional identifier of the command that caused this one
/// * `data` - The actual command payload
/// * `name` - Name/type of the command
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandWrite<Payload> {
    pub id: Uuid,
    pub correlation_id: Uuid,
    pub causation_id: Option<Uuid>,
    pub data: Payload,
    pub name: String,
}
/// A command that has been read from the command store.
///
/// # Type Parameters
/// * `Payload` - The type of the command data
///
/// # Fields
/// * `id` - Unique identifier for this command
/// * `correlation_id` - Identifier linking related commands
/// * `causation_id` - Optional identifier of the command that caused this one
/// * `data` - The actual command payload
/// * `name` - Name/type of the command
/// * `created_utc` - When the command was created
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandRead<Payload> {
    pub id: Uuid,
    pub correlation_id: Uuid,
    pub causation_id: Option<Uuid>,
    pub data: Payload,
    pub name: String,
    pub created_utc: DateTime<Utc>,
}
