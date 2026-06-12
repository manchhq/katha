use chrono::{DateTime, Utc};
/// Represents a stream of events in the event store.
///
/// An event stream is a sequence of events that share the same stream identifier.
/// This type tracks the current state of the stream, including its last version
/// and when it was last updated.
///
/// # Fields
/// * `id` - Unique identifier for the stream
/// * `last_version` - The version number of the most recent event in the stream
/// * `last_updated_utc` - When the stream was last updated
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventStream {
    pub id: String,
    pub last_version: u32,
    pub last_updated_utc: DateTime<Utc>,
}
