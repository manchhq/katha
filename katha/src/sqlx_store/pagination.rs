use crate::types::command_write::CommandRead;
use crate::types::event_read::EventRead;
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Cursor page result for event stream reads.
#[derive(Debug, Clone)]
pub struct EventCursorPage<Payload, Meta> {
    pub items: Vec<EventRead<Payload, Meta>>,
    pub next_cursor: Option<u32>,
}

/// Cursor for command pagination (created_utc + id for tie-breaking).
#[derive(Debug, Clone)]
pub struct CommandCursor {
    pub created_utc: DateTime<Utc>,
    pub id: Uuid,
}

/// Cursor page result for command reads.
#[derive(Debug, Clone)]
pub struct CommandCursorPage<Payload> {
    pub items: Vec<CommandRead<Payload>>,
    pub next_cursor: Option<CommandCursor>,
}
