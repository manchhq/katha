use chrono::{DateTime, Utc};
use katha::types::command_write::CommandRead;
use katha::types::event_read::EventRead;
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

/// Cursor for cross-stream event pagination (`created_utc` + `id` for tie-breaking).
///
/// Cross-stream reads have no single version sequence to page on, so the cursor
/// is the same `(created_utc, id)` keyset the results are ordered by.
#[derive(Debug, Clone)]
pub struct EventCursor {
    pub created_utc: DateTime<Utc>,
    pub id: Uuid,
}

/// Cursor page result for cross-stream event reads.
#[derive(Debug, Clone)]
pub struct EventStreamsCursorPage<Payload, Meta> {
    pub items: Vec<EventRead<Payload, Meta>>,
    pub next_cursor: Option<EventCursor>,
}
