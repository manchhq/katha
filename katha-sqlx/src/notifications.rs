use uuid::Uuid;

pub const DEFAULT_NOTIFICATION_BUFFER: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventNotification {
    pub store_name: String,
    pub stream_id: String,
    pub from_version: u32,
    pub to_version: u32,
    pub event_ids: Vec<Uuid>,
    pub event_names: Vec<String>,
}
