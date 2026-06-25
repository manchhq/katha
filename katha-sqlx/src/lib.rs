#![doc = include_str!("../README.md")]

mod backend;
mod command_db;
mod command_store;
mod error;
mod event_db;
mod event_store;
mod notifications;
mod pagination;
mod projection_runner;
mod types;
mod validate;

pub use notifications::{DEFAULT_NOTIFICATION_BUFFER, EventNotification};
pub use pagination::{CommandCursor, CommandCursorPage, EventCursorPage};
pub use projection_runner::ProjectionRunStats;
pub use types::{SqlxCommandStore, SqlxEventStore};
