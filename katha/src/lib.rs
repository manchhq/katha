pub mod traits;
pub mod types;

// Re-exports for ergonomic imports
pub use traits::aggregate::{
    Aggregate, load_state_and_expected_version, make_handler, next_expected_version, rehydrate,
};
pub use traits::command_store::CommandStore;
pub use traits::event_store::EventStore;
pub use traits::version::Version;
pub use types::command_write::{CommandRead, CommandWrite};
pub use types::event_read::EventRead;
pub use types::event_read_range::EventsReadRange;
pub use types::event_write::EventWrite;
pub use types::expected_version::ExpectedVersion;
