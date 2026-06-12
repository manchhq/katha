//! Types module for the Cosmo Store event sourcing library.
//!
//! This module contains all the core types used for event sourcing, including:
//! - Event types for reading and writing events
//! - Command types for handling commands
//! - Version types for optimistic concurrency control
//! - Stream types for managing event streams
//! - Filter types for querying events
pub mod command_write;
pub mod event_read;
pub mod event_read_range;
pub mod event_stream;
pub mod event_write;
pub mod expected_version;
pub mod stream_read_filter;
