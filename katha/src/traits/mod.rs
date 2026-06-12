//! Traits module for the Cosmo Store event sourcing library.
//!
//! This module contains all the core traits used for event sourcing, including:
//! - `EventStore` - For storing and retrieving events
//! - `CommandStore` - For storing and retrieving commands
//! - `Version` - For managing versioning in event-sourced systems
//! - `Aggregate` - For defining event-sourced aggregates
pub mod aggregate;
pub mod command_store;
pub mod event_name;
pub mod event_store;
pub mod version;
