use crate::types::event_read::EventRead;
use crate::types::event_read_range::EventsReadRange;
use crate::types::event_stream::EventStream;
use crate::types::event_write::EventWrite;
use crate::types::expected_version::ExpectedVersion;
use crate::types::stream_read_filter::StreamsReadFilter;
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::broadcast;
use uuid::Uuid;
/// A trait for storing and retrieving events in an event-sourced system.
///
/// This trait defines the interface for event storage, allowing events to be
/// persisted and retrieved. Events represent facts that have occurred in the system.
///
/// # Type Parameters
/// * `Payload` - The type of the event payload
/// * `Meta` - The type of the event metadata
///
/// # Examples
///
/// ```rust,ignore
/// use katha::{EventStore, EventWrite, ExpectedVersion, EventsReadRange};
/// use anyhow::Result;
/// use async_trait::async_trait;
///
/// struct InMemoryEventStore;
///
/// #[async_trait]
/// impl<Payload, Meta> EventStore<Payload, Meta> for InMemoryEventStore {
///     // Implementation of all required methods
/// }
/// ```
#[async_trait]
pub trait EventStore<Payload, Meta> {
    /// Ensures the events table exists in the database.
    ///
    /// # Returns
    /// A `Result` indicating success or failure
    ///
    /// # Errors
    /// Returns an error if the events table cannot be ensured
    async fn ensure_events_table(&self) -> Result<()>;

    /// Optional observable stream of appended events.
    ///
    /// This mirrors the old `EventAppended : IObservable<...>` concept in a Rust-friendly way.
    /// Backends that support push notifications can return a broadcast receiver here.
    /// Backends without notification support can keep the default `None`.
    fn event_appended(&self) -> Option<broadcast::Receiver<EventRead<Payload, Meta>>> {
        None
    }

    /// Appends a single event to a stream.
    ///
    /// # Arguments
    /// * `stream_id` - The ID of the stream to append to
    /// * `version` - The expected version for optimistic concurrency control
    /// * `payload` - The event to append
    ///
    /// # Returns
    /// The appended event with additional metadata
    ///
    /// # Errors
    /// Returns an error if the event cannot be appended
    async fn append_event(
        &self,
        stream_id: &str,
        version: &ExpectedVersion,
        payload: &EventWrite<Payload, Meta>,
    ) -> Result<EventRead<Payload, Meta>>;
    /// Appends multiple events to a stream.
    ///
    /// # Arguments
    /// * `stream_id` - The ID of the stream to append to
    /// * `version` - The expected version for optimistic concurrency control
    /// * `payload` - The events to append
    ///
    /// # Returns
    /// The appended events with additional metadata
    ///
    /// # Errors
    /// Returns an error if the events cannot be appended
    async fn append_events(
        &self,
        stream_id: &str,
        version: &ExpectedVersion,
        payload: Vec<EventWrite<Payload, Meta>>,
    ) -> Result<Vec<EventRead<Payload, Meta>>>;
    /// Retrieves a single event from a stream by version.
    ///
    /// # Arguments
    /// * `stream_id` - The ID of the stream to read from
    /// * `version` - The version of the event to retrieve
    ///
    /// # Returns
    /// The requested event
    ///
    /// # Errors
    /// Returns an error if the event cannot be retrieved
    async fn get_event(&self, stream_id: &str, version: u32) -> Result<EventRead<Payload, Meta>>;
    /// Retrieves multiple events from a stream based on a version range.
    ///
    /// # Arguments
    /// * `stream_id` - The ID of the stream to read from
    /// * `range` - The range of versions to retrieve
    ///
    /// # Returns
    /// The requested events
    ///
    /// # Errors
    /// Returns an error if the events cannot be retrieved
    async fn get_events(
        &self,
        stream_id: &str,
        range: &EventsReadRange,
    ) -> Result<Vec<EventRead<Payload, Meta>>>;
    /// Retrieves events by their correlation ID.
    ///
    /// # Arguments
    /// * `correlation_id` - The correlation ID to search for
    ///
    /// # Returns
    /// All events with the given correlation ID
    ///
    /// # Errors
    /// Returns an error if the events cannot be retrieved
    async fn get_events_by_correlation_id(
        &self,
        correlation_id: &Uuid,
    ) -> Result<Vec<EventRead<Payload, Meta>>>;
    /// Retrieves events by their causation ID.
    ///
    /// # Arguments
    /// * `causation_id` - The causation ID to search for
    ///
    /// # Returns
    /// All events with the given causation ID
    ///
    /// # Errors
    /// Returns an error if the events cannot be retrieved
    async fn get_events_by_causation_id(
        &self,
        causation_id: &Uuid,
    ) -> Result<Vec<EventRead<Payload, Meta>>>;
    /// Retrieves streams based on a filter.
    ///
    /// # Arguments
    /// * `filter` - The filter to apply when retrieving streams
    ///
    /// # Returns
    /// All streams matching the filter
    ///
    /// # Errors
    /// Returns an error if the streams cannot be retrieved
    async fn get_streams(&self, filter: &StreamsReadFilter) -> Result<Vec<EventStream>>;
    /// Retrieves a single stream by ID.
    ///
    /// # Arguments
    /// * `stream_id` - The ID of the stream to retrieve
    ///
    /// # Returns
    /// The requested stream
    ///
    /// # Errors
    /// Returns an error if the stream cannot be retrieved
    async fn get_stream(&self, stream_id: &str) -> Result<EventStream>;
}
