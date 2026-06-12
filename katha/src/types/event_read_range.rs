/// Defines a range of events to read from a stream.
///
/// This enum provides different ways to specify which events to read from a stream,
/// allowing for flexible querying based on version numbers.
///
/// # Variants
/// * `AllEvents` - Read all events in the stream
/// * `FromVersion(u32)` - Read all events from the specified version onwards
/// * `ToVersion(u32)` - Read all events up to the specified version
/// * `VersionRange` - Read events within a specific version range
///   * `from_version` - The starting version (inclusive)
///   * `to_version` - The ending version (inclusive)
#[derive(Clone, Debug)]
pub enum EventsReadRange {
    AllEvents,
    FromVersion(u32),
    ToVersion(u32),
    VersionRange { from_version: u32, to_version: u32 },
}
