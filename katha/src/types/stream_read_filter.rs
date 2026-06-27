use chrono::{DateTime, Utc};

/// Filter for reading streams from the event store.
///
/// This enum provides different ways to filter streams when reading from the event store.
/// It allows for flexible querying of streams based on version or time.
///
/// # Variants
/// * `AllStreams` - Read all streams without filtering
/// * `IdPrefix(String)` - Read streams whose id starts with the given prefix
/// * `BeforeVersion(u32)` - Read streams with last_version before the given version
/// * `AfterVersion(u32)` - Read streams with last_version after the given version
/// * `BetweenVersions(u32, u32)` - Read streams with last_version in the given range
/// * `BeforeTime(DateTime<Utc>)` - Read streams last updated before the given time
/// * `AfterTime(DateTime<Utc>)` - Read streams last updated after the given time
/// * `BetweenTimes(DateTime<Utc>, DateTime<Utc>)` - Read streams last updated in the given range
#[derive(Clone, Debug)]
pub enum StreamsReadFilter {
    AllStreams,
    IdPrefix(String),
    BeforeVersion(u32),
    AfterVersion(u32),
    BetweenVersions(u32, u32),
    BeforeTime(DateTime<Utc>),
    AfterTime(DateTime<Utc>),
    BetweenTimes(DateTime<Utc>, DateTime<Utc>),
}
