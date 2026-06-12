/// Represents the expected version of a stream when writing events.
///
/// This enum is used for optimistic concurrency control when appending events
/// to a stream. It allows specifying different version expectations:
///
/// # Variants
/// * `Any` - Accept any version (no concurrency control)
/// * `NoStream` - Expect that the stream doesn't exist
/// * `Exact(u32)` - Expect the stream to be at exactly this version
#[derive(Clone, Debug)]
pub enum ExpectedVersion {
    Any,
    NoStream,
    Exact(u32),
}
