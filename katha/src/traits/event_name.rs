/// Trait that provides a stable event name for payload/event types.
///
/// This helps avoid hand-written string literals in event write paths.
pub trait EventName {
    const NAME: &'static str;
}
