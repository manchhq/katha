use crate::types::expected_version::ExpectedVersion;
use anyhow::Result;
/// A trait for managing versioning in event-sourced systems.
///
/// This trait provides functionality to determine the next version number
/// based on the expected version, which is crucial for optimistic concurrency
/// control in event-sourced systems.
///
/// **Design note:** Version numbers use `u32`. By design, event streams are
/// kept small (time-sliced); `u32` (4,294,967,295 events per stream) removes
/// the 65 k surprise of the former `u16` limit for general consumers.
///
/// # Examples
/// ```rust
/// use anyhow::Result;
/// use katha::{ExpectedVersion, Version};
///
/// struct SimpleVersion;
///
/// impl Version for SimpleVersion {
///     fn next_version(&self, version: &ExpectedVersion) -> Result<u32> {
///         match version {
///             ExpectedVersion::Any => Ok(0),
///             ExpectedVersion::NoStream => Ok(0),
///             ExpectedVersion::Exact(v) => Ok(v + 1),
///         }
///     }
/// }
/// ```
pub trait Version {
    /// Calculates the next version number based on the expected version.
    ///
    /// # Arguments
    /// * `version` - The expected version that determines how the next version is calculated
    ///
    /// # Returns
    /// A `Result` containing the next version number if successful
    ///
    /// # Errors
    /// Returns an error if the version calculation fails
    fn next_version(&self, version: &ExpectedVersion) -> Result<u32>;
}
