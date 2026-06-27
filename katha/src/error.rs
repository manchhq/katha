//! Typed errors for the katha event store.

use std::fmt;

/// Error returned when an optimistic-concurrency check fails on append.
///
/// Produced when [`ExpectedVersion::Exact`](crate::ExpectedVersion::Exact) does
/// not match the stream's actual next-available version. It flows back through
/// the usual `anyhow::Result`, so existing callers keep compiling; callers that
/// care can downcast it to map onto a transport-level conflict (for example,
/// ConnectRPC `Aborted`):
///
/// ```
/// use katha::ConcurrencyConflict;
///
/// fn handle(err: &anyhow::Error) -> bool {
///     if let Some(conflict) = err.downcast_ref::<ConcurrencyConflict>() {
///         // map conflict.stream_id / expected / actual onto Aborted
///         return true;
///     }
///     false
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConcurrencyConflict {
    /// The stream the append targeted.
    pub stream_id: String,
    /// The version the caller expected (from `ExpectedVersion::Exact`).
    pub expected: u32,
    /// The stream's actual next-available version.
    pub actual: u32,
}

impl fmt::Display for ConcurrencyConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "concurrency conflict on stream '{}': expected version {} but next available version is {}",
            self.stream_id, self.expected, self.actual
        )
    }
}

impl std::error::Error for ConcurrencyConflict {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_mentions_stream_and_versions() {
        let conflict = ConcurrencyConflict {
            stream_id: "patient-123".to_string(),
            expected: 4,
            actual: 5,
        };
        let rendered = conflict.to_string();
        assert!(rendered.contains("patient-123"));
        assert!(rendered.contains('4'));
        assert!(rendered.contains('5'));
    }

    #[test]
    fn downcasts_from_anyhow() {
        let err: anyhow::Error = ConcurrencyConflict {
            stream_id: "patient-123".to_string(),
            expected: 4,
            actual: 5,
        }
        .into();
        let conflict = err
            .downcast_ref::<ConcurrencyConflict>()
            .expect("should downcast to ConcurrencyConflict");
        assert_eq!(conflict.expected, 4);
        assert_eq!(conflict.actual, 5);
        assert_eq!(conflict.stream_id, "patient-123");
    }
}
