use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncOperation {
    Push,
    Pull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncErrorKind {
    Authentication,
    Unavailable,
    Protocol,
    Configuration,
    Interrupted,
    LocalStorage,
}

/// Actionable synchronization error. Credentials are never included.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncError {
    pub operation: SyncOperation,
    pub kind: SyncErrorKind,
    pub message: String,
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} failed ({:?}): {}",
            self.operation, self.kind, self.message
        )
    }
}
impl std::error::Error for SyncError {}

impl SyncError {
    pub(crate) fn driver(
        operation: SyncOperation,
        error: turso::Error,
        token: Option<&str>,
    ) -> Self {
        let mut detail = error.to_string();
        if let Some(token) = token.filter(|s| !s.is_empty()) {
            detail = detail.replace(token, "[redacted]");
        }
        let lower = detail.to_lowercase();
        let (kind, action) = if lower.contains("status=401")
            || lower.contains("status=403")
            || lower.contains("unauthorized")
            || lower.contains("forbidden")
            || lower.contains("jwt")
            || lower.contains("invalidtoken")
        {
            (
                SyncErrorKind::Authentication,
                "Refresh credentials with set_auth_token and retry; local data remains available.",
            )
        } else if lower.contains("status=404")
            || lower.contains("protocol")
            || lower.contains("unsupported")
        {
            (
                SyncErrorKind::Protocol,
                "Verify this database supports Turso Sync; ordinary libSQL access is insufficient.",
            )
        } else if lower.contains("url")
            && (lower.contains("missing")
                || lower.contains("not set")
                || lower.contains("no remote"))
        {
            (
                SyncErrorKind::Configuration,
                "Configure a Turso Sync remote URL and retry.",
            )
        } else {
            (
                SyncErrorKind::Unavailable,
                "Retry synchronization when the remote is reachable; pending changes remain local.",
            )
        };
        Self {
            operation,
            kind,
            message: format!("{action} {detail}"),
        }
    }
    pub(crate) fn timeout(operation: SyncOperation) -> Self {
        Self { operation, kind: SyncErrorKind::Interrupted, message: "Synchronization timed out. Retry; a remote commit may already have succeeded and SDK sync metadata makes retries safe.".into() }
    }
    pub(crate) fn interrupted(operation: SyncOperation) -> Self {
        Self { operation, kind: SyncErrorKind::Interrupted, message: "The previous synchronization was interrupted. Retry using this same replica and its sync metadata.".into() }
    }
    pub(crate) fn local(operation: SyncOperation, error: anyhow::Error) -> Self {
        Self {
            operation,
            kind: SyncErrorKind::LocalStorage,
            message: format!("Check local storage permissions and available space. {error}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncStatus {
    pub pending_operations: u64,
    pub last_push: Option<DateTime<Utc>>,
    pub last_pull: Option<DateTime<Utc>>,
    pub revision: Option<String>,
    pub last_error: Option<SyncError>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn driver_errors_have_actions_and_redact_credentials() {
        let error = SyncError::driver(
            SyncOperation::Push,
            turso::Error::Error("status=400, JWT error: InvalidToken secret-value".into()),
            Some("secret-value"),
        );
        assert_eq!(error.kind, SyncErrorKind::Authentication);
        assert!(error.message.contains("set_auth_token"));
        assert!(!error.message.contains("secret-value"));
        assert_eq!(
            SyncError::driver(
                SyncOperation::Pull,
                turso::Error::Error("status=404".into()),
                None
            )
            .kind,
            SyncErrorKind::Protocol
        );
        assert_eq!(
            SyncError::driver(
                SyncOperation::Push,
                turso::Error::Error("remote URL is not set".into()),
                Some("")
            )
            .kind,
            SyncErrorKind::Configuration
        );
        assert_eq!(
            SyncError::driver(
                SyncOperation::Pull,
                turso::Error::Error("connection refused to port 4040".into()),
                None
            )
            .kind,
            SyncErrorKind::Unavailable
        );
        let timeout = SyncError::timeout(SyncOperation::Push);
        assert_eq!(timeout.kind, SyncErrorKind::Interrupted);
        assert!(timeout.to_string().contains("timed out"));
        assert_eq!(
            SyncError::local(SyncOperation::Pull, anyhow::anyhow!("disk full")).kind,
            SyncErrorKind::LocalStorage
        );
    }
}
