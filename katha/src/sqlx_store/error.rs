//! Database conversion errors for DB-to-domain type conversions.

use std::fmt;

/// Error when converting database rows to domain types.
///
/// Occurs when the database contains corrupted or unexpected data (invalid UUIDs,
/// malformed JSON, or unparseable timestamps).
#[derive(Debug)]
pub enum DbConversionError {
    UuidParse {
        field: String,
        value: String,
        source: uuid::Error,
    },
    JsonDeserialize {
        field: String,
        source: serde_json::Error,
    },
    DateTimeParse {
        field: String,
        value: String,
        source: chrono::ParseError,
    },
}

impl fmt::Display for DbConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UuidParse {
                field,
                value,
                source,
            } => {
                write!(
                    f,
                    "failed to parse UUID for field '{}' (value: {:?}): {}",
                    field, value, source
                )
            }
            Self::JsonDeserialize { field, source } => {
                write!(
                    f,
                    "failed to deserialize JSON for field '{}': {}",
                    field, source
                )
            }
            Self::DateTimeParse {
                field,
                value,
                source,
            } => {
                write!(
                    f,
                    "failed to parse datetime for field '{}' (value: {:?}): {}",
                    field, value, source
                )
            }
        }
    }
}

impl std::error::Error for DbConversionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::UuidParse { source, .. } => Some(source),
            Self::JsonDeserialize { source, .. } => Some(source),
            Self::DateTimeParse { source, .. } => Some(source),
        }
    }
}
