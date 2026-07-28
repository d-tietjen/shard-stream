use std::error::Error;
use std::fmt;
use std::io;

/// Result type used by the low-level ShardLog storage engine.
pub type StorageResult<T> = Result<T, StorageError>;

/// Failure returned while opening, validating, reading, or mutating a log.
#[derive(Debug)]
pub enum StorageError {
    /// Filesystem operation failed.
    Io(io::Error),
    /// Checksummed or structurally committed history is corrupt.
    Corrupt {
        /// Path containing the corrupt record.
        path: String,
        /// Byte offset at which validation failed.
        offset: u64,
        /// Human-readable validation failure.
        reason: String,
    },
    /// Caller supplied an invalid configuration, reservation, or operation.
    InvalidInput(String),
    /// A configured or format-level size/count limit was exceeded.
    LimitExceeded(String),
}

impl StorageError {
    pub(crate) fn corrupt(path: &std::path::Path, offset: u64, reason: impl Into<String>) -> Self {
        Self::Corrupt {
            path: path.display().to_string(),
            offset,
            reason: reason.into(),
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "storage I/O error: {error}"),
            Self::Corrupt {
                path,
                offset,
                reason,
            } => write!(formatter, "corrupt storage at {path}:{offset}: {reason}"),
            Self::InvalidInput(message) => write!(formatter, "invalid storage input: {message}"),
            Self::LimitExceeded(message) => write!(formatter, "storage limit exceeded: {message}"),
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Corrupt { .. } | Self::InvalidInput(_) | Self::LimitExceeded(_) => None,
        }
    }
}

impl From<io::Error> for StorageError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
