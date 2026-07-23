use std::error::Error;
use std::fmt;
use std::io;

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug)]
pub enum StorageError {
    Io(io::Error),
    Corrupt {
        path: String,
        offset: u64,
        reason: String,
    },
    InvalidInput(String),
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
