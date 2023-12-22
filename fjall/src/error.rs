use crate::journal::shard::RecoveryError as JournalRecoveryError;
use lsm_tree::{DeserializeError, SerializeError};

/// Errors that may occur in the storage engine
#[derive(Debug)]
pub enum Error {
    /// Error inside LSM-tree
    Storage(lsm_tree::Error),

    /// I/O error
    Io(std::io::Error),

    /// Serialization failed
    Serialize(SerializeError),

    /// Deserialization failed
    Deserialize(DeserializeError),

    /// Error during journal recovery
    JournalRecovery(JournalRecoveryError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FjallError: {self:?}")
    }
}

impl From<std::io::Error> for Error {
    fn from(inner: std::io::Error) -> Self {
        Self::Io(inner)
    }
}

impl From<SerializeError> for Error {
    fn from(value: SerializeError) -> Self {
        Self::Serialize(value)
    }
}

impl From<DeserializeError> for Error {
    fn from(value: DeserializeError) -> Self {
        Self::Deserialize(value)
    }
}

impl From<lsm_tree::Error> for Error {
    fn from(inner: lsm_tree::Error) -> Self {
        Self::Storage(inner)
    }
}

impl std::error::Error for Error {}

/// Result helper type
pub type Result<T> = std::result::Result<T, Error>;
