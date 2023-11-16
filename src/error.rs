use crate::serde::DeserializeError;

/// Represents errors that can occur in the LSM-tree
#[derive(Debug)]
pub enum Error {
    /// Deserialization failed
    Deserialize(DeserializeError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LSM-tree error: {self:?}")
    }
}

impl std::error::Error for Error {}

/// Tree result
pub type Result<T> = std::result::Result<T, Error>;
