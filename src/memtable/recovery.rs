use super::MemTable;
use crate::{
    commit_log::reader::Error as CommitIterateError, serde::SerializeError, value::SeqNo,
};

#[derive(Default)]
#[non_exhaustive]
pub enum InvalidBatchMode {
    /// Returns an error if the batch is invalid
    Error,

    #[default]
    /// Discards the batch if it is invalid
    ///
    /// This is probably the most sane option
    Discard,
}

#[derive(Default)]
pub struct Strategy {
    pub last_batch_strategy: InvalidBatchMode,
    pub invalid_batch_strategy: InvalidBatchMode,
}

#[derive(Debug)]
pub enum Error {
    MissingBatchEnd,
    UnexpectedBatchStart,
    UnexpectedBatchEnd,
    ChecksumCheckFail,
    Io(std::io::Error),
    Iterate(CommitIterateError),
    Serialize(SerializeError),
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<CommitIterateError> for Error {
    fn from(value: CommitIterateError) -> Self {
        Self::Iterate(value)
    }
}

impl From<SerializeError> for Error {
    fn from(value: SerializeError) -> Self {
        Self::Serialize(value)
    }
}

pub type Result = std::result::Result<(SeqNo, u64, MemTable), Error>;
