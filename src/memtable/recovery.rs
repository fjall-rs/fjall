use super::MemTable;
use crate::{
    commit_log::{
        marker::Marker,
        reader::{Error as CommitIterateError, Reader as CommitLogReader},
    },
    serde::{Serializable, SerializeError},
    value::SeqNo,
    Value,
};
use log::{error, warn};
use std::path::Path;

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

impl MemTable {
    /// Creates a [`MemTable`] from a commit log on disk
    pub(crate) fn from_file<P: AsRef<Path>>(path: P, strategy: &Strategy) -> Result {
        use Marker::{End, Item, Start};

        let reader = CommitLogReader::new(path)?;

        let mut hasher = crc32fast::Hasher::new();
        let mut is_in_batch = false;
        let mut batch_counter = 0;

        let mut byte_count: u64 = 0;

        let mut memtable = Self::default();
        let mut items: Vec<Value> = vec![];

        for item in reader {
            let item = item?; // TODO: result, RecoveryStrategy

            match item {
                Start(batch_size) => {
                    if is_in_batch {
                        error!("Invalid batch: found batch start inside batch");
                        return Err(Error::UnexpectedBatchStart);
                    }

                    is_in_batch = true;
                    batch_counter = batch_size;
                }
                End(crc) => {
                    // TODO: allow to drop invalid batches, not same option as LastBatchStrategy
                    if batch_counter > 0 {
                        error!(
                            "Invalid batch: reached end of batch with less entries than expected"
                        );
                        match strategy.invalid_batch_strategy {
                            InvalidBatchMode::Discard => {
                                warn!("Reached end of commit log without end marker, discarding items");
                            }
                            InvalidBatchMode::Error => {
                                error!("Reached end of commit log without end marker");
                                return Err(Error::UnexpectedBatchEnd);
                            }
                        }
                    }

                    // TODO: allow to drop invalid batches, not same option as LastBatchStrategy
                    if hasher.finalize() != crc {
                        error!("Invalid batch: checksum check failed");
                        match strategy.invalid_batch_strategy {
                            InvalidBatchMode::Discard => {
                                warn!("CRC mismatch, discarding items");
                            }
                            InvalidBatchMode::Error => {
                                error!("CRC mismatch");
                                return Err(Error::ChecksumCheckFail);
                            }
                        }
                    }

                    hasher = crc32fast::Hasher::new();
                    is_in_batch = false;
                    batch_counter = 0;

                    // NOTE: Clippy says into_iter() is better
                    // but in this case probably not
                    #[allow(clippy::iter_with_drain)]
                    for item in items.drain(..) {
                        memtable.insert(item, 0);
                    }
                }
                Item(item) => {
                    let mut bytes = Vec::new();
                    Marker::Item(item.clone()).serialize(&mut bytes)?;

                    byte_count += bytes.len() as u64;
                    hasher.update(&bytes);
                    batch_counter -= 1;

                    items.push(item);
                }
            }
        }

        if is_in_batch {
            match strategy.last_batch_strategy {
                InvalidBatchMode::Discard => {
                    warn!("Reached end of commit log without end marker, discarding items");
                }
                InvalidBatchMode::Error => {
                    error!("Reached end of commit log without end marker");
                    return Err(Error::MissingBatchEnd);
                }
            }
        }

        // memtable.size_in_bytes = byte_count;

        let lsn = memtable
            .data
            .values()
            .map(|item| item.seqno)
            .max()
            .unwrap_or_default();

        Ok((lsn, byte_count, memtable))
    }
}
