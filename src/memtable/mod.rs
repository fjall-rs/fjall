pub mod recovery;

use crate::Value;
use crate::{
    commit_log::{marker::Marker, reader::Reader as CommitLogReader},
    serde::Serializable,
};
use log::{error, warn};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::RwLock;

/// The `MemTable` serves as an intermediary storage for new items
///
/// If the memtable's size exceeds a certain threshold, it will be written to disk as a Segment and cleared
///
/// In case of a program crash, the current memtable can be rebuilt from the commit log
#[derive(Default)]
pub struct MemTable {
    items: RwLock<BTreeMap<Vec<u8>, Value>>,
    //size_in_bytes: u64,
}

impl MemTable {
    /// Returns the item by key if it exists
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<Value> {
        let lock = self.items.read().expect("should lock");
        let result = lock.get(key.as_ref());
        result.cloned()
    }

    /// Returns true if the MemTable is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Gets the item count
    pub fn len(&self) -> usize {
        let lock = self.items.read().expect("should lock");
        lock.len()
    }

    /*   #[allow(dead_code)]
    pub(crate) fn get_size(&self) -> u64 {
        self.size_in_bytes
    } */

    /* #[allow(dead_code)]
    pub(crate) fn set_size(&mut self, value: u64) {
        self.size_in_bytes = value;
    } */

    /* pub fn exceeds_threshold(&mut self, threshold: u64) -> bool {
        self.size_in_bytes > threshold
    } */

    /// Inserts an item into the MemTable
    pub fn insert(&self, entry: Value, bytes_written: usize) {
        let mut lock = self.items.write().expect("should lock");
        lock.insert(entry.key.clone(), entry);
        //self.size_in_bytes += bytes_written as u64;
    }

    /// Creates a [`MemTable`] from a commit log on disk
    pub(crate) fn from_file<P: AsRef<Path>>(
        path: P,
        strategy: &recovery::Strategy,
    ) -> recovery::Result {
        use Marker::{End, Item, Start};

        let reader = CommitLogReader::new(path)?;

        let mut hasher = crc32fast::Hasher::new();
        let mut is_in_batch = false;
        let mut batch_counter = 0;

        let mut byte_count: u64 = 0;

        let mut memtable = Self::default();
        let mut items: Vec<Value> = vec![];

        let mut lsn = 0;

        for item in reader {
            let item = item?; // TODO: result, RecoveryStrategy

            match item {
                Start(batch_size) => {
                    if is_in_batch {
                        error!("Invalid batch: found batch start inside batch");
                        return Err(recovery::Error::UnexpectedBatchStart);
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
                            recovery::InvalidBatchMode::Discard => {
                                warn!("Reached end of commit log without end marker, discarding items");
                            }
                            recovery::InvalidBatchMode::Error => {
                                error!("Reached end of commit log without end marker");
                                return Err(recovery::Error::UnexpectedBatchEnd);
                            }
                        }
                    }

                    // TODO: allow to drop invalid batches, not same option as LastBatchStrategy
                    if hasher.finalize() != crc {
                        error!("Invalid batch: checksum check failed");
                        match strategy.invalid_batch_strategy {
                            recovery::InvalidBatchMode::Discard => {
                                warn!("CRC mismatch, discarding items");
                            }
                            recovery::InvalidBatchMode::Error => {
                                error!("CRC mismatch");
                                return Err(recovery::Error::ChecksumCheckFail);
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

                    // Increase LSN if item's seqno is higher
                    lsn = lsn.max(item.seqno);

                    items.push(item);
                }
            }
        }

        if is_in_batch {
            match strategy.last_batch_strategy {
                recovery::InvalidBatchMode::Discard => {
                    warn!("Reached end of commit log without end marker, discarding items");
                }
                recovery::InvalidBatchMode::Error => {
                    error!("Reached end of commit log without end marker");
                    return Err(recovery::Error::MissingBatchEnd);
                }
            }
        }

        // memtable.size_in_bytes = byte_count;

        Ok((lsn, byte_count, memtable))
    }
}
