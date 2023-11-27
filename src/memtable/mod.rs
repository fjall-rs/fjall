pub mod recovery;

use crate::commit_log::CommitLog;
use crate::Value;
use crate::{
    commit_log::{marker::Marker, reader::Reader as CommitLogReader},
    serde::Serializable,
};
use std::collections::BTreeMap;
use std::path::Path;

/// The `MemTable` serves as an intermediary storage for new items
///
/// If the `MemTable`'s size exceeds a certain threshold, it will be written to disk as a Segment and cleared
///
/// In case of a program crash, the current `MemTable` can be rebuilt from the commit log
#[derive(Default)]
pub struct MemTable {
    pub(crate) items: BTreeMap<Vec<u8>, Value>,
    pub(crate) size_in_bytes: u32,
}

fn rewrite_commit_log<P: AsRef<Path>>(path: P, memtable: &MemTable) -> std::io::Result<()> {
    log::info!("Rewriting commit log");

    let parent = path.as_ref().parent().unwrap();
    /* let file = std::fs::File::create(parent.join("rlog"))?; */
    let mut repaired_log = CommitLog::new(parent.join("rlog"))?;

    repaired_log
        .append_batch(memtable.items.values().cloned().collect())
        .unwrap();

    repaired_log.flush()?;

    std::fs::rename(parent.join("rlog"), &path)?;

    // fsync log file
    let file = std::fs::File::open(&path)?;
    file.sync_all()?;

    // fsync folder as well
    let file = std::fs::File::open(parent)?;
    file.sync_all()?;

    log::info!("Atomically rewritten commit log");

    Ok(())
}

impl MemTable {
    /// Returns the item by key if it exists
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<Value> {
        let result = self.items.get(key.as_ref());
        result.cloned()
    }

    pub fn exceeds_threshold(&mut self, threshold: u32) -> bool {
        self.size_in_bytes > threshold
    }

    /// Inserts an item into the `MemTable`
    pub fn insert(&mut self, entry: Value, bytes_written: u32) {
        self.items.insert(entry.key.clone(), entry);
        self.size_in_bytes += bytes_written;
    }

    /// Creates a [`MemTable`] from a commit log on disk
    pub(crate) fn from_file<P: AsRef<Path>>(
        path: P,
        //strategy: &recovery::Strategy,
    ) -> recovery::Result {
        use Marker::{End, Item, Start};

        let reader = CommitLogReader::new(&path)?;

        let mut hasher = crc32fast::Hasher::new();
        let mut is_in_batch = false;
        let mut batch_counter = 0;

        let mut byte_count = 0;

        let mut memtable = Self::default();
        let mut items: Vec<Value> = vec![];

        let mut lsn = 0;

        for item in reader {
            let item = match item {
                Ok(item) => item,
                Err(error) => {
                    log::warn!("Undeserializable item found: {:?}", error);
                    rewrite_commit_log(path, &memtable)?;
                    return Ok((lsn, byte_count, memtable));
                }
            };

            match item {
                Start(batch_size) => {
                    if is_in_batch && !items.is_empty() {
                        log::warn!("Invalid batch: found batch start inside non-empty batch");
                        rewrite_commit_log(path, &memtable)?;

                        // TODO: commit log is probably corrupt from here on... need to rewrite log and atomically swap it

                        return Ok((lsn, byte_count, memtable));

                        /* match strategy.invalid_batch_strategy {
                            recovery::InvalidBatchMode::Discard => {
                                warn!("Reached end of commit log without end marker, discarding items");

                                // TODO: commit log is probably corrupt from here on... need to rewrite log and atomically swap it

                                /* memtable.size_in_bytes = byte_count as u32;
                                return Ok((lsn, byte_count, memtable)); */
                            }
                            recovery::InvalidBatchMode::Error => {
                                error!("Reached end of commit log without end marker");
                                return Err(recovery::Error::UnexpectedBatchEnd);
                            }
                        } */
                    }

                    is_in_batch = true;
                    batch_counter = batch_size;
                }
                End(crc) => {
                    // TODO: allow to drop invalid batches, not same option as LastBatchStrategy
                    if batch_counter > 0 {
                        log::warn!(
                            "Invalid batch: reached end of batch with less entries than expected"
                        );
                        rewrite_commit_log(path, &memtable)?;

                        // TODO: commit log is probably corrupt from here on... need to rewrite log and atomically swap it

                        return Ok((lsn, byte_count, memtable));
                        /* match strategy.invalid_batch_strategy {
                            recovery::InvalidBatchMode::Discard => {
                                warn!("Reached end of commit log without end marker, discarding items");



                                // TODO: commit log is probably corrupt from here on... need to rewrite log and atomically swap it

                                /* memtable.size_in_bytes = byte_count as u32;
                                return Ok((lsn, byte_count, memtable)); */
                            }
                            recovery::InvalidBatchMode::Error => {
                                error!("Reached end of commit log without end marker");
                                return Err(recovery::Error::UnexpectedBatchEnd);
                            }
                        } */
                    }

                    // TODO: allow to drop invalid batches, not same option as LastBatchStrategy
                    if hasher.finalize() != crc {
                        log::warn!("Invalid batch: checksum check failed");
                        rewrite_commit_log(path, &memtable)?;

                        // TODO: commit log is probably corrupt from here on... need to rewrite log and atomically swap it

                        return Ok((lsn, byte_count, memtable));

                        /* match strategy.invalid_batch_strategy {
                            recovery::InvalidBatchMode::Discard => {
                                warn!("CRC mismatch, discarding items");

                                // TODO: commit log is probably corrupt from here on... need to rewrite log and atomically swap it

                                /* memtable.size_in_bytes = byte_count as u32;
                                return Ok((lsn, byte_count, memtable)); */
                            }
                            recovery::InvalidBatchMode::Error => {
                                error!("CRC mismatch");
                                return Err(recovery::Error::ChecksumCheckFail);
                            }
                        } */
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
            log::warn!("Reached end of commit log without end marker, discarding items");
            rewrite_commit_log(path, &memtable)?;

            /* match strategy.last_batch_strategy {
                recovery::InvalidBatchMode::Discard => {
                    warn!("Reached end of commit log without end marker, discarding items");
                }
                recovery::InvalidBatchMode::Error => {
                    error!("Reached end of commit log without end marker");
                    return Err(recovery::Error::MissingBatchEnd);
                }
            } */
        }

        log::info!("Memtable recovered");

        memtable.size_in_bytes = byte_count as u32;

        Ok((lsn, byte_count, memtable))
    }
}
