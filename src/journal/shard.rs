use super::{marker::Marker, writer::Writer as JournalWriter};
use crate::batch::{item::Item as BatchItem, PartitionKey};
use crate::journal::reader::JournalShardReader;
use lsm_tree::{serde::Serializable, MemTable, SeqNo};
use std::{collections::HashMap, fs::OpenOptions, path::Path};

/// Recovery mode to use
///
/// Based on `RocksDB`'s WAL Recovery Modes: <https://github.com/facebook/rocksdb/wiki/WAL-Recovery-Modes>
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RecoveryMode {
    /// The last batch in the journal may be corrupt on crash,
    /// and will be discarded without error.
    ///
    /// This mode will error on any other IO or consistency error, so
    /// any data up to the tail will be consistent.
    ///
    /// This is the default mode.
    #[default]
    TolerateCorruptTail,
    // TODO: in the future?
    /*  /// Skips corrupt (invalid CRC) batches. This may violate
    /// consistency, but will recover as much data as possible.
    SkipInvalidBatches, */
    // TODO: absolute consistency
}

/// Errors that can occur during journal recovery
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryError {
    /// Batch had less items than expected, so it's incomplete
    InsufficientLength,

    /* /// Batch was not terminated, so it's possibly incomplete
    MissingTerminator, */
    /// Too many items in batch
    TooManyItems,

    /// The CRC value does not match the expected value
    CrcCheck,
}

// TODO: don't require locking for sync check
#[allow(clippy::module_name_repetitions)]
pub struct JournalShard {
    pub(crate) writer: JournalWriter,
    pub(crate) should_sync: bool,
}

impl JournalShard {
    pub fn rotate<P: AsRef<Path>>(&mut self, path: P) -> crate::Result<()> {
        self.should_sync = false;
        self.writer.rotate(path)
    }

    pub fn create_new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        Ok(Self {
            writer: JournalWriter::create_new(path)?,
            should_sync: bool::default(),
        })
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        Ok(Self {
            writer: JournalWriter::from_file(path)?,
            should_sync: bool::default(),
        })
    }

    fn truncate_to<P: AsRef<Path>>(path: P, last_valid_pos: u64) -> crate::Result<()> {
        log::trace!("Truncating shard to {last_valid_pos}");
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(last_valid_pos)?;
        file.sync_all()?;
        Ok(())
    }

    /// Recovers a journal shard and writes the items into the given memtable
    ///
    /// Will truncate the file to the position of the last valid batch
    #[allow(clippy::too_many_lines)]
    pub fn recover_and_repair<P: AsRef<Path>>(
        path: P,
        memtables: &mut HashMap<PartitionKey, MemTable>,
        whitelist: Option<&[PartitionKey]>,
        _recovery_mode: RecoveryMode, // TODO:
    ) -> crate::Result<()> {
        use crate::Error::JournalRecovery;

        let path = path.as_ref();
        let recoverer = JournalShardReader::new(path)?;

        let mut hasher = crc32fast::Hasher::new();
        let mut is_in_batch = false;
        let mut batch_counter = 0;
        let mut batch_seqno = SeqNo::default();
        let mut last_valid_pos = 0;

        let mut items: Vec<BatchItem> = vec![];

        'a: for item in recoverer {
            let (journal_file_pos, item) = item?;

            match item {
                Marker::Start { item_count, seqno } => {
                    if is_in_batch {
                        log::debug!("Invalid batch: found batch start inside batch");

                        // Discard batch
                        Self::truncate_to(path, last_valid_pos)?;

                        break 'a;
                    }

                    is_in_batch = true;
                    batch_counter = item_count;
                    batch_seqno = seqno;
                }
                Marker::End(checksum) => {
                    if batch_counter > 0 {
                        log::error!("Invalid batch: insufficient length");
                        return Err(JournalRecovery(RecoveryError::InsufficientLength));
                    }

                    if !is_in_batch {
                        log::error!("Invalid batch: found end marker without start marker");

                        // Discard batch
                        Self::truncate_to(path, last_valid_pos)?;

                        break 'a;
                    }

                    let crc = hasher.finalize();
                    hasher = crc32fast::Hasher::new();

                    if crc != checksum {
                        log::error!("Invalid batch: checksum check failed, expected: {checksum}, got: {crc}");
                        return Err(JournalRecovery(RecoveryError::CrcCheck));
                    }

                    // Reset all variables
                    is_in_batch = false;
                    batch_counter = 0;

                    // NOTE: Clippy says into_iter() is better
                    // but in this case probably not
                    #[allow(clippy::iter_with_drain)]
                    for item in items.drain(..) {
                        if let Some(whitelist) = whitelist {
                            if !whitelist.contains(&item.partition) {
                                continue;
                            }
                        }

                        let memtable = memtables.entry(item.partition).or_default();

                        let value = lsm_tree::InternalValue::from_components(
                            item.key,
                            item.value,
                            batch_seqno,
                            item.value_type,
                        );

                        memtable.insert(value);
                    }

                    last_valid_pos = journal_file_pos;
                }
                Marker::Item {
                    partition,
                    key,
                    value,
                    value_type,
                } => {
                    let item = Marker::Item {
                        partition: partition.clone(),
                        key: key.clone(),
                        value: value.clone(),
                        value_type,
                    };
                    let mut bytes = Vec::with_capacity(100);
                    item.serialize(&mut bytes)?;

                    hasher.update(&bytes);

                    if !is_in_batch {
                        log::debug!("Invalid batch: found end marker without start marker");

                        // Discard batch
                        Self::truncate_to(path, last_valid_pos)?;

                        break 'a;
                    }

                    if batch_counter == 0 {
                        log::error!("Invalid batch: Expected end marker (too many items in batch)");
                        return Err(JournalRecovery(RecoveryError::TooManyItems));
                    }

                    batch_counter -= 1;

                    items.push(BatchItem {
                        partition,
                        key,
                        value,
                        value_type,
                    });
                }
            }
        }

        if is_in_batch {
            log::debug!("Invalid batch: missing terminator, but last batch, so probably incomplete, discarding to keep atomicity");

            // Discard batch
            Self::truncate_to(path, last_valid_pos)?;
        }

        Ok(())
    }
}
