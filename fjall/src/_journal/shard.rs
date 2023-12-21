use super::marker::Marker;
use crate::{
    _journal::recovery::JournalShardReader, batch::BatchItem, memtable::MemTable,
    serde::Serializable, value::SeqNo, SerializeError,
};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

// TODO: strategy, skip invalid batches (CRC or invalid item length) or throw error
/// Errors that can occur during journal recovery
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryError {
    /// Batch had less items than expected, so it's incomplete
    InsufficientLength,

    /// Batch was not terminated, so it's possibly incomplete
    MissingTerminator,

    /// Too many items in batch
    TooManyItems,

    /// The CRC value does not match the expected value
    CrcCheck,
}

pub struct JournalShard {
    pub(crate) path: PathBuf,
    file: BufWriter<File>,
}

/// Writes a batch start marker to the journal
fn write_start(
    writer: &mut BufWriter<File>,
    item_count: u32,
    seqno: SeqNo,
) -> Result<usize, SerializeError> {
    let mut bytes = Vec::new();
    Marker::Start { item_count, seqno }.serialize(&mut bytes)?;

    writer.write_all(&bytes)?;
    Ok(bytes.len())
}

/// Writes a batch end marker to the journal
fn write_end(writer: &mut BufWriter<File>, crc: u32) -> Result<usize, SerializeError> {
    let mut bytes = Vec::new();
    Marker::End(crc).serialize(&mut bytes)?;

    writer.write_all(&bytes)?;
    Ok(bytes.len())
}

impl JournalShard {
    pub fn rotate<P: AsRef<Path>>(&mut self, path: P) -> crate::Result<()> {
        let file = File::create(&path)?;
        self.file = BufWriter::new(file);
        self.path = path.as_ref().to_path_buf();
        Ok(())
    }

    pub fn create_new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();
        let file = File::create(path)?;

        Ok(Self {
            file: BufWriter::new(file),
            path: path.to_path_buf(),
        })
    }

    /// Recovers a journal shard and writes the items into the given memtable
    ///
    /// Will truncate the file to the position of the last valid batch
    pub fn recover_and_repair<P: AsRef<Path>>(
        path: P,
        memtables: &mut HashMap<Arc<str>, MemTable>,
    ) -> crate::Result<()> {
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
                        log::warn!("Invalid batch: found batch start inside batch");

                        // Discard batch
                        log::warn!("Truncating shard to {last_valid_pos}");
                        let file = OpenOptions::new().write(true).open(path)?;
                        file.set_len(last_valid_pos)?;
                        file.sync_all()?;

                        break 'a;
                    }

                    is_in_batch = true;
                    batch_counter = item_count;
                    batch_seqno = seqno;
                }
                Marker::End(checksum) => {
                    if batch_counter > 0 {
                        log::error!("Invalid batch: insufficient length");
                        return Err(crate::Error::JournalRecovery(
                            RecoveryError::InsufficientLength,
                        ));
                    }

                    if !is_in_batch {
                        log::error!("Invalid batch: found end marker without start marker");

                        // Discard batch
                        log::warn!("Truncating shard to {last_valid_pos}");
                        let file = OpenOptions::new().write(true).open(path)?;
                        file.set_len(last_valid_pos)?;
                        file.sync_all()?;

                        break 'a;
                    }

                    let crc = hasher.finalize();
                    if crc != checksum {
                        log::error!("Invalid batch: checksum check failed, expected: {checksum}, got: {crc}");
                        return Err(crate::Error::JournalRecovery(RecoveryError::CrcCheck));
                    }

                    // Reset all variables
                    hasher = crc32fast::Hasher::new();
                    is_in_batch = false;
                    batch_counter = 0;

                    // NOTE: Clippy says into_iter() is better
                    // but in this case probably not
                    #[allow(clippy::iter_with_drain)]
                    for item in items.drain(..) {
                        memtables
                            .entry(item.partition)
                            .or_default()
                            .insert(crate::Value {
                                key: item.key,
                                value: item.value,
                                seqno: batch_seqno,
                                value_type: item.value_type,
                            });
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
                        log::warn!("Invalid batch: found end marker without start marker");

                        // Discard batch
                        log::warn!("Truncating shard to {last_valid_pos}");
                        let file = OpenOptions::new().write(true).open(path)?;
                        file.set_len(last_valid_pos)?;
                        file.sync_all()?;

                        break 'a;
                    }

                    if batch_counter == 0 {
                        log::error!("Invalid batch: Expected end marker (too many items in batch)");
                        return Err(crate::Error::JournalRecovery(RecoveryError::TooManyItems));
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
            log::warn!("Invalid batch: missing terminator, but last batch, so probably incomplete, discarding to keep atomicity");

            // Discard batch
            log::warn!("Truncating shard to {last_valid_pos}");
            let file = OpenOptions::new().write(true).open(path)?;
            file.set_len(last_valid_pos)?;
            file.sync_all()?;
        }

        Ok(())
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();

        if !path.exists() {
            return Ok(Self {
                file: BufWriter::new(
                    std::fs::OpenOptions::new()
                        .create_new(true)
                        .append(true)
                        .open(path)?,
                ),
                path: path.to_path_buf(),
            });
        }

        Ok(Self {
            file: BufWriter::new(std::fs::OpenOptions::new().append(true).open(path)?),
            path: path.to_path_buf(),
        })
    }

    /// Flushes the journal file
    pub(crate) fn flush(&mut self) -> crate::Result<()> {
        self.file.flush()?;
        self.file.get_mut().sync_all()?;
        Ok(())
    }

    /// Appends a single item wrapped in a batch to the journal
    pub(crate) fn write(&mut self, item: &BatchItem, seqno: SeqNo) -> crate::Result<usize> {
        self.write_batch(&[item], seqno)
    }

    pub fn write_batch(&mut self, items: &[&BatchItem], seqno: SeqNo) -> crate::Result<usize> {
        // NOTE: entries.len() is surely never > u32::MAX
        #[allow(clippy::cast_possible_truncation)]
        let item_count = items.len() as u32;

        let mut hasher = crc32fast::Hasher::new();
        let mut byte_count = 0;

        byte_count += write_start(&mut self.file, item_count, seqno)?;

        for item in items {
            let item = Marker::Item {
                partition: item.partition.clone(),
                key: item.key.clone(),
                value: item.value.clone(),
                value_type: item.value_type,
            };
            let mut bytes = Vec::new();
            item.serialize(&mut bytes)?;

            self.file.write_all(&bytes)?;

            hasher.update(&bytes);
            byte_count += bytes.len();
        }

        let crc = hasher.finalize();
        byte_count += write_end(&mut self.file, crc)?;

        Ok(byte_count)
    }
}
