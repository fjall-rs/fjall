// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::reader::JournalReader;
use crate::{
    file::MAGIC_BYTES, journal::entry::Entry, keyspace::InternalKeyspaceId, JournalRecoveryError,
};
use byteorder::{LittleEndian, ReadBytesExt};
use lsm_tree::{SeqNo, UserKey, UserValue, ValueType};
use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    hash::Hasher,
    io::{BufReader, Read, Seek, SeekFrom},
};

const MAX_IN_MEMORY_BATCH_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub struct ReadBatchItem {
    pub keyspace_id: InternalKeyspaceId,
    pub key: UserKey,
    pub value: UserValue,
    pub value_type: ValueType,
}

#[derive(Debug)]
pub struct Batch {
    pub(crate) seqno: SeqNo,
    pub(crate) items: BatchItems,
    pub(crate) cleared_keyspaces: Vec<InternalKeyspaceId>,
}

#[derive(Debug)]
pub enum BatchItems {
    InMemory(std::vec::IntoIter<ReadBatchItem>),
    Journal {
        reader: BufReader<File>,
        remaining: u32,
        max_value_bytes: Option<usize>,
    },
}

impl BatchItems {
    pub fn is_streaming(&self) -> bool {
        matches!(self, Self::Journal { .. })
    }
}

impl Iterator for BatchItems {
    type Item = crate::Result<ReadBatchItem>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::InMemory(items) => items.next().map(Ok),
            Self::Journal {
                reader,
                remaining,
                max_value_bytes,
            } => loop {
                if *remaining == 0 {
                    return None;
                }
                *remaining -= 1;
                match Entry::decode_from_with_value_limit(reader, *max_value_bytes) {
                    Ok(Entry::Item {
                        keyspace_id,
                        key,
                        value,
                        value_type,
                        ..
                    }) => {
                        return Some(Ok(ReadBatchItem {
                            keyspace_id,
                            key,
                            value,
                            value_type,
                        }))
                    }
                    Ok(Entry::Clear { .. }) => continue,
                    Ok(_) => return Some(Err(crate::Error::Unrecoverable)),
                    Err(error) => return Some(Err(error)),
                }
            },
        }
    }
}

#[expect(clippy::module_name_repetitions)]
pub struct JournalBatchReader {
    reader: JournalReader,
    items: Vec<ReadBatchItem>,
    item_bytes: usize,
    journal_batch_start: u64,
    journal_batch_count: u32,
    journal_items: bool,
    cleared_keyspaces: Vec<InternalKeyspaceId>,
    cleared_keyspace_ids: HashSet<InternalKeyspaceId>,
    is_in_batch: bool,
    batch_counter: u32,
    batch_seqno: SeqNo,
    last_valid_pos: u64,
    checksum_builder: xxhash_rust::xxh3::Xxh3,
}

impl JournalBatchReader {
    pub fn new(reader: JournalReader) -> Self {
        Self {
            reader,
            items: Vec::with_capacity(10),
            item_bytes: 0,
            journal_batch_start: 0,
            journal_batch_count: 0,
            journal_items: false,
            cleared_keyspaces: Vec::new(),
            cleared_keyspace_ids: HashSet::new(),
            checksum_builder: xxhash_rust::xxh3::Xxh3::new(),
            is_in_batch: false,
            batch_seqno: 0,
            last_valid_pos: 0,
            batch_counter: 0,
        }
    }

    // TODO: reallocate space
    fn truncate_to(&self, last_valid_pos: u64) -> crate::Result<()> {
        log::trace!("Truncating journal to {last_valid_pos}");

        // TODO: on windows, reading file probably needs to be closed first...?

        let file = OpenOptions::new().write(true).open(&self.reader.path)?;
        file.set_len(last_valid_pos)?;
        file.sync_all()?;

        Ok(())
    }

    fn on_close(&self) -> crate::Result<()> {
        if self.is_in_batch {
            log::debug!("Invalid batch: missing terminator, but last batch, so probably incomplete, discarding to keep atomicity");

            // Discard batch
            self.truncate_to(self.last_valid_pos)?;
        }

        Ok(())
    }

    fn hash_entry(&mut self, start: u64, end: u64) -> crate::Result<()> {
        self.reader.reader.seek(SeekFrom::Start(start))?;
        let mut remaining = end - start;
        let mut buffer = [0_u8; 8192];
        while remaining > 0 {
            let len = usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
            let chunk = buffer.get_mut(..len).ok_or(crate::Error::Unrecoverable)?;
            self.reader.reader.read_exact(chunk)?;
            self.checksum_builder.update(chunk);
            remaining -= len as u64;
        }
        self.reader.reader.seek(SeekFrom::Start(end))?;
        Ok(())
    }

    fn push_item(&mut self, item: ReadBatchItem) {
        if self.journal_items {
            return;
        }

        // Vec capacity can be almost twice its length. Charge for that spare
        // capacity as well as the key and value allocations, so tiny items
        // cannot grow the pending batch without crossing the streaming threshold.
        self.item_bytes = self
            .item_bytes
            .saturating_add(item.key.len())
            .saturating_add(item.value.len())
            .saturating_add(2 * std::mem::size_of::<ReadBatchItem>());
        if self.item_bytes > MAX_IN_MEMORY_BATCH_BYTES {
            self.items = Vec::new();
            self.journal_items = true;
            self.item_bytes = 0;
        } else {
            self.items.push(item);
        }
    }

    // A value limit is checked before its payload is allocated. If the item
    // belongs to a torn final batch, however, the old recovery behavior is to
    // discard that batch. Verify its framing and checksum with fixed-size
    // buffers before deciding whether the oversized value was committed.
    fn oversized_batch_is_committed(&self) -> crate::Result<bool> {
        let file = File::open(&self.reader.path)?;
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(self.journal_batch_start))?;
        let mut checksum = xxhash_rust::xxh3::Xxh3::new();
        let mut buffer = [0_u8; 8192];

        for _ in 0..self.journal_batch_count {
            let start = reader.stream_position()?;
            let Ok(tag) = reader.read_u8() else {
                return Ok(false);
            };
            let payload_len = match tag {
                2 => {
                    let mut header = [0_u8; 20];
                    if reader.read_exact(&mut header).is_err() {
                        return Ok(false);
                    }
                    let key_len = u16::from_le_bytes([header[10], header[11]]) as u64;
                    let value_len =
                        u32::from_le_bytes([header[16], header[17], header[18], header[19]]) as u64;
                    key_len + value_len
                }
                4 => 8,
                _ => return Ok(false),
            };
            let Some(end) = reader.stream_position()?.checked_add(payload_len) else {
                return Ok(false);
            };
            if end > file_len {
                return Ok(false);
            }
            reader.seek(SeekFrom::Start(start))?;
            let mut remaining = end - start;
            while remaining > 0 {
                let len = usize::try_from(remaining.min(buffer.len() as u64))
                    .map_err(|_| crate::Error::Unrecoverable)?;
                let chunk = buffer.get_mut(..len).ok_or(crate::Error::Unrecoverable)?;
                reader.read_exact(chunk)?;
                checksum.update(chunk);
                remaining -= u64::try_from(len).map_err(|_| crate::Error::Unrecoverable)?;
            }
        }

        if reader.read_u8().ok() != Some(3) {
            return Ok(false);
        }
        let Ok(expected_checksum) = reader.read_u64::<LittleEndian>() else {
            return Ok(false);
        };
        let mut magic = [0_u8; 4];
        if reader.read_exact(&mut magic).is_err() || magic != MAGIC_BYTES {
            return Ok(false);
        }
        if checksum.finish() != expected_checksum {
            return Err(crate::Error::JournalRecovery(
                JournalRecoveryError::ChecksumMismatch,
            ));
        }
        Ok(true)
    }
}

impl Iterator for JournalBatchReader {
    type Item = crate::Result<Batch>;

    #[expect(clippy::too_many_lines)]
    fn next(&mut self) -> Option<Self::Item> {
        use crate::Error::JournalRecovery;

        loop {
            let entry_start = self.reader.last_valid_pos;
            let Some(item) = self.reader.next() else {
                fail_iter!(self.on_close());
                return None;
            };
            let item = match item {
                Ok(item) => item,
                Err(error @ crate::Error::ValueTooLarge { .. }) if self.is_in_batch => {
                    match self.oversized_batch_is_committed() {
                        Ok(true) => return Some(Err(error)),
                        Ok(false) => {
                            fail_iter!(self.truncate_to(self.last_valid_pos));
                            return None;
                        }
                        Err(error) => return Some(Err(error)),
                    }
                }
                Err(error) => return Some(Err(error)),
            };

            let journal_file_pos = self.reader.last_valid_pos;

            match item {
                Entry::Start { item_count, seqno } => {
                    if self.is_in_batch {
                        log::debug!("Invalid batch: found batch start inside batch");

                        // Discard batch
                        fail_iter!(self.truncate_to(self.last_valid_pos));

                        return None;
                    }

                    self.is_in_batch = true;
                    self.batch_counter = item_count;
                    self.batch_seqno = seqno;
                    self.journal_batch_start = journal_file_pos;
                    self.journal_batch_count = item_count;
                }
                Entry::End(expected_checksum) => {
                    if self.batch_counter > 0 {
                        log::error!("Invalid batch: insufficient length");
                        return Some(Err(JournalRecovery(
                            JournalRecoveryError::InsufficientLength,
                        )));
                    }

                    if !self.is_in_batch {
                        log::error!("Invalid batch: found end marker without start marker");

                        // Discard batch
                        fail_iter!(self.truncate_to(self.last_valid_pos));

                        return None;
                    }

                    let got_checksum = self.checksum_builder.finish();
                    self.checksum_builder = xxhash_rust::xxh3::Xxh3::new();

                    if got_checksum != expected_checksum {
                        log::error!("Invalid batch: checksum check failed, expected: {expected_checksum}, got: {got_checksum}");
                        return Some(Err(JournalRecovery(JournalRecoveryError::ChecksumMismatch)));
                    }

                    // Reset all variables
                    self.is_in_batch = false;
                    self.batch_counter = 0;

                    self.last_valid_pos = journal_file_pos;

                    let items = if std::mem::take(&mut self.journal_items) {
                        let file = fail_iter!(File::open(&self.reader.path));
                        let mut reader = BufReader::new(file);
                        fail_iter!(reader.seek(SeekFrom::Start(self.journal_batch_start)));
                        BatchItems::Journal {
                            reader,
                            remaining: self.journal_batch_count,
                            max_value_bytes: self.reader.max_value_bytes,
                        }
                    } else {
                        BatchItems::InMemory(std::mem::take(&mut self.items).into_iter())
                    };
                    self.item_bytes = 0;
                    let cleared_keyspaces = std::mem::take(&mut self.cleared_keyspaces);
                    self.cleared_keyspace_ids = HashSet::new();
                    return Some(Ok(Batch {
                        seqno: self.batch_seqno,
                        items,
                        cleared_keyspaces,
                    }));
                }
                Entry::Item {
                    keyspace_id,
                    key,
                    value,
                    value_type,
                    compression: _,
                } => {
                    fail_iter!(self.hash_entry(entry_start, journal_file_pos));

                    if !self.is_in_batch {
                        log::debug!("Invalid batch: found end marker without start marker");

                        // Discard batch
                        fail_iter!(self.truncate_to(self.last_valid_pos));

                        return None;
                    }

                    if self.batch_counter == 0 {
                        log::error!("Invalid batch: Expected end marker (too many items in batch)");
                        return Some(Err(JournalRecovery(JournalRecoveryError::TooManyItems)));
                    }

                    self.batch_counter -= 1;

                    self.push_item(ReadBatchItem {
                        keyspace_id,
                        key,
                        value,
                        value_type,
                    });
                }
                Entry::Clear { keyspace_id } => {
                    fail_iter!(self.hash_entry(entry_start, journal_file_pos));

                    if !self.is_in_batch {
                        log::debug!("Invalid batch: found clear marker without start marker");

                        // Discard batch
                        fail_iter!(self.truncate_to(self.last_valid_pos));

                        return None;
                    }

                    if self.batch_counter == 0 {
                        log::error!("Invalid batch: Expected end marker (too many items in batch)");
                        return Some(Err(JournalRecovery(JournalRecoveryError::TooManyItems)));
                    }

                    self.batch_counter -= 1;

                    if self.cleared_keyspace_ids.insert(keyspace_id) {
                        self.cleared_keyspaces.push(keyspace_id);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod value_limit_tests {
    use super::*;
    use byteorder::WriteBytesExt;
    use std::io::Write;

    #[test]
    fn oversized_header_in_preallocated_crash_tail_is_discarded() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let path = folder.path().join("0.jnl");
        let mut file = File::create(&path)?;
        file.set_len(64 * 1024 * 1024)?;
        Entry::Start {
            item_count: 1,
            seqno: 1,
        }
        .encode_into(&mut file)?;
        file.write_u8(2)?; // Item marker
        file.write_u8(0)?; // Value type
        file.write_u8(0)?; // No compression
        file.write_u64::<LittleEndian>(1)?;
        file.write_u16::<LittleEndian>(1)?;
        file.write_u32::<LittleEndian>(2048)?;
        file.write_u32::<LittleEndian>(2048)?;
        file.flush()?;

        let reader = JournalReader::new_with_value_limit(&path, Some(1024))?;
        assert!(JournalBatchReader::new(reader).next().is_none());
        assert_eq!(path.metadata()?.len(), 0);
        Ok(())
    }
}
