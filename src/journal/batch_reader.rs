// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::reader::JournalReader;
use crate::{
    journal::entry::{serialize_item, Entry},
    keyspace::InternalKeyspaceId,
    JournalRecoveryError,
};
use lsm_tree::{SeqNo, UserKey, UserValue, ValueType};
use std::{fs::OpenOptions, hash::Hasher};

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
    pub(crate) items: Vec<ReadBatchItem>,
}

#[expect(clippy::module_name_repetitions)]
pub struct JournalBatchReader {
    reader: JournalReader,
    items: Vec<ReadBatchItem>,
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
}

impl Iterator for JournalBatchReader {
    type Item = crate::Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        use crate::Error::JournalRecovery;

        loop {
            let Some(item) = self.reader.next() else {
                fail_iter!(self.on_close());
                return None;
            };
            let item = fail_iter!(item);

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

                    let items = std::mem::take(&mut self.items);
                    return Some(Ok(Batch {
                        seqno: self.batch_seqno,
                        items,
                    }));
                }
                Entry::Item {
                    keyspace_id,
                    key,
                    value,
                    value_type,
                    compression,
                } => {
                    let item = Entry::Item {
                        keyspace_id,
                        key: key.clone(),
                        value: value.clone(),
                        value_type,
                        compression,
                    };
                    let mut bytes = Vec::with_capacity(100);
                    fail_iter!(item.encode_into(&mut bytes));

                    self.checksum_builder.update(&bytes);

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

                    self.items.push(ReadBatchItem {
                        keyspace_id,
                        key,
                        value,
                        value_type,
                    });
                }
                Entry::SingleItem {
                    keyspace_id,
                    key,
                    value,
                    value_type,
                    compression,
                    seqno,
                    checksum: expected_checksum,
                } => {
                    if self.is_in_batch {
                        log::error!("Invalid batch: Found SingleItem inside batch");

                        // Discard batch
                        fail_iter!(self.truncate_to(self.last_valid_pos));

                        return None;
                    }

                    // Verify checksum by re-serializing just the item data portion
                    let mut bytes = Vec::with_capacity(100);
                    fail_iter!(serialize_item(
                        &mut bytes,
                        keyspace_id,
                        &key,
                        &value,
                        value_type,
                        compression,
                    ));

                    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
                    hasher.update(&bytes);
                    let got_checksum = hasher.finish();

                    if got_checksum != expected_checksum {
                        log::error!("Invalid SingleItem: checksum mismatch, expected: {expected_checksum}, got: {got_checksum}");
                        return Some(Err(JournalRecovery(JournalRecoveryError::ChecksumMismatch)));
                    }

                    // Return as a batch of 1
                    self.last_valid_pos = journal_file_pos;
                    return Some(Ok(Batch {
                        seqno,
                        items: vec![ReadBatchItem {
                            keyspace_id,
                            key,
                            value,
                            value_type,
                        }],
                    }));
                }
            }
        }
    }
}
