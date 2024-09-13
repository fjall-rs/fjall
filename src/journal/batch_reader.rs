// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::reader::JournalReader;
use crate::{batch::item::Item as BatchItem, journal::marker::Marker, RecoveryError};
use lsm_tree::{coding::Encode, CompressionType, SeqNo};
use std::{fs::OpenOptions, hash::Hasher};

macro_rules! fail_iter {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return Some(Err(e.into())),
        }
    };
}

#[derive(Debug)]
pub struct Batch {
    pub(crate) seqno: SeqNo,
    pub(crate) items: Vec<BatchItem>,
}

#[allow(clippy::module_name_repetitions)]
pub struct JournalBatchReader {
    reader: JournalReader,
    items: Vec<BatchItem>,
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
    fn truncate_to(&mut self, last_valid_pos: u64) -> crate::Result<()> {
        log::trace!("Truncating journal to {last_valid_pos}");

        // TODO: on windows, reading file probably needs to be closed first...?

        let file = OpenOptions::new().write(true).open(&self.reader.path)?;
        file.set_len(last_valid_pos)?;
        file.sync_all()?;

        Ok(())
    }

    fn on_close(&mut self) -> crate::Result<()> {
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
                Marker::Start {
                    item_count,
                    seqno,
                    compression,
                } => {
                    // TODO: journal compression maybe in the future
                    assert!(
                        compression == CompressionType::None,
                        "journal compression not supported"
                    );

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
                Marker::End(expected_checksum) => {
                    if self.batch_counter > 0 {
                        log::error!("Invalid batch: insufficient length");
                        return Some(Err(JournalRecovery(RecoveryError::InsufficientLength)));
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
                        return Some(Err(JournalRecovery(RecoveryError::ChecksumMismatch)));
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
                        return Some(Err(JournalRecovery(RecoveryError::TooManyItems)));
                    }

                    self.batch_counter -= 1;

                    self.items.push(BatchItem {
                        partition,
                        key,
                        value,
                        value_type,
                    });
                }
            }
        }
    }
}
