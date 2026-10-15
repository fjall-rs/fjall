// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::entry::Entry;
use super::error::RecoveryMode;
use crate::file::MAGIC_BYTES;
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

/// Reads and emits through the entries in a journal file, but doesn't
/// check the validity of batches
///
/// Will truncate the file to the last valid position to prevent corrupt
/// bytes at the end of the file, which would jeopardize future writes into the file.
#[expect(clippy::module_name_repetitions)]
pub struct JournalReader {
    pub(crate) path: PathBuf,
    pub(crate) reader: BufReader<File>,
    pub(crate) last_valid_pos: u64,
    recovery_mode: RecoveryMode,
    /// Set once a mid-journal corruption was reported: the journal cannot be read
    /// further, and reporting it again at the next byte would rescan the file for
    /// nothing.
    is_poisoned: bool,
}

impl JournalReader {
    pub fn with_recovery_mode<P: AsRef<Path>>(
        path: P,
        recovery_mode: RecoveryMode,
    ) -> crate::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        Ok(Self {
            path: path.as_ref().into(),
            reader: BufReader::new(file),
            last_valid_pos: 0,
            recovery_mode,
            is_poisoned: false,
        })
    }

    /// Whether a fully written batch terminator follows the current position.
    ///
    /// The trailer magic is written after the batch checksum, so it is only in the
    /// file once the whole batch has been written, and the padding after the last
    /// write never contains it. Finding one means the unreadable region is not a
    /// half-written tail: a batch completed further along.
    fn completed_batch_follows(&mut self) -> crate::Result<bool> {
        let pos = self.reader.stream_position()?;

        let mut rest = Vec::new();
        self.reader.read_to_end(&mut rest)?;
        self.reader.seek(SeekFrom::Start(pos))?;

        // The journal is pre-allocated and what was never written is zeroes, so
        // matching the first byte of the trailer first keeps this one cheap pass
        // over the file instead of a comparison per byte.
        let Some(&first) = MAGIC_BYTES.first() else {
            return Ok(false);
        };

        Ok(rest
            .windows(MAGIC_BYTES.len())
            .any(|window| window.first() == Some(&first) && window == MAGIC_BYTES))
    }

    fn truncate_file(&mut self, pos: u64) -> crate::Result<()> {
        log::debug!("truncating journal to {pos}");
        self.reader.get_mut().set_len(pos)?;
        self.reader.get_mut().sync_all()?;
        Ok(())
    }

    fn maybe_truncate_file_to_last_valid_pos(&mut self) -> crate::Result<()> {
        let stream_pos = self.reader.stream_position()?;

        if stream_pos > self.last_valid_pos {
            self.truncate_file(self.last_valid_pos)?;
        }

        Ok(())
    }
}

impl Iterator for JournalReader {
    type Item = crate::Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_poisoned {
            return None;
        }

        match Entry::decode_from(&mut self.reader) {
            Ok(item) => {
                self.last_valid_pos = fail_iter!(self.reader.stream_position());
                Some(Ok(item))
            }
            Err(e) => {
                if self.recovery_mode == RecoveryMode::Strict
                    && fail_iter!(self.completed_batch_follows())
                {
                    self.is_poisoned = true;
                    return Some(Err(crate::Error::JournalRecovery(
                        super::error::RecoveryError::MidJournalCorruption,
                    )));
                }

                if let crate::Error::Io(e) = e {
                    match e.kind() {
                        std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::Other => {
                            fail_iter!(self.maybe_truncate_file_to_last_valid_pos());
                            None
                        }
                        _ => Some(Err(crate::Error::Io(e))),
                    }
                } else {
                    fail_iter!(self.maybe_truncate_file_to_last_valid_pos());
                    None
                }
            }
        }
    }
}
