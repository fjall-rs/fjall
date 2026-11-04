// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::entry::{Entry, Tag};
use super::error::RecoveryMode;
use crate::file::MAGIC_BYTES;
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

/// Size of the chunks the journal tail is scanned in, so a pre-allocated
/// journal is never read into memory in one go.
pub(super) const SCAN_CHUNK_LEN: usize = 64 * 1_024;

/// In a batch trailer, the `Tag::End` byte sits this far before the trailer magic:
/// the tag itself plus the little-endian batch checksum.
pub(super) const TRAILER_TAG_OFFSET: usize = 1 + std::mem::size_of::<u64>();

/// Bytes of the previous chunk that are kept in front of the next one, so a trailer
/// that a chunk boundary splits apart is still seen.
pub(super) const SCAN_CARRY_LEN: usize = TRAILER_TAG_OFFSET + MAGIC_BYTES.len() - 1;

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

    /// Whether a fully written batch terminator follows the last entry that decoded.
    ///
    /// The trailer magic is written after the batch checksum, so it is only in the
    /// file once the whole batch has been written, and the padding after the last
    /// write never contains it. Finding a trailer means the unreadable region is not
    /// a half-written tail: a batch completed further along.
    fn completed_batch_follows(&mut self) -> crate::Result<bool> {
        let pos = self.reader.stream_position()?;

        // A failed decode can read past the corrupt entry — an oversized length is read to its
        // end — so the scan starts at the end of the last entry that decoded. Starting after the
        // failed read would start past a trailer the corrupt entry swallowed. The region the
        // failed decode consumed is scanned too, so its bytes (an arbitrary key or value) can now
        // produce a `MidJournalCorruption` the old read-then-scan did not: the alternative is
        // missing a completed batch and truncating it.
        self.reader.seek(SeekFrom::Start(self.last_valid_pos))?;
        let found = self.scan_for_batch_trailer();

        // Truncation still starts where the decode stopped, not at the scan start.
        self.reader.seek(SeekFrom::Start(pos))?;

        found
    }

    fn scan_for_batch_trailer(&mut self) -> crate::Result<bool> {
        scan_for_trailer(&mut self.reader)
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

/// Scans towards the end of the reader in bounded chunks for a batch trailer.
///
/// The magic is only [`MAGIC_BYTES`] long, so a corrupt key or value can contain
/// it; a trailer is only accepted when the byte [`TRAILER_TAG_OFFSET`] bytes
/// before the magic is `Tag::End`.
///
/// The checksum of that `End` record is deliberately **not** verified: it is xxh3
/// over the batch body (see [`super::batch_reader`]), and an undecodable body is
/// exactly why this scan runs. Re-hashing a corrupt body would reject the
/// completed-batch-after-corruption case that strict recovery exists to report, so
/// the framing is the necessary and sufficient condition.
///
/// It takes any reader so a test can put the trailer exactly on a chunk boundary.
pub(super) fn scan_for_trailer<R: Read>(reader: &mut R) -> crate::Result<bool> {
    let mut buf = vec![0u8; SCAN_CARRY_LEN + SCAN_CHUNK_LEN];

    // The journal is pre-allocated and what was never written is zeroes, so
    // matching the first byte of the trailer first keeps this one cheap pass
    // over the file instead of a comparison per byte.
    let Some(&first) = MAGIC_BYTES.first() else {
        return Ok(false);
    };

    let end_tag = u8::from(Tag::End);

    loop {
        let read = {
            let (_, chunk) = buf.split_at_mut(SCAN_CARRY_LEN);
            reader.read(chunk)?
        };

        if read == 0 {
            return Ok(false);
        }

        // `read` plus the carried bytes; the windows of the next chunk overlap with
        // the carried tail, so nothing is skipped at a chunk boundary
        let len = SCAN_CARRY_LEN + read;
        let found = buf
            .windows(MAGIC_BYTES.len())
            .enumerate()
            .take(len - MAGIC_BYTES.len() + 1)
            .any(|(pos, window)| {
                window.first() == Some(&first)
                    && window == MAGIC_BYTES
                    && pos
                        .checked_sub(TRAILER_TAG_OFFSET)
                        .and_then(|tag_pos| buf.get(tag_pos))
                        == Some(&end_tag)
            });

        if found {
            return Ok(true);
        }

        buf.copy_within(read..len, 0);
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
