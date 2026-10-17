// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::entry::Entry;
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Seek},
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
    pub(crate) max_value_bytes: Option<usize>,
}

impl JournalReader {
    pub fn new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        Self::new_with_value_limit(path, None)
    }

    pub fn new_with_value_limit<P: AsRef<Path>>(
        path: P,
        max_value_bytes: Option<usize>,
    ) -> crate::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        Ok(Self {
            path: path.as_ref().into(),
            reader: BufReader::new(file),
            last_valid_pos: 0,
            max_value_bytes,
        })
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
        match Entry::decode_from_with_value_limit(&mut self.reader, self.max_value_bytes) {
            Ok(item) => {
                self.last_valid_pos = fail_iter!(self.reader.stream_position());
                Some(Ok(item))
            }
            Err(e) => {
                if matches!(e, crate::Error::ValueTooLarge { .. }) {
                    return Some(Err(e));
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
