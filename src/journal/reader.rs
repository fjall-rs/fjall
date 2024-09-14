// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::marker::Marker;
use lsm_tree::{coding::Decode, DecodeError};
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Seek},
    path::{Path, PathBuf},
};

macro_rules! fail_iter {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return Some(Err(e.into())),
        }
    };
}

/// Reads and emits through the entries in a journal file, but doesn't
/// check the validity of batches
///
/// Will truncate the file to the last valid position to prevent corrupt
/// bytes at the end of the file, which would jeopardize future writes into the file.
#[allow(clippy::module_name_repetitions)]
pub struct JournalReader {
    pub(crate) path: PathBuf,
    pub(crate) reader: BufReader<File>,
    pub(crate) last_valid_pos: u64,
}

impl JournalReader {
    pub fn new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        Ok(Self {
            path: path.as_ref().into(),
            reader: BufReader::new(file),
            last_valid_pos: 0,
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
    type Item = crate::Result<Marker>;

    fn next(&mut self) -> Option<Self::Item> {
        match Marker::decode_from(&mut self.reader) {
            Ok(item) => {
                self.last_valid_pos = fail_iter!(self.reader.stream_position());
                Some(Ok(item))
            }
            Err(e) => {
                if let DecodeError::Io(e) = e {
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
