use super::marker::Marker;
use lsm_tree::{serde::Deserializable, DeserializeError};
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Seek},
    path::Path,
};

/// Reads and emits through the entries in a journal shard file, but doesn't
/// check the validity of batches
///
/// Will truncate the file to the last valid position to prevent corrupt
/// bytes at the end of the file, which would jeopardize future writes into the file
#[allow(clippy::module_name_repetitions)]
pub struct JournalShardReader {
    reader: BufReader<File>,
    last_valid_pos: u64,
}

impl JournalShardReader {
    pub fn new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        Ok(Self {
            reader: BufReader::new(file),
            last_valid_pos: 0,
        })
    }

    fn truncate_file(&mut self, pos: u64) -> crate::Result<()> {
        log::debug!("truncating log to {pos}");
        self.reader.get_mut().set_len(pos)?;
        self.reader.get_mut().sync_all()?;
        Ok(())
    }

    fn truncate_file_to_last_valid_pos(&mut self) -> crate::Result<()> {
        self.truncate_file(self.last_valid_pos)
    }
}

impl Iterator for JournalShardReader {
    type Item = crate::Result<(u64, Marker)>;

    fn next(&mut self) -> Option<Self::Item> {
        match Marker::deserialize(&mut self.reader) {
            Ok(abc) => {
                self.last_valid_pos = self
                    .reader
                    .stream_position()
                    .expect("should get stream position of journal reader");

                Some(Ok((self.last_valid_pos, abc)))
            }
            Err(e) => match e {
                DeserializeError::Io(e) => match e.kind() {
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::Other => {
                        let stream_pos = self
                            .reader
                            .stream_position()
                            .expect("should get stream position of journal reader");

                        if stream_pos > self.last_valid_pos {
                            self.truncate_file_to_last_valid_pos()
                                .expect("should truncate journal");
                        }
                        None
                    }
                    _ => Some(Err(crate::Error::Io(e))),
                },
                DeserializeError::InvalidTag(_) | DeserializeError::InvalidTrailer => {
                    let stream_pos = self
                        .reader
                        .stream_position()
                        .expect("should get stream position of journal reader");

                    if stream_pos > self.last_valid_pos {
                        self.truncate_file_to_last_valid_pos()
                            .expect("should truncate journal");
                    }
                    None
                }
            },
        }
    }
}
