use super::marker::Marker;
use crate::serde::Deserializable;
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Seek},
    path::Path,
};

pub struct LogRecovery {
    reader: BufReader<File>,
    last_valid_pos: u64,
}

impl LogRecovery {
    pub fn new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        Ok(Self {
            reader: BufReader::new(file),
            last_valid_pos: 0,
        })
    }

    fn truncate_file(&mut self) -> crate::Result<()> {
        log::debug!("truncating log to {}", self.last_valid_pos);
        self.reader.get_mut().set_len(self.last_valid_pos)?;
        self.reader.get_mut().sync_all()?;
        Ok(())
    }
}

impl Iterator for LogRecovery {
    type Item = crate::Result<Marker>;

    fn next(&mut self) -> Option<Self::Item> {
        match Marker::deserialize(&mut self.reader) {
            Ok(abc) => {
                self.last_valid_pos = self
                    .reader
                    .stream_position()
                    .expect("should get stream position of journal reader");

                Some(Ok(abc))
            }
            Err(e) => match e {
                crate::serde::DeserializeError::Io(e) => match e.kind() {
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::Other => {
                        let stream_pos = self
                            .reader
                            .stream_position()
                            .expect("should get stream position of journal reader");

                        if stream_pos > self.last_valid_pos {
                            self.truncate_file().expect("should truncate journal");
                        }
                        None
                    }
                    _ => Some(Err(crate::Error::Io(e))),
                },
                crate::serde::DeserializeError::InvalidTag(_) => {
                    unimplemented!();
                }
            },
        }
    }
}
