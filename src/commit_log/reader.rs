use super::marker::Marker;
use crate::serde::{Deserializable, DeserializeError};
use std::{fs::File, io::BufReader, path::Path};

/// The log file iterator emits every entry in the commit log file,
/// without checking for any kind of data integrity
pub struct Reader {
    file_reader: BufReader<File>,
}

impl Reader {
    pub fn new<P: AsRef<Path>>(file: P) -> std::io::Result<Self> {
        let file_handle = File::open(file)?;
        let file_reader = BufReader::with_capacity(128_000, file_handle);
        Ok(Self { file_reader })
    }
}

#[derive(Debug)]
pub enum Error {
    Deserialize(DeserializeError),
    Io(std::io::Error),
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<DeserializeError> for Error {
    fn from(value: DeserializeError) -> Self {
        Self::Deserialize(value)
    }
}

impl Iterator for Reader {
    type Item = Result<Marker, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let deserialized = Marker::deserialize(&mut self.file_reader);

        match deserialized {
            Ok(item) => Some(Ok(item)),
            Err(error) => match error {
                DeserializeError::Io(io_error) => match io_error.kind() {
                    std::io::ErrorKind::UnexpectedEof => None,
                    kind => Some(Err(DeserializeError::Io(kind.into()).into())),
                },
                _ => Some(Err(error.into())),
            },
        }
    }
}
