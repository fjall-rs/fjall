pub mod iterator;
pub mod marker;

use self::marker::Marker;
use crate::{
    serde::{Serializable, SerializeError},
    Value,
};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
};

/// The commit logs durably stores items before they are added to the [`MemTable`]
/// to allow recovering the in-memory state after a failure
pub struct CommitLog {
    writer: BufWriter<File>,
}

impl CommitLog {
    /// Flushes the commit log file
    pub(crate) fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()?;
        self.writer.get_mut().sync_all()
    }

    /// Creates a new commit log file to append data to
    pub(crate) fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        let writer = BufWriter::with_capacity(128_000, file);

        Ok(Self { writer })
    }

    /// Appends a single entry wrapped in a batch to the commit log
    pub(crate) fn append(&mut self, entry: Value) -> Result<(), SerializeError> {
        self.append_batch(vec![entry])
    }

    /// Writes a batch start marker to the commit log
    fn write_start(&mut self, len: u32) -> Result<(), SerializeError> {
        Marker::Start(len).serialize(&mut self.writer)
    }

    /// Writes a batch end marker to the commit log
    fn write_end(&mut self, crc: u32) -> Result<(), SerializeError> {
        Marker::End(crc).serialize(&mut self.writer)
    }

    /// Writes a batch item to the commit log
    fn write_item(&mut self, item: Value) -> Result<Vec<u8>, SerializeError> {
        let mut bytes = Vec::new();
        Marker::Item(item).serialize(&mut bytes)?;
        self.writer.write_all(&bytes)?;
        Ok(bytes)
    }

    /// Appends a batch to the commit log
    pub(crate) fn append_batch(&mut self, items: Vec<Value>) -> Result<(), SerializeError> {
        let mut hasher = crc32fast::Hasher::new();
        //let mut bytes = 0;

        // NOTE: entries.len() is surely never > u32::MAX
        #[allow(clippy::cast_possible_truncation)]
        self.write_start(items.len() as u32)?;

        for entry in items {
            let serialized = self.write_item(entry)?;

            // bytes += serialized.len();
            hasher.update(&serialized);
        }

        self.write_end(hasher.finalize())?;

        Ok(())
    }
}
