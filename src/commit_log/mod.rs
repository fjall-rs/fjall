pub mod marker;
pub mod reader;

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

struct Writer;

impl Writer {
    /// Writes a batch start marker to the commit log
    fn write_start(writer: &mut BufWriter<File>, len: u32) -> Result<usize, SerializeError> {
        let mut bytes = Vec::new();
        Marker::Start(len).serialize(&mut bytes)?;

        writer.write_all(&bytes)?;
        Ok(bytes.len())
    }

    /// Writes a batch end marker to the commit log
    fn write_end(writer: &mut BufWriter<File>, crc: u32) -> Result<usize, SerializeError> {
        let mut bytes = Vec::new();
        Marker::End(crc).serialize(&mut bytes)?;

        writer.write_all(&bytes)?;
        Ok(bytes.len())
    }
}

impl CommitLog {
    /// Flushes the commit log file
    pub(crate) fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()?;
        self.writer.get_mut().sync_all()
    }

    /// Creates a new commit log file to append data to
    pub(crate) fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        std::fs::create_dir_all(path.as_ref().parent().expect("path should have parent"))?;

        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let writer = BufWriter::with_capacity(128_000, file);

        Ok(Self { writer })
    }

    /// Appends a single item wrapped in a batch to the commit log
    pub(crate) fn append(&mut self, item: Value) -> Result<usize, SerializeError> {
        self.append_batch(vec![item])
    }

    /// Appends a batch to the commit log
    pub(crate) fn append_batch(&mut self, items: Vec<Value>) -> Result<usize, SerializeError> {
        // NOTE: entries.len() is surely never > u32::MAX
        #[allow(clippy::cast_possible_truncation)]
        let item_count = items.len() as u32;

        let mut hasher = crc32fast::Hasher::new();
        let mut byte_count = 0;

        byte_count += Writer::write_start(&mut self.writer, item_count)?;

        for item in items {
            let marker = Marker::Item(item);

            let mut bytes = Vec::new();
            marker.serialize(&mut bytes)?;
            self.writer.write_all(&bytes)?;

            hasher.update(&bytes);
            byte_count += bytes.len();
        }

        let crc = hasher.finalize();
        byte_count += Writer::write_end(&mut self.writer, crc)?;

        Ok(byte_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use test_log::test;

    #[test]
    fn test_write() -> crate::Result<()> {
        let folder = tempdir()?.into_path();
        let log_path = folder.join("commit_log");
        let mut commit_log = CommitLog::new(&log_path)?;

        let items = vec![
            Value::new(vec![1, 2, 3], vec![4, 5, 6], false, 42),
            Value::new(vec![7, 8, 9], vec![], false, 43),
        ];

        commit_log.append_batch(items)?;
        commit_log.flush()?;

        let file_content = std::fs::read(log_path)?;

        assert_eq!(
            &[
                0, 0, 0, 0, 2, 1, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 3, 1, 2, 3, 0, 0, 0, 3, 4, 5, 6,
                1, 0, 0, 0, 0, 0, 0, 0, 43, 0, 0, 3, 7, 8, 9, 0, 0, 0, 0, 2, 131, 84, 123, 163
            ],
            &*file_content
        );

        Ok(())
    }
}
