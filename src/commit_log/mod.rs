pub mod marker;
pub mod reader;

use self::marker::Marker;
use crate::{
    serde::{Serializable, SerializeError},
    Value,
};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::{Mutex, MutexGuard},
};

/// The commit logs durably stores items before they are added to the [`MemTable`]
/// to allow recovering the in-memory state after a failure
pub struct CommitLog {
    writer: Mutex<BufWriter<File>>,
}

struct Writer;

impl Writer {
    /// Writes a batch start marker to the commit log
    fn write_start(
        writer: &mut MutexGuard<BufWriter<File>>,
        len: u32,
    ) -> Result<(), SerializeError> {
        Marker::Start(len).serialize(writer.get_mut())
    }

    /// Writes a batch end marker to the commit log
    fn write_end(writer: &mut MutexGuard<BufWriter<File>>, crc: u32) -> Result<(), SerializeError> {
        Marker::End(crc).serialize(writer.get_mut())
    }

    /// Writes a batch item to the commit log
    fn write_item(
        writer: &mut MutexGuard<BufWriter<File>>,
        item: Value,
    ) -> Result<Vec<u8>, SerializeError> {
        let mut bytes = Vec::new();
        Marker::Item(item).serialize(&mut bytes)?;
        writer.write_all(&bytes)?;
        Ok(bytes)
    }
}

impl CommitLog {
    /// Flushes the commit log file
    pub(crate) fn flush(&self) -> std::io::Result<()> {
        let mut writer = self.writer.lock().expect("should lock mutex");

        writer.flush()?;
        writer.get_mut().sync_all()
    }

    /// Creates a new commit log file to append data to
    pub(crate) fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        std::fs::create_dir_all(path.as_ref().parent().expect("path should have parent"))?;

        let file = File::create(path)?;
        let writer = Mutex::new(BufWriter::with_capacity(128_000, file));

        Ok(Self { writer })
    }

    /// Appends a single item wrapped in a batch to the commit log
    pub(crate) fn append(&self, item: Value) -> Result<(), SerializeError> {
        self.append_batch(vec![item])
    }

    /// Appends a batch to the commit log
    pub(crate) fn append_batch(&self, items: Vec<Value>) -> Result<(), SerializeError> {
        let mut hasher = crc32fast::Hasher::new();

        let mut writer = self.writer.lock().expect("should lock mutex");

        // NOTE: entries.len() is surely never > u32::MAX
        #[allow(clippy::cast_possible_truncation)]
        Writer::write_start(&mut writer, items.len() as u32)?;

        for item in items {
            let serialized = Writer::write_item(&mut writer, item)?;
            hasher.update(&serialized);
        }

        Writer::write_end(&mut writer, hasher.finalize())?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use test_log::test;

    #[test]
    fn test_write() -> std::io::Result<()> {
        let folder = tempdir()?.into_path();
        let log_path = folder.join("commit_log");
        let commit_log = CommitLog::new(&log_path)?;

        let items = vec![
            Value::new(vec![1, 2, 3], vec![4, 5, 6], false, 42),
            Value::new(vec![7, 8, 9], vec![], false, 43),
        ];

        commit_log.append_batch(items).unwrap();
        commit_log.flush()?;

        let file_content = std::fs::read(log_path)?;

        assert_eq!(
            &[
                0, 0, 0, 0, 2, 2, 131, 84, 123, 163, 1, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 3, 1, 2, 3,
                0, 0, 0, 3, 4, 5, 6, 1, 0, 0, 0, 0, 0, 0, 0, 43, 0, 0, 3, 7, 8, 9, 0, 0, 0, 0
            ],
            &*file_content
        );

        Ok(())
    }
}
