use crate::{journal::recovery::LogRecovery, serde::Serializable, SerializeError, Value};

use super::{marker::Marker, mem_table::MemTable};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

pub struct JournalShard {
    pub(crate) memtable: MemTable,
    pub(crate) path: PathBuf,
    file: BufWriter<File>,
}

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

impl JournalShard {
    pub fn rotate<P: AsRef<Path>>(&mut self, path: P) -> crate::Result<()> {
        let file = File::create(path)?;
        self.memtable = MemTable::default();
        self.file = BufWriter::new(file);
        Ok(())
    }

    pub fn create_new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();
        let file = File::create(path)?;

        Ok(Self {
            memtable: MemTable::default(),
            file: BufWriter::new(file),
            path: path.to_path_buf(),
        })
    }

    pub fn recover<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();
        let mut memtable = MemTable::default();

        if !path.exists() {
            return Ok(Self {
                file: BufWriter::new(
                    std::fs::OpenOptions::new()
                        .create_new(true)
                        .append(true)
                        .open(path)?,
                ),
                memtable,
                path: path.to_path_buf(),
            });
        }

        let recoverer = LogRecovery::new(path)?;

        for item in recoverer {
            let item = item?;

            // TODO: proper recovery

            if let Marker::Item(item) = item {
                memtable.insert(item);
            }
        }

        log::trace!("Recovered journal shard {} items", memtable.len());

        Ok(Self {
            file: BufWriter::new(std::fs::OpenOptions::new().append(true).open(path)?),
            memtable,
            path: path.to_path_buf(),
        })
    }

    /// Flushes the commit log file
    pub(crate) fn flush(&mut self) -> crate::Result<()> {
        self.file.flush()?;
        self.file.get_mut().sync_all()?;
        Ok(())
    }

    /// Appends a single item wrapped in a batch to the commit log
    pub(crate) fn write(&mut self, item: Value) -> crate::Result<usize> {
        self.write_batch(vec![item])
    }

    pub fn write_batch(&mut self, items: Vec<Value>) -> crate::Result<usize> {
        // NOTE: entries.len() is surely never > u32::MAX
        #[allow(clippy::cast_possible_truncation)]
        let item_count = items.len() as u32;

        let mut hasher = crc32fast::Hasher::new();
        let mut byte_count = 0;

        byte_count += write_start(&mut self.file, item_count)?;

        for item in &items {
            let marker = Marker::Item(item.clone());

            let mut bytes = Vec::new();
            marker.serialize(&mut bytes)?;
            self.file.write_all(&bytes)?;

            hasher.update(&bytes);
            byte_count += bytes.len();
        }

        let crc = hasher.finalize();
        byte_count += write_end(&mut self.file, crc)?;

        for item in items {
            self.memtable.insert(item);
        }

        Ok(byte_count)
    }
}
