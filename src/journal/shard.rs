use byteorder::WriteBytesExt;

use super::marker::{Marker, Tag};
use crate::{serde::Serializable, SerializeError, Value};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

pub struct JournalShard {
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
        let file = File::create(&path)?;
        self.file = BufWriter::new(file);
        self.path = path.as_ref().to_path_buf();
        Ok(())
    }

    pub fn create_new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();
        let file = File::create(path)?;

        Ok(Self {
            file: BufWriter::new(file),
            path: path.to_path_buf(),
        })
    }

    pub fn recover<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();

        if !path.exists() {
            return Ok(Self {
                file: BufWriter::new(
                    std::fs::OpenOptions::new()
                        .create_new(true)
                        .append(true)
                        .open(path)?,
                ),
                path: path.to_path_buf(),
            });
        }

        Ok(Self {
            file: BufWriter::new(std::fs::OpenOptions::new().append(true).open(path)?),
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
    pub(crate) fn write(&mut self, item: &Value) -> crate::Result<usize> {
        self.write_batch(&vec![item])
    }

    pub fn write_batch(&mut self, items: &Vec<&Value>) -> crate::Result<usize> {
        // NOTE: entries.len() is surely never > u32::MAX
        #[allow(clippy::cast_possible_truncation)]
        let item_count = items.len() as u32;

        let mut hasher = crc32fast::Hasher::new();
        let mut byte_count = 0;

        byte_count += write_start(&mut self.file, item_count)?;

        for item in items {
            // NOTE: Not using Marker::Item(item).serialize to avoid an item clone
            let mut bytes = Vec::new();
            bytes.write_u8(Tag::Item.into())?;
            item.serialize(&mut bytes)?;

            self.file.write_all(&bytes)?;

            hasher.update(&bytes);
            byte_count += bytes.len();
        }

        let crc = hasher.finalize();
        byte_count += write_end(&mut self.file, crc)?;

        Ok(byte_count)
    }
}
