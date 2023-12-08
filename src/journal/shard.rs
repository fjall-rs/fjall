use super::marker::Marker;
use crate::{serde::Serializable, value::SeqNo, SerializeError, Value};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

pub struct JournalShard {
    pub(crate) path: PathBuf,
    file: BufWriter<File>,
}

/// Writes a batch start marker to the journal
fn write_start(
    writer: &mut BufWriter<File>,
    item_count: u32,
    seqno: SeqNo,
) -> Result<usize, SerializeError> {
    let mut bytes = Vec::new();
    Marker::Start { item_count, seqno }.serialize(&mut bytes)?;

    writer.write_all(&bytes)?;
    Ok(bytes.len())
}

/// Writes a batch end marker to the journal
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

    /// Flushes the journal file
    pub(crate) fn flush(&mut self) -> crate::Result<()> {
        self.file.flush()?;
        self.file.get_mut().sync_all()?;
        Ok(())
    }

    /// Appends a single item wrapped in a batch to the journal
    pub(crate) fn write(&mut self, item: &Value) -> crate::Result<usize> {
        self.write_batch(&[item])
    }

    pub fn write_batch(&mut self, items: &[&Value]) -> crate::Result<usize> {
        // NOTE: entries.len() is surely never > u32::MAX
        #[allow(clippy::cast_possible_truncation)]
        let item_count = items.len() as u32;

        let mut hasher = crc32fast::Hasher::new();
        let mut byte_count = 0;

        byte_count += write_start(&mut self.file, item_count, items[0].seqno)?;

        for item in items {
            let item = Marker::Item {
                value_type: item.value_type,
                key: item.key.clone(),
                value: item.value.clone(),
            };
            let mut bytes = Vec::new();
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
