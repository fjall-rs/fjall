use super::marker::Marker;
use crate::batch::item::Item as BatchItem;
use lsm_tree::{serde::Serializable, SeqNo, SerializeError};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
};

pub const PRE_ALLOCATED_BYTES: u64 = 8 * 1_024 * 1_024;

pub struct Writer {
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

impl Writer {
    pub fn rotate<P: AsRef<Path>>(&mut self, path: P) -> crate::Result<()> {
        let file = File::create(&path)?;
        file.set_len(PRE_ALLOCATED_BYTES)?;

        self.file = BufWriter::new(file);

        Ok(())
    }

    pub fn create_new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();
        let file = File::create(path)?;
        file.set_len(PRE_ALLOCATED_BYTES)?;

        Ok(Self {
            file: BufWriter::new(file),
        })
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();

        if !path.try_exists()? {
            let file = OpenOptions::new().create_new(true).write(true).open(path)?;
            file.set_len(PRE_ALLOCATED_BYTES)?;

            return Ok(Self {
                file: BufWriter::new(file),
            });
        }

        let file = OpenOptions::new().append(true).open(path)?;

        Ok(Self {
            file: BufWriter::new(file),
        })
    }

    /// Flushes the journal file
    pub(crate) fn flush(&mut self, sync_metadata: bool) -> crate::Result<()> {
        self.file.flush()?;

        if sync_metadata {
            self.file.get_mut().sync_all()
        } else {
            self.file.get_mut().sync_data()
        }?;

        Ok(())
    }

    /// Appends a single item wrapped in a batch to the journal
    pub(crate) fn write(&mut self, item: &BatchItem, seqno: SeqNo) -> crate::Result<usize> {
        self.write_batch(&[item], seqno)
    }

    pub fn write_batch(&mut self, items: &[&BatchItem], seqno: SeqNo) -> crate::Result<usize> {
        // NOTE: entries.len() is surely never > u32::MAX
        #[allow(clippy::cast_possible_truncation)]
        let item_count = items.len() as u32;

        let mut hasher = crc32fast::Hasher::new();
        let mut byte_count = 0;

        byte_count += write_start(&mut self.file, item_count, seqno)?;

        for item in items {
            let item = Marker::Item {
                partition: item.partition.clone(),
                key: item.key.clone(),
                value: item.value.clone(),
                value_type: item.value_type,
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
