// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::marker::Marker;
use crate::batch::item::Item as BatchItem;
use lsm_tree::{serde::Serializable, SeqNo, SerializeError};
use std::{
    fs::{File, OpenOptions},
    hash::Hasher,
    io::{BufWriter, Write},
    path::Path,
};

pub const PRE_ALLOCATED_BYTES: u64 = 8 * 1_024 * 1_024;
pub const JOURNAL_BUFFER_BYTES: usize = 8 * 1_024;

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
    Marker::Start {
        item_count,
        seqno,
        compression: lsm_tree::CompressionType::None,
    }
    .serialize(&mut bytes)?;

    writer.write_all(&bytes)?;
    Ok(bytes.len())
}

/// Writes a batch end marker to the journal
fn write_end(writer: &mut BufWriter<File>, checksum: u64) -> Result<usize, SerializeError> {
    let mut bytes = Vec::new();
    Marker::End(checksum).serialize(&mut bytes)?;

    writer.write_all(&bytes)?;
    Ok(bytes.len())
}

/// The persist mode allows setting the durability guarantee of previous writes
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PersistMode {
    /// Flushes data to OS buffers. This allows the OS to write out data in case of an
    /// application crash.
    ///
    /// When this function returns, data is **not** guaranteed to be persisted in case
    /// of a power loss event or OS crash.
    Buffer,

    /// Flushes data using `fdatasync`.
    ///
    /// Use if you know that `fdatasync` is sufficient for your file system and/or operating system.
    SyncData,

    /// Flushes data + metadata using `fsync`.
    SyncAll,
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
                file: BufWriter::with_capacity(JOURNAL_BUFFER_BYTES, file),
            });
        }

        let file = OpenOptions::new().append(true).open(path)?;

        Ok(Self {
            file: BufWriter::with_capacity(JOURNAL_BUFFER_BYTES, file),
        })
    }

    /// Flushes the journal file.
    pub(crate) fn flush(&mut self, mode: PersistMode) -> std::io::Result<()> {
        self.file.flush()?;

        match mode {
            PersistMode::SyncAll => self.file.get_mut().sync_all(),
            PersistMode::SyncData => self.file.get_mut().sync_data(),
            PersistMode::Buffer => Ok(()),
        }
    }

    /// Appends a single item wrapped in a batch to the journal
    pub(crate) fn write(&mut self, item: &BatchItem, seqno: SeqNo) -> crate::Result<usize> {
        self.write_batch(&[item], seqno)
    }

    pub fn write_batch(&mut self, items: &[&BatchItem], seqno: SeqNo) -> crate::Result<usize> {
        if items.is_empty() {
            return Ok(0);
        }

        // NOTE: entries.len() is surely never > u32::MAX
        #[allow(clippy::cast_possible_truncation)]
        let item_count = items.len() as u32;

        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
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

        let checksum = hasher.finish();
        byte_count += write_end(&mut self.file, checksum)?;

        Ok(byte_count)
    }
}
