// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::marker::{serialize_marker_item, Marker};
use crate::{batch::item::Item as BatchItem, file::fsync_directory, journal::recovery::JournalId};
use lsm_tree::{coding::Encode, EncodeError, SeqNo, ValueType};
use std::{
    fs::{File, OpenOptions},
    hash::Hasher,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

// TODO: this should be a keyspace configuration
pub const PRE_ALLOCATED_BYTES: u64 = 32 * 1_024 * 1_024;

pub const JOURNAL_BUFFER_BYTES: usize = 8 * 1_024;

pub struct Writer {
    pub(crate) path: PathBuf,
    file: BufWriter<File>,
    buf: Vec<u8>,

    is_buffer_dirty: bool,
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
    pub fn len(&self) -> crate::Result<u64> {
        Ok(self.file.get_ref().metadata()?.len())
    }

    pub fn rotate(&mut self) -> crate::Result<(PathBuf, PathBuf)> {
        self.persist(PersistMode::SyncAll)?;

        log::debug!(
            "Sealing active journal at {:?}, len={}B",
            self.path,
            self.path
                .metadata()
                .inspect_err(|e| {
                    log::error!(
                        "Failed to get file metadata of journal file at {:?}: {e:?}",
                        self.path
                    );
                })?
                .len(),
        );

        let prev_path = self.path.clone();

        let folder = self
            .path
            .parent()
            .expect("should have parent")
            .to_path_buf();

        let journal_id = self
            .path
            .file_name()
            .expect("should be valid file name")
            .to_str()
            .expect("should be valid journal file name")
            .parse::<JournalId>()
            .expect("should be valid journal ID");

        let new_path = folder.join((journal_id + 1).to_string());
        log::debug!("Rotating active journal to {new_path:?}");

        *self = Self::create_new(new_path.clone())?;

        // IMPORTANT: fsync folder on Unix
        fsync_directory(&folder)?;

        Ok((prev_path, new_path))
    }

    pub fn create_new<P: Into<PathBuf>>(path: P) -> crate::Result<Self> {
        let path = path.into();

        let file = File::create(&path).inspect_err(|e| {
            log::error!("Failed to create journal file at {path:?}: {e:?}");
        })?;

        file.set_len(PRE_ALLOCATED_BYTES).inspect_err(|e| {
            log::error!(
                "Failed to set journal file size to {PRE_ALLOCATED_BYTES}B at {path:?}: {e:?}"
            );
        })?;

        file.sync_all().inspect_err(|e| {
            log::error!("Failed to fsync journal file at {path:?}: {e:?}");
        })?;

        Ok(Self {
            path,
            file: BufWriter::new(file),
            buf: Vec::new(),
            is_buffer_dirty: false,
        })
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();

        if !path.try_exists()? {
            let file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path)
                .inspect_err(|e| {
                    log::error!("Failed to create journal file at {path:?}: {e:?}");
                })?;

            file.set_len(PRE_ALLOCATED_BYTES).inspect_err(|e| {
                log::error!(
                    "Failed to set journal file size to {PRE_ALLOCATED_BYTES}B at {path:?}: {e:?}"
                );
            })?;

            file.sync_all().inspect_err(|e| {
                log::error!("Failed to fsync journal file at {path:?}: {e:?}");
            })?;

            return Ok(Self {
                path: path.into(),
                file: BufWriter::with_capacity(JOURNAL_BUFFER_BYTES, file),
                buf: Vec::new(),
                is_buffer_dirty: false,
            });
        }

        let file = OpenOptions::new()
            .append(true)
            .open(path)
            .inspect_err(|e| {
                log::error!("Failed to open journal file at {path:?}: {e:?}");
            })?;

        Ok(Self {
            path: path.into(),
            file: BufWriter::with_capacity(JOURNAL_BUFFER_BYTES, file),
            buf: Vec::new(),
            is_buffer_dirty: false,
        })
    }

    /// Persists the journal file.
    pub(crate) fn persist(&mut self, mode: PersistMode) -> std::io::Result<()> {
        log::trace!("Persisting journal at {:?} with mode={mode:?}", self.path);

        if self.is_buffer_dirty {
            self.file.flush().inspect_err(|e| {
                log::error!(
                    "Failed to flush journal IO buffers at {:?}: {e:?}",
                    self.path
                );
            })?;
            self.is_buffer_dirty = false;
        }

        match mode {
            PersistMode::SyncAll => self.file.get_mut().sync_all().inspect_err(|e| {
                log::error!("Failed to fsync journal file at {:?}: {e:?}", self.path);
            }),
            PersistMode::SyncData => self.file.get_mut().sync_data().inspect_err(|e| {
                log::error!("Failed to fsyncdata journal file at {:?}: {e:?}", self.path);
            }),
            PersistMode::Buffer => Ok(()),
        }
    }

    /// Writes a batch start marker to the journal
    fn write_start(&mut self, item_count: u32, seqno: SeqNo) -> Result<usize, EncodeError> {
        debug_assert!(self.buf.is_empty());

        Marker::Start {
            item_count,
            seqno,
            compression: lsm_tree::CompressionType::None,
        }
        .encode_into(&mut self.buf)?;

        self.file.write_all(&self.buf)?;

        Ok(self.buf.len())
    }

    /// Writes a batch end marker to the journal
    fn write_end(&mut self, checksum: u64) -> Result<usize, EncodeError> {
        debug_assert!(self.buf.is_empty());

        Marker::End(checksum).encode_into(&mut self.buf)?;

        self.file.write_all(&self.buf)?;

        Ok(self.buf.len())
    }

    pub(crate) fn write_raw(
        &mut self,
        partition: &str,
        key: &[u8],
        value: &[u8],
        value_type: ValueType,
        seqno: u64,
    ) -> crate::Result<usize> {
        self.is_buffer_dirty = true;

        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        let mut byte_count = 0;

        self.buf.clear();
        byte_count += self.write_start(1, seqno)?;
        self.buf.clear();

        serialize_marker_item(&mut self.buf, partition, key, value, value_type)?;

        self.file.write_all(&self.buf)?;

        hasher.update(&self.buf);
        byte_count += self.buf.len();

        self.buf.clear();
        let checksum = hasher.finish();
        byte_count += self.write_end(checksum)?;

        Ok(byte_count)
    }

    pub fn write_batch<'a>(
        &mut self,
        items: impl Iterator<Item = &'a BatchItem>,
        batch_size: usize,
        seqno: SeqNo,
    ) -> crate::Result<usize> {
        if batch_size == 0 {
            return Ok(0);
        }

        self.is_buffer_dirty = true;

        self.buf.clear();

        // NOTE: entries.len() is surely never > u32::MAX
        #[allow(clippy::cast_possible_truncation)]
        let item_count = batch_size as u32;

        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        let mut byte_count = 0;

        byte_count += self.write_start(item_count, seqno)?;
        self.buf.clear();

        for item in items {
            debug_assert!(self.buf.is_empty());

            serialize_marker_item(
                &mut self.buf,
                &item.partition,
                &item.key,
                &item.value,
                item.value_type,
            )?;

            self.file.write_all(&self.buf)?;

            hasher.update(&self.buf);
            byte_count += self.buf.len();

            self.buf.clear();
        }

        let checksum = hasher.finish();
        byte_count += self.write_end(checksum)?;

        Ok(byte_count)
    }
}
