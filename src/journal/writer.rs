// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::entry::{serialize_marker_item, Entry};
use crate::{
    batch::item::Item as BatchItem,
    file::fsync_directory,
    journal::{
        entry::{serialize_item, Tag},
        recovery::JournalId,
    },
    keyspace::InternalKeyspaceId,
};
use byteorder::{LittleEndian, WriteBytesExt};
use lsm_tree::{CompressionType, SeqNo, ValueType};
use std::{
    fs::{File, OpenOptions},
    hash::Hasher,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

/// A writer that computes a checksum as data is written through it
struct HashingWriter<'a, W: Write> {
    writer: &'a mut W,
    hasher: xxhash_rust::xxh3::Xxh3,
}

impl<'a, W: Write> HashingWriter<'a, W> {
    fn new(writer: &'a mut W) -> Self {
        Self {
            writer,
            hasher: xxhash_rust::xxh3::Xxh3::default(),
        }
    }

    fn finish(self) -> u64 {
        self.hasher.finish()
    }
}

impl<'a, W: Write> Write for HashingWriter<'a, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        self.writer.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

// TODO: this should be a database configuration
pub const PRE_ALLOCATED_BYTES: u64 = 64 * 1_024 * 1_024;

pub const JOURNAL_BUFFER_BYTES: usize = 8 * 1_024;

pub struct Writer {
    pub(crate) path: PathBuf,
    file: BufWriter<File>,
    buf: Vec<u8>,
    is_buffer_dirty: bool,

    compression: CompressionType,
    compression_threshold: usize,
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
    pub fn set_compression(&mut self, comp: CompressionType, threshold: usize) {
        self.compression = comp;
        self.compression_threshold = threshold;
    }

    pub fn len(&self) -> crate::Result<u64> {
        Ok(self.file.get_ref().metadata()?.len())
    }

    pub fn rotate(&mut self) -> crate::Result<(PathBuf, PathBuf)> {
        self.persist(PersistMode::SyncAll)?;

        log::debug!(
            "Sealing active journal at {}, len={}B",
            self.path.display(),
            self.path
                .metadata()
                .inspect_err(|e| {
                    log::error!(
                        "Failed to get file metadata of journal file at {}: {e:?}",
                        self.path.display()
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

        let Some(basename) = self
            .path
            .file_name()
            .expect("should be valid file name")
            .to_str()
            .expect("should be valid utf-8")
            .strip_suffix(".jnl")
        else {
            log::error!("Invalid journal file name: {}", self.path.display());
            return Err(crate::Error::JournalRecovery(
                crate::JournalRecoveryError::InvalidFileName,
            ));
        };

        let journal_id = basename.parse::<JournalId>().map_err(|_| {
            log::error!("Invalid journal file name: {}", self.path.display());
            crate::Error::JournalRecovery(crate::JournalRecoveryError::InvalidFileName)
        })?;

        let new_path = folder.join(format!("{}.jnl", journal_id + 1));
        log::debug!("Rotating active journal to {}", new_path.display());

        let comp = self.compression;
        let compt = self.compression_threshold;
        *self = Self::create_new(new_path.clone())?;
        self.set_compression(comp, compt);

        // IMPORTANT: fsync folder on Unix
        fsync_directory(&folder)?;

        Ok((prev_path, new_path))
    }

    pub fn create_new<P: Into<PathBuf>>(path: P) -> crate::Result<Self> {
        let path = path.into();

        let file = File::create_new(&path).inspect_err(|e| {
            log::error!("Failed to create journal file at {}: {e:?}", path.display());
        })?;

        file.set_len(PRE_ALLOCATED_BYTES).inspect_err(|e| {
            log::error!(
                "Failed to set journal file size to {PRE_ALLOCATED_BYTES}B at {}: {e:?}",
                path.display(),
            );
        })?;

        file.sync_all().inspect_err(|e| {
            log::error!("Failed to fsync journal file at {}: {e:?}", path.display());
        })?;

        Ok(Self {
            path,
            file: BufWriter::new(file),
            buf: Vec::new(),
            is_buffer_dirty: false,
            compression: CompressionType::None,
            compression_threshold: 0,
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
                    log::error!("Failed to create journal file at {}: {e:?}", path.display());
                })?;

            file.set_len(PRE_ALLOCATED_BYTES).inspect_err(|e| {
                log::error!(
                    "Failed to set journal file size to {PRE_ALLOCATED_BYTES}B at {}: {e:?}",
                    path.display(),
                );
            })?;

            file.sync_all().inspect_err(|e| {
                log::error!("Failed to fsync journal file at {}: {e:?}", path.display());
            })?;

            return Ok(Self {
                path: path.into(),
                file: BufWriter::with_capacity(JOURNAL_BUFFER_BYTES, file),
                buf: Vec::new(),
                is_buffer_dirty: false,
                compression: CompressionType::None,
                compression_threshold: 0,
            });
        }

        let file = OpenOptions::new()
            .append(true)
            .open(path)
            .inspect_err(|e| {
                log::error!("Failed to open journal file at {}: {e:?}", path.display());
            })?;

        Ok(Self {
            path: path.into(),
            file: BufWriter::with_capacity(JOURNAL_BUFFER_BYTES, file),
            buf: Vec::new(),
            is_buffer_dirty: false,
            compression: CompressionType::None,
            compression_threshold: 0,
        })
    }

    /// Persists the journal file.
    pub(crate) fn persist(&mut self, mode: PersistMode) -> std::io::Result<()> {
        log::trace!(
            "Persisting journal at {} with mode={mode:?}",
            self.path.display(),
        );

        if self.is_buffer_dirty {
            self.file.flush().inspect_err(|e| {
                log::error!(
                    "Failed to flush journal IO buffers at {}: {e:?}",
                    self.path.display(),
                );
            })?;
            self.is_buffer_dirty = false;
        }

        match mode {
            PersistMode::SyncAll => self.file.get_mut().sync_all().inspect_err(|e| {
                log::error!(
                    "Failed to fsync journal file at {}: {e:?}",
                    self.path.display(),
                );
            }),
            PersistMode::SyncData => self.file.get_mut().sync_data().inspect_err(|e| {
                log::error!(
                    "Failed to fsyncdata journal file at {}: {e:?}",
                    self.path.display(),
                );
            }),
            PersistMode::Buffer => Ok(()),
        }
    }

    /// Writes a batch start marker to the journal
    fn write_start(&mut self, item_count: u32, seqno: SeqNo) -> Result<usize, crate::Error> {
        debug_assert!(self.buf.is_empty());

        Entry::Start { item_count, seqno }.encode_into(&mut self.buf)?;

        self.file.write_all(&self.buf)?;

        Ok(self.buf.len())
    }

    /// Writes a batch end marker to the journal
    fn write_end(&mut self, checksum: u64) -> Result<usize, crate::Error> {
        debug_assert!(self.buf.is_empty());

        Entry::End(checksum).encode_into(&mut self.buf)?;

        self.file.write_all(&self.buf)?;

        Ok(self.buf.len())
    }

    pub(crate) fn write_raw(
        &mut self,
        keyspace_id: InternalKeyspaceId,
        key: &[u8],
        value: &[u8],
        value_type: ValueType,
        seqno: u64,
    ) -> crate::Result<usize> {
        self.is_buffer_dirty = true;
        self.buf.clear();

        let compression =
            if self.compression_threshold > 0 && value.len() >= self.compression_threshold {
                self.compression
            } else {
                CompressionType::None
            };

        self.buf.write_u8(Tag::SingleItem.into())?;
        self.buf.write_u64::<LittleEndian>(seqno)?;

        let mut hashing_writer = HashingWriter::new(&mut self.buf);

        serialize_item(
            &mut hashing_writer,
            keyspace_id,
            key,
            value,
            value_type,
            compression,
        )?;

        let checksum = hashing_writer.finish();

        self.buf.write_u64::<LittleEndian>(checksum)?;

        self.file.write_all(&self.buf)?;

        Ok(self.buf.len())
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
        #[expect(clippy::cast_possible_truncation)]
        let item_count = batch_size as u32;

        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        let mut byte_count = 0;

        byte_count += self.write_start(item_count, seqno)?;
        self.buf.clear();

        for item in items {
            debug_assert!(self.buf.is_empty());

            serialize_marker_item(
                &mut self.buf,
                item.keyspace.id,
                &item.key,
                &item.value,
                item.value_type,
                if self.compression_threshold > 0 && item.value.len() >= self.compression_threshold
                {
                    self.compression
                } else {
                    CompressionType::None
                },
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
