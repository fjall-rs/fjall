// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::entry::{serialize_marker_item, Entry};
use crate::{
    batch::item::Item as BatchItem, file::fsync_directory, journal::recovery::JournalId,
    keyspace::InternalKeyspaceId,
};
use lsm_tree::{CompressionType, SeqNo, ValueType};
use std::{
    fs::{File, OpenOptions},
    hash::Hasher,
    io::{BufWriter, Seek, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

// TODO: this should be a database configuration
pub const PRE_ALLOCATED_BYTES: u64 = 64 * 1_024 * 1_024;

pub const JOURNAL_BUFFER_BYTES: usize = 8 * 1_024;

struct DeferredSyncState {
    folder: PathBuf,
    sealed: Writer,
}

/// Wraps the two fsyncs that must happen after a journal rotation
/// (directory fsync + sealed journal fsync). Shared between the new
/// journal writer and the worker pool via [`Arc`]. Whichever side calls
/// [`DeferredSync::persist`] first does the actual work; the other is a no-op.
#[derive(Clone)]
pub(crate) struct DeferredSync {
    inner: Arc<Mutex<Option<DeferredSyncState>>>,
}

impl DeferredSync {
    fn new(folder: PathBuf, sealed: Writer) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(DeferredSyncState { folder, sealed }))),
        }
    }

    /// Performs the deferred fsyncs (directory + sealed journal).
    /// Safe to call concurrently and multiple times. Only the first call does something.
    pub(crate) fn persist(&self) -> std::io::Result<()> {
        #[expect(clippy::expect_used)]
        let mut guard = self.inner.lock().expect("lock is poisoned");

        let Some(mut state) = guard.take() else {
            return Ok(());
        };

        fsync_directory(&state.folder)?;
        state.sealed.persist(PersistMode::SyncAll)?;

        Ok(())
    }
}

pub struct Writer {
    pub(crate) path: PathBuf,
    file: BufWriter<File>,
    buf: Vec<u8>,
    is_buffer_dirty: bool,

    /// Deferred sync obligations from a recent journal rotation.
    pub(crate) deferred_sync: Option<DeferredSync>,

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

    pub fn pos(&mut self) -> crate::Result<u64> {
        self.file.stream_position().map_err(Into::into)
    }

    pub fn len(&self) -> crate::Result<u64> {
        Ok(self.file.get_ref().metadata()?.len())
    }

    /// Creates the next journal file inline, swaps `self` to it, and returns a [`DeferredSync`]
    /// (shared with `self.deferred_sync`) and the sealed journal's path.
    ///
    /// The caller must call [`DeferredSync::persist`] outside the journal lock. The new writer
    /// also holds a clone and will resolve it on its next [`PersistMode::SyncData`] or
    /// [`PersistMode::SyncAll`] call, whichever comes first.
    pub(crate) fn rotate_no_fsync(&mut self) -> crate::Result<(DeferredSync, PathBuf)> {
        // Resolve any pending deferred sync from a prior rotation before creating a new one.
        // This ensures an unresolved sync is not silently dropped when self.deferred_sync is
        // overwritten below.
        if let Some(deferred) = self.deferred_sync.take() {
            deferred.persist()?;
        }

        // Flush write buffer to kernel (no fsync, that's the caller's job).
        self.persist(PersistMode::Buffer)?;

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
        let mut swapped = Self::create_new(new_path)?;
        swapped.set_compression(comp, compt);
        // Make `self` the new active writer.
        std::mem::swap(self, &mut swapped);

        let sealed_path = swapped.path.clone();
        let deferred = DeferredSync::new(folder, swapped);
        self.deferred_sync = Some(deferred.clone());

        Ok((deferred, sealed_path))
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
            deferred_sync: None,
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
                deferred_sync: None,
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
            deferred_sync: None,
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

        // Resolve any deferred rotation sync obligations before fsyncing this journal,
        // so a Sync* caller gets a true end-to-end durability guarantee.
        if matches!(mode, PersistMode::SyncData | PersistMode::SyncAll) {
            if let Some(deferred) = self.deferred_sync.take() {
                deferred.persist()?;
            }
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

        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        let mut byte_count = 0;

        self.buf.clear();
        byte_count += self.write_start(1, seqno)?;
        self.buf.clear();

        serialize_marker_item(
            &mut self.buf,
            keyspace_id,
            key,
            value,
            value_type,
            if self.compression_threshold > 0 && value.len() >= self.compression_threshold {
                self.compression
            } else {
                CompressionType::None
            },
        )?;

        self.file.write_all(&self.buf)?;

        hasher.update(&self.buf);
        byte_count += self.buf.len();

        self.buf.clear();
        let checksum = hasher.finish();
        byte_count += self.write_end(checksum)?;

        Ok(byte_count)
    }

    pub(crate) fn write_clear(
        &mut self,
        keyspace_id: InternalKeyspaceId,
        seqno: SeqNo,
    ) -> crate::Result<usize> {
        self.is_buffer_dirty = true;

        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        let mut byte_count = 0;

        self.buf.clear();
        byte_count += self.write_start(1, seqno)?;
        self.buf.clear();

        Entry::Clear { keyspace_id }.encode_into(&mut self.buf)?;
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
