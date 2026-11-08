// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod encoder;
mod recovery;

use byteorder::WriteBytesExt;
use hwal::SeqNo;
use lsm_tree::{CompressionType, ValueType};
use varint_rs::VarintWriter;

pub use recovery::recover_wal;

use crate::{
    batch::item::Item as BatchItem, db::Keyspaces, keyspace::InternalKeyspaceId,
    wal::encoder::Encoder, DurabilityLevel,
};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum RecordType {
    /// A single KV, contained in its own write batch
    Single = 0,

    /// A full write batch
    Full,
    // Start,
    // Middle,
    // End
}

/// Highest seqno of each keyspace that may still need a WAL fragment
type FragmentWatermarks = Vec<(InternalKeyspaceId, SeqNo)>;

/// Write-ahead log
pub struct Wal {
    inner: hwal::Wal,
    encoder: Encoder,

    /// Watermarks of the sealed fragments: a sealed fragment can be pruned once every listed
    /// keyspace's flush watermark is at or above its seqno (or the keyspace was deleted).
    ///
    /// NOTE: A sealed fragment without an entry has nothing left to wait for, e.g. after recovery,
    /// when all its items were skipped (already flushed, or of deleted keyspaces)
    sealed_watermarks: BTreeMap<hwal::FragmentId, FragmentWatermarks>,
}

impl Wal {
    pub fn new(inner: hwal::Wal) -> Self {
        Self {
            inner,
            encoder: Encoder::default(),
            sealed_watermarks: BTreeMap::new(),
        }
    }

    pub fn with_compression(
        mut self,
        compression_type: CompressionType,
        compression_threshold: u32,
    ) -> Self {
        self.encoder = self
            .encoder
            .with_compression(compression_type, compression_threshold);
        self
    }

    pub fn disk_usage(&self) -> crate::Result<u64> {
        self.inner.disk_usage().map_err(Into::into)
    }

    /// Seals the active WAL fragment, and continues in a new one.
    pub(crate) fn rotate(&mut self, keyspaces: &Keyspaces) -> crate::Result<()> {
        use lsm_tree::AbstractTree;

        let sealed_id = self.inner.active_fragment_id();

        // NOTE: Nothing happens if the active fragment is empty
        if self.inner.rotate()?.is_none() {
            return Ok(());
        }

        let mut watermarks = FragmentWatermarks::with_capacity(keyspaces.len());

        for keyspace in keyspaces.values() {
            // NOTE: Empty memtables = everything of that keyspace is flushed already
            if let Some(seqno) = keyspace.tree.get_highest_memtable_seqno() {
                watermarks.push((keyspace.id(), seqno));
            }
        }

        log::trace!("Sealed WAL fragment #{sealed_id} with watermarks {watermarks:?}");
        self.sealed_watermarks.insert(sealed_id, watermarks);

        Ok(())
    }

    /// Prunes sealed WAL fragments that are safe to delete.
    pub(crate) fn prune(&mut self, keyspaces: &Keyspaces) -> crate::Result<()> {
        use lsm_tree::AbstractTree;

        let sealed_watermarks = &self.sealed_watermarks;
        let mut last_pruned = None;

        self.inner.prune(|fragment_id| {
            let prunable = sealed_watermarks
                .get(&fragment_id)
                .is_none_or(|watermarks| {
                    watermarks.iter().all(|&(keyspace_id, seqno)| {
                        keyspaces
                            .values()
                            .find(|ks| ks.id() == keyspace_id)
                            // NOTE: Deleted keyspaces are not in the registry anymore,
                            // their items are never replayed
                            .is_none_or(|ks| ks.tree.get_flush_watermark() >= Some(seqno))
                    })
                });

            if prunable {
                log::debug!("Pruning fully flushed WAL fragment #{fragment_id}");
                last_pruned = Some(fragment_id);
            }

            prunable
        })?;

        // NOTE: Fragments are pruned oldest first, so forget everything up to the last pruned one
        if let Some(last_pruned) = last_pruned {
            self.sealed_watermarks = self.sealed_watermarks.split_off(&(last_pruned + 1));
        }

        Ok(())
    }

    /// Takes over the watermarks found during WAL recovery.
    fn set_recovered_watermarks(&mut self, watermarks: recovery::Watermarks) {
        let active_id = self.inner.active_fragment_id();

        for (fragment_id, keyspace_watermarks) in watermarks {
            // NOTE: The active fragment is only sealed by a later rotation,
            // which computes its watermarks then
            if fragment_id == active_id {
                continue;
            }

            self.sealed_watermarks
                .insert(fragment_id, keyspace_watermarks.into_iter().collect());
        }
    }

    pub fn sealed_fragment_count(&self) -> usize {
        self.inner.sealed_fragment_count()
    }

    /// Returns the size of the active WAL fragment in bytes.
    pub fn active_fragment_size(&self) -> u64 {
        self.inner.active_fragment_size()
    }

    fn encode(
        &mut self,
        mut f: impl FnMut(&mut Encoder) -> crate::Result<()>,
    ) -> crate::Result<usize> {
        debug_assert!(self.encoder.is_empty(), "buffer should be empty");

        f(&mut self.encoder)?;

        self.inner.append(&self.encoder)?;
        let bytes = self.encoder.len();
        self.encoder.clear();

        Ok(bytes)
    }

    pub fn write_batch(&mut self, seqno: SeqNo, items: &[BatchItem]) -> crate::Result<usize> {
        self.encode(|encoder| {
            encoder.write_u8(RecordType::Full as u8)?;
            encoder.write_u64_varint(seqno)?;

            // Write batch length
            encoder.write_u64_varint(items.len() as u64)?;

            // Write KVs
            for kv in items {
                encoder.encode_kv(kv.keyspace.id(), &kv.key, &kv.value, kv.value_type)?;
            }

            Ok(())
        })
    }

    /// Writes a single KV contained in its own write batch to the WAL.
    pub fn write(
        &mut self,
        keyspace_id: InternalKeyspaceId,
        key: &[u8],
        val: &[u8],
        seqno: SeqNo,
        value_type: ValueType,
    ) -> crate::Result<usize> {
        self.encode(|encoder| {
            encoder.write_u8(RecordType::Single as u8)?;
            encoder.write_u64_varint(seqno)?;

            // Write KV
            encoder.encode_kv(keyspace_id, key, val, value_type)
        })
    }

    pub fn persist(&mut self, lvl: Option<DurabilityLevel>) -> crate::Result<()> {
        let durability = lvl.and_then(|d| match d {
            DurabilityLevel::Buffer => Some(hwal::DurabilityLevel::Buffer),
            DurabilityLevel::SyncData => Some(hwal::DurabilityLevel::SyncData),
            DurabilityLevel::SyncAll => Some(hwal::DurabilityLevel::SyncAll),
            DurabilityLevel::Visible => None,
        });

        if let Some(lvl) = durability {
            self.inner
                .flush(lvl)
                .inspect_err(|e| {
                    log::error!("persist failed, which is a FATAL, and possibly hardware-related, failure: {e:?}");
                })?;
        }

        Ok(())
    }
}
