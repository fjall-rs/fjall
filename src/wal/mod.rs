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
    batch::item::Item as BatchItem, keyspace::InternalKeyspaceId, wal::encoder::Encoder,
    DurabilityLevel,
};

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

pub struct Wal {
    inner: hwal::Wal,
    encoder: Encoder,
}

impl Wal {
    pub fn new(inner: hwal::Wal) -> Self {
        Self {
            inner,
            encoder: Encoder::default(),
        }
    }

    // TODO: v4 call this
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

    pub fn sealed_fragment_count(&self) -> usize {
        self.inner.sealed_fragment_count()
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
