// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod recovery;

use byteorder::WriteBytesExt;
use hwal::SeqNo;
use lsm_tree::ValueType;
use std::io::Write;
use varint_rs::VarintWriter;

pub use recovery::recover_wal;

use crate::{batch::item::Item as BatchItem, keyspace::InternalKeyspaceId, DurabilityLevel};

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
    buf: Vec<u8>,
}

impl Wal {
    pub fn new(inner: hwal::Wal) -> Self {
        Self {
            inner,
            buf: Vec::default(),
        }
    }

    pub fn disk_usage(&self) -> crate::Result<u64> {
        self.inner.disk_usage().map_err(Into::into)
    }

    pub fn sealed_fragment_count(&self) -> usize {
        self.inner.sealed_fragment_count()
    }

    fn encode(
        &mut self,
        mut f: impl FnMut(&mut Vec<u8>) -> crate::Result<()>,
    ) -> crate::Result<usize> {
        debug_assert!(self.buf.is_empty(), "buffer should be empty");

        f(&mut self.buf)?;

        self.inner.append(&self.buf)?;
        let bytes = self.buf.len();
        self.buf.clear();

        Ok(bytes)
    }

    pub fn write_batch(&mut self, seqno: SeqNo, items: &[BatchItem]) -> crate::Result<usize> {
        self.encode(|buf| {
            buf.write_u8(RecordType::Full as u8)?;
            buf.write_u64_varint(seqno)?;

            // Write batch length
            buf.write_u64_varint(items.len() as u64)?;

            // Write KVs
            for kv in items {
                Self::encode_kv(buf, kv.keyspace.id(), &kv.key, &kv.value, kv.value_type)?;
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
        self.encode(|buf| {
            buf.write_u8(RecordType::Single as u8)?;
            buf.write_u64_varint(seqno)?;

            // Write KV
            Self::encode_kv(buf, keyspace_id, key, val, value_type)
        })
    }

    fn encode_kv(
        buf: &mut impl Write,
        keyspace_id: InternalKeyspaceId,
        key: &[u8],
        val: &[u8],
        value_type: ValueType,
    ) -> crate::Result<()> {
        buf.write_u64_varint(keyspace_id)?;

        #[expect(clippy::cast_possible_truncation, reason = "limit in lsm-tree")]
        buf.write_u16_varint(key.len() as u16)?;
        buf.write_all(key)?;

        #[expect(clippy::cast_possible_truncation, reason = "limit in lsm-tree")]
        buf.write_u32_varint(val.len() as u32)?;
        buf.write_all(val)?;

        buf.write_u8(value_type.into())?;

        Ok(())
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
