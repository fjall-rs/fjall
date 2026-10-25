use byteorder::WriteBytesExt;
use hwal::SeqNo;
use lsm_tree::ValueType;
use std::io::Write;
use varint_rs::VarintWriter;

use crate::{keyspace::InternalKeyspaceId, DurabilityLevel};

pub struct Wal {
    inner: hwal::Wal,
    buf: Vec<u8>,
}

impl Wal {
    pub fn new(inner: hwal::Wal) -> Self {
        Self {
            inner,
            buf: Vec::new(),
        }
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
        debug_assert!(self.buf.is_empty(), "buffer should be empty");

        self.buf.write_u8(3)?;

        self.buf.write_u64_varint(keyspace_id)?;

        self.buf.write_u16_varint(key.len() as u16)?;
        self.buf.write_all(key)?;

        self.buf.write_u32_varint(val.len() as u32)?;
        self.buf.write_all(val)?;

        self.buf.write_u64_varint(seqno)?;
        self.buf.write_u8(value_type.into())?;

        self.inner.append(&self.buf)?;
        let bytes = self.buf.len();

        self.buf.clear();

        Ok(bytes)
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
