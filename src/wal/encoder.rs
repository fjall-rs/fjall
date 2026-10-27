// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::keyspace::InternalKeyspaceId;
use byteorder::WriteBytesExt;
use lsm_tree::{CompressionType, ValueType};
use std::io::Write;
use varint_rs::VarintWriter;

pub struct Encoder {
    buf: Vec<u8>,
    compression: CompressionType,
    compression_threshold: u32,
}

impl Default for Encoder {
    fn default() -> Self {
        Self {
            buf: Vec::default(),
            compression: CompressionType::None,
            compression_threshold: 0,
        }
    }
}

impl std::ops::Deref for Encoder {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

impl std::ops::DerefMut for Encoder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf
    }
}

impl Encoder {
    pub fn with_compression(
        mut self,
        compression_type: CompressionType,
        compression_threshold: u32,
    ) -> Self {
        self.compression = compression_type;
        self.compression_threshold = compression_threshold;
        self
    }

    pub fn encode_kv(
        &mut self,
        keyspace_id: InternalKeyspaceId,
        key: &[u8],
        val: &[u8],
        value_type: ValueType,
    ) -> crate::Result<()> {
        use lsm_tree::coding::Encode;

        self.buf.write_u64_varint(keyspace_id)?;

        #[expect(clippy::cast_possible_truncation, reason = "limit in lsm-tree")]
        self.buf.write_u16_varint(key.len() as u16)?;
        self.buf.write_all(key)?;

        #[expect(clippy::cast_possible_truncation, reason = "limit in lsm-tree")]
        let uncompressed_val_len = val.len() as u32;

        self.buf.write_u32_varint(uncompressed_val_len)?;

        let val = match self.compression {
            #[cfg(feature = "lz4")]
            CompressionType::Lz4 if uncompressed_val_len >= self.compression_threshold => {
                self.compression.encode_into(&mut self.buf)?;
                let compressed = lz4_flex::compress(val);

                #[expect(clippy::cast_possible_truncation, reason = "limit in lsm-tree")]
                self.buf.write_u32_varint(compressed.len() as u32)?;

                std::borrow::Cow::Owned(compressed)
            }

            _ => {
                CompressionType::None.encode_into(&mut self.buf)?;
                std::borrow::Cow::Borrowed(val)
            }
        };

        self.buf.write_all(&val)?;

        self.buf.write_u8(value_type.into())?;

        Ok(())
    }
}
