// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{file::MAGIC_BYTES, keyspace::InternalKeyspaceId, Slice};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use lsm_tree::{
    coding::{Decode, Encode},
    CompressionType, SeqNo, UserKey, UserValue, ValueType,
};
use std::{
    hash::Hasher,
    io::{Read, Write},
};

/// Write adapter that hashes bytes as they pass through to the inner writer.
/// Used by `SingleItem` encoding to compute the checksum without a temp buffer.
pub(super) struct HashingWriter<'a, W: Write, H: Hasher> {
    inner: &'a mut W,
    hasher: &'a mut H,
}

impl<'a, W: Write, H: Hasher> HashingWriter<'a, W, H> {
    pub(super) fn new(inner: &'a mut W, hasher: &'a mut H) -> Self {
        Self { inner, hasher }
    }
}

impl<W: Write, H: Hasher> Write for HashingWriter<'_, W, H> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        if let Some(written) = buf.get(..n) {
            self.hasher.write(written);
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Journal entry. Batches are encoded either as:
/// - a `Start` entry, followed by N `Item` entries, followed by an `End` entry, or
/// - a single `SingleItem` entry (compact encoding for single-item batches).
///
/// - The start entry contains the number of items. If the number of items following doesn't match, the batch is broken.
///
/// - The end entry contains a checksum value. If the checksum of the items doesn't match that, the batch is broken.
///
/// - Each encoded batch (both multi-item and `SingleItem`) is terminated with
///   the magic bytes: [`MAGIC_BYTES`]. For multi-item batches, the trailer is
///   written as part of the `End` entry.
///
/// - If a start entry is detected while already inside a batch, the batch is broken.
#[derive(Debug, Eq, PartialEq)]
pub enum Entry {
    Start {
        item_count: u32,
        seqno: SeqNo,
    },
    Item {
        keyspace_id: InternalKeyspaceId,
        key: UserKey,
        value: UserValue,
        value_type: ValueType,
        compression: CompressionType,
    },
    End(u64),
    Clear {
        keyspace_id: InternalKeyspaceId,
    },
    /// Compact encoding for single-item batches.
    /// Combines Start + Item + End into one entry, saving 6 bytes of overhead.
    /// Checksum is stored here and verified by `JournalBatchReader` (consistent
    /// with how multi-item batch checksums are handled).
    SingleItem {
        seqno: SeqNo,
        checksum: u64,
        keyspace_id: InternalKeyspaceId,
        key: UserKey,
        value: UserValue,
        value_type: ValueType,
        compression: CompressionType,
    },
}

/// Serializes item payload (value type, compression, keyspace ID, key, value)
/// without the entry tag byte. Used by both `serialize_marker_item` and the
/// compact `SingleItem` format.
pub(super) fn serialize_item_payload<W: Write>(
    writer: &mut W,
    keyspace_id: InternalKeyspaceId,
    key: &[u8],
    value: &[u8],
    value_type: ValueType,
    compression: CompressionType,
) -> Result<(), lsm_tree::Error> {
    writer.write_u8(u8::from(value_type))?;

    compression.encode_into(writer)?;

    let compressed_value = match compression {
        CompressionType::None => std::borrow::Cow::Borrowed(value),

        #[cfg(feature = "lz4")]
        CompressionType::Lz4 => {
            let compressed = lz4_flex::compress(value);
            std::borrow::Cow::Owned(compressed)
        }
    };

    // NOTE: Truncation is okay and actually needed
    writer.write_u64::<LittleEndian>(keyspace_id)?;

    // NOTE: Truncation is okay and actually needed
    #[expect(clippy::cast_possible_truncation)]
    writer.write_u16::<LittleEndian>(key.len() as u16)?;

    // NOTE: Truncation is okay and actually needed
    #[expect(clippy::cast_possible_truncation)]
    writer.write_u32::<LittleEndian>(value.len() as u32)?;

    // NOTE: Truncation is okay and actually needed
    #[expect(clippy::cast_possible_truncation)]
    writer.write_u32::<LittleEndian>(compressed_value.len() as u32)?;

    writer.write_all(key)?;

    writer.write_all(&compressed_value)?;

    Ok(())
}

pub(super) fn serialize_marker_item<W: Write>(
    writer: &mut W,
    keyspace_id: InternalKeyspaceId,
    key: &[u8],
    value: &[u8],
    value_type: ValueType,
    compression: CompressionType,
) -> Result<(), lsm_tree::Error> {
    writer.write_u8(Tag::Item.into())?;
    serialize_item_payload(writer, keyspace_id, key, value, value_type, compression)
}

pub enum Tag {
    Start = 1,
    Item = 2,
    End = 3,
    Clear = 4,
    SingleItem = 5,
}

impl TryFrom<u8> for Tag {
    type Error = crate::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        use Tag::{Clear, End, Item, SingleItem, Start};

        match value {
            1 => Ok(Start),
            2 => Ok(Item),
            3 => Ok(End),
            4 => Ok(Clear),
            5 => Ok(SingleItem),
            _ => Err(crate::Error::InvalidTag(("JournalMarkerTag", value))),
        }
    }
}

impl From<Tag> for u8 {
    fn from(val: Tag) -> Self {
        val as Self
    }
}

/// Decodes item payload fields (everything after the tag byte) from a reader.
/// Shared between `Tag::Item` and `Tag::SingleItem` decoding.
fn decode_item_payload<R: Read>(
    reader: &mut R,
) -> Result<
    (
        InternalKeyspaceId,
        UserKey,
        UserValue,
        ValueType,
        CompressionType,
    ),
    crate::Error,
> {
    let value_type = reader.read_u8()?;
    let value_type = value_type
        .try_into()
        .map_err(|()| lsm_tree::Error::InvalidTag(("ValueType", value_type)))?;

    let compression = CompressionType::decode_from(reader)?;

    let keyspace_id = reader.read_u64::<LittleEndian>()?;
    let key_len = reader.read_u16::<LittleEndian>()?;
    let value_len = reader.read_u32::<LittleEndian>()?;
    let on_disk_value_len = reader.read_u32::<LittleEndian>()?;

    let key = Slice::from_reader(reader, usize::from(key_len))?;

    let value = match compression {
        CompressionType::None => {
            debug_assert_eq!(value_len, on_disk_value_len);
            Slice::from_reader(reader, on_disk_value_len as usize)?
        }

        #[cfg(feature = "lz4")]
        CompressionType::Lz4 => {
            let compressed_value = Slice::from_reader(reader, on_disk_value_len as usize)?;

            #[expect(
                unsafe_code,
                reason = "unzeroed buffer for LZ4 decompression performance"
            )]
            // SAFETY: decompress_into writes exactly value_len bytes on success
            // (validated by the size check below). The buffer is fully initialized
            // before freeze() is called.
            let mut value = unsafe { Slice::builder_unzeroed(value_len as usize) };

            let size = lz4_flex::decompress_into(&compressed_value, &mut value).map_err(|e| {
                log::error!("LZ4 decompression failed: {e}");
                crate::Error::Decompress(CompressionType::Lz4)
            })?;

            if size != value.len() {
                log::error!("Decompressed size does not match expected value size");
                return Err(crate::Error::Decompress(CompressionType::Lz4));
            }

            Slice::from(value.freeze())
        }
    };

    Ok((keyspace_id, key, value, value_type, compression))
}

impl Entry {
    #[cfg(test)]
    pub fn encode_into_vec(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode_into(&mut buf).expect("should encode");
        buf
    }

    pub(crate) fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), crate::Error> {
        use Entry::{Clear, End, Item, SingleItem, Start};

        match self {
            Start { item_count, seqno } => {
                writer.write_u8(Tag::Start.into())?;
                writer.write_u32::<LittleEndian>(*item_count)?;
                writer.write_u64::<LittleEndian>(*seqno)?;
            }
            Item {
                keyspace_id,
                key,
                value,
                value_type,
                compression,
            } => {
                serialize_marker_item(writer, *keyspace_id, key, value, *value_type, *compression)?;
            }
            End(val) => {
                writer.write_u8(Tag::End.into())?;
                writer.write_u64::<LittleEndian>(*val)?;

                // NOTE: Write some fixed trailer bytes so we know the end marker is fully written
                // Otherwise we couldn't know if the checksum value is maybe mangled
                // (only partially written, with the rest being padding zeroes)
                writer.write_all(MAGIC_BYTES)?;
            }
            Clear { keyspace_id } => {
                writer.write_u8(Tag::Clear.into())?;
                writer.write_u64::<LittleEndian>(*keyspace_id)?;
            }
            SingleItem {
                seqno,
                checksum,
                keyspace_id,
                key,
                value,
                value_type,
                compression,
            } => {
                writer.write_u8(Tag::SingleItem.into())?;
                writer.write_u64::<LittleEndian>(*seqno)?;

                // Serialize payload directly into the destination writer
                // while computing checksum, avoiding a temp buffer allocation.
                let mut hasher = xxhash_rust::xxh3::Xxh3::default();
                {
                    let mut hashing_writer = HashingWriter::new(writer, &mut hasher);
                    serialize_item_payload(
                        &mut hashing_writer,
                        *keyspace_id,
                        key,
                        value,
                        *value_type,
                        *compression,
                    )?;
                }
                let computed_checksum = hasher.finish();

                // The checksum field is dual-purpose:
                //   - During encoding: ignored (always recomputed from payload).
                //     Callers may pass 0. The hot path (write_raw) bypasses
                //     Entry entirely and writes directly from borrowed slices.
                //   - During decoding: populated from the on-disk bytes and
                //     verified by JournalBatchReader.
                // debug_assert catches test mistakes where a pre-computed
                // checksum doesn't match, but is not a runtime invariant.
                debug_assert!(
                    *checksum == 0 || *checksum == computed_checksum,
                    "SingleItem checksum field does not match computed checksum"
                );

                writer.write_u64::<LittleEndian>(computed_checksum)?;
                writer.write_all(MAGIC_BYTES)?;
            }
        }
        Ok(())
    }

    pub(crate) fn decode_from<R: Read>(reader: &mut R) -> Result<Self, crate::Error> {
        match reader.read_u8()?.try_into()? {
            Tag::Start => {
                let item_count = reader.read_u32::<LittleEndian>()?;
                let seqno = reader.read_u64::<LittleEndian>()?;
                Ok(Self::Start { item_count, seqno })
            }
            Tag::Item => {
                let (keyspace_id, key, value, value_type, compression) =
                    decode_item_payload(reader)?;

                Ok(Self::Item {
                    keyspace_id,
                    key,
                    value,
                    value_type,
                    compression,
                })
            }
            Tag::End => {
                let checksum = reader.read_u64::<LittleEndian>()?;

                // Check trailer
                let mut magic = [0u8; MAGIC_BYTES.len()];
                reader.read_exact(&mut magic)?;

                if magic != MAGIC_BYTES {
                    return Err(crate::Error::InvalidTrailer);
                }

                Ok(Self::End(checksum))
            }
            Tag::Clear => {
                let keyspace_id = reader.read_u64::<LittleEndian>()?;
                Ok(Self::Clear { keyspace_id })
            }
            Tag::SingleItem => {
                let seqno = reader.read_u64::<LittleEndian>()?;

                let (keyspace_id, key, value, value_type, compression) =
                    decode_item_payload(reader)?;

                // Read checksum + trailer (verification deferred to JournalBatchReader,
                // consistent with how multi-item batch checksums are handled)
                let checksum = reader.read_u64::<LittleEndian>()?;

                let mut magic = [0u8; MAGIC_BYTES.len()];
                reader.read_exact(&mut magic)?;

                if magic != MAGIC_BYTES {
                    return Err(crate::Error::InvalidTrailer);
                }

                Ok(Self::SingleItem {
                    seqno,
                    checksum,
                    keyspace_id,
                    key,
                    value,
                    value_type,
                    compression,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn test_serialize_and_deserialize_success() -> crate::Result<()> {
        let item = Entry::Item {
            keyspace_id: 0,
            key: vec![1, 2, 3].into(),
            value: vec![].into(),
            value_type: ValueType::Value,
            compression: CompressionType::None,
        };

        let serialized_data = item.encode_into_vec();
        let mut reader = &serialized_data[..];
        let deserialized_item = Entry::decode_from(&mut reader)?;

        assert_eq!(item, deserialized_item);

        Ok(())
    }

    #[test]
    fn test_invalid_deserialize() {
        let invalid_data = [Tag::Start as u8; 1]; // Should be followed by a u32

        // Try to deserialize with invalid data
        let mut reader = &invalid_data[..];
        let result = Entry::decode_from(&mut reader);

        match result {
            Ok(_) => panic!("should error"),
            Err(error) => match error {
                crate::Error::Io(e) => match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {}
                    _ => panic!("should throw UnexpectedEof"),
                },
                _ => panic!("should throw UnexpectedEof"),
            },
        }
    }

    #[test]
    fn test_invalid_tag() {
        let invalid_data = [6u8; 1]; // Invalid tag

        // Try to deserialize with invalid data
        let mut reader = &invalid_data[..];
        let result = Entry::decode_from(&mut reader);

        match result {
            Ok(_) => panic!("should error"),
            Err(error) => match error {
                crate::Error::InvalidTag(("JournalMarkerTag", 6)) => {}
                _ => panic!("should throw InvalidTag"),
            },
        }
    }

    /// Helper to compute the checksum for a `SingleItem` payload.
    fn compute_single_item_checksum(
        keyspace_id: u64,
        key: &[u8],
        value: &[u8],
        value_type: ValueType,
        compression: CompressionType,
    ) -> u64 {
        let mut buf = Vec::new();
        #[expect(clippy::expect_used, reason = "test helper, Vec<u8> write cannot fail")]
        serialize_item_payload(&mut buf, keyspace_id, key, value, value_type, compression)
            .expect("encoding should not fail");
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        hasher.update(&buf);
        hasher.finish()
    }

    #[test]
    fn test_single_item_roundtrip() -> crate::Result<()> {
        let checksum = compute_single_item_checksum(
            7,
            &[1, 2, 3],
            &[4, 5, 6],
            ValueType::Value,
            CompressionType::None,
        );

        let entry = Entry::SingleItem {
            seqno: 42,
            checksum,
            keyspace_id: 7,
            key: vec![1, 2, 3].into(),
            value: vec![4, 5, 6].into(),
            value_type: ValueType::Value,
            compression: CompressionType::None,
        };

        let serialized = entry.encode_into_vec();
        let mut reader = &serialized[..];
        let decoded = Entry::decode_from(&mut reader)?;

        assert_eq!(entry, decoded);

        Ok(())
    }

    #[test]
    fn test_single_item_smaller_than_batch() -> crate::Result<()> {
        // Encode a single item as both SingleItem and Start+Item+End batch
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];

        let checksum =
            compute_single_item_checksum(0, &key, &value, ValueType::Value, CompressionType::None);

        let single = Entry::SingleItem {
            seqno: 1,
            checksum,
            keyspace_id: 0,
            key: key.clone().into(),
            value: value.clone().into(),
            value_type: ValueType::Value,
            compression: CompressionType::None,
        };
        let single_bytes = single.encode_into_vec();

        let mut batch_bytes = Vec::new();
        Entry::Start {
            item_count: 1,
            seqno: 1,
        }
        .encode_into(&mut batch_bytes)?;
        Entry::Item {
            keyspace_id: 0,
            key: key.into(),
            value: value.into(),
            value_type: ValueType::Value,
            compression: CompressionType::None,
        }
        .encode_into(&mut batch_bytes)?;
        // Compute checksum the same way the batch reader does
        let item_bytes = Entry::Item {
            keyspace_id: 0,
            key: vec![1, 2, 3].into(),
            value: vec![4, 5, 6].into(),
            value_type: ValueType::Value,
            compression: CompressionType::None,
        }
        .encode_into_vec();
        let mut hasher = xxhash_rust::xxh3::Xxh3::default();
        hasher.update(&item_bytes);
        let checksum = hasher.finish();
        Entry::End(checksum).encode_into(&mut batch_bytes)?;

        // SingleItem format should be 6 bytes shorter
        assert_eq!(
            batch_bytes.len() - single_bytes.len(),
            6,
            "SingleItem saves exactly 6 bytes: batch={}, single={}",
            batch_bytes.len(),
            single_bytes.len()
        );

        Ok(())
    }
}
