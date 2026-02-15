// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{file::MAGIC_BYTES, keyspace::InternalKeyspaceId, Slice};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use lsm_tree::{
    coding::{Decode, Encode},
    CompressionType, SeqNo, UserKey, UserValue, ValueType,
};
use std::io::{Read, Write};

/// Journal entry. Every batch is composed as a Start, followed by N items, followed by an End.
/// Single items use the `SingleItem` entry type.
///
/// Batch format:
/// - The start entry contains the numbers of items. If the numbers of items following doesn't match, the batch is broken.
///
/// - The end entry contains a checksum value. If the checksum of the items doesn't match that, the batch is broken.
///
/// - The end entry terminates each batch with the magic string: [`TRAILER_MAGIC`].
///
/// - If a start entry is detected, while inside a batch, the batch is broken.
///
/// `SingleItem` format:
/// - A complete standalone entry with embedded seqno and checksum
///
/// - Should never appear inside a batch (Start...End)
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
    SingleItem {
        keyspace_id: InternalKeyspaceId,
        key: UserKey,
        value: UserValue,
        value_type: ValueType,
        compression: CompressionType,
        seqno: SeqNo,
        checksum: u64,
    },
}

pub fn serialize_marker_item<W: Write>(
    writer: &mut W,
    keyspace_id: InternalKeyspaceId,
    key: &[u8],
    value: &[u8],
    value_type: ValueType,
    compression: CompressionType,
) -> Result<(), lsm_tree::Error> {
    writer.write_u8(Tag::Item.into())?;

    serialize_item(writer, keyspace_id, key, value, value_type, compression)
}

/// Serializes the core item data (everything except the tag and SingleItem-specific fields)
pub(crate) fn serialize_item<W: Write>(
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

pub enum Tag {
    Start = 1,
    Item = 2,
    End = 3,
    SingleItem = 4,
}

impl TryFrom<u8> for Tag {
    type Error = crate::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        use Tag::{End, Item, SingleItem, Start};

        match value {
            1 => Ok(Start),
            2 => Ok(Item),
            3 => Ok(End),
            4 => Ok(SingleItem),
            _ => Err(crate::Error::InvalidTag(("JournalMarkerTag", value))),
        }
    }
}

impl From<Tag> for u8 {
    fn from(val: Tag) -> Self {
        val as Self
    }
}

impl Entry {
    #[cfg(test)]
    pub fn encode_into_vec(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode_into(&mut buf).expect("should encode");
        buf
    }

    pub(crate) fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), crate::Error> {
        use Entry::{End, Item, SingleItem, Start};

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
            SingleItem {
                keyspace_id,
                key,
                value,
                value_type,
                compression,
                seqno,
                checksum,
            } => {
                writer.write_u8(Tag::SingleItem.into())?;
                writer.write_u64::<LittleEndian>(*seqno)?;

                serialize_item(writer, *keyspace_id, key, value, *value_type, *compression)?;

                writer.write_u64::<LittleEndian>(*checksum)?;
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
                let (keyspace_id, key, value, value_type, compression) = decode_item_data(reader)?;

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
            Tag::SingleItem => {
                let seqno = reader.read_u64::<LittleEndian>()?;

                let (keyspace_id, key, value, value_type, compression) = decode_item_data(reader)?;

                let checksum = reader.read_u64::<LittleEndian>()?;

                Ok(Self::SingleItem {
                    keyspace_id,
                    key,
                    value,
                    value_type,
                    compression,
                    seqno,
                    checksum,
                })
            }
        }
    }
}

/// Decodes the core item data (everything after tag and SingleItem-specific fields)
fn decode_item_data<R: Read>(
    reader: &mut R,
) -> Result<(InternalKeyspaceId, Slice, Slice, ValueType, CompressionType), crate::Error> {
    let value_type = reader.read_u8()?;
    let value_type = value_type
        .try_into()
        .map_err(|()| lsm_tree::Error::InvalidTag(("ValueType", value_type)))?;

    let compression = CompressionType::decode_from(reader)?;

    // Read keyspace ID
    let keyspace_id = reader.read_u64::<LittleEndian>()?;

    // Read key len
    let key_len = reader.read_u16::<LittleEndian>()?;

    // Read real value size
    let value_len = reader.read_u32::<LittleEndian>()?;

    // Read on-disk value size
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

            #[warn(unsafe_code)]
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
    fn test_serialize_and_deserialize_single_item() -> crate::Result<()> {
        use std::hash::Hasher;

        // Calculate the correct checksum for this data
        let keyspace_id = 1;
        let key = vec![1, 2, 3];
        let value = vec![11, 12, 13];
        let value_type = ValueType::Value;
        let compression = CompressionType::None;

        // Serialize item data to calculate checksum
        let mut temp_buf = Vec::new();
        serialize_item(
            &mut temp_buf,
            keyspace_id,
            &key,
            &value,
            value_type,
            compression,
        )?;

        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        hasher.update(&temp_buf);
        let checksum = hasher.finish();

        let item = Entry::SingleItem {
            keyspace_id,
            key: key.into(),
            value: value.into(),
            value_type,
            compression,
            seqno: 42,
            checksum, // Use the calculated checksum
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
        let invalid_data = [5u8; 1]; // Invalid tag

        // Try to deserialize with invalid data
        let mut reader = &invalid_data[..];
        let result = Entry::decode_from(&mut reader);

        match result {
            Ok(_) => panic!("should error"),
            Err(error) => match error {
                crate::Error::InvalidTag(("JournalMarkerTag", 5)) => {}
                _ => panic!("should throw InvalidTag"),
            },
        }
    }
}
