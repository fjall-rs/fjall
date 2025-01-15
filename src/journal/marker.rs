// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{batch::PartitionKey, file::MAGIC_BYTES};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use lsm_tree::{
    coding::{Decode, Encode},
    CompressionType, DecodeError, EncodeError, SeqNo, UserKey, UserValue, ValueType,
};
use std::io::{Read, Write};

/// Journal marker. Every batch is wrapped in a Start marker, followed by N items, followed by an end marker.
///
/// - The start marker contains the numbers of items. If the numbers of items following doesn't match, the batch is broken.
///
/// - The end marker contains a checksum value. If the checksum of the items doesn't match that, the batch is broken.
///
/// - The end marker terminates each batch with the magic string: [`TRAILER_MAGIC`].
///
/// - If a start marker is detected, while inside a batch, the batch is broken.
#[derive(Debug, Eq, PartialEq)]
pub enum Marker {
    Start {
        item_count: u32,
        seqno: SeqNo,
        compression: CompressionType,
    },
    Item {
        partition: PartitionKey,
        key: UserKey,
        value: UserValue,
        value_type: ValueType,
    },
    End(u64),
}

pub fn serialize_marker_item<W: Write>(
    writer: &mut W,
    partition: &str,
    key: &[u8],
    value: &[u8],
    value_type: ValueType,
) -> Result<(), EncodeError> {
    writer.write_u8(Tag::Item.into())?;

    writer.write_u8(u8::from(value_type))?;

    // NOTE: Truncation is okay and actually needed
    #[allow(clippy::cast_possible_truncation)]
    writer.write_u8(partition.len() as u8)?;
    writer.write_all(partition.as_bytes())?;

    // NOTE: Truncation is okay and actually needed
    #[allow(clippy::cast_possible_truncation)]
    writer.write_u16::<BigEndian>(key.len() as u16)?;
    writer.write_all(key)?;

    // NOTE: Truncation is okay and actually needed
    #[allow(clippy::cast_possible_truncation)]
    writer.write_u32::<BigEndian>(value.len() as u32)?;
    writer.write_all(value)?;

    Ok(())
}

pub enum Tag {
    Start = 1,
    Item = 2,
    End = 3,
}

impl TryFrom<u8> for Tag {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        use Tag::{End, Item, Start};

        match value {
            1 => Ok(Start),
            2 => Ok(Item),
            3 => Ok(End),
            _ => Err(DecodeError::InvalidTag(("JournalMarkerTag", value))),
        }
    }
}

impl From<Tag> for u8 {
    fn from(val: Tag) -> Self {
        val as Self
    }
}

impl Encode for Marker {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        use Marker::{End, Item, Start};

        match self {
            Start {
                item_count,
                seqno,
                compression,
            } => {
                writer.write_u8(Tag::Start.into())?;
                writer.write_u32::<BigEndian>(*item_count)?;
                writer.write_u64::<BigEndian>(*seqno)?;
                compression.encode_into(writer)?;
            }
            Item {
                partition,
                key,
                value,
                value_type,
            } => {
                serialize_marker_item(writer, partition, key, value, *value_type)?;
            }
            End(val) => {
                writer.write_u8(Tag::End.into())?;
                writer.write_u64::<BigEndian>(*val)?;

                // NOTE: Write some fixed trailer bytes so we know the end marker is fully written
                // Otherwise we couldn't know if the checksum value is maybe mangled
                // (only partially written, with the rest being padding zeroes)
                writer.write_all(MAGIC_BYTES)?;
            }
        }
        Ok(())
    }
}

impl Decode for Marker {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        match reader.read_u8()?.try_into()? {
            Tag::Start => {
                let item_count = reader.read_u32::<BigEndian>()?;
                let seqno = reader.read_u64::<BigEndian>()?;
                let compression = CompressionType::decode_from(reader)?;

                assert_eq!(
                    compression,
                    CompressionType::None,
                    "invalid compression type"
                );

                Ok(Self::Start {
                    item_count,
                    seqno,
                    compression,
                })
            }
            Tag::Item => {
                let value_type = reader.read_u8()?;
                let value_type = value_type
                    .try_into()
                    .map_err(|()| DecodeError::InvalidTag(("ValueType", value_type)))?;

                // Read partition key
                let partition_len = reader.read_u8()?;
                let mut partition = vec![0; partition_len.into()];
                reader.read_exact(&mut partition)?;
                let partition = std::str::from_utf8(&partition)?;

                // Read key
                let key_len = reader.read_u16::<BigEndian>()?;
                let mut key = vec![0; key_len.into()];
                reader.read_exact(&mut key)?;

                // Read value
                let value_len = reader.read_u32::<BigEndian>()?;
                let mut value = vec![0; value_len as usize];
                reader.read_exact(&mut value)?;

                Ok(Self::Item {
                    partition: partition.into(),
                    key: key.into(),
                    value: value.into(),
                    value_type,
                })
            }
            Tag::End => {
                let checksum = reader.read_u64::<BigEndian>()?;

                // Check trailer
                let mut magic = [0u8; MAGIC_BYTES.len()];
                reader.read_exact(&mut magic)?;

                if magic != MAGIC_BYTES {
                    return Err(DecodeError::InvalidTrailer);
                }

                Ok(Self::End(checksum))
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
        let item = Marker::Item {
            partition: "default".into(),
            key: vec![1, 2, 3].into(),
            value: vec![].into(),
            value_type: ValueType::Value,
        };

        let serialized_data = item.encode_into_vec();
        let mut reader = &serialized_data[..];
        let deserialized_item = Marker::decode_from(&mut reader)?;

        assert_eq!(item, deserialized_item);

        Ok(())
    }

    #[test]
    fn test_invalid_deserialize() {
        let invalid_data = [Tag::Start as u8; 1]; // Should be followed by a u32

        // Try to deserialize with invalid data
        let mut reader = &invalid_data[..];
        let result = Marker::decode_from(&mut reader);

        match result {
            Ok(_) => panic!("should error"),
            Err(error) => match error {
                DecodeError::Io(error) => match error.kind() {
                    std::io::ErrorKind::UnexpectedEof => {}
                    _ => panic!("should throw UnexpectedEof"),
                },
                _ => panic!("should throw UnexpectedEof"),
            },
        }
    }

    #[test]
    fn test_invalid_tag() {
        let invalid_data = [4u8; 1]; // Invalid tag

        // Try to deserialize with invalid data
        let mut reader = &invalid_data[..];
        let result = Marker::decode_from(&mut reader);

        match result {
            Ok(_) => panic!("should error"),
            Err(error) => match error {
                DecodeError::InvalidTag(("JournalMarkerTag", 4)) => {}
                _ => panic!("should throw InvalidTag"),
            },
        }
    }
}
