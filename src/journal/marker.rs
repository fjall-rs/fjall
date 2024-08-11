// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::batch::PartitionKey;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use lsm_tree::{
    serde::{Deserializable, Serializable},
    DeserializeError, SeqNo, SerializeError, UserKey, UserValue, ValueType,
};
use std::io::{Read, Write};

const TRAILER_MAGIC: &[u8] = &[b'F', b'J', b'L', b'L', b'T', b'R', b'L', b'2'];

/// Journal marker. Every batch is wrapped in a Start marker, followed by N items, followed by an end marker.
///
/// - The start marker contains the numbers of items. If the numbers of items following doesn't match, the batch is broken.
///
/// - The end marker contains a CRC value. If the CRC of the items doesn't match that, the batch is broken.
///
/// - The end marker terminates each batch with the magic u64 value: [`TRAILER_MAGIC`].
///
/// - If a start marker is detected, while inside a batch, the batch is broken.
#[derive(Debug, Eq, PartialEq)]
pub enum Marker {
    Start {
        item_count: u32,
        seqno: SeqNo,
    },
    Item {
        partition: PartitionKey,
        key: UserKey,
        value: UserValue,
        value_type: ValueType,
    },
    End(u32),
}

pub enum Tag {
    Start = 1,
    Item = 2,
    End = 3,
}

impl TryFrom<u8> for Tag {
    type Error = DeserializeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        use Tag::{End, Item, Start};

        match value {
            1 => Ok(Start),
            2 => Ok(Item),
            3 => Ok(End),
            _ => Err(DeserializeError::InvalidTag(("JournalMarkerTag", value))),
        }
    }
}

impl From<Tag> for u8 {
    fn from(val: Tag) -> Self {
        val as Self
    }
}

impl Serializable for Marker {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        use Marker::{End, Item, Start};

        match self {
            Start { item_count, seqno } => {
                writer.write_u8(Tag::Start.into())?;
                writer.write_u32::<BigEndian>(*item_count)?;
                writer.write_u64::<BigEndian>(*seqno)?;
            }
            Item {
                partition,
                key,
                value,
                value_type,
            } => {
                writer.write_u8(Tag::Item.into())?;

                writer.write_u8(u8::from(*value_type))?;

                // NOTE: Truncation is okay and actually needed
                #[allow(clippy::cast_possible_truncation)]
                writer.write_u8(partition.as_bytes().len() as u8)?;
                writer.write_all(partition.as_bytes())?;

                // NOTE: Truncation is okay and actually needed
                #[allow(clippy::cast_possible_truncation)]
                writer.write_u16::<BigEndian>(key.len() as u16)?;
                writer.write_all(key)?;

                // NOTE: Truncation is okay and actually needed
                #[allow(clippy::cast_possible_truncation)]
                writer.write_u32::<BigEndian>(value.len() as u32)?;
                writer.write_all(value)?;
            }
            End(val) => {
                writer.write_u8(Tag::End.into())?;
                writer.write_u32::<BigEndian>(*val)?;

                // NOTE: Write some fixed trailer bytes so we know the end marker is fully written
                // Otherwise we couldn't know if the CRC value is maybe mangled
                // (only partially written, with the rest being padding zeroes)
                writer.write_all(TRAILER_MAGIC)?;
            }
        }
        Ok(())
    }
}

impl Deserializable for Marker {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        match reader.read_u8()?.try_into()? {
            Tag::Start => {
                let item_count = reader.read_u32::<BigEndian>()?;
                let seqno = reader.read_u64::<BigEndian>()?;
                Ok(Self::Start { item_count, seqno })
            }
            Tag::Item => {
                let value_type = reader.read_u8()?.into();

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
                let crc = reader.read_u32::<BigEndian>()?;

                // Check trailer
                let mut magic = [0u8; TRAILER_MAGIC.len()];
                reader.read_exact(&mut magic)?;

                if magic != TRAILER_MAGIC {
                    return Err(DeserializeError::InvalidTrailer);
                }

                Ok(Self::End(crc))
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

        // Serialize
        let mut serialized_data = Vec::new();
        item.serialize(&mut serialized_data)?;

        // Deserialize
        let mut reader = &serialized_data[..];
        let deserialized_item = Marker::deserialize(&mut reader)?;

        assert_eq!(item, deserialized_item);

        Ok(())
    }

    #[test]
    fn test_invalid_deserialize() {
        let invalid_data = [Tag::Start as u8; 1]; // Should be followed by a u32

        // Try to deserialize with invalid data
        let mut reader = &invalid_data[..];
        let result = Marker::deserialize(&mut reader);

        match result {
            Ok(_) => panic!("should error"),
            Err(error) => match error {
                DeserializeError::Io(error) => match error.kind() {
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
        let result = Marker::deserialize(&mut reader);

        match result {
            Ok(_) => panic!("should error"),
            Err(error) => match error {
                DeserializeError::InvalidTag(("JournalMarkerTag", 4)) => {}
                _ => panic!("should throw InvalidTag"),
            },
        }
    }
}
