use crate::{
    serde::{Deserializable, DeserializeError, Serializable, SerializeError},
    Value,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

/// Commit log marker. Every batch is wrapped in a Start marker, followed by N items, followed by an end marker.
///
/// The start marker contains the numbers of items. If the numbers of items following doesn't match, the batch is broken.
///
/// The end marker contains a CRC value. If the CRC of the items doesn't match that, the batch is broken.
///
/// If a start marker is detected, while inside a batch, the batch is broken.
///
/// # Disk representation
///
/// start: \[tag (0x0); 1 byte] \[item count; 4 byte]
///
/// item: \[tag (0x1); 1 byte] \[item; (see [`Value`])]
///
/// end: \[tag (0x2): 1 byte] \[crc value; 4 byte]
#[derive(Debug, Eq, PartialEq)]
pub enum Marker {
    Start(u32),
    Item(Value),
    End(u32),
}

enum MarkerTag {
    Start = 0,
    Item = 1,
    End = 2,
}

impl TryFrom<u8> for MarkerTag {
    type Error = DeserializeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        use MarkerTag::{End, Item, Start};

        match value {
            0 => Ok(Start),
            1 => Ok(Item),
            2 => Ok(End),
            _ => Err(DeserializeError::InvalidTag(value)),
        }
    }
}

impl From<MarkerTag> for u8 {
    fn from(val: MarkerTag) -> Self {
        val as Self
    }
}

impl Serializable for Marker {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        use Marker::{End, Item, Start};

        match self {
            Start(val) => {
                writer.write_u8(MarkerTag::Start.into())?;
                writer.write_u32::<BigEndian>(*val)?;
            }
            Item(value) => {
                writer.write_u8(MarkerTag::Item.into())?;
                value.serialize(writer)?;
            }
            End(val) => {
                writer.write_u8(MarkerTag::End.into())?;
                writer.write_u32::<BigEndian>(*val)?;
            }
        }
        Ok(())
    }
}

impl Deserializable for Marker {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        match reader.read_u8()?.try_into()? {
            MarkerTag::Start => {
                let item_count = reader.read_u32::<BigEndian>()?;
                Ok(Self::Start(item_count))
            }
            MarkerTag::Item => {
                let value = Value::deserialize(reader)?;
                Ok(Self::Item(value))
            }
            MarkerTag::End => {
                let crc = reader.read_u32::<BigEndian>()?;
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
        let item = Marker::Item(Value::new(vec![1, 2, 3], vec![], false, 42));

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
        let invalid_data = [0u8; 1]; // Should be followed by a u32

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
        let invalid_data = [3u8; 1]; // Invalid tag

        // Try to deserialize with invalid data
        let mut reader = &invalid_data[..];
        let result = Marker::deserialize(&mut reader);

        match result {
            Ok(_) => panic!("should error"),
            Err(error) => match error {
                DeserializeError::InvalidTag(3) => {}
                _ => panic!("should throw InvalidTag"),
            },
        }
    }
}
