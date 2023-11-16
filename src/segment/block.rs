use crate::{
    serde::{Deserializable, DeserializeError, Serializable, SerializeError},
    Value,
};
use crc32fast::Hasher;
use std::io::{Read, Write};

/// Value blocks are the building blocks of a [`Segment`]. Each block is a sorted list of [`Value`]s,
/// and stored in compressed form on disk.
///
/// The integrity of a block can be checked using the CRC value that is saved in it.
#[derive(Debug)]
struct ValueBlock {
    items: Vec<Value>,
    crc: u32,
}

impl ValueBlock {
    /// Calculates the CRC from a list of values
    fn create_crc(items: &Vec<Value>) -> u32 {
        let mut hasher: Hasher = Hasher::new();

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        hasher.update(&(items.len() as u32).to_be_bytes());

        for value in items {
            let mut serialized_value = Vec::new();
            value
                .serialize(&mut serialized_value)
                .expect("should serialize into vec");
            hasher.update(&serialized_value);
        }

        hasher.finalize()
    }
}

impl Serializable for ValueBlock {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        // Write number of items

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_all(&(self.items.len() as u32).to_be_bytes())?;

        // Serialize each value
        for value in &self.items {
            value.serialize(writer)?;
        }

        // Write CRC
        writer.write_all(&self.crc.to_be_bytes())?;

        Ok(())
    }
}

impl Deserializable for ValueBlock {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        // Read number of items
        let mut item_count_bytes = [0; std::mem::size_of::<u32>()];
        reader.read_exact(&mut item_count_bytes)?;
        let item_count = u32::from_be_bytes(item_count_bytes) as usize;

        // Deserialize each value
        let mut items = Vec::with_capacity(item_count);
        for _ in 0..item_count {
            items.push(Value::deserialize(reader)?);
        }

        // Read CRC
        let mut crc_bytes = [0; std::mem::size_of::<u32>()];
        reader.read_exact(&mut crc_bytes)?;
        let expected_crc = u32::from_be_bytes(crc_bytes);

        let crc = Self::create_crc(&items);

        if crc == expected_crc {
            Ok(Self { items, crc })
        } else {
            Err(DeserializeError::CrcCheck(crc))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blocky_deserialization_success() {
        let item1 = Value {
            seqno: 42,
            key: vec![1, 2, 3],
            value: vec![4, 5, 6],
            is_tombstone: false,
        };

        let item2 = Value {
            seqno: 43,
            key: vec![7, 8, 9],
            value: vec![10, 11, 12],
            is_tombstone: false,
        };

        let items = vec![item1.clone(), item2.clone()];
        let crc = ValueBlock::create_crc(&items);

        let block = ValueBlock { items, crc };

        // Serialize to bytes
        let mut serialized = Vec::new();
        block
            .serialize(&mut serialized)
            .expect("Serialization failed");

        // Deserialize from bytes
        let deserialized = ValueBlock::deserialize(&mut &serialized[..]);

        match deserialized {
            Ok(block) => {
                assert_eq!(2, block.items.len());
                assert_eq!(block.items.get(0).cloned(), Some(item1));
                assert_eq!(block.items.get(1).cloned(), Some(item2));
                assert_eq!(crc, block.crc);
            }
            Err(err) => panic!("Deserialization failed: {err:?}"),
        }
    }

    #[test]
    fn test_blocky_deserialization_failure_crc() {
        let value1 = Value {
            seqno: 42,
            key: vec![1, 2, 3],
            value: vec![4, 5, 6],
            is_tombstone: false,
        };

        let value2 = Value {
            seqno: 43,
            key: vec![7, 8, 9],
            value: vec![10, 11, 12],
            is_tombstone: false,
        };

        let block = ValueBlock {
            items: vec![value1, value2],
            crc: 12345,
        };

        // Serialize to bytes
        let mut serialized = Vec::new();
        block
            .serialize(&mut serialized)
            .expect("Serialization failed");

        // Deserialize from bytes
        let deserialized = ValueBlock::deserialize(&mut &serialized[..]);

        // Check if deserialization fails due to CRC mismatch
        match deserialized {
            Err(DeserializeError::CrcCheck(_)) => {}
            _ => panic!("Unexpected deserialization error"),
        }
    }
}
