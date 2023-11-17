use crate::serde::{Deserializable, DeserializeError, Serializable, SerializeError};
use lz4_flex::decompress_size_prepended;
use std::{
    fs::File,
    io::{BufReader, Cursor, Read, Seek, Write},
    path::Path,
};

/// Contains the items of a block after decompressing & deserializing.
///
/// The integrity of a block can be checked using the CRC value that is saved in it.
#[derive(Clone, Debug)]
pub struct DiskBlock<T: Clone + Serializable + Deserializable> {
    pub(crate) items: Vec<T>,
    pub(crate) crc: u32,
}

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Deserialize(DeserializeError),
    Decompress,
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<DeserializeError> for Error {
    fn from(value: DeserializeError) -> Self {
        Self::Deserialize(value)
    }
}

impl<T: Clone + Serializable + Deserializable> DiskBlock<T> {
    pub fn from_reader_compressed<R: Read>(
        reader: &mut R,
        offset: u64,
        size: u32,
    ) -> Result<Self, Error> {
        let mut bytes = vec![0u8; size as usize];
        reader.read_exact(&mut bytes)?;

        let bytes = decompress_size_prepended(&bytes).map_err(|_| Error::Decompress)?;
        let mut bytes = Cursor::new(bytes);

        let block = Self::deserialize(&mut bytes)?;
        Ok(block)
    }

    pub fn from_file_compressed<P: AsRef<Path>>(
        path: P,
        offset: u64,
        size: u32,
    ) -> Result<Self, Error> {
        // Read bytes from disk
        let mut reader = BufReader::new(File::open(path)?);
        reader.seek(std::io::SeekFrom::Start(offset))?;
        Self::from_reader_compressed(&mut reader, offset, size)
    }
}

impl<T: Clone + Serializable + Deserializable> DiskBlock<T> {
    /// Calculates the CRC from a list of values
    pub(crate) fn create_crc(items: &Vec<T>) -> u32 {
        let mut hasher = crc32fast::Hasher::new();

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

impl<T: Clone + Serializable + Deserializable> Serializable for DiskBlock<T> {
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

impl<T: Clone + Serializable + Deserializable> Deserializable for DiskBlock<T> {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        // Read number of items
        let mut item_count_bytes = [0; std::mem::size_of::<u32>()];
        reader.read_exact(&mut item_count_bytes)?;
        let item_count = u32::from_be_bytes(item_count_bytes) as usize;

        // Deserialize each value
        let mut items = Vec::with_capacity(item_count);
        for _ in 0..item_count {
            items.push(T::deserialize(reader)?);
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
    use crate::Value;
    use test_log::test;

    #[test]
    fn test_blocky_deserialization_success() {
        let item1 = Value::new(vec![1, 2, 3], vec![4, 5, 6], false, 42);
        let item2 = Value::new(vec![7, 8, 9], vec![10, 11, 12], false, 43);

        let items = vec![item1.clone(), item2.clone()];
        let crc = DiskBlock::create_crc(&items);

        let block = DiskBlock { items, crc };

        // Serialize to bytes
        let mut serialized = Vec::new();
        block
            .serialize(&mut serialized)
            .expect("Serialization failed");

        // Deserialize from bytes
        let deserialized = DiskBlock::deserialize(&mut &serialized[..]);

        match deserialized {
            Ok(block) => {
                assert_eq!(2, block.items.len());
                assert_eq!(block.items.get(0).cloned(), Some(item1));
                assert_eq!(block.items.get(1).cloned(), Some(item2));
                assert_eq!(crc, block.crc);
            }
            Err(error) => panic!("Deserialization failed: {error:?}"),
        }
    }

    #[test]
    fn test_blocky_deserialization_failure_crc() {
        let item1 = Value::new(vec![1, 2, 3], vec![4, 5, 6], false, 42);
        let item2 = Value::new(vec![7, 8, 9], vec![10, 11, 12], false, 43);

        let block = DiskBlock {
            items: vec![item1, item2],
            crc: 12345,
        };

        // Serialize to bytes
        let mut serialized = Vec::new();
        block
            .serialize(&mut serialized)
            .expect("Serialization failed");

        // Deserialize from bytes
        let deserialized = DiskBlock::<Value>::deserialize(&mut &serialized[..]);

        // Check if deserialization fails due to CRC mismatch
        match deserialized {
            Err(DeserializeError::CrcCheck(_)) => {}
            _ => panic!("Unexpected deserialization error"),
        }
    }
}
