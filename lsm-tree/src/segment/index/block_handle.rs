use crate::serde::{Deserializable, Serializable};
use crate::value::UserKey;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};
use std::sync::Arc;

/// Points to a block on file
///
/// # Disk representation
///
/// \[offset; 8 bytes] - \[size; 4 bytes] - \[key length; 2 bytes] - \[key; N bytes]
#[derive(Clone, Debug)]
pub struct BlockHandle {
    /// Key of first item in block
    pub start_key: UserKey,

    /// Position of block in file
    pub offset: u64,

    /// Size of block in bytes
    pub size: u32,
}

impl Serializable for BlockHandle {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), crate::SerializeError> {
        writer.write_u64::<BigEndian>(self.offset)?;
        writer.write_u32::<BigEndian>(self.size)?;

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u16::<BigEndian>(self.start_key.len() as u16)?;

        writer.write_all(&self.start_key)?;

        Ok(())
    }
}

impl Deserializable for BlockHandle {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, crate::DeserializeError>
    where
        Self: Sized,
    {
        let offset = reader.read_u64::<BigEndian>()?;
        let size = reader.read_u32::<BigEndian>()?;

        let key_len = reader.read_u16::<BigEndian>()?;

        let mut key = vec![0; key_len.into()];
        reader.read_exact(&mut key)?;

        Ok(Self {
            offset,
            size,
            start_key: Arc::from(key),
        })
    }
}
