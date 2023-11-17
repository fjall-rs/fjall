use crate::serde::{Deserializable, DeserializeError, Serializable, SerializeError};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

pub type SeqNo = u64;

/// Represents a value in the LSM-tree
///
/// `key` and `value` are arbitrary user-defined byte arrays
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Value {
    /// User-defined key - an arbitrary byte array
    ///
    /// Supports up to 2^16 bytes
    pub key: Vec<u8>,

    /// User-defined value - an arbitrary byte array
    ///
    /// Supports up to 2^32 bytes
    pub value: Vec<u8>,

    /// Sequence number
    pub seqno: SeqNo,

    /// Tombstone marker - if this is true, the value has been deleted
    pub is_tombstone: bool,
}

impl Value {
    /// Creates a new [`Value`].
    ///
    /// # Panics
    ///
    /// Panics if the key length is empty or greater than 2^16, or the value length is greater than 2^32
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Value;
    /// #
    /// let value = Value::new(*b"key-1", *b"my-value", false, 5);
    /// assert_eq!(b"key-1", &*value.key);
    /// assert_eq!(b"my-value", &*value.value);
    /// assert_eq!(5, value.seqno);
    /// assert_eq!(false, value.is_tombstone);
    ///
    /// ```
    pub fn new<K: Into<Vec<u8>>, V: Into<Vec<u8>>>(
        key: K,
        value: V,
        is_tombstone: bool,
        seqno: u64,
    ) -> Self {
        let k = key.into();
        let v = value.into();

        assert!(!k.is_empty());
        assert!(k.len() <= u16::MAX.into());
        assert!(u32::try_from(v.len()).is_ok());

        Self {
            key: k,
            value: v,
            is_tombstone,
            seqno,
        }
    }

    #[must_use]
    #[doc(hidden)]
    pub fn size(&self) -> usize {
        let key_size = self.key.len();
        let value_size = self.value.len();
        std::mem::size_of::<Self>() + key_size + value_size
    }

    /// Computes the internal key based on the user key + seqno
    ///
    /// ### Example
    ///
    /// ```
    /// # use lsm_tree::Value;
    /// #
    /// let value = Value::new(*b"abc", *b"my-value", false, 5);
    /// assert_eq!(&[0x61, 0x62, 0x63, 0, 0, 0, 0, 0, 0, 0, 5], &*value.get_internal_key());
    /// ```
    #[must_use]
    #[doc(hidden)]
    pub fn get_internal_key(&self) -> Vec<u8> {
        let mut internal_key = Vec::with_capacity(self.key.len() + std::mem::size_of::<u64>());
        internal_key.extend_from_slice(&self.key);
        internal_key.extend_from_slice(&self.seqno.to_be_bytes());
        internal_key
    }
}

impl Serializable for Value {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        writer.write_u64::<BigEndian>(self.seqno)?;
        writer.write_u8(u8::from(self.is_tombstone))?;

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u16::<BigEndian>(self.key.len() as u16)?;
        writer.write_all(&self.key)?;

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u32::<BigEndian>(self.value.len() as u32)?;
        writer.write_all(&self.value)?;

        Ok(())
    }
}

impl Deserializable for Value {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let seqno = reader.read_u64::<BigEndian>()?;
        let is_tombstone = reader.read_u8()? > 0;

        let key_len = reader.read_u16::<BigEndian>()?;
        let mut key = vec![0; key_len.into()];
        reader.read_exact(&mut key)?;

        let value_len = reader.read_u32::<BigEndian>()?;
        let mut value = vec![0; value_len as usize];
        reader.read_exact(&mut value)?;

        Ok(Self::new(key, value, is_tombstone, seqno))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn test_empty_value() {
        // Create an empty Value instance
        let value = Value::new(vec![1, 2, 3], vec![], false, 42);

        // Serialize the empty Value
        let mut serialized = Vec::new();
        value
            .serialize(&mut serialized)
            .expect("Serialization failed");

        // Deserialize the empty Value
        let deserialized = Value::deserialize(&mut &serialized[..]);

        // Check if deserialization is successful
        assert!(deserialized.is_ok());

        // Check if deserialized Value is equivalent to the original empty Value
        let deserialized = deserialized.expect("Deserialization failed");
        assert_eq!(value, deserialized);
    }
}
