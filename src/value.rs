use crate::serde::{Deserializable, DeserializeError, Serializable, SerializeError};
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
        // Write seqno
        writer.write_all(&self.seqno.to_be_bytes())?;

        // Write tombstone
        let tombstone = u8::from(self.is_tombstone);
        writer.write_all(&tombstone.to_be_bytes())?;

        // Write key length and key

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_all(&(self.key.len() as u16).to_be_bytes())?;
        writer.write_all(&self.key)?;

        // Write value length and value

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_all(&(self.value.len() as u32).to_be_bytes())?;
        writer.write_all(&self.value)?;

        Ok(())
    }
}

impl Deserializable for Value {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        // Read seqno
        let mut seqno_bytes = [0; std::mem::size_of::<SeqNo>()];
        reader.read_exact(&mut seqno_bytes)?;
        let seqno = SeqNo::from_be_bytes(seqno_bytes);

        // Read tombstone
        let mut tomb_bytes = [0; std::mem::size_of::<u8>()];
        reader.read_exact(&mut tomb_bytes)?;
        let tomb_byte = u8::from_be_bytes(tomb_bytes);
        let is_tombstone = tomb_byte > 0;

        // Read key length
        let mut key_len_bytes = [0; std::mem::size_of::<u16>()];
        reader.read_exact(&mut key_len_bytes)?;
        let key_len = u16::from_be_bytes(key_len_bytes) as usize;

        // Read key
        let mut key = vec![0; key_len];
        reader.read_exact(&mut key)?;

        // Read value length
        let mut value_len_bytes = [0; std::mem::size_of::<u32>()];
        reader.read_exact(&mut value_len_bytes)?;
        let value_len = u32::from_be_bytes(value_len_bytes) as usize;

        // Read value
        let mut value = vec![0; value_len];
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
