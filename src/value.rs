use crate::serde::{Deserializable, DeserializeError, Serializable, SerializeError};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    cmp::Reverse,
    io::{Read, Write},
    sync::Arc,
};

/// User defined data
pub type UserKey = Arc<[u8]>;

/// User defined data (blob of bytes)
pub type UserData = Arc<[u8]>;

/// Sequence number
pub type SeqNo = u64;

#[derive(Clone, PartialEq, Eq)]
pub struct ParsedInternalKey {
    pub user_key: UserKey,
    pub seqno: SeqNo,
    pub is_tombstone: bool,
}

impl std::fmt::Debug for ParsedInternalKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{}:{}",
            self.user_key,
            self.seqno,
            u8::from(self.is_tombstone)
        )
    }
}

impl ParsedInternalKey {
    pub fn new<K: Into<UserKey>>(user_key: K, seqno: SeqNo, is_tombstone: bool) -> Self {
        Self {
            user_key: user_key.into(),
            seqno,
            is_tombstone,
        }
    }
}

impl PartialOrd for ParsedInternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Order by user key, THEN by sequence number
// This is one of the most important functions
// Otherwise queries will not match expected behaviour
impl Ord for ParsedInternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.user_key, Reverse(self.seqno)).cmp(&(&other.user_key, Reverse(other.seqno)))
    }
}

/// Represents a value in the LSM-tree
///
/// `key` and `value` are arbitrary user-defined byte arrays
///
/// # Disk representation
///
/// \[seqno; 8 bytes] \[tombstone; 1 byte] \[key length; 2 bytes] \[key; N bytes] \[value length; 2 bytes] \[value: N bytes]
#[derive(Clone, PartialEq, Eq)]
pub struct Value {
    /// User-defined key - an arbitrary byte array
    ///
    /// Supports up to 2^16 bytes
    pub key: UserKey,

    /// User-defined value - an arbitrary byte array
    ///
    /// Supports up to 2^16 bytes
    pub value: UserData,

    /// Sequence number
    pub seqno: SeqNo,

    /// Tombstone marker - if this is true, the value has been deleted
    pub is_tombstone: bool,
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{}:{} => {:?}",
            self.key,
            self.seqno,
            u8::from(self.is_tombstone),
            self.value
        )
    }
}

impl From<(ParsedInternalKey, UserData)> for Value {
    fn from(val: (ParsedInternalKey, UserData)) -> Self {
        let key = val.0;

        Self {
            key: key.user_key,
            seqno: key.seqno,
            is_tombstone: key.is_tombstone,
            value: val.1,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Order by user key, THEN by sequence number
// This is one of the most important functions
// Otherwise queries will not match expected behaviour
impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.key, Reverse(self.seqno)).cmp(&(&other.key, Reverse(other.seqno)))
    }
}

impl Value {
    /// Creates a new [`Value`].
    ///
    /// # Panics
    ///
    /// Panics if the key length is empty or greater than 2^16, or the value length is greater than 2^16
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Value;
    /// #
    /// let value = Value::new("key-1".as_bytes(), "my-value".as_bytes(), false, 5);
    /// assert_eq!(b"key-1", &*value.key);
    /// assert_eq!(b"my-value", &*value.value);
    /// assert_eq!(5, value.seqno);
    /// assert_eq!(false, value.is_tombstone);
    ///
    /// ```
    pub fn new<K: Into<UserKey>, V: Into<UserData>>(
        key: K,
        value: V,
        is_tombstone: bool,
        seqno: u64,
    ) -> Self {
        let k = key.into();
        let v = value.into();

        assert!(!k.is_empty());
        assert!(k.len() <= u16::MAX.into());
        assert!(v.len() <= u16::MAX.into());

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
}

impl From<Value> for ParsedInternalKey {
    fn from(val: Value) -> Self {
        Self {
            user_key: val.key,
            seqno: val.seqno,
            is_tombstone: val.is_tombstone,
        }
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
        writer.write_u16::<BigEndian>(self.value.len() as u16)?;
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

        let value_len = reader.read_u16::<BigEndian>()?;
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
    fn test_empty_value() -> crate::Result<()> {
        // Create an empty Value instance
        let value = Value::new(vec![1, 2, 3], vec![], false, 42);

        // Serialize the empty Value
        let mut serialized = Vec::new();
        value.serialize(&mut serialized)?;

        // Deserialize the empty Value
        let deserialized = Value::deserialize(&mut &serialized[..])?;

        // Check if deserialized Value is equivalent to the original empty Value
        assert_eq!(value, deserialized);

        Ok(())
    }
}
