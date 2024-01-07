use crate::serde::{Deserializable, DeserializeError, Serializable, SerializeError};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    cmp::Reverse,
    io::{Read, Write},
    sync::Arc,
};

/// User defined key
pub type UserKey = Arc<[u8]>;

/// User defined data (blob of bytes)
pub type UserValue = Arc<[u8]>;

/// Sequence number, a monotonically increasing counter
///
/// Values with the same seqno are part of the same batch.
///
/// A value with a higher sequence number shadows an item with the
/// same key and lower sequence number. This enables MVCC.
///
/// Items are lazily garbage-bollected during compaction.
pub type SeqNo = u64;

/// Value type (regular value or tombstone)
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[allow(clippy::module_name_repetitions)]
pub enum ValueType {
    /// Existing value
    Value,

    /// Deleted value
    Tombstone,
}

impl From<u8> for ValueType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Value,
            _ => Self::Tombstone,
        }
    }
}

impl From<ValueType> for u8 {
    fn from(value: ValueType) -> Self {
        match value {
            ValueType::Value => 0,
            ValueType::Tombstone => 1,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ParsedInternalKey {
    pub user_key: UserKey,
    pub seqno: SeqNo,
    pub value_type: ValueType,
}

impl std::fmt::Debug for ParsedInternalKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{}:{}",
            self.user_key,
            self.seqno,
            u8::from(self.value_type)
        )
    }
}

impl ParsedInternalKey {
    pub fn new<K: Into<UserKey>>(user_key: K, seqno: SeqNo, value_type: ValueType) -> Self {
        Self {
            user_key: user_key.into(),
            seqno,
            value_type,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.value_type == ValueType::Tombstone
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
/// \[seqno; 8 bytes] \[tombstone; 1 byte] \[key length; 2 bytes] \[key; N bytes] \[value length; 4 bytes] \[value: N bytes]
#[derive(Clone, PartialEq, Eq)]
pub struct Value {
    /// User-defined key - an arbitrary byte array
    ///
    /// Supports up to 2^16 bytes
    pub key: UserKey,

    /// User-defined value - an arbitrary byte array
    ///
    /// Supports up to 2^32 bytes
    pub value: UserValue,

    /// Sequence number
    pub seqno: SeqNo,

    /// Tombstone marker - if this is true, the value has been deleted
    pub value_type: ValueType,
}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:{}:{} => {:?}",
            self.key,
            self.seqno,
            match self.value_type {
                ValueType::Value => "V",
                ValueType::Tombstone => "T",
            },
            self.value
        )
    }
}

impl From<(ParsedInternalKey, UserValue)> for Value {
    fn from(val: (ParsedInternalKey, UserValue)) -> Self {
        let key = val.0;

        Self {
            key: key.user_key,
            seqno: key.seqno,
            value_type: key.value_type,
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
    pub fn new<K: Into<UserKey>, V: Into<UserValue>>(
        key: K,
        value: V,
        seqno: u64,
        value_type: ValueType,
    ) -> Self {
        let k = key.into();
        let v = value.into();

        assert!(!k.is_empty());
        assert!(k.len() <= u16::MAX.into());
        assert!(u32::try_from(v.len()).is_ok());

        Self {
            key: k,
            value: v,
            value_type,
            seqno,
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn size(&self) -> usize {
        let key_size = self.key.len();
        let value_size = self.value.len();
        std::mem::size_of::<Self>() + key_size + value_size
    }

    #[doc(hidden)]
    #[must_use]
    pub fn is_tombstone(&self) -> bool {
        self.value_type == ValueType::Tombstone
    }
}

impl From<Value> for ParsedInternalKey {
    fn from(val: Value) -> Self {
        Self {
            user_key: val.key,
            seqno: val.seqno,
            value_type: val.value_type,
        }
    }
}

impl Serializable for Value {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        writer.write_u64::<BigEndian>(self.seqno)?;
        writer.write_u8(u8::from(self.value_type))?;

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
        let value_type = reader.read_u8()?.into();

        let key_len = reader.read_u16::<BigEndian>()?;
        let mut key = vec![0; key_len.into()];
        reader.read_exact(&mut key)?;

        let value_len = reader.read_u32::<BigEndian>()?;
        let mut value = vec![0; value_len as usize];
        reader.read_exact(&mut value)?;

        Ok(Self::new(key, value, seqno, value_type))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn test_empty_value() -> crate::Result<()> {
        // Create an empty Value instance
        let value = Value::new(vec![1, 2, 3], vec![], 42, ValueType::Value);

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
