use crate::{
    serde::{Deserializable, Serializable},
    value::UserKey,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    ops::Bound::{Excluded, Unbounded},
    sync::Arc,
};

/// A reference to a block on disk
///
/// Stores the block's position and size in bytes
///
/// # Disk representation
///
/// \[offset; 8 bytes] - \[size; 4 byte]
#[derive(Debug, PartialEq, Eq)]
pub struct DiskBlockReference {
    pub offset: u64,
    pub size: u32,
}

impl Serializable for DiskBlockReference {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), crate::SerializeError> {
        writer.write_u64::<BigEndian>(self.offset)?;
        writer.write_u32::<BigEndian>(self.size)?;

        Ok(())
    }
}

impl Deserializable for DiskBlockReference {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, crate::DeserializeError>
    where
        Self: Sized,
    {
        let offset = reader.read_u64::<BigEndian>()?;
        let size = reader.read_u32::<BigEndian>()?;

        Ok(Self { offset, size })
    }
}

/// The block index stores references to the positions of blocks on a file and their position
///
/// __________________ <- 'A': 0x0
/// |                |
/// |     BLOCK0     |
/// |________________| <- 'K': 0x...
/// |                |
/// |     BLOCK1     |
/// |________________| <- 'Z': 0x...
/// |                |
/// |     BLOCK2     |
/// |________________|
///
/// The block information can be accessed by key.
/// Because the blocks are sorted, any entries not covered by the index (it is sparse) can be
/// found by finding the highest block that has a lower key than the searched key (by performing in-memory binary search).
/// In the diagram above, searching for 'L' yields the block starting with 'K'.
/// L must be in that block, because the next block starts with 'Z').
#[allow(clippy::module_name_repetitions)]
#[derive(Default, Debug)]
pub struct DiskBlockIndex {
    pub data: BTreeMap<UserKey, DiskBlockReference>,
}

impl DiskBlockIndex {
    /// Creates a new block index
    pub fn new(data: BTreeMap<UserKey, DiskBlockReference>) -> Self {
        Self { data }
    }

    /// Returns the first key that is not covered by the given prefix anymore
    pub(crate) fn get_prefix_upper_bound(
        &self,
        prefix: &[u8],
    ) -> Option<(&UserKey, &DiskBlockReference)> {
        let key: Arc<[u8]> = prefix.into();

        let mut iter = self.data.range(key..);

        loop {
            let (key, block_ref) = iter.next()?;
            if !key.starts_with(prefix) {
                return Some((key, block_ref));
            }
        }
    }

    /// Returns the block which contains an item with a given key
    pub(crate) fn get_lower_bound_block_info(
        &self,
        key: &[u8],
    ) -> Option<(&UserKey, &DiskBlockReference)> {
        let key: Arc<[u8]> = key.into();

        self.data.range(..=key).next_back()
    }

    /// Returns the key of the first block
    pub fn get_first_block_key(&self) -> (&UserKey, &DiskBlockReference) {
        // NOTE: Index is never empty
        #[allow(clippy::unwrap_used)]
        self.data.iter().next().unwrap()
    }

    /// Returns the key of the last block
    pub fn get_last_block_key(&self) -> (&UserKey, &DiskBlockReference) {
        // NOTE: Index is never empty
        #[allow(clippy::unwrap_used)]
        self.data.iter().next_back().unwrap()
    }

    /// Returns the key of the block before the input key, if it exists, or None
    pub fn get_previous_block_key(&self, key: &[u8]) -> Option<(&UserKey, &DiskBlockReference)> {
        let key: Arc<[u8]> = key.into();

        self.data.range(..key).next_back()
    }

    /// Returns the key of the block after the input key, if it exists, or None
    pub fn get_next_block_key(&self, key: &[u8]) -> Option<(&UserKey, &DiskBlockReference)> {
        let key: Arc<[u8]> = key.into();

        self.data.range((Excluded(key), Unbounded)).next()
    }
}
