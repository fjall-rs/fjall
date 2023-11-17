use crate::serde::{Deserializable, Serializable};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    ops::Bound::{Excluded, Unbounded},
};

/// A reference to a block on disk
///
/// Stores the block's position and size in bytes
#[derive(Debug, PartialEq, Eq)]
pub struct DiskBlockReference {
    pub offset: u64,
    pub size: u32,
}

impl Serializable for DiskBlockReference {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), crate::SerializeError> {
        writer.write_all(&self.offset.to_be_bytes())?;
        writer.write_all(&self.size.to_be_bytes())?;
        Ok(())
    }
}

impl Deserializable for DiskBlockReference {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, crate::DeserializeError>
    where
        Self: Sized,
    {
        let mut offset_bytes = [0u8; std::mem::size_of::<u64>()];
        reader.read_exact(&mut offset_bytes)?;
        let offset = u64::from_be_bytes(offset_bytes);

        let mut size_bytes = [0u8; std::mem::size_of::<u32>()];
        reader.read_exact(&mut size_bytes)?;
        let size = u32::from_be_bytes(size_bytes);

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
    pub data: BTreeMap<Vec<u8>, DiskBlockReference>,
}

// TODO: not needed...? just use Vec? like in DiskBlock

impl DiskBlockIndex {
    pub fn new(data: BTreeMap<Vec<u8>, DiskBlockReference>) -> Self {
        Self { data }
    }

    /*  pub fn get(&self, key: &[u8]) -> Option<&DiskBlockReference> {
        self.data.get(key)
    } */

    /// Returns the first key that is not covered by the given prefix anymore
    /*  pub(crate) fn get_prefix_upper_bound(&self, prefix: &[u8]) -> Option<&Vec<u8>> {
        let mut iter = self.data.range(prefix.to_vec()..);

        loop {
            let (key, _) = iter.next()?;
            if !key.starts_with(prefix) {
                return Some(key);
            }
        }
    } */

    pub(crate) fn get_lower_bound_block_info(
        &self,
        key: &[u8],
    ) -> Option<(&Vec<u8>, &DiskBlockReference)> {
        self.data.range(..=key.to_vec()).next_back()
    }

    /* pub(crate) fn get_upper_bound_block_info(
        &self,
        key: &[u8],
    ) -> Option<(&Vec<u8>, &DiskBlockReference)> {
        self.data.range(key.to_vec()..).next()
    } */

    /// Returns the key of the first block
    pub fn get_first_block_key(&self) -> (&Vec<u8>, &DiskBlockReference) {
        self.data.iter().next().unwrap()
    }

    /// Returns the key of the last block
    pub fn get_last_block_key(&self) -> (&Vec<u8>, &DiskBlockReference) {
        self.data.iter().next_back().unwrap()
    }

    /// Returns the key of the block before the input key, if it exists, or None
    pub fn get_previous_block_key(&self, key: &[u8]) -> Option<(&Vec<u8>, &DiskBlockReference)> {
        self.data.range(..key.to_vec()).next_back()
    }

    /// Returns the key of the block after the input key, if it exists, or None
    pub fn get_next_block_key(&self, key: &[u8]) -> Option<(&Vec<u8>, &DiskBlockReference)> {
        self.data.range((Excluded(key.to_vec()), Unbounded)).next()
    }
}
