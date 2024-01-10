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

/// A reference to a block handle block on disk
///
/// Stores the block's position and size in bytes
/// The start key is stored in the in-memory search tree, see [`TopLevelIndex`] below.
///
/// # Disk representation
///
/// \[offset; 8 bytes] - \[size; 4 bytes]
//
// NOTE: Yes the name is absolutely ridiculous, but it's not the
// same as a regular BlockHandle (to a data block), because the
// start key is not required (it's already in the index, see below)
#[derive(Debug, PartialEq, Eq)]
pub struct BlockHandleBlockHandle {
    pub offset: u64,
    pub size: u32,
}

impl Serializable for BlockHandleBlockHandle {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), crate::SerializeError> {
        writer.write_u64::<BigEndian>(self.offset)?;
        writer.write_u32::<BigEndian>(self.size)?;
        Ok(())
    }
}

impl Deserializable for BlockHandleBlockHandle {
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
pub struct TopLevelIndex {
    // NOTE: UserKey is the start key of the block
    pub data: BTreeMap<UserKey, BlockHandleBlockHandle>,
}

impl TopLevelIndex {
    /// Creates a new block index
    #[must_use]
    pub fn new(data: BTreeMap<UserKey, BlockHandleBlockHandle>) -> Self {
        Self { data }
    }

    /// Returns a handle to the first block that is not covered by the given prefix anymore
    pub(crate) fn get_prefix_upper_bound(
        &self,
        prefix: &[u8],
    ) -> Option<(&UserKey, &BlockHandleBlockHandle)> {
        let key: Arc<[u8]> = prefix.into();

        let mut iter = self.data.range(key..);

        loop {
            let (key, block_handle) = iter.next()?;

            if !key.starts_with(prefix) {
                return Some((key, block_handle));
            }
        }
    }

    /// Returns a handle to the block which should contain an item with a given key
    pub(crate) fn get_block_containing_item(
        &self,
        key: &[u8],
    ) -> Option<(&UserKey, &BlockHandleBlockHandle)> {
        let key: Arc<[u8]> = key.into();
        self.data.range(..=key).next_back()
    }

    /// Returns a handle to the first block
    #[must_use]
    pub fn get_first_block_handle(&self) -> (&UserKey, &BlockHandleBlockHandle) {
        // NOTE: Index is never empty
        #[allow(clippy::expect_used)]
        self.data.iter().next().expect("index should not be empty")
    }

    /// Returns a handle to the last block
    #[must_use]
    pub fn get_last_block_handle(&self) -> (&UserKey, &BlockHandleBlockHandle) {
        // NOTE: Index is never empty
        #[allow(clippy::expect_used)]
        self.data
            .iter()
            .next_back()
            .expect("index should not be empty")
    }

    /// Returns a handle to the block before the one containing the input key, if it exists, or None
    #[must_use]
    pub fn get_previous_block_handle(
        &self,
        key: &[u8],
    ) -> Option<(&UserKey, &BlockHandleBlockHandle)> {
        let key: Arc<[u8]> = key.into();
        self.data.range(..key).next_back()
    }

    /// Returns a handle to the block after the one containing the input key, if it exists, or None
    #[must_use]
    pub fn get_next_block_handle(&self, key: &[u8]) -> Option<(&UserKey, &BlockHandleBlockHandle)> {
        let key: Arc<[u8]> = key.into();
        self.data.range((Excluded(key), Unbounded)).next()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::string_lit_as_bytes)]
mod tests {
    use super::*;
    use test_log::test;

    fn bh(offset: u64, size: u32) -> BlockHandleBlockHandle {
        BlockHandleBlockHandle { offset, size }
    }

    #[test]
    fn test_get_next_block_handle() {
        let mut index = TopLevelIndex::default();

        index.data.insert("a".as_bytes().into(), bh(0, 10));
        index.data.insert("g".as_bytes().into(), bh(10, 10));
        index.data.insert("l".as_bytes().into(), bh(20, 10));
        index.data.insert("t".as_bytes().into(), bh(30, 10));

        let (next_key, _) = index.get_next_block_handle(b"g").expect("should exist");
        assert_eq!(*next_key, "l".as_bytes().into());

        let result_without_next = index.get_next_block_handle(b"t");
        assert!(result_without_next.is_none());
    }

    #[test]
    fn test_get_previous_block_handle() {
        let mut index = TopLevelIndex::default();

        index.data.insert("a".as_bytes().into(), bh(0, 10));
        index.data.insert("g".as_bytes().into(), bh(10, 10));
        index.data.insert("l".as_bytes().into(), bh(20, 10));
        index.data.insert("t".as_bytes().into(), bh(30, 10));

        let (previous_key, _) = index.get_previous_block_handle(b"l").expect("should exist");
        assert_eq!(*previous_key, "g".as_bytes().into());

        let previous_result = index.get_previous_block_handle(b"a");
        assert!(previous_result.is_none());
    }

    #[test]
    fn test_get_first_block_handle() {
        let mut index = TopLevelIndex::default();

        index.data.insert("a".as_bytes().into(), bh(0, 10));
        index.data.insert("g".as_bytes().into(), bh(10, 10));
        index.data.insert("l".as_bytes().into(), bh(20, 10));
        index.data.insert("t".as_bytes().into(), bh(30, 10));

        let (key, _) = index.get_first_block_handle();
        assert_eq!(*key, "a".as_bytes().into());
    }

    #[test]
    fn test_get_last_block_handle() {
        let mut index = TopLevelIndex::default();

        index.data.insert("a".as_bytes().into(), bh(0, 10));
        index.data.insert("g".as_bytes().into(), bh(10, 10));
        index.data.insert("l".as_bytes().into(), bh(20, 10));
        index.data.insert("t".as_bytes().into(), bh(30, 10));

        let (key, _) = index.get_last_block_handle();
        assert_eq!(*key, "t".as_bytes().into());
    }

    #[test]

    fn test_get_block_containing_item() {
        let mut index = TopLevelIndex::default();

        index.data.insert("a".as_bytes().into(), bh(0, 10));
        index.data.insert("g".as_bytes().into(), bh(10, 10));
        index.data.insert("l".as_bytes().into(), bh(20, 10));
        index.data.insert("t".as_bytes().into(), bh(30, 10));

        for search_key in ["a", "g", "l", "t"] {
            let (key, _) = index
                .get_block_containing_item(search_key.as_bytes())
                .expect("should exist");
            assert_eq!(*key, search_key.as_bytes().into());
        }

        let (key, _) = index.get_block_containing_item(b"f").expect("should exist");
        assert_eq!(*key, "a".as_bytes().into());

        let (key, _) = index.get_block_containing_item(b"k").expect("should exist");
        assert_eq!(*key, "g".as_bytes().into());

        let (key, _) = index.get_block_containing_item(b"p").expect("should exist");
        assert_eq!(*key, "l".as_bytes().into());

        let (key, _) = index.get_block_containing_item(b"z").expect("should exist");
        assert_eq!(*key, "t".as_bytes().into());
    }

    #[test]

    fn test_get_prefix_upper_bound() {
        let mut index = TopLevelIndex::default();

        index.data.insert("a".as_bytes().into(), bh(0, 10));
        index.data.insert("abc".as_bytes().into(), bh(10, 10));
        index.data.insert("abcabc".as_bytes().into(), bh(20, 10));
        index.data.insert("abcabcabc".as_bytes().into(), bh(30, 10));
        index.data.insert("abcysw".as_bytes().into(), bh(40, 10));
        index.data.insert("basd".as_bytes().into(), bh(50, 10));
        index.data.insert("cxy".as_bytes().into(), bh(70, 10));
        index.data.insert("ewqeqw".as_bytes().into(), bh(60, 10));

        let (key, _) = index.get_prefix_upper_bound(b"a").expect("should exist");
        assert_eq!(*key, "basd".as_bytes().into());

        let (key, _) = index.get_prefix_upper_bound(b"abc").expect("should exist");
        assert_eq!(*key, "basd".as_bytes().into());

        let (key, _) = index.get_prefix_upper_bound(b"basd").expect("should exist");
        assert_eq!(*key, "cxy".as_bytes().into());

        let (key, _) = index.get_prefix_upper_bound(b"cxy").expect("should exist");
        assert_eq!(*key, "ewqeqw".as_bytes().into());

        let result = index.get_prefix_upper_bound(b"ewqeqw");
        assert!(result.is_none());
    }
}
