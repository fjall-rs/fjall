pub mod writer;

use super::block::ValueBlock;
use crate::disk_block::{DiskBlock, Error as DiskBlockError};
use crate::disk_block_index::{DiskBlockIndex, DiskBlockReference};
use crate::serde::{Deserializable, Serializable};
use scc::HashIndex;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Points to a block on file
#[derive(Clone, Debug)]
pub struct IndexEntry {
    /// Position of block in file
    pub offset: u64,

    /// Size of block in bytes
    pub size: u32,

    /// Key of first item in block
    pub start_key: Vec<u8>,
}

impl Serializable for IndexEntry {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), crate::SerializeError> {
        writer.write_all(&self.offset.to_be_bytes())?;
        writer.write_all(&self.size.to_be_bytes())?;

        // Write key length and key

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_all(&(self.start_key.len() as u16).to_be_bytes())?;
        writer.write_all(&self.start_key)?;

        Ok(())
    }
}

impl Deserializable for IndexEntry {
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

        // Read key length
        let mut key_len_bytes = [0; std::mem::size_of::<u16>()];
        reader.read_exact(&mut key_len_bytes)?;
        let key_len = u16::from_be_bytes(key_len_bytes) as usize;

        // Read key
        let mut key = vec![0; key_len];
        reader.read_exact(&mut key)?;

        Ok(Self {
            offset,
            size,
            start_key: key,
        })
    }
}

type IndexBlock = DiskBlock<IndexEntry>;

/// In-memory index that translates item keys to block refs.
///
/// See <https://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html>
pub struct MetaIndex {
    /// Base folder path
    path: PathBuf,

    /// Level-0 index ("fence pointers"). Is read-only and always fully loaded.
    ///
    /// This index points to index blocks inside the level-1 index.
    pub index: DiskBlockIndex,

    /// Level-1 index. This index is only partially loaded into memory, decreasing memory usage, compared to a fully loaded one.
    ///
    /// However to find a disk block, one layer of indirection is required:
    ///
    /// To find a reference to a segment block, first the level-0 index needs to be checked,
    /// then the corresponding index block needs to be loaded, which contains the wanted disk block reference.
    pub blocks: HashIndex<Vec<u8>, Arc<IndexBlock>>, // TODO: change this to a struct
    // TODO: real block cache, don't do actual loading in this index
    pub real_blocks: HashIndex<Vec<u8>, Arc<ValueBlock>>,
}

impl IndexBlock {
    pub(crate) fn get_previous_block_info(&self, key: &[u8]) -> Option<&IndexEntry> {
        self.items.iter().rev().find(|x| &*x.start_key < key)
    }

    pub(crate) fn get_next_block_info(&self, key: &[u8]) -> Option<&IndexEntry> {
        self.items.iter().find(|x| &*x.start_key > key)
    }

    /// Finds the block that contains a key
    pub(crate) fn get_lower_bound_block_info(&self, key: &[u8]) -> Option<&IndexEntry> {
        self.items.iter().rev().find(|x| &*x.start_key <= key)
    }
}

impl MetaIndex {
    pub fn get_prefix_upper_bound(&self, key: &[u8]) -> Option<IndexEntry> {
        let (block_key, block_ref) = self.index.get_prefix_upper_bound(key)?;
        let index_block = self.load_index_block(block_key, block_ref);
        index_block.items.first().cloned()
    }

    pub fn get_upper_bound_block_info(&self, key: &[u8]) -> Option<IndexEntry> {
        let (block_key, block_ref) = self.index.get_lower_bound_block_info(key)?;

        let index_block = self.load_index_block(block_key, block_ref);
        let next_block = index_block.get_next_block_info(key);

        match next_block {
            Some(block) => Some(block).cloned(),
            None => {
                // The upper bound block is not in the same index block as the key, so load next index block
                let (block_key, block_ref) = self.index.get_next_block_key(key)?;

                Some(IndexEntry {
                    offset: block_ref.offset,
                    size: block_ref.size,
                    start_key: block_key.clone(),
                })
            }
        }
    }

    /// Gets the reference to a disk block that should contain the given item
    pub fn get_lower_bound_block_info(&self, key: &[u8]) -> Option<IndexEntry> {
        let (block_key, block_ref) = self.index.get_lower_bound_block_info(key)?;
        let index_block = self.load_index_block(block_key, block_ref);
        index_block.get_lower_bound_block_info(key).cloned()
    }

    /// Returns the previous index block's key, if it exists, or None
    pub fn get_previous_block_key(&self, key: &[u8]) -> Option<IndexEntry> {
        let (first_block_key, first_block_ref) = self.index.get_lower_bound_block_info(key)?;
        let index_block = self.load_index_block(first_block_key, first_block_ref);

        let maybe_prev = index_block.get_previous_block_info(key);

        match maybe_prev {
            Some(item) => Some(item).cloned(),
            None => {
                let (prev_block_key, prev_block_ref) =
                    self.index.get_previous_block_key(first_block_key)?;
                let index_block = self.load_index_block(prev_block_key, prev_block_ref);
                index_block.items.last().cloned()
            }
        }
    }

    /// Returns the next index block's key, if it exists, or None
    pub fn get_next_block_key(&self, key: &[u8]) -> Option<IndexEntry> {
        let (first_block_key, first_block_ref) = self.index.get_lower_bound_block_info(key)?;
        let index_block = self.load_index_block(first_block_key, first_block_ref);

        let maybe_next = index_block.get_next_block_info(key);

        match maybe_next {
            Some(item) => Some(item).cloned(),
            None => {
                let (next_block_key, next_block_ref) =
                    self.index.get_next_block_key(first_block_key)?;

                let index_block = self.load_index_block(next_block_key, next_block_ref);

                index_block.items.first().cloned()
            }
        }
    }

    /// Returns the first block's key
    pub fn get_first_block_key(&self) -> IndexEntry {
        let (block_key, block_ref) = self.index.get_first_block_key();
        let index_block = self.load_index_block(block_key, block_ref);
        index_block.items.first().unwrap().clone()
    }

    /// Returns the last block's key
    pub fn get_last_block_key(&self) -> IndexEntry {
        let (block_key, block_ref) = self.index.get_last_block_key();
        let index_block = self.load_index_block(block_key, block_ref);
        index_block.items.last().unwrap().clone()
    }

    /// Load an index block from disk
    fn load_index_block(
        &self,
        block_key: &[u8],
        block_ref: &DiskBlockReference,
    ) -> Arc<DiskBlock<IndexEntry>> {
        match self.blocks.get(block_key) {
            Some(block) => block.get().clone(),
            None => {
                let block = IndexBlock::from_file_compressed(
                    self.path.join("index_blocks"),
                    block_ref.offset,
                    block_ref.size,
                )
                .unwrap(); // TODO:

                let block = Arc::new(block);

                // Cache block
                self.blocks.insert(block_key.into(), Arc::clone(&block));

                block
            }
        }
    }

    /*    /// Gets the reference to a disk block that should contain the given item
    pub fn get_ref(&self, key: &[u8]) -> Option<IndexEntry> {
        let (block_key, block_ref) = self.index.get_lower_bound_block_info(key)?;
        let index_block = self.load_index_block(block_key, block_ref);
        index_block.get_lower_bound_block_info(key).cloned()
    } */

    // TODO: use this in Segment::get(_latest) instead
    // TODO: need to use prefix iterator and get last seqno
    pub fn get_latest(&self, key: &[u8]) -> Option<crate::Value> {
        let (block_key, index_block_ref) = self.index.get_lower_bound_block_info(key)?;

        let index_block = match self.blocks.get(block_key) {
            Some(block) => block.get().clone(),
            None => {
                let block = IndexBlock::from_file_compressed(
                    self.path.join("index_blocks"),
                    index_block_ref.offset,
                    index_block_ref.size,
                )
                .unwrap(); // TODO:

                let block = Arc::new(block);

                self.blocks.insert(block_key.clone(), Arc::clone(&block));

                block
            }
        };

        let block_ref = index_block.get_lower_bound_block_info(key)?;

        let real_block = match self.real_blocks.get(&block_ref.start_key) {
            Some(block) => block.get().clone(),
            None => {
                let block = ValueBlock::from_file_compressed(
                    self.path.join("blocks"),
                    block_ref.offset,
                    block_ref.size,
                )
                .unwrap(); // TODO: panic

                let block: Arc<ValueBlock> = Arc::new(block);

                self.real_blocks
                    .insert(block_ref.start_key.clone(), Arc::clone(&block));

                block
            }
        };

        /* let real_block = ValueBlock::from_file_compressed(
            self.path.join("blocks"),
            block_ref.offset,
            block_ref.size,
        )
        .unwrap(); // TODO: panic */

        let item_index = real_block.items.binary_search_by(|x| (*x.key).cmp(key));
        item_index.map_or(None, |idx| Some(real_block.items[idx].clone()))
    }

    // TODO: use this instead of from_file after writing Segment somehow...
    pub fn from_items<P: AsRef<Path>>(path: P, items: Vec<IndexEntry>) -> Self {
        let mut tree = BTreeMap::new();

        for item in items {
            tree.insert(
                item.start_key,
                DiskBlockReference {
                    offset: item.offset,
                    size: item.size,
                },
            );
        }

        Self {
            path: path.as_ref().into(),
            index: DiskBlockIndex::new(tree),
            blocks: HashIndex::default(),
            real_blocks: HashIndex::default(),
        }
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, DiskBlockError> {
        let size = std::fs::metadata(path.as_ref().join("index"))?.len();
        let index = IndexBlock::from_file_compressed(path.as_ref().join("index"), 0, size as u32)?;

        let crc = DiskBlock::create_crc(&index.items);

        // TODO: no panic
        assert!(crc == index.crc, "crc fail");

        debug_assert!(!index.items.is_empty());

        Ok(Self::from_items(path, index.items))
    }
}
