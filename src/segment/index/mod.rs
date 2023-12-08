pub mod block_handle;
pub mod top_level;
pub mod writer;

use self::block_handle::BlockHandle;
use crate::block_cache::BlockCache;
use crate::descriptor_table::FileDescriptorTable;
use crate::disk_block::DiskBlock;
use crate::file::{BLOCKS_FILE, TOP_LEVEL_INDEX_FILE};
use crate::value::UserKey;
use crate::version::Version;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use top_level::{BlockHandleBlockHandle, TopLevelIndex};

pub type BlockHandleBlock = DiskBlock<BlockHandle>;

impl BlockHandleBlock {
    pub(crate) fn get_previous_block_info(&self, key: &[u8]) -> Option<&BlockHandle> {
        self.items.iter().rev().find(|x| &*x.start_key < key)
    }

    pub(crate) fn get_next_block_info(&self, key: &[u8]) -> Option<&BlockHandle> {
        self.items.iter().find(|x| &*x.start_key > key)
    }

    /// Finds the block that contains a key
    pub(crate) fn get_lower_bound_block_info(&self, key: &[u8]) -> Option<&BlockHandle> {
        self.items.iter().rev().find(|x| &*x.start_key <= key)
    }
}

pub struct BlockHandleBlockIndex(Arc<BlockCache>);

impl BlockHandleBlockIndex {
    pub fn insert(&self, segment_id: String, key: UserKey, value: Arc<BlockHandleBlock>) {
        self.0.insert_block_handle_block(segment_id, key, value);
    }

    pub fn get(&self, segment_id: String, key: &[u8]) -> Option<Arc<BlockHandleBlock>> {
        self.0.get_block_handle_block(segment_id, key)
    }
}

/// Index that translates item keys to block handles.
///
/// The index is only partially loaded into memory.
///
/// See <https://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html>
pub struct BlockIndex {
    descriptor_table: Arc<FileDescriptorTable>,

    /// Segment ID
    segment_id: String,

    /// Level-0 index ("fence pointers"). Is read-only and always fully loaded.
    ///
    /// This index points to index blocks inside the level-1 index.
    top_level_index: TopLevelIndex,

    /// Level-1 index. This index is only partially loaded into memory, decreasing memory usage, compared to a fully loaded one.
    ///
    /// However to find a disk block, one layer of indirection is required:
    ///
    /// To find a reference to a segment block, first the level-0 index needs to be checked,
    /// then the corresponding index block needs to be loaded, which contains the wanted disk block handle.
    blocks: BlockHandleBlockIndex,
}

impl BlockIndex {
    pub fn get_prefix_upper_bound(&self, key: &[u8]) -> crate::Result<Option<BlockHandle>> {
        let Some((block_key, block_handle)) = self.top_level_index.get_prefix_upper_bound(key)
        else {
            return Ok(None);
        };

        let index_block = self.load_index_block(block_key, block_handle)?;
        Ok(index_block.items.first().cloned())
    }

    pub fn get_upper_bound_block_info(&self, key: &[u8]) -> crate::Result<Option<BlockHandle>> {
        let Some((block_key, block_handle)) = self.top_level_index.get_lower_bound_block_info(key)
        else {
            return Ok(None);
        };

        let index_block = self.load_index_block(block_key, block_handle)?;

        let next_block = index_block.get_next_block_info(key);

        match next_block {
            Some(block) => Ok(Some(block).cloned()),
            None => {
                // The upper bound block is not in the same index block as the key, so load next index block
                let Some((block_key, block_handle)) = self.top_level_index.get_next_block_key(key)
                else {
                    return Ok(None);
                };

                Ok(Some(BlockHandle {
                    offset: block_handle.offset,
                    size: block_handle.size,
                    start_key: block_key.to_vec().into(),
                }))
            }
        }
    }

    /// Gets the reference to a disk block that should contain the given item
    pub fn get_lower_bound_block_info(&self, key: &[u8]) -> crate::Result<Option<BlockHandle>> {
        let Some((block_key, block_handle)) = self.top_level_index.get_lower_bound_block_info(key)
        else {
            return Ok(None);
        };

        let index_block = self.load_index_block(block_key, block_handle)?;
        Ok(index_block.get_lower_bound_block_info(key).cloned())
    }

    /// Returns the previous index block's key, if it exists, or None
    pub fn get_previous_block_key(&self, key: &[u8]) -> crate::Result<Option<BlockHandle>> {
        let Some((first_block_key, first_block_handle)) =
            self.top_level_index.get_lower_bound_block_info(key)
        else {
            return Ok(None);
        };

        let index_block = self.load_index_block(first_block_key, first_block_handle)?;

        let maybe_prev = index_block.get_previous_block_info(key);

        match maybe_prev {
            Some(item) => Ok(Some(item).cloned()),
            None => {
                let Some((prev_block_key, prev_block_handle)) =
                    self.top_level_index.get_previous_block_key(first_block_key)
                else {
                    return Ok(None);
                };

                let index_block = self.load_index_block(prev_block_key, prev_block_handle)?;

                Ok(index_block.items.last().cloned())
            }
        }
    }

    /// Returns the next index block's key, if it exists, or None
    pub fn get_next_block_key(&self, key: &[u8]) -> crate::Result<Option<BlockHandle>> {
        let Some((first_block_key, first_block_handle)) =
            self.top_level_index.get_lower_bound_block_info(key)
        else {
            return Ok(None);
        };

        let index_block = self.load_index_block(first_block_key, first_block_handle)?;

        let maybe_next = index_block.get_next_block_info(key);

        match maybe_next {
            Some(item) => Ok(Some(item).cloned()),
            None => {
                let Some((next_block_key, next_block_handle)) =
                    self.top_level_index.get_next_block_key(first_block_key)
                else {
                    return Ok(None);
                };

                let index_block = self.load_index_block(next_block_key, next_block_handle)?;

                Ok(index_block.items.first().cloned())
            }
        }
    }

    /// Returns the first block's key
    pub fn get_first_block_key(&self) -> crate::Result<BlockHandle> {
        let (block_key, block_handle) = self.top_level_index.get_first_block_key();
        let index_block = self.load_index_block(block_key, block_handle)?;

        Ok(index_block
            .items
            .first()
            .expect("block should not be empty")
            .clone())
    }

    /// Returns the last block's key
    pub fn get_last_block_key(&self) -> crate::Result<BlockHandle> {
        let (block_key, block_handle) = self.top_level_index.get_last_block_key();
        let index_block = self.load_index_block(block_key, block_handle)?;

        Ok(index_block
            .items
            .last()
            .expect("block should not be empty")
            .clone())
    }

    /// Load an index block from disk
    fn load_index_block(
        &self,
        block_key: &[u8],
        block_handle: &BlockHandleBlockHandle,
    ) -> crate::Result<Arc<DiskBlock<BlockHandle>>> {
        match self.blocks.get(self.segment_id.clone(), block_key) {
            Some(block) => {
                // Cache hit: Copy from block

                Ok(block)
            }
            None => {
                // Cache miss: load from disk

                let mut file_reader = self.descriptor_table.access();

                let block = BlockHandleBlock::from_file_compressed(
                    &mut *file_reader,
                    block_handle.offset,
                    block_handle.size,
                )?;

                drop(file_reader);

                let block = Arc::new(block);

                self.blocks.insert(
                    self.segment_id.clone(),
                    block_key.into(),
                    Arc::clone(&block),
                );

                Ok(block)
            }
        }
    }

    pub fn get_latest<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<BlockHandle>> {
        let key = key.as_ref();

        let Some((block_key, index_block_handle)) =
            self.top_level_index.get_lower_bound_block_info(key)
        else {
            return Ok(None);
        };

        let index_block = self.load_index_block(block_key, index_block_handle)?;

        Ok(index_block.get_lower_bound_block_info(key).cloned())
    }

    // TODO: use this instead of from_file after writing Segment somehow...
    pub fn from_items(
        segment_id: String,
        descriptor_table: Arc<FileDescriptorTable>,
        items: Vec<BlockHandle>,
        block_cache: Arc<BlockCache>,
    ) -> crate::Result<Self> {
        let mut tree = BTreeMap::new();

        for item in items {
            tree.insert(
                item.start_key,
                BlockHandleBlockHandle {
                    offset: item.offset,
                    size: item.size,
                },
            );
        }

        Ok(Self {
            descriptor_table,
            segment_id,
            top_level_index: TopLevelIndex::new(tree),
            blocks: BlockHandleBlockIndex(Arc::clone(&block_cache)),
        })
    }

    /// Only used for tests
    #[allow(dead_code)]
    pub(crate) fn new(segment_id: String, block_cache: Arc<BlockCache>) -> crate::Result<Self> {
        let index_block_index = BlockHandleBlockIndex(Arc::clone(&block_cache));

        Ok(Self {
            // NOTE: It's just a test
            #[allow(clippy::expect_used)]
            descriptor_table: Arc::new(
                FileDescriptorTable::new("Cargo.toml").expect("should open"),
            ),
            segment_id,
            blocks: index_block_index,
            top_level_index: TopLevelIndex::new(BTreeMap::default()),
        })
    }

    pub fn from_file<P: AsRef<Path>>(
        segment_id: String,
        descriptor_table: Arc<FileDescriptorTable>,
        path: P,
        block_cache: Arc<BlockCache>,
    ) -> crate::Result<Self> {
        log::debug!("Reading block index from {}", path.as_ref().display());

        // TODO: change to debug asserts
        assert!(
            path.as_ref().exists(),
            "{} missing",
            path.as_ref().display()
        );
        assert!(
            path.as_ref().join(TOP_LEVEL_INDEX_FILE).exists(),
            "{} missing",
            path.as_ref().display()
        );
        assert!(
            path.as_ref().join(BLOCKS_FILE).exists(),
            "{} missing",
            path.as_ref().display()
        );

        let version_size = Version::len().into();
        let file_size = std::fs::metadata(path.as_ref().join(TOP_LEVEL_INDEX_FILE))?.len();

        let index = BlockHandleBlock::from_file_compressed(
            &mut BufReader::new(File::open(path.as_ref().join(TOP_LEVEL_INDEX_FILE))?), // TODO:
            version_size,
            (file_size - version_size) as u32,
        )?;

        /* if !index.check_crc(index.crc)? {
            return Err(crate::Error::CrcCheck);
        } */

        debug_assert!(!index.items.is_empty());

        Self::from_items(segment_id, descriptor_table, index.items, block_cache)
    }
}
