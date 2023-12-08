use crate::segment::{block::ValueBlock, index::BlockHandleBlock};
use crate::{
    either::{
        Either,
        Either::{Left, Right},
    },
    value::UserKey,
};
use quick_cache::sync::Cache;
use std::sync::Arc;

// Type (disk or index), Segment ID, Block key
type Key = (u8, String, UserKey);
type Item = Either<Arc<ValueBlock>, Arc<BlockHandleBlock>>;

/// Block cache
///
/// # Examples
///
/// Sharing block cache between multiple trees
///
/// ```
/// use lsm_tree::{Tree, Config, BlockCache};
/// use std::sync::Arc;
///
/// // Provide 10'000 blocks (10'000 * 4 KiB = 40 MB) of cache capacity
/// let block_cache = Arc::new(BlockCache::with_capacity_blocks(10_000));
///
/// # let folder = tempfile::tempdir()?;
/// let tree1 = Config::new(folder).block_cache(block_cache.clone()).open()?;
/// # let folder = tempfile::tempdir()?;
/// let tree2 = Config::new(folder).block_cache(block_cache.clone()).open()?;
/// #
/// # Ok::<(), lsm_tree::Error>(())
/// ```
pub struct BlockCache {
    data: Cache<Key, Item>,
    capacity: usize,
}

const DATA_BLOCK_TAG: u8 = 0;
const INDEX_BLOCK_TAG: u8 = 0;

impl BlockCache {
    /// Creates a new block cache with roughly `n` blocks of capacity
    ///
    /// Multiply n by the block size to get the approximate byte count
    #[must_use]
    pub fn with_capacity_blocks(n: usize) -> Self {
        Self {
            data: Cache::new(n),
            capacity: n,
        }
    }

    /// Returns the number of cached blocks
    ///
    /// Multiply n by the block size to get the approximate byte count
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if there are no cached blocks
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    ///
    pub(crate) fn insert_disk_block(
        &self,
        segment_id: String,
        key: UserKey,
        value: Arc<ValueBlock>,
    ) {
        if self.capacity > 0 {
            self.data
                .insert((DATA_BLOCK_TAG, segment_id, key), Left(value));
        }
    }

    pub(crate) fn insert_block_handle_block(
        &self,
        segment_id: String,
        key: UserKey,
        value: Arc<BlockHandleBlock>,
    ) {
        if self.capacity > 0 {
            self.data
                .insert((INDEX_BLOCK_TAG, segment_id, key), Right(value));
        }
    }

    pub(crate) fn get_disk_block(
        &self,
        segment_id: String,
        key: &UserKey,
    ) -> Option<Arc<ValueBlock>> {
        let key = (DATA_BLOCK_TAG, segment_id, key.clone());
        let item = self.data.get(&key)?;
        Some(item.left().clone())
    }

    pub(crate) fn get_block_handle_block(
        &self,
        segment_id: String,
        key: &UserKey,
    ) -> Option<Arc<BlockHandleBlock>> {
        let key = (INDEX_BLOCK_TAG, segment_id, key.clone());
        let item = self.data.get(&key)?;
        Some(item.right().clone())
    }
}
