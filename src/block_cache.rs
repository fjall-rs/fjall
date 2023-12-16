use crate::segment::{block::ValueBlock, index::BlockHandleBlock};
use crate::{
    either::{
        Either,
        Either::{Left, Right},
    },
    value::UserKey,
};
use quick_cache::{sync::Cache, Equivalent};
use std::sync::Arc;

const DATA_BLOCK_TAG: u8 = 0;
const INDEX_BLOCK_TAG: u8 = 1;

type Item = Either<Arc<ValueBlock>, Arc<BlockHandleBlock>>;

// (Type (disk or index), Segment ID, Block key)
#[derive(Eq, std::hash::Hash, PartialEq)]
struct CacheKey((u8, Arc<str>, UserKey));

impl From<(u8, Arc<str>, UserKey)> for CacheKey {
    fn from(value: (u8, Arc<str>, UserKey)) -> Self {
        Self(value)
    }
}

impl std::ops::Deref for CacheKey {
    type Target = (u8, Arc<str>, UserKey);

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Equivalent<CacheKey> for (u8, &str, &UserKey) {
    fn equivalent(&self, key: &CacheKey) -> bool {
        let inner = &**key;
        self.0 == inner.0 && self.1 == &*inner.1 && self.2 == &inner.2
    }
}

/// Block cache, in which blocks are cached in-memory
/// after being retrieved from disk. This speeds up
/// consecutive queries to nearby data, improving read
/// performance for hot data.
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
    data: Cache<CacheKey, Item>,
    capacity: usize,
}

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
        segment_id: Arc<str>,
        key: UserKey,
        value: Arc<ValueBlock>,
    ) {
        if self.capacity > 0 {
            self.data
                .insert((DATA_BLOCK_TAG, segment_id, key).into(), Left(value));
        }
    }

    pub(crate) fn insert_block_handle_block(
        &self,
        segment_id: Arc<str>,
        key: UserKey,
        value: Arc<BlockHandleBlock>,
    ) {
        if self.capacity > 0 {
            self.data
                .insert((INDEX_BLOCK_TAG, segment_id, key).into(), Right(value));
        }
    }

    pub(crate) fn get_disk_block(
        &self,
        segment_id: &str,
        key: &UserKey,
    ) -> Option<Arc<ValueBlock>> {
        let key = (DATA_BLOCK_TAG, segment_id, key);
        let item = self.data.get(&key)?;
        Some(item.left().clone())
    }

    pub(crate) fn get_block_handle_block(
        &self,
        segment_id: &str,
        key: &UserKey,
    ) -> Option<Arc<BlockHandleBlock>> {
        let key = (INDEX_BLOCK_TAG, segment_id, key);
        let item = self.data.get(&key)?;
        Some(item.right().clone())
    }
}
