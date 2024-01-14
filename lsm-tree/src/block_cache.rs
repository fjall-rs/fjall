use crate::segment::index::block_handle::BlockHandle;
use crate::segment::{block::ValueBlock, index::BlockHandleBlock};
use crate::{
    either::{
        Either,
        Either::{Left, Right},
    },
    value::UserKey,
};
use quick_cache::Weighter;
use quick_cache::{sync::Cache, Equivalent};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum BlockTag {
    Data = 0,
    Index = 1,
}

type Item = Either<Arc<ValueBlock>, Arc<BlockHandleBlock>>;

// (Type (disk or index), Segment ID, Block key)
#[derive(Eq, std::hash::Hash, PartialEq)]
struct CacheKey((BlockTag, Arc<str>, UserKey));

impl From<(BlockTag, Arc<str>, UserKey)> for CacheKey {
    fn from(value: (BlockTag, Arc<str>, UserKey)) -> Self {
        Self(value)
    }
}

impl std::ops::Deref for CacheKey {
    type Target = (BlockTag, Arc<str>, UserKey);

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Equivalent<CacheKey> for (BlockTag, &str, &UserKey) {
    fn equivalent(&self, key: &CacheKey) -> bool {
        let inner = &**key;
        self.0 == inner.0 && self.1 == &*inner.1 && self.2 == &inner.2
    }
}

#[derive(Clone)]
struct BlockWeighter;

impl Weighter<CacheKey, Item> for BlockWeighter {
    fn weight(&self, _: &CacheKey, block: &Item) -> u32 {
        // NOTE: Truncation is fine: blocks are ~64K max
        #[allow(clippy::cast_possible_truncation)]
        match block {
            Either::Left(block) => block.size() as u32,
            Either::Right(block) => block
                .items
                .iter()
                .map(|x| x.start_key.len() + std::mem::size_of::<BlockHandle>())
                .sum::<usize>() as u32,
        }
    }
}

/// Block cache, in which blocks are cached in-memory
/// after being retrieved from disk
///
/// This speeds up consecutive queries to nearby data, improving
/// read performance for hot data.
///
/// # Examples
///
/// Sharing block cache between multiple trees
///
/// ```
/// # use lsm_tree::{Tree, Config, BlockCache};
/// # use std::sync::Arc;
/// #
/// // Provide 10'000 blocks 40 MB of cache capacity
/// let block_cache = Arc::new(BlockCache::with_capacity_bytes(40 * 1_000 * 1_000));
///
/// # let folder = tempfile::tempdir()?;
/// let tree1 = Config::new(folder).block_cache(block_cache.clone()).open()?;
/// # let folder = tempfile::tempdir()?;
/// let tree2 = Config::new(folder).block_cache(block_cache.clone()).open()?;
/// #
/// # Ok::<(), lsm_tree::Error>(())
/// ```
pub struct BlockCache {
    data: Cache<CacheKey, Item, BlockWeighter>,
    capacity: u64,
}

impl BlockCache {
    /// Creates a new block cache with roughly `n` blocks of capacity
    ///
    /// Multiply n by the block size to get the approximate byte count
    #[must_use]
    pub fn with_capacity_bytes(bytes: u64) -> Self {
        Self {
            data: Cache::with_weighter(10_000, bytes, BlockWeighter),
            capacity: bytes,
        }
    }

    /// Returns the cache capacity in blocks
    ///
    /// Multiply n by the block size to get the approximate byte count
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.capacity
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

    #[doc(hidden)]
    pub fn insert_disk_block(&self, segment_id: Arc<str>, key: UserKey, value: Arc<ValueBlock>) {
        if self.capacity > 0 {
            self.data
                .insert((BlockTag::Data, segment_id, key).into(), Left(value));
        }
    }

    #[doc(hidden)]
    pub fn insert_block_handle_block(
        &self,
        segment_id: Arc<str>,
        key: UserKey,
        value: Arc<BlockHandleBlock>,
    ) {
        if self.capacity > 0 {
            self.data
                .insert((BlockTag::Index, segment_id, key).into(), Right(value));
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_disk_block(&self, segment_id: &str, key: &UserKey) -> Option<Arc<ValueBlock>> {
        let key = (BlockTag::Data, segment_id, key);
        let item = self.data.get(&key)?;
        Some(item.left().clone())
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_block_handle_block(
        &self,
        segment_id: &str,
        key: &UserKey,
    ) -> Option<Arc<BlockHandleBlock>> {
        let key = (BlockTag::Index, segment_id, key);
        let item = self.data.get(&key)?;
        Some(item.right().clone())
    }
}
