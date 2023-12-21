use crate::{
    compaction::{self, CompactionStrategy},
    BlockCache, Tree,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone)]
/// Tree configuration
pub struct Config {
    /// Folder path
    pub path: PathBuf,

    /// Block size of data and index blocks
    pub block_size: u32,

    /// Block cache
    pub block_cache: Arc<BlockCache>,

    /// Amount of levels of the LSM tree (depth of tree)
    pub level_count: u8,

    /// Size ratio between levels of the LSM tree (a.k.a fanout, growth rate).
    ///
    /// This is the exponential growth of the from one
    /// level to the next
    ///
    /// A level target size is: max_memtable_size * level_ratio.pow(#level + 1)
    pub level_ratio: u8,

    /// Compaction strategy to use
    pub(crate) compaction_strategy: Arc<dyn CompactionStrategy + Send + Sync>,
    // TODO: 0.3.0 remove strategy from Config
}

const DEFAULT_FILE_FOLDER: &str = ".lsm.data";

impl Default for Config {
    fn default() -> Self {
        Self {
            path: DEFAULT_FILE_FOLDER.into(),
            block_size: 4_096,
            block_cache: Arc::new(BlockCache::with_capacity_blocks(4_096)),
            level_count: 7,
            level_ratio: 8,
            compaction_strategy: Arc::new(compaction::Levelled::default()),
        }
    }
}

impl Config {
    /// Initializes a new config
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().into(),
            ..Default::default()
        }
    }

    /// Sets the amount of levels of the LSM tree (depth of tree).
    ///
    /// Defaults to 7, like `LevelDB` and `RocksDB`.
    ///
    /// # Panics
    ///
    /// Panics if `n` is 0.
    #[must_use]
    pub fn level_count(mut self, n: u8) -> Self {
        assert!(n > 0);

        self.level_count = n;
        self
    }

    /// Sets the size ratio between levels of the LSM tree (a.k.a. fanout, growth rate).
    ///
    /// Defaults to 10.
    ///
    /// # Panics
    ///
    /// Panics if `n` is less than 2.
    #[must_use]
    pub fn level_ratio(mut self, n: u8) -> Self {
        assert!(n > 1);

        self.level_ratio = n;
        self
    }

    /// Sets the block size.
    ///
    /// Defaults to 4 KiB (4096 bytes).
    ///
    /// # Panics
    ///
    /// Panics if the block size is smaller than 1 KiB (1024 bytes).
    #[must_use]
    pub fn block_size(mut self, block_size: u32) -> Self {
        assert!(block_size >= 1024);

        self.block_size = block_size;
        self
    }

    /// Sets the block cache.
    ///
    /// You can create a global [`BlockCache`] and share it between multiple
    /// trees to cap global cache memory usage.
    ///
    /// Defaults to a block cache with 16 MiB of capacity *per tree*.
    #[must_use]
    pub fn block_cache(mut self, block_cache: Arc<BlockCache>) -> Self {
        self.block_cache = block_cache;
        self
    }

    /// Sets the compaction strategy to use.
    ///
    /// Defaults to [`compaction::Levelled`]
    #[must_use]
    pub fn compaction_strategy(
        mut self,
        strategy: Arc<dyn CompactionStrategy + Send + Sync>,
    ) -> Self {
        self.compaction_strategy = strategy;
        self
    }

    /// Opens a tree using the config.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn open(self) -> crate::Result<Tree> {
        Tree::open(self)
    }
}
