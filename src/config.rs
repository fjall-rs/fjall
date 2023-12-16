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

    /// Maximum size in bytes of the write buffer
    pub max_memtable_size: u32,

    /// Amount of levels of the LSM tree (depth of tree)
    pub level_count: u8,

    /// Size ratio between levels of the LSM tree (a.k.a fanout, growth rate).
    ///
    /// This is the exponential growth of the from one
    /// level to the next
    ///
    /// A level target size is: max_memtable_size * level_ratio.pow(#level + 1)
    pub level_ratio: u8,

    /// Maximum amount of concurrent flush threads
    pub flush_threads: u8,

    /// Starts a thread that will periodically fsync the journals for durability
    pub fsync_ms: Option<usize>,

    /// Compaction strategy to use
    pub(crate) compaction_strategy: Arc<dyn CompactionStrategy + Send + Sync>,
}

const DEFAULT_FILE_FOLDER: &str = ".lsm.data";

impl Default for Config {
    fn default() -> Self {
        Self {
            path: DEFAULT_FILE_FOLDER.into(),
            block_size: 4_096,
            block_cache: Arc::new(BlockCache::with_capacity_blocks(16_384)),
            max_memtable_size: 16 * 1_024 * 1_024,
            level_count: 7,
            level_ratio: 8,
            compaction_strategy: Arc::new(compaction::Levelled::default()),
            flush_threads: 4,
            fsync_ms: Some(1_000),
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

    /// Starts a thread that will periodically fsync the journal for durability.
    ///
    /// That means in case of a fatal crash (not proper unwind) at most the last `ms` of data may be lost.
    /// Without fsyncing, your data is at the mercy of your operating system's syncing logic.
    /// If you want to make sure a write is definitely durable, call [`Tree::flush`] manually after writing.
    /// Flushing after every write has dramatic performance implications (100x-1000x slower for SSDs).
    /// Even when disabled, the tree will always try to fsync when it is being dropped.
    ///
    /// Defaults to 1 second.
    ///
    /// # Panics
    ///
    /// Panics if ms is below 100.
    #[must_use]
    pub fn fsync_ms(mut self, ms: Option<usize>) -> Self {
        if let Some(ms) = ms {
            assert!(ms >= 100);
        }

        self.fsync_ms = ms;
        self
    }

    /// Maximum amount of concurrent flush threads.
    ///
    /// You may want to increase this the more CPU cores you have.
    ///
    /// Defaults to 4.
    ///
    /// # Panics
    ///
    /// Panics if count is 0.
    #[must_use]
    pub fn flush_threads(mut self, count: u8) -> Self {
        assert!(count > 0);

        self.flush_threads = count;
        self
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

    /// Sets the maximum memtable size.
    ///
    /// Defaults to 16 MiB.
    #[must_use]
    pub fn max_memtable_size(mut self, bytes: u32) -> Self {
        self.max_memtable_size = bytes;
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
    /// Defaults to a block cache with 64 MiB of capacity.
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
