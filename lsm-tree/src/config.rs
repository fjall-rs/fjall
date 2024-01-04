use crate::{descriptor_table::FileDescriptorTable, BlockCache, Tree};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum TreeType {
    Standard,
}

/// Tree configuration
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PersistedConfig {
    /// Folder path
    pub path: PathBuf, // TODO: not needed, move to Config

    /// Block size of data and index blocks
    pub block_size: u32,

    /// Amount of levels of the LSM tree (depth of tree)
    pub level_count: u8,

    /// Size ratio between levels of the LSM tree (a.k.a fanout, growth rate).
    ///
    /// This is the exponential growth of the from one
    /// level to the next
    ///
    /// A level target size is: max_memtable_size * level_ratio.pow(#level + 1)
    pub level_ratio: u8,

    r#type: TreeType,
}

const DEFAULT_FILE_FOLDER: &str = ".lsm.data";

impl Default for PersistedConfig {
    fn default() -> Self {
        Self {
            path: DEFAULT_FILE_FOLDER.into(),
            block_size: 4_096,
            level_count: 7,
            level_ratio: 8,
            r#type: TreeType::Standard,
        }
    }
}

/// Tree configuration builder
pub struct Config {
    /// Persistent configuration
    ///
    /// Once set, this configuration is permanent
    #[doc(hidden)]
    pub inner: PersistedConfig,

    /// Block cache to use
    #[doc(hidden)]
    pub block_cache: Arc<BlockCache>,

    /// Descriptor table to use
    #[doc(hidden)]
    pub descriptor_table: Arc<FileDescriptorTable>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            block_cache: Arc::new(BlockCache::with_capacity_bytes(8 * 1_024 * 1_024)),
            descriptor_table: Arc::new(FileDescriptorTable::new(960, 4)),
            inner: PersistedConfig::default(),
        }
    }
}

impl Config {
    /// Initializes a new config
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let inner = PersistedConfig {
            path: path.as_ref().into(),
            ..Default::default()
        };

        Self {
            inner,
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

        self.inner.level_count = n;
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

        self.inner.level_ratio = n;
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

        self.inner.block_size = block_size;
        self
    }

    /// Sets the block cache.
    ///
    /// You can create a global [`BlockCache`] and share it between multiple
    /// trees to cap global cache memory usage.
    ///
    /// Defaults to a block cache with 8 MiB of capacity *per tree*.
    #[must_use]
    pub fn block_cache(mut self, block_cache: Arc<BlockCache>) -> Self {
        self.block_cache = block_cache;
        self
    }

    #[must_use]
    #[doc(hidden)]
    pub fn descriptor_table(mut self, descriptor_table: Arc<FileDescriptorTable>) -> Self {
        self.descriptor_table = descriptor_table;
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
