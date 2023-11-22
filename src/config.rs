use std::path::{Path, PathBuf};

use crate::Tree;

/// Tree configuration
pub struct Config {
    /// Folder path
    ///
    /// Defaults to `./.lsm.data`
    pub(crate) path: PathBuf,

    /// Block size of data and index blocks
    ///
    /// Defaults to 4 KiB (4096 bytes)
    pub(crate) block_size: u32,

    /// Block cache size in block
    ///
    /// Defaults to 1,000
    pub(crate) block_cache_size: u32,

    /// [`MemTable`] maximum size in bytes
    ///
    /// Defaults to 128 MiB
    pub(crate) max_memtable_size: u32,

    /// Amount of levels of the LSM tree (depth of tree)
    ///
    /// Defaults to 7, like RocksDB
    pub(crate) levels: u8,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: ".lsm.data".into(),
            block_size: 4_096,
            block_cache_size: 1_000,
            max_memtable_size: 128 * 1_024 * 1_024,
            levels: 7,
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

    /// Sets the compaction strategy to use (default: Tiered compaction)
    #[must_use]
    pub fn level_count(mut self, count: u8) -> Self {
        self.levels = count;
        self
    }

    /// Sets the maximum memtable size (default: 128 MiB)
    #[must_use]
    pub fn max_memtable_size(mut self, bytes: u32) -> Self {
        self.max_memtable_size = bytes;
        self
    }

    /// Sets the block size
    ///
    /// # Panics
    ///
    /// Panics if the block size is smaller than 1 KiB (1024 bytes)
    #[must_use]
    pub fn block_size(mut self, block_size: u32) -> Self {
        assert!(block_size >= 1024);

        self.block_size = block_size;
        self
    }

    /// Sets the block cache size
    #[must_use]
    pub fn block_cache_size(mut self, block_cache_size: u32) -> Self {
        self.block_cache_size = block_cache_size;
        self
    }

    /// Opens a tree using the config
    ///
    /// # Errors
    ///
    /// - Will return `Err` if an IO error occurs
    pub fn open(self) -> crate::Result<Tree> {
        Tree::open(self)
    }
}
