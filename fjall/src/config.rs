use crate::Keyspace;
use lsm_tree::BlockCache;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// Global keyspace configuration
pub struct Config {
    /// Base path of database
    pub(crate) path: PathBuf,

    /// Block cache that will be shared between partitions
    pub(crate) block_cache: Arc<BlockCache>,

    /// Max size of all journals in bytes
    pub(crate) max_journaling_size_in_bytes: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: ".fjall_data".into(),
            // TODO: partitions may have different block sizes... need bytes as capacity...
            block_cache: Arc::new(BlockCache::with_capacity_blocks(16 * 1_024)),
            max_journaling_size_in_bytes: /* 128 MiB */ 128 * 1_024 * 1_024,
        }
    }
}

impl Config {
    /// Creates a new configuration
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().into(),
            ..Default::default()
        }
    }

    /// Sets the block cache.
    ///
    /// You can create a global [`BlockCache`] and share it between multiple
    /// keyspaces to cap global cache memory usage.
    ///
    /// Defaults to a block cache 64 16 MiB of capacity *per keyspace*.
    #[must_use]
    pub fn block_cache(mut self, block_cache: Arc<BlockCache>) -> Self {
        self.block_cache = block_cache;
        self
    }

    /// Max size of all journals in MiB.
    ///
    /// Note: This option should be at least 24 MiB, as one journal takes up at least 16 MiB, so
    /// anything less will immediately stall the system.
    ///
    /// Default = 128 MiB
    #[must_use]
    pub fn max_journaling_size(mut self, mib: u32) -> Self {
        self.max_journaling_size_in_bytes = mib;
        self
    }

    /// Opens a keyspace using the config.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn open(self) -> crate::Result<Keyspace> {
        Keyspace::open(self)
    }
}
