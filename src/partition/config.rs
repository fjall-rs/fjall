use lsm_tree::{CompressionType, TreeType};

/// Options to configure a partition
pub struct CreateOptions {
    /// Block size of data and index blocks.
    pub(crate) block_size: u32,

    /// Amount of levels of the LSM tree (depth of tree).
    pub(crate) level_count: u8,

    /// Size ratio between levels of the LSM tree (a.k.a fanout, growth rate).
    ///
    /// This is the exponential growth of the from one
    /// level to the next
    ///
    /// A level target size is: `max_memtable_size * level_ratio.pow(#level + 1)`
    pub(crate) level_ratio: u8,

    /// Tree type, see [`TreeType`].
    pub(crate) tree_type: TreeType,

    /// Compression to use.
    pub(crate) compression: CompressionType,
}

impl Default for CreateOptions {
    fn default() -> Self {
        let default_tree_config = lsm_tree::Config::default();

        Self {
            block_size: default_tree_config.inner.block_size,
            level_count: default_tree_config.inner.level_count,
            level_ratio: default_tree_config.inner.level_ratio,
            tree_type: TreeType::Standard,

            #[cfg(feature = "lz4")]
            compression: CompressionType::Lz4,

            #[cfg(all(feature = "miniz", not(feature = "lz4")))]
            compression: CompressionType::Miniz(6),

            #[cfg(not(any(feature = "lz4", feature = "miniz")))]
            compression: CompressionType::None,
        }
    }
}

impl CreateOptions {
    /// Sets the compression method.
    ///
    /// Once set for a partition, this property is not considered in the future.
    ///
    /// Default = In order: Lz4 -> Miniz -> None, depending on compilation flags
    #[must_use]
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    /// Sets the block size.
    ///
    /// Once set for a partition, this property is not considered in the future.
    ///
    /// Default = 4 KiB
    ///
    /// For point read heavy workloads (get) a sensible default is
    /// somewhere between 4 - 8 KiB, depending on the average value size.
    ///
    /// For scan heavy workloads (range, prefix), use 16 - 64 KiB
    /// which also increases compression efficiency.
    ///
    /// # Panics
    ///
    /// Panics if the block size is smaller than 1 KiB (1024 bytes).
    #[must_use]
    pub fn block_size(mut self, block_size: u32) -> Self {
        assert!(block_size >= 1_024);

        self.block_size = block_size;
        self
    }

    /// Sets the size ratio between levels of the LSM tree (a.k.a fanout, growth rate).
    ///
    /// Once set for a partition, this property is not considered in the future.
    ///
    /// Default = 8
    ///
    /// # Panics
    ///
    /// Panics if `n` is 0.
    #[must_use]
    pub fn level_ratio(mut self, n: u8) -> Self {
        assert!(n > 0);

        self.level_ratio = n;
        self
    }

    /// Sets the level count (depth of the tree).
    ///
    /// Once set for a partition, this property is not considered in the future.
    ///
    /// Default = 7
    ///
    /// # Panics
    ///
    /// Panics if `n` is less than 2.
    #[must_use]
    pub fn level_count(mut self, n: u8) -> Self {
        assert!(n > 1);

        self.level_count = n;
        self
    }

    /// Enables key-value separation for this partition.
    ///
    /// Key-value separation is intended for large value scenarios (1 KiB+ per KV).
    /// Large values will be separated into a log-structured value log, which heavily
    /// decreases compaction overhead at the cost of slightly higher read latency
    /// and higher temporary space usage.
    /// Also, garbage collection for deleted or outdated values becomes lazy, so
    /// GC needs to be triggered *manually*.
    ///
    /// Once set for a partition, this property is not considered in the future.
    ///
    /// Default = disabled
    #[must_use]
    pub fn use_kv_separation(mut self) -> Self {
        self.tree_type = TreeType::Blob;
        self
    }
}
