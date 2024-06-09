use lsm_tree::TreeType;

/// Options to configure a partition
pub struct CreateOptions {
    /// Block size of data and index blocks
    ///
    /// Once set for a partition, this property is not considered in the future.
    pub(crate) block_size: u32,

    /// Amount of levels of the LSM tree (depth of tree)
    ///
    /// Once set for a partition, this property is not considered in the future.
    pub(crate) level_count: u8,

    /// Size ratio between levels of the LSM tree (a.k.a fanout, growth rate).
    ///
    /// This is the exponential growth of the from one
    /// level to the next
    ///
    /// A level target size is: `max_memtable_size * level_ratio.pow(#level + 1)`
    pub(crate) level_ratio: u8,

    /// Tree type, see [`TreeType`].
    ///
    /// Once set for a partition, this property is not considered in the future.
    pub(crate) tree_type: TreeType,
}

impl Default for CreateOptions {
    fn default() -> Self {
        let default_tree_config = lsm_tree::Config::default();

        Self {
            block_size: default_tree_config.inner.block_size,
            level_count: default_tree_config.inner.level_count,
            level_ratio: default_tree_config.level_ratio,
            tree_type: TreeType::Standard,
        }
    }
}

impl CreateOptions {
    /// Sets the block size.
    ///
    /// Default = 4 KiB
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
    /// Once set for a partition, this property is not considered in the future.
    #[must_use]
    pub fn use_kv_separation(mut self) -> Self {
        self.tree_type = TreeType::Blob;
        self
    }
}
