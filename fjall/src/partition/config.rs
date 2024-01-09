/// Partition create options.
///
/// Partitions are generally pretty inexpensive, so having a bunch of
/// inactive partitions is definitely valid.
///
/// An inactive partition generally only takes a little bit of memory and disk space.
pub struct CreateOptions {
    /// Block size of data and index blocks
    ///
    /// Once set for a partition, this property is not considered in the future.
    pub block_size: u32,

    /// Amount of levels of the LSM tree (depth of tree)
    ///
    /// Once set for a partition, this property is not considered in the future.
    pub level_count: u8,

    /// Size ratio between levels of the LSM tree (a.k.a fanout, growth rate).
    ///
    /// This is the exponential growth of the from one
    /// level to the next
    ///
    /// A level target size is: max_memtable_size * level_ratio.pow(#level + 1)
    ///
    /// Once set for a partition, this property is not considered in the future.
    pub level_ratio: u8,
}

impl Default for CreateOptions {
    fn default() -> Self {
        let default_tree_config = lsm_tree::Config::default();

        Self {
            block_size: default_tree_config.inner.block_size,
            level_count: default_tree_config.inner.level_count,
            level_ratio: default_tree_config.inner.level_ratio,
        }
    }
}

impl CreateOptions {
    /// Sets the block size.
    ///
    /// Default = 4 KiB
    ///
    #[must_use]
    pub fn block_size(mut self, n: u32) -> Self {
        self.block_size = n;
        self
    }

    /// Sets the size ratio between levels of the LSM tree (a.k.a fanout, growth rate).
    ///
    /// Default = 8
    ///
    #[must_use]
    pub fn level_ratio(mut self, n: u8) -> Self {
        self.level_ratio = n;
        self
    }

    /// Sets the level count (depth of the tree).
    ///
    /// Default = 7
    #[must_use]
    pub fn level_count(mut self, n: u8) -> Self {
        self.level_count = n;
        self
    }
}
