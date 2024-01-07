use lsm_tree::compaction::CompactionStrategy;
use std::sync::Arc;

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

    /// Compaction strategy to use for this partition
    pub compaction_strategy: Arc<dyn CompactionStrategy + Send + Sync>,

    /// Maximum size of this partition's active memtable
    pub max_memtable_size: u32,
}

impl Default for CreateOptions {
    fn default() -> Self {
        let default_tree_config = lsm_tree::Config::default();

        Self {
            block_size: default_tree_config.inner.block_size,
            level_count: default_tree_config.inner.level_count,
            level_ratio: default_tree_config.inner.level_ratio,
            compaction_strategy: Arc::new(lsm_tree::compaction::Levelled::default()),
            max_memtable_size: 8 * 1_024 * 1_024,
        }
    }
}

impl CreateOptions {
    /// Sets the block size.
    ///
    /// Default = 4 KiB
    ///
    pub fn block_size(mut self, n: u32) -> Self {
        self.block_size = n;
        self
    }

    /// Sets the maximum size of this partition's active memtable.
    ///
    /// Default = 8
    ///
    pub fn max_memtable_size(mut self, n: u32) -> Self {
        self.max_memtable_size = n;
        self
    }

    /// Sets the size ratio between levels of the LSM tree (a.k.a fanout, growth rate).
    ///
    /// Default = 8
    ///
    pub fn level_ratio(mut self, n: u8) -> Self {
        self.level_ratio = n;
        self
    }

    /// Sets the level count (depth of the tree).
    ///
    /// Default = 7
    pub fn level_count(mut self, n: u8) -> Self {
        self.level_count = n;
        self
    }

    /// Sets the compaction strategy for this partition
    ///
    /// Default = Levelled
    pub fn compaction_strategy(
        mut self,
        strategy: Arc<dyn CompactionStrategy + Send + Sync>,
    ) -> Self {
        self.compaction_strategy = strategy;
        self
    }
}
