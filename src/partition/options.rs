// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use lsm_tree::{CompressionType, TreeType};

/// Options to configure a partition
pub struct Options {
    /// Maximum size of this partition's memtable - can be changed during runtime
    pub(crate) max_memtable_size: u32,

    /// Block size of data blocks.
    pub(crate) data_block_size: u32,

    /// Block size of index blocks.
    pub(crate) index_block_size: u32,

    /// Amount of levels of the LSM tree (depth of tree).
    pub(crate) level_count: u8,

    /// Bits per key for levels that are not L0, L1, L2
    // NOTE: bloom_bits_per_key is not conditionally compiled,
    // because that would change the file format
    pub bloom_bits_per_key: i8,

    /// Tree type, see [`TreeType`].
    pub(crate) tree_type: TreeType,

    /// Compression to use.
    pub(crate) compression: CompressionType,

    /// Compression to use for blobs.
    pub(crate) blob_compression: CompressionType,

    /// Blob file (value log segment) target size in bytes
    #[doc(hidden)]
    pub blob_file_target_size: u64,

    /// Key-value separation threshold in bytes
    #[doc(hidden)]
    pub blob_file_separation_threshold: u32,

    pub(crate) manual_journal_persist: bool,
}

impl Default for Options {
    fn default() -> Self {
        let default_tree_config = lsm_tree::Config::default();

        Self {
            manual_journal_persist: false,

            max_memtable_size: /* 16 MiB */ 16 * 1_024 * 1_024,

            data_block_size: default_tree_config.data_block_size,
            index_block_size: default_tree_config.index_block_size,
            bloom_bits_per_key: default_tree_config.bloom_bits_per_key,
            level_count: default_tree_config.level_count,

            tree_type: TreeType::Standard,

            #[cfg(feature = "lz4")]
            compression: CompressionType::Lz4,

            #[cfg(all(feature = "miniz", not(feature = "lz4")))]
            compression: CompressionType::Miniz(6),

            #[cfg(not(any(feature = "lz4", feature = "miniz")))]
            compression: CompressionType::None,

            #[cfg(feature = "lz4")]
            blob_compression: CompressionType::Lz4,

            #[cfg(all(feature = "miniz", not(feature = "lz4")))]
            blob_compression: CompressionType::Miniz(6),

            #[cfg(not(any(feature = "lz4", feature = "miniz")))]
            blob_compression: CompressionType::None,

            blob_file_target_size: /* 64 MiB */ 64 * 1_024 * 1_024,
            blob_file_separation_threshold: /* 4 KiB */ 4 * 1_024,
        }
    }
}

impl Options {
    /// Sets the compression method.
    ///
    /// Once set for a partition, this property is not considered in the future.
    ///
    /// Default = In order: Lz4 -> Miniz -> None, depending on compilation flags
    #[must_use]
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self.blob_compression = compression;
        self
    }

    /// If `false`, writes will flush data to the operating system.
    ///
    /// Default = false
    ///
    /// Set to `true` to handle persistence manually, e.g. manually using `PersistMode::SyncData`.
    #[must_use]
    pub fn manual_journal_persist(mut self, flag: bool) -> Self {
        self.manual_journal_persist = flag;
        self
    }

    /// Sets the maximum memtable size.
    ///
    /// Default = 16 MiB
    ///
    /// Recommended size 8 - 64 MiB, depending on how much memory
    /// is available.
    ///
    /// Note that the memory usage may temporarily be `max_memtable_size * flush_worker_count`
    /// because of parallel flushing.
    /// Use the keyspace's `max_write_buffer_size` to cap global memory usage.
    ///
    /// Conversely, if `max_memtable_size` is larger than 64 MiB,
    /// it may require increasing the keyspace's `max_write_buffer_size`.
    #[must_use]
    pub fn max_memtable_size(mut self, bytes: u32) -> Self {
        self.max_memtable_size = bytes;
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
    /// Panics if the block size is smaller than 1 KiB or larger than 512 KiB.
    #[must_use]
    pub fn block_size(mut self, block_size: u32) -> Self {
        assert!(block_size >= 1_024);
        assert!(block_size <= 512 * 1_024);

        self.data_block_size = block_size;
        self.index_block_size = block_size;

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
    pub fn use_kv_separation(mut self, enabled: bool) -> Self {
        self.tree_type = if enabled {
            TreeType::Blob
        } else {
            TreeType::Standard
        };
        self
    }
}
