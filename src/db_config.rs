// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::path::absolute_path;
use lsm_tree::{Cache, CompressionType, DescriptorTable};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

pub type CompactionFilterAssigner =
    Arc<dyn Fn(&str) -> Option<Arc<dyn lsm_tree::compaction::filter::Factory>> + Send + Sync>;

/// Global database configuration
#[derive(Clone)]
pub struct Config {
    /// Base path of database
    pub(crate) path: PathBuf,

    /// When true, the path will be deleted upon drop
    pub(crate) clean_path_on_drop: bool,

    #[doc(hidden)]
    pub cache: Arc<Cache>,

    /// Descriptor table that will be shared between keyspaces
    pub(crate) descriptor_table: Option<Arc<DescriptorTable>>,

    /// Max size of all journals in bytes
    pub(crate) max_journaling_size_in_bytes: u64, // TODO: should be configurable during runtime: AtomicU64

    /// Max size of all active memtables
    ///
    /// This can be used to cap the memory usage if there are
    /// many (possibly inactive) keyspaces.
    pub(crate) max_write_buffer_size_in_bytes: Option<u64>, // TODO: should be configurable during runtime: AtomicU64

    /// Number of concurrent worker threads
    pub(crate) worker_threads: usize,

    #[doc(hidden)]
    pub(crate) write_pipeline_leader_capacity: usize,

    /// If `true`, writes are never stalled or halted (L0 run count, sealed memtables)
    ///
    /// Used for deterministic testing without background workers, where
    /// flushes and compactions are run manually.
    #[doc(hidden)]
    pub(crate) disable_write_stalls: bool,

    /// The DB manifest is rotated (a snapshot is written into a new fragment)
    /// once its active fragment is larger than this
    ///
    /// Default = 4 MiB
    #[doc(hidden)]
    pub(crate) manifest_rotation_threshold: u64,

    /// Write-ahead log value compression
    ///
    /// See `wal_compression_threshold`.
    ///
    /// Default = Lz4, if available; otherwise None
    pub(crate) wal_compression_type: CompressionType,

    /// Value size threshold in write-ahead log
    ///
    /// Values equal or larger the threshold are compressed
    ///
    /// Default = 4 KiB
    pub(crate) wal_compression_threshold: u32,

    // pub(crate) journal_recovery_mode: RecoveryMode,
    //
    pub(crate) compaction_filter_factory_assigner: Option<CompactionFilterAssigner>,
}

const DEFAULT_CPU_CORES: usize = 4;

fn get_open_file_limit() -> usize {
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    return 900;

    #[cfg(target_os = "windows")]
    return 400;

    #[cfg(target_os = "macos")]
    return 150;
}

impl Config {
    /// Creates a new configuration
    pub fn new(path: &Path) -> Self {
        let queried_cores = std::thread::available_parallelism().map(usize::from);
        let worker_threads = queried_cores.unwrap_or(1).min(DEFAULT_CPU_CORES);

        Self {
            path: absolute_path(path),
            clean_path_on_drop: false,
            descriptor_table: Some(Arc::new(DescriptorTable::new(get_open_file_limit()))),
            max_write_buffer_size_in_bytes: None,
            max_journaling_size_in_bytes: /* 512 MiB */ 512 * 1_024 * 1_024,
            worker_threads,
            write_pipeline_leader_capacity: 1_000_000,
            disable_write_stalls: false,
            manifest_rotation_threshold: /* 4 MiB */ 4 * 1_024 * 1_024,
            // journal_recovery_mode: RecoveryMode::default(),

            #[cfg(not(feature = "lz4"))]
            wal_compression_type: CompressionType::None,

            #[cfg(feature = "lz4")]
            wal_compression_type: CompressionType::Lz4,

            wal_compression_threshold: 4_096,

            cache: Arc::new(Cache::with_capacity_bytes(/* 64 MiB */ 64 * 1_024 * 1_024)),

            compaction_filter_factory_assigner: None,
        }
    }
}
