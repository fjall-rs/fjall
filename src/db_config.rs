// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::path::absolute_path;
use lsm_tree::{Cache, CompressionType, DescriptorTable};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

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
    pub(crate) descriptor_table: Arc<DescriptorTable>,

    /// Max size of all journals in bytes
    pub(crate) max_journaling_size_in_bytes: u64, // TODO: should be configurable during runtime: AtomicU64

    /// Max size of all active memtables
    ///
    /// This can be used to cap the memory usage if there are
    /// many (possibly inactive) keyspaces.
    pub(crate) max_write_buffer_size_in_bytes: u64, // TODO: should be configurable during runtime: AtomicU64

    pub(crate) manual_journal_persist: bool,

    /// Amount of concurrent worker threads
    pub(crate) worker_count: usize,

    pub(crate) journal_compression_type: CompressionType,

    pub(crate) journal_compression_threshold: usize,
    // pub(crate) journal_recovery_mode: RecoveryMode,
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
        let worker_count = queried_cores.unwrap_or(1).min(DEFAULT_CPU_CORES);

        Self {
            path: absolute_path(path),
            clean_path_on_drop: false,
            descriptor_table: Arc::new(DescriptorTable::new(get_open_file_limit())),
            max_write_buffer_size_in_bytes: /* 128 MiB */ 128 * 1_024 * 1_024,
            max_journaling_size_in_bytes: /* 512 MiB */ 512 * 1_024 * 1_024,
            worker_count,
            // journal_recovery_mode: RecoveryMode::default(),
            manual_journal_persist: false,

            #[cfg(not(feature = "lz4"))]
            journal_compression_type: CompressionType::None,

            #[cfg(feature = "lz4")]
            journal_compression_type: CompressionType::Lz4,

            journal_compression_threshold: 4_096,

            cache: Arc::new(Cache::with_capacity_bytes(/* 32 MiB */ 32 * 1_024 * 1_024)),
        }
    }
}
