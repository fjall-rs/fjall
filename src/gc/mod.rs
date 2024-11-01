// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::PartitionHandle;
use lsm_tree::{gc::Report as GcReport, AnyTree};

/// Functions for garbage collection strategies
///
/// These functions are to be used with a key-value separated partition.
pub trait GarbageCollection {
    /// Collects statistics about blob fragmentation inside the partition.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    ///
    /// # Panics
    ///
    /// Panics if the partition is not KV-separated.
    fn gc_scan(&self) -> crate::Result<GcReport>;

    /// Rewrites blobs in order to achieve the given space amplification factor.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, GarbageCollection, PersistMode, Keyspace, PartitionCreateOptions};
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// let opts = PartitionCreateOptions::default().with_kv_separation(Default::default());
    /// let blobs = keyspace.open_partition("my_blobs", opts)?;
    ///
    /// blobs.insert("a", "hello".repeat(1_000))?;
    /// blobs.insert("b", "hello".repeat(1_000))?;
    /// blobs.insert("c", "hello".repeat(1_000))?;
    /// blobs.insert("d", "hello".repeat(1_000))?;
    /// blobs.insert("e", "hello".repeat(1_000))?;
    /// # blobs.rotate_memtable_and_wait()?;
    /// blobs.remove("a")?;
    /// blobs.remove("b")?;
    /// blobs.remove("c")?;
    /// blobs.remove("d")?;
    ///
    /// let report = blobs.gc_scan()?;
    /// # // assert_eq!(0.8, report.stale_ratio());
    /// # // assert_eq!(5.0, report.space_amp());
    /// # // assert_eq!(5, report.total_blobs);
    /// # // assert_eq!(4, report.stale_blobs);
    /// # // assert_eq!(0, report.stale_segment_count);
    /// # // assert_eq!(1, report.segment_count);
    ///
    /// let bytes_freed = blobs.gc_with_space_amp_target(1.5)?;
    /// # // assert!(bytes_freed >= 0);
    ///
    /// let report = blobs.gc_scan()?;
    /// # // assert_eq!(0.0, report.stale_ratio());
    /// # // assert_eq!(1.0, report.space_amp());
    /// # // assert_eq!(1, report.total_blobs);
    /// # // assert_eq!(0, report.stale_blobs);
    /// # // assert_eq!(0, report.stale_segment_count);
    /// # // assert_eq!(1, report.segment_count);
    /// #
    /// # Ok::<_, fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    ///
    /// # Panics
    ///
    /// Panics if the partition is not KV-separated.
    fn gc_with_space_amp_target(&self, factor: f32) -> crate::Result<u64>;

    /// Rewrites blobs that have reached a given staleness threshold.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, GarbageCollection, PersistMode, Keyspace, PartitionCreateOptions};
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// let opts = PartitionCreateOptions::default().with_kv_separation(Default::default());
    /// let blobs = keyspace.open_partition("my_blobs", opts)?;
    ///
    /// blobs.insert("a", "hello".repeat(1_000))?;
    /// blobs.insert("b", "hello".repeat(1_000))?;
    /// blobs.insert("c", "hello".repeat(1_000))?;
    /// blobs.insert("d", "hello".repeat(1_000))?;
    /// blobs.insert("e", "hello".repeat(1_000))?;
    /// # blobs.rotate_memtable_and_wait()?;
    /// blobs.remove("a")?;
    /// blobs.remove("b")?;
    /// blobs.remove("c")?;
    /// blobs.remove("d")?;
    ///
    /// let report = blobs.gc_scan()?;
    /// # // assert_eq!(0.8, report.stale_ratio());
    /// # // assert_eq!(5.0, report.space_amp());
    /// # // assert_eq!(5, report.total_blobs);
    /// # // assert_eq!(4, report.stale_blobs);
    /// # // assert_eq!(0, report.stale_segment_count);
    /// # // assert_eq!(1, report.segment_count);
    ///
    /// let bytes_freed = blobs.gc_with_staleness_threshold(0.5)?;
    /// # // assert!(bytes_freed >= 0);
    ///
    /// let report = blobs.gc_scan()?;
    /// # // assert_eq!(0.0, report.stale_ratio());
    /// # // assert_eq!(1.0, report.space_amp());
    /// # // assert_eq!(1, report.total_blobs);
    /// # // assert_eq!(0, report.stale_blobs);
    /// # // assert_eq!(0, report.stale_segment_count);
    /// # // assert_eq!(1, report.segment_count);
    /// #
    /// # Ok::<_, fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    ///
    /// # Panics
    ///
    /// Panics if the partition is not KV-separated.
    ///
    /// Panics if the threshold is negative.
    ///
    /// Values above 1.0 will be treated as 1.0.
    /// If you want to drop only fully stale segments, use [`GarbageCollector::drop_stale_segments`] instead.
    fn gc_with_staleness_threshold(&self, threshold: f32) -> crate::Result<u64>;

    /// Drops fully stale segments.
    ///
    /// This is called implicitly by other garbage collection strategies.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, GarbageCollection, PersistMode, Keyspace, PartitionCreateOptions};
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// let opts = PartitionCreateOptions::default().with_kv_separation(Default::default());
    /// let blobs = keyspace.open_partition("my_blobs", opts)?;
    ///
    /// blobs.insert("a", "hello".repeat(1_000))?;
    /// assert!(blobs.contains_key("a")?);
    ///
    /// # blobs.rotate_memtable_and_wait()?;
    /// blobs.remove("a")?;
    /// assert!(!blobs.contains_key("a")?);
    ///
    /// let report = blobs.gc_scan()?;
    /// # // assert_eq!(1.0, report.stale_ratio());
    /// # // assert_eq!(1, report.stale_blobs);
    /// # // assert_eq!(1, report.stale_segment_count);
    /// # // assert_eq!(1, report.segment_count);
    ///
    /// let bytes_freed = blobs.gc_drop_stale_segments()?;
    /// # // assert!(bytes_freed >= 0);
    ///
    /// let report = blobs.gc_scan()?;
    /// # // assert_eq!(0.0, report.stale_ratio());
    /// # // assert_eq!(0, report.stale_blobs);
    /// # // assert_eq!(0, report.stale_segment_count);
    /// # // assert_eq!(0, report.segment_count);
    /// #
    /// # Ok::<_, fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    ///
    /// # Panics
    ///
    /// Panics if the partition is not KV-separated.
    fn gc_drop_stale_segments(&self) -> crate::Result<u64>;
}

pub struct GarbageCollector;

impl GarbageCollector {
    pub fn scan(partition: &PartitionHandle) -> crate::Result<GcReport> {
        if let AnyTree::Blob(tree) = &partition.tree {
            return tree
                .gc_scan_stats(
                    partition.seqno.get(),
                    partition.snapshot_tracker.get_seqno_safe_to_gc(),
                )
                .map_err(Into::into);
        }
        panic!("Cannot use GC for non-KV-separated tree");
    }

    pub fn with_space_amp_target(partition: &PartitionHandle, factor: f32) -> crate::Result<u64> {
        if let AnyTree::Blob(tree) = &partition.tree {
            let strategy = lsm_tree::gc::SpaceAmpStrategy::new(factor);

            tree.apply_gc_strategy(&strategy, partition.seqno.next())
                .map_err(Into::into)
        } else {
            panic!("Cannot use GC for non-KV-separated tree");
        }
    }

    pub fn with_staleness_threshold(
        partition: &PartitionHandle,
        threshold: f32,
    ) -> crate::Result<u64> {
        if let AnyTree::Blob(tree) = &partition.tree {
            let strategy = lsm_tree::gc::StaleThresholdStrategy::new(threshold);

            return tree
                .apply_gc_strategy(&strategy, partition.seqno.next())
                .map_err(Into::into);
        }
        panic!("Cannot use GC for non-KV-separated tree");
    }

    pub fn drop_stale_segments(partition: &PartitionHandle) -> crate::Result<u64> {
        if let AnyTree::Blob(tree) = &partition.tree {
            return tree.gc_drop_stale().map_err(Into::into);
        }
        panic!("Cannot use GC for non-KV-separated tree");
    }
}
