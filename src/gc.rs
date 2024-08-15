// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::PartitionHandle;
use lsm_tree::{AnyTree, GcReport};

/// Functions for garbage collection strategies
pub struct GarbageCollector;

impl GarbageCollector {
    /// Collects statistics about blob fragmentation inside the partition.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    ///
    /// # Panics
    ///
    /// Panics if the partition is not KV-separated.
    pub fn scan(partition: &PartitionHandle) -> crate::Result<GcReport> {
        if let AnyTree::Blob(tree) = &partition.tree {
            return tree
                .gc_scan_stats(partition.seqno.get())
                .map_err(Into::into);
        }
        panic!("Cannot use GC for non-KV-separated tree");
    }

    /// Rewrites blobs in order to achieve the given space amplification factor.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Gc, PersistMode, Keyspace, PartitionCreateOptions};
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// let opts = PartitionCreateOptions::default().use_kv_separation();
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
    /// let report = Gc::scan(&blobs)?;
    /// assert_eq!(0.8, report.stale_ratio());
    /// assert_eq!(5.0, report.space_amp());
    /// assert_eq!(5, report.total_blobs);
    /// assert_eq!(4, report.stale_blobs);
    /// assert_eq!(0, report.stale_segment_count);
    /// assert_eq!(1, report.segment_count);
    ///
    /// // TODO: 2.0.0 bytes_freed
    /// let bytes_freed = Gc::with_space_amp_target(&blobs, 1.5)?;
    ///
    /// let report = Gc::scan(&blobs)?;
    /// assert_eq!(0.0, report.stale_ratio());
    /// assert_eq!(1.0, report.space_amp());
    /// assert_eq!(1, report.total_blobs);
    /// assert_eq!(0, report.stale_blobs);
    /// assert_eq!(0, report.stale_segment_count);
    /// assert_eq!(1, report.segment_count);
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
    /// Panics if the threshold is < 1.5.
    pub fn with_space_amp_target(partition: &PartitionHandle, threshold: f32) -> crate::Result<()> {
        assert!(
            threshold >= 1.5,
            "space amp threshold should be 1.5x or higher"
        );

        if let AnyTree::Blob(tree) = &partition.tree {
            return tree
                .gc_with_space_amp_target(threshold, partition.seqno.next())
                .map_err(Into::into);
        }
        panic!("Cannot use GC for non-KV-separated tree");
    }

    /// Rewrites blobs that have reached a given staleness threshold.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Gc, PersistMode, Keyspace, PartitionCreateOptions};
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// let opts = PartitionCreateOptions::default().use_kv_separation();
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
    /// let report = Gc::scan(&blobs)?;
    /// assert_eq!(0.8, report.stale_ratio());
    /// assert_eq!(5.0, report.space_amp());
    /// assert_eq!(5, report.total_blobs);
    /// assert_eq!(4, report.stale_blobs);
    /// assert_eq!(0, report.stale_segment_count);
    /// assert_eq!(1, report.segment_count);
    ///
    /// // TODO: 2.0.0 bytes_freed
    /// let bytes_freed = Gc::with_staleness_threshold(&blobs, 0.5)?;
    ///
    /// let report = Gc::scan(&blobs)?;
    /// assert_eq!(0.0, report.stale_ratio());
    /// assert_eq!(1.0, report.space_amp());
    /// assert_eq!(1, report.total_blobs);
    /// assert_eq!(0, report.stale_blobs);
    /// assert_eq!(0, report.stale_segment_count);
    /// assert_eq!(1, report.segment_count);
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
    pub fn with_staleness_threshold(
        partition: &PartitionHandle,
        threshold: f32,
    ) -> crate::Result<()> {
        assert!(threshold >= 0.0, "invalid staleness threshold");

        let threshold = threshold.min(1.0);

        if let AnyTree::Blob(tree) = &partition.tree {
            return tree
                .gc_with_staleness_threshold(threshold, partition.seqno.next())
                .map_err(Into::into);
        }
        panic!("Cannot use GC for non-KV-separated tree");
    }

    /// Drops fully stale segments.
    ///
    /// This is called implicitly by other garbage collection strategies.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Gc, PersistMode, Keyspace, PartitionCreateOptions};
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open()?;
    /// let opts = PartitionCreateOptions::default().use_kv_separation();
    /// let blobs = keyspace.open_partition("my_blobs", opts)?;
    ///
    /// blobs.insert("a", "hello".repeat(1_000))?;
    /// # blobs.rotate_memtable_and_wait()?;
    /// blobs.remove("a")?;
    ///
    /// let report = Gc::scan(&blobs)?;
    /// assert_eq!(1.0, report.stale_ratio());
    /// assert_eq!(1, report.stale_blobs);
    /// assert_eq!(1, report.stale_segment_count);
    /// assert_eq!(1, report.segment_count);
    ///
    /// // TODO: 2.0.0 bytes_freed
    /// let bytes_freed = Gc::drop_stale_segments(&blobs)?;
    ///
    /// let report = Gc::scan(&blobs)?;
    /// assert_eq!(0.0, report.stale_ratio());
    /// assert_eq!(0, report.stale_blobs);
    /// assert_eq!(0, report.stale_segment_count);
    /// assert_eq!(0, report.segment_count);
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
    pub fn drop_stale_segments(partition: &PartitionHandle) -> crate::Result<()> {
        if let AnyTree::Blob(tree) = &partition.tree {
            return tree.gc_drop_stale().map_err(Into::into);
        }
        panic!("Cannot use GC for non-KV-separated tree");
    }
}
