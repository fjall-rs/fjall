// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::Keyspace;
use lsm_tree::{gc::Report as GcReport, AnyTree};

/// Functions for garbage collection strategies
///
/// These functions are to be used with a key-value separated keyspace.
pub trait GarbageCollection {
    /// Collects statistics about blob fragmentation inside the keyspace.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    ///
    /// # Panics
    ///
    /// Panics if the keyspace is not KV-separated.
    fn gc_scan(&self) -> crate::Result<GcReport>;

    /// Rewrites blobs in order to achieve the given space amplification factor.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{GarbageCollection, PersistMode, Database, KeyspaceCreateOptions};
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// let opts = KeyspaceCreateOptions::default().with_kv_separation(Default::default());
    /// let blobs = db.keyspace("my_blobs", opts)?;
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
    /// Panics if the keyspace is not KV-separated.
    fn gc_with_space_amp_target(&self, factor: f32) -> crate::Result<u64>;

    /// Rewrites blobs that have reached a given staleness threshold.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{ GarbageCollection, PersistMode, Database, KeyspaceCreateOptions};
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// let opts = KeyspaceCreateOptions::default().with_kv_separation(Default::default());
    /// let blobs = db.keyspace("my_blobs", opts)?;
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
    /// Panics if the keyspace is not KV-separated.
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
    /// # use fjall::{GarbageCollection, PersistMode, Database, KeyspaceCreateOptions};
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// let opts = KeyspaceCreateOptions::default().with_kv_separation(Default::default());
    /// let blobs = db.keyspace("my_blobs", opts)?;
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
    /// Panics if the keyspace is not KV-separated.
    fn gc_drop_stale_segments(&self) -> crate::Result<u64>;
}

pub struct GarbageCollector;

impl GarbageCollector {
    pub fn scan(keyspace: &Keyspace) -> crate::Result<GcReport> {
        if let AnyTree::Blob(tree) = &keyspace.tree {
            return tree
                .gc_scan_stats(
                    keyspace.seqno.get(),
                    keyspace.snapshot_tracker.get_seqno_safe_to_gc(),
                )
                .map_err(Into::into);
        }
        panic!("Cannot use GC for non-KV-separated tree");
    }

    pub fn with_space_amp_target(keyspace: &Keyspace, factor: f32) -> crate::Result<u64> {
        if let AnyTree::Blob(tree) = &keyspace.tree {
            let strategy = lsm_tree::gc::SpaceAmpStrategy::new(factor);

            tree.apply_gc_strategy(&strategy, keyspace.seqno.next())
                .map_err(Into::into)
        } else {
            panic!("Cannot use GC for non-KV-separated tree");
        }
    }

    pub fn with_staleness_threshold(keyspace: &Keyspace, threshold: f32) -> crate::Result<u64> {
        if let AnyTree::Blob(tree) = &keyspace.tree {
            let strategy = lsm_tree::gc::StaleThresholdStrategy::new(threshold);

            return tree
                .apply_gc_strategy(&strategy, keyspace.seqno.next())
                .map_err(Into::into);
        }
        panic!("Cannot use GC for non-KV-separated tree");
    }

    pub fn drop_stale_segments(keyspace: &Keyspace) -> crate::Result<u64> {
        if let AnyTree::Blob(tree) = &keyspace.tree {
            return tree.gc_drop_stale().map_err(Into::into);
        }
        panic!("Cannot use GC for non-KV-separated tree");
    }
}
