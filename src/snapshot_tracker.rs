// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{snapshot_nonce::SnapshotNonce, SeqNo};
use dashmap::DashMap;
use lsm_tree::SequenceNumberCounter;
use std::sync::{atomic::AtomicU64, Arc, RwLock};

/// Keeps track of open snapshots
#[allow(clippy::module_name_repetitions)]
pub struct SnapshotTrackerInner {
    seqno: SequenceNumberCounter,

    gc_lock: RwLock<()>,

    // TODO: maybe use rustc_hash or ahash
    data: DashMap<SeqNo, usize, xxhash_rust::xxh3::Xxh3Builder>,

    freed_count: AtomicU64,

    safety_gap: u64,

    lowest_freed_instant: AtomicU64,
}

#[derive(Clone)]
pub struct SnapshotTracker(Arc<SnapshotTrackerInner>);

impl std::ops::Deref for SnapshotTracker {
    type Target = SnapshotTrackerInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SnapshotTracker {
    pub fn new(seqno: SequenceNumberCounter) -> Self {
        Self(Arc::new(SnapshotTrackerInner {
            data: DashMap::default(),
            safety_gap: 50,
            freed_count: AtomicU64::default(),
            lowest_freed_instant: AtomicU64::default(),
            seqno,
            gc_lock: RwLock::default(),
        }))
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn open_snapshots(&self) -> usize {
        self.data.iter().map(|r| *r.value()).sum()
    }

    pub fn open(&self) -> SnapshotNonce {
        let _lock = self.gc_lock.read().expect("lock is poisoned");

        let seqno = self.seqno.get();

        self.data
            .entry(seqno)
            .and_modify(|x| {
                *x += 1;
            })
            .or_insert(1);

        SnapshotNonce::new(seqno, self.clone())
    }

    pub fn close(&self, nonce: &SnapshotNonce) {
        self.close_raw(nonce.instant);
    }

    pub(crate) fn close_raw(&self, instant: SeqNo) {
        let lock = self.gc_lock.read().expect("lock is poisoned");

        self.data.alter(&instant, |_, v| v.saturating_sub(1));

        let freed = self
            .freed_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;

        drop(lock);

        if freed.is_multiple_of(1_000) {
            self.gc();
        }
    }

    // TODO: after recovery, we may need to set the GC watermark once to current_seqno - safety_gap
    // so there cannot be compactions scheduled immediately with gc_watermark=0
    pub fn get_seqno_safe_to_gc(&self) -> SeqNo {
        self.lowest_freed_instant
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn gc(&self) {
        let _lock = self.gc_lock.write().expect("lock is poisoned");

        let seqno_threshold = self.seqno.get().saturating_sub(self.safety_gap);

        let mut lowest_retained = 0;

        self.data.retain(|&k, v| {
            let should_be_retained = *v > 0 || k > seqno_threshold;

            if should_be_retained {
                lowest_retained = match lowest_retained {
                    0 => k,
                    lo => lo.min(k),
                };
            }

            should_be_retained
        });

        self.lowest_freed_instant.fetch_max(
            lowest_retained.saturating_sub(1),
            std::sync::atomic::Ordering::AcqRel,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn seqno_tracker_basic() {
        let global_seqno = SequenceNumberCounter::default();

        let map = SnapshotTracker::new(global_seqno.clone());

        let nonce = map.open();
        assert_eq!(0, nonce.instant);
        drop(nonce);

        assert_eq!(map.get_seqno_safe_to_gc(), 0);

        let _ = global_seqno.next();

        let nonce = map.open();
        assert_eq!(1, nonce.instant);
        drop(nonce);
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn seqno_tracker_increase_watermark() {
        let global_seqno = SequenceNumberCounter::default();

        let map = SnapshotTracker::new(global_seqno.clone());

        // Simulates some tx committing
        for _ in 0..100_000 {
            let _ = global_seqno.next();
            let nonce = map.open();
            drop(nonce);
        }

        assert!(map.get_seqno_safe_to_gc() > 0);
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn seqno_tracker_prevent_watermark() {
        let global_seqno = SequenceNumberCounter::default();

        let map = SnapshotTracker::new(global_seqno.clone());

        // This nonce prevents the watermark from increasing
        let _old_nonce = map.open();

        // Simlates some inserts happening
        for _ in 0..1_000 {
            let _ = global_seqno.next();
        }

        // Simulates more read tx opening and closing
        for _ in 0..10_000 {
            let nonce = map.open();
            drop(nonce);
        }

        assert_eq!(map.get_seqno_safe_to_gc(), 0);
    }

    // #[test]
    // #[allow(clippy::field_reassign_with_default)]
    // fn seqno_tracker_simple_2() {
    //     let mut map = SnapshotTracker::new(SequenceNumberCounter::default());
    //     map.safety_gap = 5;

    //     map.open(1);
    //     assert_eq!(map.get_seqno_safe_to_gc(), 0);

    //     map.open(2);
    //     assert_eq!(map.get_seqno_safe_to_gc(), 0);

    //     map.open(3);
    //     assert_eq!(map.get_seqno_safe_to_gc(), 0);

    //     map.open(4);
    //     assert_eq!(map.get_seqno_safe_to_gc(), 0);

    //     map.open(5);
    //     assert_eq!(map.get_seqno_safe_to_gc(), 0);

    //     map.open(6);
    //     assert_eq!(map.get_seqno_safe_to_gc(), 0);

    //     map.close(1);
    //     assert_eq!(map.get_seqno_safe_to_gc(), 0);

    //     map.close(2);
    //     assert_eq!(map.get_seqno_safe_to_gc(), 0);

    //     map.close(3);
    //     assert_eq!(map.get_seqno_safe_to_gc(), 0);

    //     map.close(4);
    //     assert_eq!(map.get_seqno_safe_to_gc(), 0);

    //     map.close(5);
    //     assert_eq!(map.get_seqno_safe_to_gc(), 0);

    //     map.close(6);
    //     map.gc(6);
    //     assert_eq!(map.get_seqno_safe_to_gc(), 1);

    //     map.open(7);
    //     map.close(7);
    //     map.gc(7);
    //     assert_eq!(map.get_seqno_safe_to_gc(), 2);

    //     map.open(8);
    //     map.open(9);
    //     map.close(9);
    //     map.gc(9);
    //     assert_eq!(map.get_seqno_safe_to_gc(), 4);

    //     map.close(8);
    //     map.gc(8);
    //     assert_eq!(map.get_seqno_safe_to_gc(), 4);
    // }
}
