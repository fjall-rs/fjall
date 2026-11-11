// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{snapshot_nonce::SnapshotNonce, SeqNo};
use lsm_tree::SequenceNumberCounter;
use std::{
    collections::BTreeMap,
    hash::Hash,
    sync::{atomic::AtomicU64, Arc, Mutex},
};

/// A shard of open snapshots: seqno -> number of open snapshots at it
pub type Shard = Arc<Mutex<BTreeMap<SeqNo, usize>>>;

/// Keeps track of open snapshots
pub struct SnapshotTrackerInner {
    seqno: SequenceNumberCounter,
    shards: Vec<Shard>,
    lowest_watermark: AtomicU64,
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
            seqno,
            shards: {
                let cores = std::thread::available_parallelism()
                    .map(std::num::NonZero::get)
                    .unwrap_or_default()
                    .max(1);

                let shards = 4 * cores;

                (0..shards).map(|_| Shard::default()).collect()
            },
            lowest_watermark: AtomicU64::default(),
        }))
    }

    pub fn get_ref(&self) -> SequenceNumberCounter {
        self.seqno.clone()
    }

    /// Used in database recovery.
    ///
    /// # Caution
    ///
    /// Don't use this to get a snapshot read.
    pub fn get(&self) -> SeqNo {
        self.seqno.get()
    }

    /// Used in database recovery.
    pub fn set(&self, value: SeqNo) {
        self.seqno.fetch_max(value);
    }

    /// Picks the shard for a new snapshot (by the current thread, to spread contention).
    #[expect(clippy::cast_possible_truncation)]
    fn pick_shard(&self) -> usize {
        let hash = {
            let mut h = xxhash_rust::xxh3::Xxh3Default::default();
            std::thread::current().id().hash(&mut h);
            h.digest() as usize
        };

        hash % self.shards.len()
    }

    #[expect(clippy::indexing_slicing)]
    fn shard(&self, shard: usize) -> &Shard {
        &self.shards[shard]
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        let mut len = 0;

        for item in &self.shards {
            let item = item.lock().expect("should not be poisoned");
            len += item.len();
        }

        len
    }

    #[cfg(test)]
    pub fn open_snapshots(&self) -> usize {
        let mut len = 0;

        for item in &self.shards {
            let item = item.lock().expect("should not be poisoned");
            len += item.values().copied().sum::<usize>();
        }

        len
    }

    pub fn open(&self) -> SnapshotNonce {
        let shard = self.shard(self.pick_shard());

        #[expect(clippy::expect_used)]
        let mut guard = shard.lock().expect("should not be poisoned");

        let seqno = self.seqno.get();

        guard.entry(seqno).and_modify(|v| *v += 1).or_insert(1);
        drop(guard);

        SnapshotNonce::new(seqno, shard.clone())
    }

    /// Publish write completion
    pub fn publish(&self, batch_seqno: SeqNo) {
        self.seqno.fetch_max(batch_seqno + 1);
    }

    /// Returns the highest seqno that no open snapshot needs anymore, so older versions
    /// of a key below it can be dropped by compactions.
    pub fn get_seqno_safe_to_gc(&self) -> SeqNo {
        // IMPORTANT: Read before scanning the shards: a snapshot the scan misses was opened
        // after its shard was scanned, so it read a seqno at or above this one (see `open`)
        let mut min = self.seqno.get();

        for shard in &self.shards {
            let shard = shard.lock().expect("should not be poisoned");

            // NOTE: Sorted, so the first key is the shard's lowest open snapshot
            if let Some((&seqno, _)) = shard.first_key_value() {
                min = min.min(seqno);
            }
        }

        let free = min.saturating_sub(1);

        let previous = self
            .lowest_watermark
            .fetch_max(free, std::sync::atomic::Ordering::AcqRel);

        previous.max(free)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn snapshot_tracker_basic() {
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
    fn snapshot_tracker_increase_watermark() {
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
    fn snapshot_tracker_prevent_watermark() {
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

    /// A snapshot can be cloned and dropped on other threads than the one that opened it
    /// (it remembers its shard), and then no longer holds back the GC watermark.
    #[test]
    #[expect(clippy::expect_used)]
    fn snapshot_tracker_close_on_other_thread() {
        let global_seqno = SequenceNumberCounter::default();
        let map = SnapshotTracker::new(global_seqno.clone());

        let nonce = map.open();

        for _ in 0..100 {
            let _ = global_seqno.next();
        }
        assert_eq!(
            map.get_seqno_safe_to_gc(),
            0,
            "held back by the open snapshot"
        );

        std::thread::spawn(move || {
            let clone = nonce.clone();
            drop(nonce);
            drop(clone);
        })
        .join()
        .expect("thread should not panic");

        assert_eq!(map.open_snapshots(), 0);
        assert_eq!(map.len(), 0);
        assert_eq!(map.get_seqno_safe_to_gc(), 99);
    }

    #[test]
    fn snapshot_tracker_concurrent_open_same_seqno_counts_correctly() {
        let global_seqno = SequenceNumberCounter::default();
        let map = SnapshotTracker::new(global_seqno);

        // make sure seqno doesn't change between two opens
        let n1 = map.open();
        let n2 = map.open();
        assert_eq!(n1.instant, n2.instant);
        assert_eq!(map.open_snapshots(), 2);

        // closing one decreases count
        drop(n1);
        assert_eq!(map.open_snapshots(), 1);

        // closing the other removes the last
        drop(n2);
        assert_eq!(map.open_snapshots(), 0);
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn snapshot_tracker_publish_moves_seqno_forward_and_ignores_older() {
        let global_seqno = SequenceNumberCounter::default();
        let map = SnapshotTracker::new(global_seqno);

        let big = 100u64;
        map.publish(big);
        assert_eq!(map.get(), (big + 1));

        let before = map.get();
        map.publish(1);
        assert_eq!(map.get(), before);
    }

    #[test]
    fn snapshot_tracker_clone_snapshot_behaves_like_second_open() {
        let global_seqno = SequenceNumberCounter::default();
        let map = SnapshotTracker::new(global_seqno);

        let orig = map.open();
        let clone = orig.clone();

        assert_eq!(orig.instant, clone.instant);
        assert_eq!(map.open_snapshots(), 2);

        // closing one leaves one
        drop(orig);
        assert_eq!(map.open_snapshots(), 1);

        // closing the clone removes all
        drop(clone);
        assert_eq!(map.open_snapshots(), 0);
    }

    /// Microbenchmark: threads opening and dropping snapshots (the hot path of every read).
    ///
    /// Run with `cargo test --release --lib bench_snapshot_open_close -- --ignored --nocapture`
    #[test]
    #[ignore = "benchmark"]
    fn bench_snapshot_open_close() {
        const OPS_PER_THREAD: usize = 2_000_000;
        const REPEATS: usize = 5;

        for threads in [1, 2, 4, 8, 16, 32] {
            let mut best = f64::MAX;

            for _ in 0..REPEATS {
                let map = SnapshotTracker::new(SequenceNumberCounter::default());
                let barrier = std::sync::Barrier::new(threads + 1);

                let elapsed = std::thread::scope(|s| {
                    for _ in 0..threads {
                        s.spawn(|| {
                            barrier.wait();

                            for _ in 0..OPS_PER_THREAD {
                                let nonce = map.open();
                                std::hint::black_box(&nonce);
                                drop(nonce);
                            }
                        });
                    }

                    barrier.wait();
                    let start = std::time::Instant::now();

                    // NOTE: Scope joins all threads before returning
                    start
                })
                .elapsed()
                .as_secs_f64();

                best = best.min(elapsed);
            }

            #[expect(clippy::cast_precision_loss)]
            let mops = (threads * OPS_PER_THREAD) as f64 / best / 1_000_000.0;
            eprintln!("threads={threads:>2}  {mops:>8.2} M open+close/s  (best of {REPEATS})");
        }
    }

    /// Like [`bench_snapshot_open_close`], but only the shard work (no `SnapshotNonce`,
    /// so no `Arc` clone and drop of the tracker)
    #[test]
    #[ignore = "benchmark"]
    fn bench_snapshot_shards_only() {
        const OPS_PER_THREAD: usize = 2_000_000;
        const REPEATS: usize = 5;

        for threads in [1, 2, 4, 8, 16, 32] {
            let mut best = f64::MAX;

            for _ in 0..REPEATS {
                let map = SnapshotTracker::new(SequenceNumberCounter::default());
                let barrier = std::sync::Barrier::new(threads + 1);

                let elapsed = std::thread::scope(|s| {
                    for _ in 0..threads {
                        s.spawn(|| {
                            barrier.wait();

                            let shard_idx = map.pick_shard();

                            for _ in 0..OPS_PER_THREAD {
                                let seqno = {
                                    let mut shard = map.shard(shard_idx).lock().expect("lock");
                                    let seqno = map.seqno.get();
                                    *shard.entry(seqno).or_insert(0) += 1;
                                    seqno
                                };
                                std::hint::black_box(seqno);

                                let mut shard = map.shard(shard_idx).lock().expect("lock");
                                if let Some(v) = shard.get_mut(&seqno) {
                                    *v -= 1;
                                    if *v == 0 {
                                        shard.remove(&seqno);
                                    }
                                }
                            }
                        });
                    }

                    barrier.wait();
                    std::time::Instant::now()
                })
                .elapsed()
                .as_secs_f64();

                best = best.min(elapsed);
            }

            #[expect(clippy::cast_precision_loss)]
            let mops = (threads * OPS_PER_THREAD) as f64 / best / 1_000_000.0;
            eprintln!("threads={threads:>2}  {mops:>8.2} M open+close/s  (best of {REPEATS})");
        }
    }
}
