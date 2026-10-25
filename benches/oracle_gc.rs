use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use fjall::{CommittedTxnTracker, ConflictManager};

fn benchmark_gc(c: &mut Criterion) {
    let mut group = c.benchmark_group("oracle_gc");

    for item_count in [100, 1_000, 10_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(item_count),
            &item_count,
            |b, &item_count| {
                b.iter_batched(
                    || {
                        let mut tracker = CommittedTxnTracker::default();
                        for seqno in 0..item_count {
                            tracker.insert(seqno, ConflictManager::default());
                        }
                        tracker
                    },
                    |mut tracker| {
                        tracker.gc(item_count / 2);
                        std::hint::black_box(tracker);
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, benchmark_gc);
criterion_main!(benches);
