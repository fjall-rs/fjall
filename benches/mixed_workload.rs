// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fjall::{Database, KeyspaceCreateOptions};

fn mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");

    group.bench_function("read_heavy", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                for i in 0..1_000u64 {
                    ks.insert(i.to_be_bytes(), b"value").unwrap();
                }
                (folder, db, ks)
            },
            |(_folder, _db, ks)| {
                // 90% reads, 10% writes
                for i in 0..1_000u64 {
                    if i % 10 == 0 {
                        ks.insert((i + 1_000).to_be_bytes(), b"new_value").unwrap();
                    } else {
                        ks.get(i.to_be_bytes()).unwrap();
                    }
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("write_heavy", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                for i in 0..1_000u64 {
                    ks.insert(i.to_be_bytes(), b"value").unwrap();
                }
                (folder, db, ks)
            },
            |(_folder, _db, ks)| {
                // 10% reads, 90% writes
                for i in 0..1_000u64 {
                    if i % 10 == 0 {
                        ks.get(i.to_be_bytes()).unwrap();
                    } else {
                        ks.insert((i + 1_000).to_be_bytes(), b"new_value").unwrap();
                    }
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("balanced", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                for i in 0..1_000u64 {
                    ks.insert(i.to_be_bytes(), b"value").unwrap();
                }
                (folder, db, ks)
            },
            |(_folder, _db, ks)| {
                // 50% reads, 50% writes
                for i in 0..1_000u64 {
                    if i % 2 == 0 {
                        ks.get(i.to_be_bytes()).unwrap();
                    } else {
                        ks.insert((i + 1_000).to_be_bytes(), b"new_value").unwrap();
                    }
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(benches, mixed_workload);
criterion_main!(benches);
