// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fjall::{Database, KeyspaceCreateOptions};

fn recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("recovery");

    group.bench_function("open/empty", |b| {
        b.iter_batched(
            || tempfile::tempdir().unwrap(),
            |folder| {
                let db = Database::builder(&folder).open().unwrap();
                let _ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                drop(db);
                folder
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("reopen/1k_items", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                {
                    let db = Database::builder(&folder).open().unwrap();
                    let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                    for i in 0..1_000u64 {
                        ks.insert(i.to_be_bytes(), b"value").unwrap();
                    }
                }
                folder
            },
            |folder| {
                let _db = Database::builder(&folder).open().unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("reopen/10k_items", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                {
                    let db = Database::builder(&folder).open().unwrap();
                    let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                    for i in 0..10_000u64 {
                        ks.insert(i.to_be_bytes(), b"value").unwrap();
                    }
                }
                folder
            },
            |folder| {
                let _db = Database::builder(&folder).open().unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(benches, recovery);
criterion_main!(benches);
