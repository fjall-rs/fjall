// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fjall::{Database, KeyspaceCreateOptions};

fn large_values(c: &mut Criterion) {
    let value_1kb = vec![0xABu8; 1_024];
    let value_64kb = vec![0xABu8; 65_536];

    let mut group = c.benchmark_group("large_values");

    group.bench_function("insert/1kb", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                (folder, db, ks)
            },
            |(_folder, _db, ks)| {
                for i in 0..1_000u64 {
                    ks.insert(i.to_be_bytes(), &value_1kb).unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("insert/64kb", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                (folder, db, ks)
            },
            |(_folder, _db, ks)| {
                for i in 0..100u64 {
                    ks.insert(i.to_be_bytes(), &value_64kb).unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("get/1kb", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                for i in 0..1_000u64 {
                    ks.insert(i.to_be_bytes(), &value_1kb).unwrap();
                }
                (folder, db, ks)
            },
            |(_folder, _db, ks)| {
                for i in 0..1_000u64 {
                    ks.get(i.to_be_bytes()).unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("get/64kb", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                for i in 0..100u64 {
                    ks.insert(i.to_be_bytes(), &value_64kb).unwrap();
                }
                (folder, db, ks)
            },
            |(_folder, _db, ks)| {
                for i in 0..100u64 {
                    ks.get(i.to_be_bytes()).unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("scan/1kb", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                for i in 0..1_000u64 {
                    ks.insert(i.to_be_bytes(), &value_1kb).unwrap();
                }
                (folder, db, ks)
            },
            |(_folder, _db, ks)| {
                let count = ks.iter().count();
                assert_eq!(1_000, count);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(benches, large_values);
criterion_main!(benches);
