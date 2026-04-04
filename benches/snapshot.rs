// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fjall::{Database, KeyspaceCreateOptions, Readable};

fn snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot");

    group.bench_function("get/hit", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                for i in 0..1_000u64 {
                    ks.insert(i.to_be_bytes(), b"value").unwrap();
                }
                let snap = db.snapshot();
                (folder, db, ks, snap)
            },
            |(_folder, _db, ks, snap)| {
                for i in 0..1_000u64 {
                    snap.get(&ks, i.to_be_bytes()).unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("get/miss", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                for i in 0..1_000u64 {
                    ks.insert(i.to_be_bytes(), b"value").unwrap();
                }
                let snap = db.snapshot();
                (folder, db, ks, snap)
            },
            |(_folder, _db, ks, snap)| {
                for i in 2_000..3_000u64 {
                    snap.get(&ks, i.to_be_bytes()).unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("iter", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                for i in 0..1_000u64 {
                    ks.insert(i.to_be_bytes(), b"value").unwrap();
                }
                let snap = db.snapshot();
                (folder, db, ks, snap)
            },
            |(_folder, _db, ks, snap)| {
                let count = snap.iter(&ks).count();
                assert_eq!(1_000, count);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("range", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                for i in 0..1_000u64 {
                    ks.insert(i.to_be_bytes(), b"value").unwrap();
                }
                let snap = db.snapshot();
                let lo = 100u64.to_be_bytes();
                let hi = 500u64.to_be_bytes();
                (folder, db, ks, snap, lo, hi)
            },
            |(_folder, _db, ks, snap, lo, hi)| {
                let _count = snap.range(&ks, lo..=hi).count();
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(benches, snapshot);
criterion_main!(benches);
