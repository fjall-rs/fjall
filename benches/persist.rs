// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fjall::{Database, KeyspaceCreateOptions, PersistMode};

fn persist(c: &mut Criterion) {
    let mut group = c.benchmark_group("persist");

    group.bench_function("buffer", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                for i in 0..100u64 {
                    ks.insert(i.to_be_bytes(), b"value").unwrap();
                }
                (folder, db, ks)
            },
            |(_folder, db, _ks)| {
                db.persist(PersistMode::Buffer).unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("sync_data", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                for i in 0..100u64 {
                    ks.insert(i.to_be_bytes(), b"value").unwrap();
                }
                (folder, db, ks)
            },
            |(_folder, db, _ks)| {
                db.persist(PersistMode::SyncData).unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("sync_all", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                for i in 0..100u64 {
                    ks.insert(i.to_be_bytes(), b"value").unwrap();
                }
                (folder, db, ks)
            },
            |(_folder, db, _ks)| {
                db.persist(PersistMode::SyncAll).unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(benches, persist);
criterion_main!(benches);
