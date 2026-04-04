// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fjall::{Database, KeyspaceCreateOptions};

fn multi_keyspace(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_keyspace");

    group.bench_function("insert/2ks", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks1 = db.keyspace("ks1", KeyspaceCreateOptions::default).unwrap();
                let ks2 = db.keyspace("ks2", KeyspaceCreateOptions::default).unwrap();
                (folder, db, ks1, ks2)
            },
            |(_folder, _db, ks1, ks2)| {
                for i in 0..500u64 {
                    ks1.insert(i.to_be_bytes(), b"value").unwrap();
                    ks2.insert(i.to_be_bytes(), b"value").unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("batch/2ks", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks1 = db.keyspace("ks1", KeyspaceCreateOptions::default).unwrap();
                let ks2 = db.keyspace("ks2", KeyspaceCreateOptions::default).unwrap();
                (folder, db, ks1, ks2)
            },
            |(_folder, db, ks1, ks2)| {
                let mut batch = db.batch();
                for i in 0..500u64 {
                    batch.insert(&ks1, i.to_be_bytes(), b"value");
                    batch.insert(&ks2, i.to_be_bytes(), b"value");
                }
                batch.commit().unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("get/2ks", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks1 = db.keyspace("ks1", KeyspaceCreateOptions::default).unwrap();
                let ks2 = db.keyspace("ks2", KeyspaceCreateOptions::default).unwrap();
                for i in 0..500u64 {
                    ks1.insert(i.to_be_bytes(), b"value").unwrap();
                    ks2.insert(i.to_be_bytes(), b"value").unwrap();
                }
                (folder, db, ks1, ks2)
            },
            |(_folder, _db, ks1, ks2)| {
                for i in 0..500u64 {
                    ks1.get(i.to_be_bytes()).unwrap();
                    ks2.get(i.to_be_bytes()).unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(benches, multi_keyspace);
criterion_main!(benches);
