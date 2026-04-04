// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, Readable, SingleWriterTxDatabase};

fn tx_optimistic(c: &mut Criterion) {
    let mut group = c.benchmark_group("tx_optimistic");

    group.bench_function("write/commit", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = OptimisticTxDatabase::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                (folder, db, ks)
            },
            |(_folder, db, ks)| {
                let mut tx = db.write_tx().unwrap();
                for i in 0..1_000u64 {
                    tx.insert(&ks, i.to_be_bytes(), b"value");
                }
                tx.commit().unwrap().unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("read_tx/get", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = OptimisticTxDatabase::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                {
                    let mut tx = db.write_tx().unwrap();
                    for i in 0..1_000u64 {
                        tx.insert(&ks, i.to_be_bytes(), b"value");
                    }
                    tx.commit().unwrap().unwrap();
                }
                (folder, db, ks)
            },
            |(_folder, db, ks)| {
                let snap = db.read_tx();
                for i in 0..1_000u64 {
                    snap.get(&ks, i.to_be_bytes()).unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("write/rollback", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = OptimisticTxDatabase::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                (folder, db, ks)
            },
            |(_folder, db, ks)| {
                let mut tx = db.write_tx().unwrap();
                for i in 0..1_000u64 {
                    tx.insert(&ks, i.to_be_bytes(), b"value");
                }
                tx.rollback();
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn tx_single_writer(c: &mut Criterion) {
    let mut group = c.benchmark_group("tx_single_writer");

    group.bench_function("write/commit", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = SingleWriterTxDatabase::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                (folder, db, ks)
            },
            |(_folder, db, ks)| {
                let mut tx = db.write_tx();
                for i in 0..1_000u64 {
                    tx.insert(&ks, i.to_be_bytes(), b"value");
                }
                tx.commit().unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("read_tx/get", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = SingleWriterTxDatabase::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                {
                    let mut tx = db.write_tx();
                    for i in 0..1_000u64 {
                        tx.insert(&ks, i.to_be_bytes(), b"value");
                    }
                    tx.commit().unwrap();
                }
                (folder, db, ks)
            },
            |(_folder, db, ks)| {
                let snap = db.read_tx();
                for i in 0..1_000u64 {
                    snap.get(&ks, i.to_be_bytes()).unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("write/rollback", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = SingleWriterTxDatabase::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                (folder, db, ks)
            },
            |(_folder, db, ks)| {
                let mut tx = db.write_tx();
                for i in 0..1_000u64 {
                    tx.insert(&ks, i.to_be_bytes(), b"value");
                }
                tx.rollback();
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(benches, tx_optimistic, tx_single_writer);
criterion_main!(benches);
