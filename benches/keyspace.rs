// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fjall::{Database, KeyspaceCreateOptions};

fn boundary_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("boundary_keys");

    group.bench_function("first_key_value", |b| {
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
                for _ in 0..1_000 {
                    let _guard = ks.first_key_value();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("last_key_value", |b| {
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
                for _ in 0..1_000 {
                    let _guard = ks.last_key_value();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("size_of/hit", |b| {
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
                for i in 0..1_000u64 {
                    ks.size_of(i.to_be_bytes()).unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn contains_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("contains_key");

    group.bench_function("hit", |b| {
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
                for i in 0..1_000u64 {
                    ks.contains_key(i.to_be_bytes()).unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("miss", |b| {
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
                for i in 2_000..3_000u64 {
                    ks.contains_key(i.to_be_bytes()).unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn clear(c: &mut Criterion) {
    let mut group = c.benchmark_group("clear");

    group.bench_function("1k_items", |b| {
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
                ks.clear().unwrap();
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn reverse_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("reverse_iter");

    group.bench_function("full", |b| {
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
                let count = ks.iter().rev().count();
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
                let lo = 100u64.to_be_bytes();
                let hi = 500u64.to_be_bytes();
                (folder, db, ks, lo, hi)
            },
            |(_folder, _db, ks, lo, hi)| {
                let _count = ks.range(lo..=hi).rev().count();
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("prefix", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                for i in 0..1_000u32 {
                    let key = format!("user:{i:05}:data");
                    ks.insert(key.as_bytes(), b"value").unwrap();
                }
                (folder, db, ks)
            },
            |(_folder, _db, ks)| {
                let _count = ks.prefix("user:001").rev().count();
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");

    group.bench_function("sequential", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                (folder, db, ks)
            },
            |(_folder, _db, ks)| {
                for i in 0..1_000u64 {
                    ks.insert(i.to_be_bytes(), b"value").unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("random", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                // Pre-generate random keys
                let keys: Vec<[u8; 8]> = (0..1_000u64)
                    .map(|i| xxhash_rust::xxh3::xxh3_64(&i.to_be_bytes()).to_be_bytes())
                    .collect();
                (folder, db, ks, keys)
            },
            |(_folder, _db, ks, keys)| {
                for key in &keys {
                    ks.insert(key, b"value").unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

fn get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");

    group.bench_function("hit", |b| {
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
                for i in 0..1_000u64 {
                    ks.get(i.to_be_bytes()).unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("miss", |b| {
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
                for i in 2_000..3_000u64 {
                    ks.get(i.to_be_bytes()).unwrap();
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");

    group.bench_function("full", |b| {
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
                let count = ks.iter().count();
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
                let lo = 100u64.to_be_bytes();
                let hi = 500u64.to_be_bytes();
                (folder, db, ks, lo, hi)
            },
            |(_folder, _db, ks, lo, hi)| {
                let _count = ks.range(lo..=hi).count();
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("prefix", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                for i in 0..1_000u32 {
                    let key = format!("user:{i:05}:data");
                    ks.insert(key.as_bytes(), b"value").unwrap();
                }
                (folder, db, ks)
            },
            |(_folder, _db, ks)| {
                let _count = ks.prefix("user:001").count();
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn batch(c: &mut Criterion) {
    c.bench_function("batch/commit", |b| {
        b.iter_batched(
            || {
                let folder = tempfile::tempdir().unwrap();
                let db = Database::builder(&folder).open().unwrap();
                let ks = db.keyspace("default", KeyspaceCreateOptions::default).unwrap();
                (folder, db, ks)
            },
            |(_folder, db, ks)| {
                let mut batch = db.batch();
                for i in 0..1_000u64 {
                    batch.insert(&ks, i.to_be_bytes(), b"value");
                }
                batch.commit().unwrap();
            },
            BatchSize::PerIteration,
        );
    });
}

fn remove(c: &mut Criterion) {
    c.bench_function("remove", |b| {
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
                for i in 0..1_000u64 {
                    ks.remove(i.to_be_bytes()).unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });
}

criterion_group!(
    benches,
    insert,
    get,
    scan,
    batch,
    remove,
    boundary_keys,
    contains_key,
    clear,
    reverse_iter,
);
criterion_main!(benches);
