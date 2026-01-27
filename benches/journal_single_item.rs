use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fjall::batch::item::Item as BatchItem;
use fjall::journal::writer::Writer;
use fjall::{Database, KeyspaceCreateOptions};
use lsm_tree::{UserKey, UserValue, ValueType};
use tempfile::tempdir;

/// Generate realistic timeseries key-value pairs
fn generate_entry(index: u64) -> (Vec<u8>, Vec<u8>) {
    let sensor_id = index % 100;
    let timestamp = 1_000_000_000 + index;

    let key = format!("sensor_{:03}:{}", sensor_id, timestamp);
    let temp = 20.0 + (sensor_id as f32 * 0.1) % 15.0;
    let pressure = 1000.0 + (timestamp as f32 * 0.01) % 50.0;
    let value = format!("temp:{:.1},pressure:{:.2}", temp, pressure);

    (key.into_bytes(), value.into_bytes())
}

fn write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_throughput");

    // Test with specific dataset sizes: 1K, 10K, 100K, 500K, 1M
    for &size in &[1_000u64, 10_000, 100_000, 500_000, 1_000_000] {
        group.throughput(Throughput::Elements(size));

        // Benchmark 1: write_raw (SingleItem format)
        group.bench_with_input(BenchmarkId::new("single_item", size), &size, |b, &size| {
            b.iter(|| {
                let dir = tempdir().unwrap();
                let journal_path = dir.path().join("journal.log");

                let mut writer = Writer::create_new(&journal_path).unwrap();

                for i in 0..size {
                    let (key, value) = generate_entry(i);
                    writer
                        .write_raw(
                            black_box(1), // keyspace_id
                            black_box(&key),
                            black_box(&value),
                            black_box(ValueType::Value),
                            black_box(i), // seqno
                        )
                        .unwrap();
                }

                drop(writer);
            });
        });

        // Benchmark 2: write_batch with 1 item per batch
        group.bench_with_input(BenchmarkId::new("batch_1_item", size), &size, |b, &size| {
            b.iter(|| {
                let dir = tempdir().unwrap();

                // Create a real Database and Keyspace for BatchItem
                let db_dir = dir.path().join("db");
                let db = Database::builder(&db_dir).open().unwrap();
                let keyspace = db
                    .keyspace("bench", KeyspaceCreateOptions::default)
                    .unwrap();

                let journal_path = dir.path().join("journal.log");
                let mut writer = Writer::create_new(&journal_path).unwrap();

                for i in 0..size {
                    let (key, value) = generate_entry(i);

                    // Create BatchItem with real Keyspace
                    let item = BatchItem {
                        keyspace: keyspace.clone(),
                        key: UserKey::from(key),
                        value: UserValue::from(value),
                        value_type: ValueType::Value,
                    };

                    writer
                        .write_batch(
                            black_box(std::iter::once(&item)),
                            black_box(1),
                            black_box(i),
                        )
                        .unwrap();
                }

                drop(writer);
            });
        });
    }

    group.finish();
}

fn small_value_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("small_value_overhead");

    let size = 100_000u64;
    group.throughput(Throughput::Elements(size));

    // Test different value sizes to show where overhead matters most
    let value_sizes = vec![
        ("4_bytes", 4),
        ("8_bytes", 8),
        ("16_bytes", 16),
        ("32_bytes", 32),
        ("64_bytes", 64),
        ("128_bytes", 128),
        ("256_bytes", 256),
    ];

    for (name, value_size) in value_sizes {
        // SingleItem format
        group.bench_with_input(
            BenchmarkId::new(format!("single_item_{}", name), size),
            &value_size,
            |b, &value_size| {
                b.iter(|| {
                    let dir = tempdir().unwrap();
                    let journal_path = dir.path().join("journal.log");
                    let mut writer = Writer::create_new(&journal_path).unwrap();

                    for i in 0..size {
                        let key = format!("key_{:08}", i).into_bytes();
                        let value = vec![b'x'; value_size];

                        writer
                            .write_raw(
                                black_box(1),
                                black_box(&key),
                                black_box(&value),
                                black_box(ValueType::Value),
                                black_box(i),
                            )
                            .unwrap();
                    }

                    drop(writer);
                });
            },
        );

        // Batch(1) format
        group.bench_with_input(
            BenchmarkId::new(format!("batch_1_{}", name), size),
            &value_size,
            |b, &value_size| {
                let setup_dir = tempdir().unwrap();
                let db = Database::builder(setup_dir.path()).open().unwrap();
                let keyspace = db
                    .keyspace("bench", KeyspaceCreateOptions::default)
                    .unwrap();

                b.iter(|| {
                    let dir = tempdir().unwrap();
                    let journal_path = dir.path().join("journal.log");
                    let mut writer = Writer::create_new(&journal_path).unwrap();

                    for i in 0..size {
                        let key = format!("key_{:08}", i).into_bytes();
                        let value = vec![b'x'; value_size];

                        let item = BatchItem {
                            keyspace: keyspace.clone(),
                            key: UserKey::from(key),
                            value: UserValue::from(value),
                            value_type: ValueType::Value,
                        };

                        writer
                            .write_batch(
                                black_box(std::iter::once(&item)),
                                black_box(1),
                                black_box(i),
                            )
                            .unwrap();
                    }

                    drop(writer);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, write_throughput, small_value_overhead);
criterion_main!(benches);
