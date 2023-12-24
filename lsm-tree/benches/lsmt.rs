use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{generate_segment_id, segment::block::ValueBlock, BlockCache, Value};
use rand::Rng;
use std::sync::Arc;

fn value_block_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("ValueBlock::size");

    for item_count in [10, 100, 1_000] {
        group.bench_function(format!("ValueBlock::size - {item_count} items"), |b| {
            let items = (0..item_count)
                .map(|_| {
                    Value::new(
                        "a".repeat(16).as_bytes(),
                        "a".repeat(100).as_bytes(),
                        63,
                        lsm_tree::ValueType::Tombstone,
                    )
                })
                .collect();

            let block = ValueBlock { items, crc: 0 };

            b.iter(|| {
                block.size();
            })
        });
    }
}

fn block_cache_insert(c: &mut Criterion) {
    let block_cache = BlockCache::with_capacity_bytes(1_000);

    let items = (0..100)
        .map(|_| {
            Value::new(
                "a".repeat(16).as_bytes(),
                "a".repeat(100).as_bytes(),
                63,
                lsm_tree::ValueType::Tombstone,
            )
        })
        .collect();

    let block = Arc::new(ValueBlock { items, crc: 0 });

    c.bench_function("BlockCache::insert_disk_block", |b| {
        b.iter(|| {
            block_cache.insert_disk_block(
                generate_segment_id(),
                "asdasdasdasd".as_bytes().into(),
                block.clone(),
            );
        });
    });
}

fn block_cache_get(c: &mut Criterion) {
    let block_cache = BlockCache::with_capacity_bytes(u64::MAX);

    let items = (0..100)
        .map(|_| {
            Value::new(
                "a".repeat(16).as_bytes(),
                "a".repeat(100).as_bytes(),
                63,
                lsm_tree::ValueType::Tombstone,
            )
        })
        .collect();

    let seg_id = generate_segment_id();
    let block = Arc::new(ValueBlock { items, crc: 0 });

    (0u64..100_000).for_each(|idx| {
        block_cache.insert_disk_block(seg_id.clone(), idx.to_be_bytes().into(), block.clone())
    });
    assert_eq!(100_000, block_cache.len());

    let mut rng = rand::thread_rng();

    c.bench_function("BlockCache::get_disk_block", |b| {
        b.iter(|| {
            let key = rng.gen_range(0u64..100_000).to_be_bytes();
            let key: Arc<[u8]> = key.into();
            block_cache.get_disk_block(&seg_id, &key).unwrap();
        });
    });
}

criterion_group!(
    benches,
    value_block_size,
    block_cache_insert,
    block_cache_get
);
criterion_main!(benches);
