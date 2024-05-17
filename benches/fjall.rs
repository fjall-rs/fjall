use criterion::{criterion_group, criterion_main, Criterion};
use fjall::BlockCache;
use lsm_tree::segment::{
    block::header::Header as BlockHeader, meta::CompressionType, value_block::ValueBlock,
};
use lsm_tree::Value;
use rand::Rng;
use std::sync::Arc;

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

    let block = Arc::new(ValueBlock {
        items,
        header: BlockHeader {
            compression: CompressionType::Lz4,
            crc: 0,
            previous_block_offset: 0,
            data_length: 0,
        },
    });

    let mut id = 0;

    c.bench_function("BlockCache::insert_disk_block", |b| {
        b.iter(|| {
            block_cache.insert_disk_block((0, id).into(), 40, block.clone());
            id += 1;
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

    let seg_id = (0, 0).into();
    let block = Arc::new(ValueBlock {
        items,
        header: BlockHeader {
            compression: CompressionType::Lz4,
            crc: 0,
            previous_block_offset: 0,
            data_length: 0,
        },
    });

    (0u64..100_000).for_each(|idx| block_cache.insert_disk_block(seg_id, idx, block.clone()));
    assert_eq!(100_000, block_cache.len());

    let mut rng = rand::thread_rng();

    c.bench_function("BlockCache::get_disk_block", |b| {
        b.iter(|| {
            let key = rng.gen_range(0u64..100_000);
            block_cache.get_disk_block(seg_id, key).unwrap();
        });
    });
}

criterion_group!(benches, block_cache_insert, block_cache_get);
criterion_main!(benches);
