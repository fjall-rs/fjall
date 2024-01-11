use lsm_tree::{id::generate_segment_id, segment::block::ValueBlock, BlockCache, Value};
use std::sync::Arc;
use test_log::test;

#[test]
fn block_cache() -> lsm_tree::Result<()> {
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

    (0u64..100_000).for_each(|idx| {
        block_cache
            .get_disk_block(&seg_id, &idx.to_be_bytes().into())
            .unwrap();
    });

    Ok(())
}
