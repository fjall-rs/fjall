use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{segment::block::ValueBlock, serde::Serializable, Value};
use lz4_flex::compress_prepend_size;
use std::io::Write;

fn value_block_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("ValueBlock::size");

    for item_count in [10, 100, 1_000] {
        group.bench_function(format!("{item_count} items"), |b| {
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

fn load_block_from_disk(c: &mut Criterion) {
    let mut group = c.benchmark_group("Load block from disk");

    for block_size in [1, 4, 8, 16, 32, 64] {
        group.bench_function(format!("{block_size} KiB"), |b| {
            let block_size = block_size * 1_024;

            let mut size = 0;

            let mut items = vec![];

            for x in 0u64.. {
                let value = Value::new(
                    x.to_be_bytes(),
                    x.to_string().repeat(100).as_bytes(),
                    63,
                    lsm_tree::ValueType::Tombstone,
                );

                size += value.size();

                items.push(value);

                if size >= block_size {
                    break;
                }
            }

            let mut block = ValueBlock { items, crc: 0 };
            let mut file = tempfile::tempfile().unwrap();

            let mut bytes = Vec::with_capacity(u16::MAX.into());
            block.crc = ValueBlock::create_crc(&block.items).unwrap();
            block.serialize(&mut bytes).unwrap();
            let bytes = compress_prepend_size(&bytes);
            file.write_all(&bytes).unwrap();

            let block_size_on_disk = bytes.len();

            b.iter(|| {
                let loaded_block =
                    ValueBlock::from_file_compressed(&mut file, 0, block_size_on_disk as u32)
                        .unwrap();

                assert_eq!(loaded_block.items.len(), block.items.len());
                assert_eq!(loaded_block.crc, block.crc);
            });
        });
    }
}

criterion_group!(benches, value_block_size, load_block_from_disk);
criterion_main!(benches);
