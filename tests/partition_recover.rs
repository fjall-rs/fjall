use fjall::{Config, KvSeparationOptions, PartitionCreateOptions};
use test_log::test;

const ITEM_COUNT: usize = 100;

#[test]
fn reload_partition_config() -> fjall::Result<()> {
    use lsm_tree::coding::Encode;

    let folder = tempfile::tempdir()?;

    let serialized_config = {
        let keyspace = Config::new(&folder).open()?;
        let tree = keyspace.open_partition(
            "default",
            PartitionCreateOptions::default()
                .compaction_strategy(fjall::compaction::Strategy::SizeTiered(
                    fjall::compaction::SizeTiered::default(),
                ))
                .block_size(10_000)
                .with_kv_separation(
                    KvSeparationOptions::default()
                        .separation_threshold(4_000)
                        .file_target_size(150_000_000),
                ),
        )?;

        tree.config.encode_into_vec()
    };

    {
        let keyspace = Config::new(&folder).open()?;
        let tree = keyspace.open_partition("default", PartitionCreateOptions::default())?;
        assert_eq!(serialized_config, tree.config.encode_into_vec());
    }

    Ok(())
}

#[test]
fn reload_with_partitions() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    // NOTE: clippy bug
    #[allow(unused_assignments)]
    {
        let keyspace = Config::new(&folder).open()?;

        let partitions = &[
            keyspace.open_partition("default1", PartitionCreateOptions::default())?,
            keyspace.open_partition("default2", PartitionCreateOptions::default())?,
            keyspace.open_partition("default3", PartitionCreateOptions::default())?,
        ];

        for tree in partitions {
            for x in 0..ITEM_COUNT as u64 {
                let key = x.to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value.as_bytes())?;
            }

            for x in 0..ITEM_COUNT as u64 {
                let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value.as_bytes())?;
            }
        }

        for tree in partitions {
            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);
        }
    }

    for _ in 0..10 {
        let keyspace = Config::new(&folder).open()?;

        let partitions = &[
            keyspace.open_partition("default1", PartitionCreateOptions::default())?,
            keyspace.open_partition("default2", PartitionCreateOptions::default())?,
            keyspace.open_partition("default3", PartitionCreateOptions::default())?,
        ];

        for tree in partitions {
            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);
        }
    }

    Ok(())
}
