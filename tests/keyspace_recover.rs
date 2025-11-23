use std::sync::Arc;

use fjall::config::{
    BlockSizePolicy, BloomConstructionPolicy, FilterPolicy, FilterPolicyEntry, HashRatioPolicy,
    PinningPolicy, RestartIntervalPolicy,
};
use fjall::{CompressionType, Database, KeyspaceCreateOptions, KvSeparationOptions};
use lsm_tree::compaction::CompactionStrategy;
use test_log::test;

const ITEM_COUNT: usize = 100;

#[test]
fn reload_keyspace_config_fifo() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let strategy = fjall::compaction::Fifo::new(555, Some(6));

    let expected_kvs = strategy.get_config();

    {
        let db = Database::builder(&folder).open()?;

        let _tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default().compaction_strategy(Arc::new(strategy))
        })?;
    };

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert_eq!(
            expected_kvs,
            tree.config.compaction_strategy.get_config(),
            "compaction strategy config does not match",
        );
    }

    Ok(())
}

#[test]
fn reload_keyspace_config_leveled() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let strategy = fjall::compaction::Leveled::default()
        .with_l0_threshold(6)
        .with_level_ratio_policy(vec![4.0, 6.0, 8.0])
        .with_table_target_size(100_000_000);

    let expected_kvs = strategy.get_config();

    {
        let db = Database::builder(&folder).open()?;

        let _tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default().compaction_strategy(Arc::new(strategy))
        })?;
    };

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert_eq!(
            expected_kvs,
            tree.config.compaction_strategy.get_config(),
            "compaction strategy config does not match",
        );
    }

    Ok(())
}

#[test]
fn reload_keyspace_config() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let data_block_size = BlockSizePolicy::all(6_666);

    let data_block_interval_policy = RestartIntervalPolicy::all(8);
    // let index_block_interval_policy = RestartIntervalPolicy::all(9);

    let filter_block_pinning_policy = PinningPolicy::new([true, true, true, false]);
    let index_block_pinning_policy = PinningPolicy::new([true, true, false]);

    let filter_block_partitioning_policy = PinningPolicy::new([false, false, true]);
    let index_block_partitioning_policy = PinningPolicy::new([false, false, false, false, true]);

    let filter_policy = FilterPolicy::new(&[
        FilterPolicyEntry::Bloom(BloomConstructionPolicy::BitsPerKey(10.0)),
        FilterPolicyEntry::Bloom(BloomConstructionPolicy::BitsPerKey(9.0)),
        FilterPolicyEntry::None,
    ]);

    let data_block_hash_ratio_policy = HashRatioPolicy::all(0.5);

    {
        let db = Database::builder(&folder).open()?;

        let _tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default()
                .data_block_size_policy(data_block_size.clone())
                .data_block_restart_interval_policy(data_block_interval_policy.clone())
                // .index_block_restart_interval_policy(index_block_policy.clone())
                .filter_block_pinning_policy(filter_block_pinning_policy.clone())
                .index_block_pinning_policy(index_block_pinning_policy.clone())
                .filter_block_partitioning_policy(filter_block_partitioning_policy.clone())
                .index_block_partitioning_policy(index_block_partitioning_policy.clone())
                .expect_point_read_hits(true)
                .filter_policy(filter_policy.clone())
                .data_block_hash_ratio_policy(data_block_hash_ratio_policy.clone())
        })?;
    };

    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        assert_eq!(data_block_size, tree.config.data_block_size_policy);
        assert_eq!(
            data_block_interval_policy,
            tree.config.data_block_restart_interval_policy,
        );
        // assert_eq!(
        //     index_block_interval_policy,
        //     tree.config.index_block_restart_interval_policy,
        // );
        assert_eq!(
            filter_block_partitioning_policy,
            tree.config.filter_block_partitioning_policy,
        );
        assert_eq!(
            filter_block_pinning_policy,
            tree.config.filter_block_pinning_policy,
        );
        assert_eq!(
            index_block_partitioning_policy,
            tree.config.index_block_partitioning_policy,
        );
        assert_eq!(
            index_block_pinning_policy,
            tree.config.index_block_pinning_policy,
        );
        assert!(tree.config.expect_point_read_hits);
        assert_eq!(filter_policy, tree.config.filter_policy);

        assert_eq!(
            data_block_hash_ratio_policy,
            tree.config.data_block_hash_ratio_policy,
        );
    }

    Ok(())
}

#[test]
fn reload_keyspace_config_blob_opts() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;

        let _tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default().with_kv_separation(Some(
                KvSeparationOptions::default()
                    .age_cutoff(0.55)
                    .compression(CompressionType::None)
                    .file_target_size(124)
                    .separation_threshold(515)
                    .staleness_threshold(0.77),
            ))
        })?;
    };

    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        let blob_opts = tree.config.kv_separation_opts.as_ref().unwrap();

        assert_eq!(blob_opts.compression, CompressionType::None);
        assert_eq!(blob_opts.age_cutoff, 0.55);
        assert_eq!(blob_opts.file_target_size, 124);
        assert_eq!(blob_opts.separation_threshold, 515);
        assert_eq!(blob_opts.staleness_threshold, 0.77);
    }

    Ok(())
}

#[test]
fn reload_with_keyspaces() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;

        let keyspaces = &[
            db.keyspace("default1", KeyspaceCreateOptions::default)?,
            db.keyspace("default2", KeyspaceCreateOptions::default)?,
            db.keyspace("default3", KeyspaceCreateOptions::default)?,
        ];

        for tree in keyspaces {
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

        for tree in keyspaces {
            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flat_map(|x| x.key()).count(), ITEM_COUNT * 2);
            assert_eq!(
                tree.iter().rev().flat_map(|x| x.key()).count(),
                ITEM_COUNT * 2
            );
        }
    }

    for _ in 0..10 {
        let db = Database::builder(&folder).open()?;

        let keyspaces = &[
            db.keyspace("default1", KeyspaceCreateOptions::default)?,
            db.keyspace("default2", KeyspaceCreateOptions::default)?,
            db.keyspace("default3", KeyspaceCreateOptions::default)?,
        ];

        for tree in keyspaces {
            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flat_map(|x| x.key()).count(), ITEM_COUNT * 2);
            assert_eq!(
                tree.iter().rev().flat_map(|x| x.key()).count(),
                ITEM_COUNT * 2
            );
        }
    }

    Ok(())
}
