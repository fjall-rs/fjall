use fjall::config::{
    BlockSizePolicy, BloomConstructionPolicy, FilterPolicy, FilterPolicyEntry, HashRatioPolicy,
    PinningPolicy, RestartIntervalPolicy,
};
use fjall::{Database, KeyspaceCreateOptions};
use lsm_tree::Guard;
use test_log::test;

const ITEM_COUNT: usize = 100;

#[test]
fn reload_keyspace_config() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let data_block_size = BlockSizePolicy::all(6_666);
    let index_block_size = BlockSizePolicy::all(7_777);

    let data_block_interval_policy = RestartIntervalPolicy::all(8);
    // let index_block_interval_policy = RestartIntervalPolicy::all(9);

    let filter_block_pinning_policy = PinningPolicy::new(&[true, true, true, false]);
    let index_block_pinning_policy = PinningPolicy::new(&[true, true, false]);

    let filter_policy = FilterPolicy::new(&[
        FilterPolicyEntry::Bloom(BloomConstructionPolicy::BitsPerKey(10.0)),
        FilterPolicyEntry::Bloom(BloomConstructionPolicy::BitsPerKey(9.0)),
        FilterPolicyEntry::None,
    ]);

    let data_block_hash_ratio_policy = HashRatioPolicy::all(0.5);

    {
        let db = Database::builder(&folder).open()?;

        let _tree = db.keyspace(
            "default",
            KeyspaceCreateOptions::default()
                .data_block_size_policy(data_block_size.clone())
                .index_block_size_policy(index_block_size.clone())
                .data_block_restart_interval_policy(data_block_interval_policy.clone())
                // .index_block_restart_interval_policy(index_block_policy.clone())
                .filter_block_pinning_policy(filter_block_pinning_policy.clone())
                .index_block_pinning_policy(index_block_pinning_policy.clone())
                .expect_point_read_hits(true)
                .filter_policy(filter_policy.clone())
                .data_block_hash_ratio_policy(data_block_hash_ratio_policy.clone()),
        )?;
    };

    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
        assert_eq!(data_block_size, tree.config.data_block_size_policy);
        assert_eq!(index_block_size, tree.config.index_block_size_policy);
        assert_eq!(
            data_block_interval_policy,
            tree.config.data_block_restart_interval_policy,
        );
        // assert_eq!(
        //     index_block_interval_policy,
        //     tree.config.index_block_restart_interval_policy,
        // );
        assert_eq!(
            filter_block_pinning_policy,
            tree.config.filter_block_pinning_policy,
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
fn reload_with_keyspaces() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    // NOTE: clippy bug
    #[allow(unused_assignments)]
    {
        let db = Database::builder(&folder).open()?;

        let keyspaces = &[
            db.keyspace("default1", KeyspaceCreateOptions::default())?,
            db.keyspace("default2", KeyspaceCreateOptions::default())?,
            db.keyspace("default3", KeyspaceCreateOptions::default())?,
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
            db.keyspace("default1", KeyspaceCreateOptions::default())?,
            db.keyspace("default2", KeyspaceCreateOptions::default())?,
            db.keyspace("default3", KeyspaceCreateOptions::default())?,
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
