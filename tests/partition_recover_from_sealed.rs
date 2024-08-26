use fjall::{Config, PartitionCreateOptions};
use test_log::test;

const ITEM_COUNT: usize = 10;

#[test]
fn recover_sealed_journal() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    // NOTE: clippy bug
    #[allow(unused_assignments)]
    {
        let keyspace = Config::new(&folder)
            .flush_workers(0)
            .compaction_workers(0)
            .open()?;

        let partitions = &[
            keyspace.open_partition("tree1", PartitionCreateOptions::default())?,
            keyspace.open_partition("tree2", PartitionCreateOptions::default())?,
            keyspace.open_partition("tree3", PartitionCreateOptions::default())?,
        ];

        for tree in partitions {
            for x in 0..ITEM_COUNT as u64 {
                let key = x.to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value.as_bytes())?;
            }
        }

        for tree in partitions {
            assert_eq!(tree.len()?, ITEM_COUNT);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT);
        }

        {
            use lsm_tree::AbstractTree;

            let first = partitions.first().unwrap();
            first.rotate_memtable()?;
            assert_eq!(1, first.tree.sealed_memtable_count());
        }

        for tree in partitions {
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

        {
            use lsm_tree::AbstractTree;

            let first = partitions.first().unwrap();
            assert_eq!(1, first.tree.sealed_memtable_count());
        }
    }

    for _ in 0..10 {
        let keyspace = Config::new(&folder)
            .flush_workers(0)
            .compaction_workers(0)
            .open()?;

        let partitions = &[
            keyspace.open_partition("tree1", PartitionCreateOptions::default())?,
            keyspace.open_partition("tree2", PartitionCreateOptions::default())?,
            keyspace.open_partition("tree3", PartitionCreateOptions::default())?,
        ];

        {
            use lsm_tree::AbstractTree;

            let first = partitions.first().unwrap();
            assert_eq!(1, first.tree.sealed_memtable_count());
        }

        for tree in partitions {
            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);
        }

        {
            use lsm_tree::AbstractTree;

            let first = partitions.first().unwrap();
            assert_eq!(1, first.tree.sealed_memtable_count());
        }
    }

    Ok(())
}

#[test]
fn recover_sealed_journal_blob() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    // NOTE: clippy bug
    #[allow(unused_assignments)]
    {
        let keyspace = Config::new(&folder)
            .flush_workers(0)
            .compaction_workers(0)
            .open()?;

        let partitions = &[
            keyspace.open_partition("tree1", PartitionCreateOptions::default())?,
            keyspace.open_partition("tree2", PartitionCreateOptions::default())?,
            keyspace.open_partition("tree3", PartitionCreateOptions::default())?,
        ];

        for tree in partitions {
            for x in 0..ITEM_COUNT as u64 {
                let key = x.to_be_bytes();
                let value = nanoid::nanoid!().repeat(1_000);
                tree.insert(key, value.as_bytes())?;
            }
        }

        for tree in partitions {
            assert_eq!(tree.len()?, ITEM_COUNT);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT);
        }

        {
            use lsm_tree::AbstractTree;

            let first = partitions.first().unwrap();
            first.rotate_memtable()?;
            assert_eq!(1, first.tree.sealed_memtable_count());
        }

        for tree in partitions {
            for x in 0..ITEM_COUNT as u64 {
                let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
                let value = nanoid::nanoid!().repeat(1_000);
                tree.insert(key, value.as_bytes())?;
            }
        }

        for tree in partitions {
            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);
        }

        {
            use lsm_tree::AbstractTree;

            let first = partitions.first().unwrap();
            assert_eq!(1, first.tree.sealed_memtable_count());
        }
    }

    for _ in 0..10 {
        let keyspace = Config::new(&folder)
            .flush_workers(0)
            .compaction_workers(0)
            .open()?;

        let partitions = &[
            keyspace.open_partition("tree1", PartitionCreateOptions::default())?,
            keyspace.open_partition("tree2", PartitionCreateOptions::default())?,
            keyspace.open_partition("tree3", PartitionCreateOptions::default())?,
        ];

        {
            use lsm_tree::AbstractTree;

            let first = partitions.first().unwrap();
            assert_eq!(1, first.tree.sealed_memtable_count());
        }

        for tree in partitions {
            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);
        }

        {
            use lsm_tree::AbstractTree;

            let first = partitions.first().unwrap();
            assert_eq!(1, first.tree.sealed_memtable_count());
        }
    }

    Ok(())
}
