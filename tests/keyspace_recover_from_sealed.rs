use fjall::{Database, KeyspaceCreateOptions};
use test_log::test;

const ITEM_COUNT: usize = 10;

#[test]
fn recover_sealed_journal() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    // NOTE: clippy bug
    #[allow(unused_assignments)]
    {
        let db = Database::builder(&folder)
            .flush_workers(0)
            .compaction_workers(0)
            .open()?;

        let keyspaces = &[
            db.keyspace("tree1", KeyspaceCreateOptions::default())?,
            db.keyspace("tree2", KeyspaceCreateOptions::default())?,
            db.keyspace("tree3", KeyspaceCreateOptions::default())?,
        ];

        for tree in keyspaces {
            for x in 0..ITEM_COUNT as u64 {
                let key = x.to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value.as_bytes())?;
            }
        }

        for tree in keyspaces {
            assert_eq!(tree.len()?, ITEM_COUNT);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT);
        }

        {
            use lsm_tree::AbstractTree;

            let first = keyspaces.first().unwrap();
            first.rotate_memtable()?;
            assert_eq!(1, first.tree.sealed_memtable_count());
        }

        for tree in keyspaces {
            for x in 0..ITEM_COUNT as u64 {
                let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value.as_bytes())?;
            }
        }

        for tree in keyspaces {
            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);
        }

        {
            use lsm_tree::AbstractTree;

            let first = keyspaces.first().unwrap();
            assert_eq!(1, first.tree.sealed_memtable_count());
        }
    }

    for _ in 0..10 {
        let db = Database::builder(&folder)
            .flush_workers(0)
            .compaction_workers(0)
            .open()?;

        let keyspaces = &[
            db.keyspace("tree1", KeyspaceCreateOptions::default())?,
            db.keyspace("tree2", KeyspaceCreateOptions::default())?,
            db.keyspace("tree3", KeyspaceCreateOptions::default())?,
        ];

        {
            use lsm_tree::AbstractTree;

            let first = keyspaces.first().unwrap();
            assert_eq!(1, first.tree.sealed_memtable_count());
        }

        for tree in keyspaces {
            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);
        }

        {
            use lsm_tree::AbstractTree;

            let first = keyspaces.first().unwrap();
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
        let db = Database::builder(&folder)
            .flush_workers(0)
            .compaction_workers(0)
            .open()?;

        let keyspaces = &[
            db.keyspace("tree1", KeyspaceCreateOptions::default())?,
            db.keyspace("tree2", KeyspaceCreateOptions::default())?,
            db.keyspace("tree3", KeyspaceCreateOptions::default())?,
        ];

        for tree in keyspaces {
            for x in 0..ITEM_COUNT as u64 {
                let key = x.to_be_bytes();
                let value = nanoid::nanoid!().repeat(1_000);
                tree.insert(key, value.as_bytes())?;
            }
        }

        for tree in keyspaces {
            assert_eq!(tree.len()?, ITEM_COUNT);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT);
        }

        {
            use lsm_tree::AbstractTree;

            let first = keyspaces.first().unwrap();
            first.rotate_memtable()?;
            assert_eq!(1, first.tree.sealed_memtable_count());
        }

        for tree in keyspaces {
            for x in 0..ITEM_COUNT as u64 {
                let key: [u8; 8] = (x + ITEM_COUNT as u64).to_be_bytes();
                let value = nanoid::nanoid!().repeat(1_000);
                tree.insert(key, value.as_bytes())?;
            }
        }

        for tree in keyspaces {
            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);
        }

        {
            use lsm_tree::AbstractTree;

            let first = keyspaces.first().unwrap();
            assert_eq!(1, first.tree.sealed_memtable_count());
        }
    }

    for _ in 0..10 {
        let db = Database::builder(&folder)
            .flush_workers(0)
            .compaction_workers(0)
            .open()?;

        let keyspaces = &[
            db.keyspace("tree1", KeyspaceCreateOptions::default())?,
            db.keyspace("tree2", KeyspaceCreateOptions::default())?,
            db.keyspace("tree3", KeyspaceCreateOptions::default())?,
        ];

        {
            use lsm_tree::AbstractTree;

            let first = keyspaces.first().unwrap();
            assert_eq!(1, first.tree.sealed_memtable_count());
        }

        for tree in keyspaces {
            assert_eq!(tree.len()?, ITEM_COUNT * 2);
            assert_eq!(tree.iter().flatten().count(), ITEM_COUNT * 2);
            assert_eq!(tree.iter().rev().flatten().count(), ITEM_COUNT * 2);
        }

        {
            use lsm_tree::AbstractTree;

            let first = keyspaces.first().unwrap();
            assert_eq!(1, first.tree.sealed_memtable_count());
        }
    }

    Ok(())
}
