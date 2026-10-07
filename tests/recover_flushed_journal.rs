use fjall::{AbstractTree, Database, KeyspaceCreateOptions};
use std::sync::Arc;
use test_log::test;

const ITEM_COUNT: u64 = 100;

fn open_fifo(db: &Database) -> fjall::Result<fjall::Keyspace> {
    db.keyspace("default", || {
        KeyspaceCreateOptions::default()
            .compaction_strategy(Arc::new(fjall::compaction::Fifo::new(u64::MAX, None)))
    })
}

#[test]
fn recover_does_not_replay_flushed_items() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    // FIFO needs strictly monotonic keys, so count down
    let key = |x: u64| (u64::MAX - x).to_be_bytes();

    {
        let db = Database::builder(&folder).open()?;
        let tree = open_fifo(&db)?;

        for x in 0..ITEM_COUNT {
            tree.insert(key(x), "a")?;
        }

        // Flushed, but still in the active journal
        tree.rotate_memtable_and_wait()?;
        assert_eq!(1, tree.tree.l0_run_count());
    }

    {
        let db = Database::builder(&folder).open()?;
        let tree = open_fifo(&db)?;

        assert_eq!(0, tree.tree.active_memtable().len());
        assert_eq!(tree.len()?, ITEM_COUNT as usize);

        for x in ITEM_COUNT..ITEM_COUNT * 2 {
            tree.insert(key(x), "a")?;
        }

        tree.rotate_memtable_and_wait()?;
        assert_eq!(1, tree.tree.l0_run_count());
        assert_eq!(tree.len()?, ITEM_COUNT as usize * 2);

        // Would fail if the FIFO compaction panicked and poisoned the database
        tree.insert(key(ITEM_COUNT * 2), "a")?;
    }

    Ok(())
}

#[test]
fn recover_flushed_items_after_clear() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        for x in 0..ITEM_COUNT {
            tree.insert(x.to_be_bytes(), "old")?;
        }
        tree.rotate_memtable_and_wait()?;

        tree.clear()?;

        for x in ITEM_COUNT..ITEM_COUNT * 2 {
            tree.insert(x.to_be_bytes(), "new")?;
        }
        tree.rotate_memtable_and_wait()?;
    }

    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        // Replaying the clear drops the flushed table, so the items written after it
        // have to come back from the journal
        assert_eq!(tree.len()?, ITEM_COUNT as usize);
        assert!(tree.iter().all(|x| x.value().is_ok_and(|v| &*v == b"new")));
    }

    Ok(())
}
