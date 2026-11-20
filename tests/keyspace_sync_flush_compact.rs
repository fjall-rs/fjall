use fjall::{Database, DurabilityLevel::Buffer, Keyspace, KeyspaceCreateOptions};
use test_log::test;

/// NOTE: No background workers, so only `flush()` flushes, and only `compact()` compacts
fn open(folder: &tempfile::TempDir) -> fjall::Result<(Database, Keyspace)> {
    let db = Database::builder(folder)
        .worker_threads_unchecked(0)
        .open()?;

    let tree = db.keyspace("default", || {
        KeyspaceCreateOptions::default().compaction_strategy(std::sync::Arc::new(
            fjall::compaction::Leveled::default().with_l0_threshold(2),
        ))
    })?;

    Ok((db, tree))
}

/// Writes one overlapping table per round: every round overwrites all keys with its round number
fn write_rounds(tree: &Keyspace, rounds: std::ops::Range<u8>) -> fjall::Result<()> {
    for round in rounds {
        for key in 0..100u8 {
            tree.insert([key], [round], Buffer)?;
        }
        tree.flush(true)?;
    }
    Ok(())
}

fn assert_round(tree: &Keyspace, round: u8) -> fjall::Result<()> {
    assert_eq!(100, tree.len()?);

    for key in 0..100u8 {
        assert_eq!(Some([round].as_slice()), tree.get([key])?.as_deref());
    }

    Ok(())
}

#[test]
fn sync_flush() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let (_db, tree) = open(&folder)?;

        // NOTE: Nothing sealed yet, so there is nothing to flush
        tree.insert("a", "a", Buffer)?;
        tree.flush(false)?;
        assert_eq!(0, tree.table_count());

        tree.flush(true)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(0, tree.sealed_memtable_count());

        // NOTE: Flushes an already sealed memtable
        tree.insert("b", "b", Buffer)?;
        tree.rotate_memtable()?;
        tree.flush(false)?;
        assert_eq!(2, tree.table_count());
        assert_eq!(0, tree.sealed_memtable_count());
    }

    {
        let (_db, tree) = open(&folder)?;
        assert_eq!(2, tree.table_count());
        assert_eq!(2, tree.len()?);
    }

    Ok(())
}

#[test]
fn sync_compact_leveled() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let (_db, tree) = open(&folder)?;
        write_rounds(&tree, 0..4)?;
        assert_eq!(4, tree.l0_table_count());

        tree.compact()?;

        assert_eq!(0, tree.l0_table_count());
        assert!(tree.table_count() > 0);
        assert_round(&tree, 3)?;
    }

    {
        let (_db, tree) = open(&folder)?;
        assert_eq!(0, tree.l0_table_count());
        assert_round(&tree, 3)?;
    }

    Ok(())
}
