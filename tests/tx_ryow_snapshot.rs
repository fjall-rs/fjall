use fjall::Readable;

/// Tests that modifications to a write transaction do not dirty a previously started snapshot
#[test_log::test]
fn tx_ryow_snapshot() -> fjall::Result<()> {
    use fjall::{KeyspaceCreateOptions, SingleWriterTxDatabase};

    let folder = tempfile::tempdir()?;

    let db = SingleWriterTxDatabase::builder(&folder).open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    let mut tx = db.write_tx();

    {
        let iter = tx.iter(&tree);
        for x in 0u64..100 {
            tx.insert(&tree, x.to_be_bytes(), "a");
        }
        assert_eq!(0, iter.count());
    }

    assert_eq!(100, tx.iter(&tree).count());

    Ok(())
}

#[test_log::test]
fn tx_ryow_snapshot_ssi() -> fjall::Result<()> {
    use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase};

    let folder = tempfile::tempdir()?;

    let db = OptimisticTxDatabase::builder(&folder).open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    let mut tx = db.write_tx()?;

    {
        let iter = tx.iter(&tree);
        for x in 0u64..100 {
            tx.insert(&tree, x.to_be_bytes(), "a");
        }
        assert_eq!(0, iter.count());
    }

    assert_eq!(100, tx.iter(&tree).count());

    tree.insert("2", "2")?;
    assert_eq!(100, tx.iter(&tree).count());

    let iter = tx.iter(&tree);
    tree.insert("3", "3")?;
    assert_eq!(100, iter.count());

    Ok(())
}
