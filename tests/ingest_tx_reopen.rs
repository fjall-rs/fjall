use fjall::Readable;

/// Regression test for fjall#265:
/// Data written via start_ingestion(), persisted with SyncAll,
/// must be visible in read_tx() after reopening as SingleWriterTxDatabase.
#[test_log::test]
fn ingested_data_visible_in_tx_after_reopen() -> fjall::Result<()> {
    use fjall::{KeyspaceCreateOptions, PersistMode, SingleWriterTxDatabase};

    let folder = tempfile::tempdir()?;

    {
        let db = SingleWriterTxDatabase::builder(&folder).open()?;
        let items = db.keyspace("my_items", KeyspaceCreateOptions::default)?;

        let mut ingestion = items.inner().start_ingestion()?;
        ingestion.write("key1", "value1")?;
        ingestion.write("key2", "value2")?;
        ingestion.write("key3", "value3")?;
        ingestion.finish()?;

        db.persist(PersistMode::SyncAll)?;

        assert_eq!(b"value1", &*items.inner().get("key1")?.unwrap());

        let snapshot = db.read_tx();
        assert_eq!(
            b"value1",
            &*snapshot.get(&items, "key1")?.unwrap(),
            "ingested data should be visible in read_tx before close"
        );

        drop(db);
    }

    {
        let db = SingleWriterTxDatabase::builder(&folder).open()?;
        let items = db.keyspace("my_items", KeyspaceCreateOptions::default)?;

        assert_eq!(
            b"value1",
            &*items.inner().get("key1")?.unwrap(),
            "ingested data should be visible via direct get after reopen"
        );

        let snapshot = db.read_tx();
        assert_eq!(
            b"value1",
            &*snapshot.get(&items, "key1")?.unwrap(),
            "ingested data should be visible in read_tx after reopen"
        );
        assert_eq!(
            b"value2",
            &*snapshot.get(&items, "key2")?.unwrap(),
            "ingested data should be visible in read_tx after reopen"
        );
        assert_eq!(
            b"value3",
            &*snapshot.get(&items, "key3")?.unwrap(),
            "ingested data should be visible in read_tx after reopen"
        );
    }

    Ok(())
}

/// Same test but with regular Database + snapshot()
#[test_log::test]
fn ingested_data_visible_in_snapshot_after_reopen() -> fjall::Result<()> {
    use fjall::{Database, KeyspaceCreateOptions, PersistMode};

    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;
        let items = db.keyspace("my_items", KeyspaceCreateOptions::default)?;

        let mut ingestion = items.start_ingestion()?;
        ingestion.write("key1", "value1")?;
        ingestion.write("key2", "value2")?;
        ingestion.finish()?;

        db.persist(PersistMode::SyncAll)?;

        let snapshot = db.snapshot();
        assert_eq!(
            b"value1",
            &*snapshot.get(&items, "key1")?.unwrap(),
            "ingested data should be visible in snapshot before close"
        );

        drop(db);
    }

    {
        let db = Database::builder(&folder).open()?;
        let items = db.keyspace("my_items", KeyspaceCreateOptions::default)?;

        assert_eq!(b"value1", &*items.get("key1")?.unwrap());
        assert_eq!(b"value2", &*items.get("key2")?.unwrap());

        let snapshot = db.snapshot();
        assert_eq!(
            b"value1",
            &*snapshot.get(&items, "key1")?.unwrap(),
            "ingested data should be visible in snapshot after reopen"
        );
        assert_eq!(
            b"value2",
            &*snapshot.get(&items, "key2")?.unwrap(),
            "ingested data should be visible in snapshot after reopen"
        );
    }

    Ok(())
}
