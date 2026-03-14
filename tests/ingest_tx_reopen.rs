use fjall::Readable;

/// Regression test for fjall#265:
/// Data written via start_ingestion(), persisted with SyncAll,
/// must be visible in read_tx() after reopening as SingleWriterTxDatabase.
///
/// Ingestion uses the public Database API (not the #[doc(hidden)] inner() escape hatch),
/// then reopens as SingleWriterTxDatabase to verify read_tx() visibility.
#[test_log::test]
fn ingested_data_visible_in_tx_after_reopen() -> fjall::Result<()> {
    use fjall::{Database, KeyspaceCreateOptions, PersistMode, SingleWriterTxDatabase};

    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;
        let items = db.keyspace("my_items", KeyspaceCreateOptions::default)?;

        let mut ingestion = items.start_ingestion()?;
        ingestion.write("key1", "value1")?;
        ingestion.write("key2", "value2")?;
        ingestion.write("key3", "value3")?;
        ingestion.finish()?;

        db.persist(PersistMode::SyncAll)?;

        assert_eq!(b"value1", &*items.get("key1")?.unwrap());
    }

    {
        let db = SingleWriterTxDatabase::builder(&folder).open()?;
        let items = db.keyspace("my_items", KeyspaceCreateOptions::default)?;

        assert_eq!(
            b"value1",
            &*items.get("key1")?.unwrap(),
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
