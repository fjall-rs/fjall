use crate::{Database, KeyspaceCreateOptions, UserKey, UserValue};

#[test]
fn write_halt_requests_compaction_until_l0_recovers() -> crate::Result<()> {
    use crate::worker_pool::WorkerMessage;
    use lsm_tree::AbstractTree;
    use std::time::Duration;

    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder)
        .worker_threads_unchecked(0)
        .open()?;
    let items = db.keyspace("items", KeyspaceCreateOptions::default)?;

    for i in 0u64..30 {
        items.insert("overlap", i.to_be_bytes().to_vec())?;
        items.tree.rotate_memtable();
        let lock = items.tree.get_flush_lock();
        items.tree.flush(&lock, 0)?;
    }
    assert_eq!(30, items.tree.l0_run_count());
    db.worker_pool.rx.drain().count();

    std::thread::scope(|scope| -> crate::Result<()> {
        let writer = scope.spawn(|| items.insert("final", "value"));
        let requested = db.worker_pool.rx.recv_timeout(Duration::from_secs(2));
        // Release the blocked writer even when the assertion below fails.
        crate::compaction::worker::run(&items, &db.supervisor.snapshot_tracker, &db.stats)?;
        writer.join().expect("writer panicked")?;
        assert!(matches!(requested, Ok(WorkerMessage::Compact(_))));
        Ok(())
    })?;

    assert!(items.tree.l0_run_count() < 30);
    assert_eq!(
        items.get("overlap")?.as_deref(),
        Some(29u64.to_be_bytes().as_slice())
    );
    assert_eq!(items.get("final")?.as_deref(), Some(b"value".as_slice()));
    Ok(())
}

#[test_log::test]
#[ignore = "flimsy because of the compaction check, probably race condition... run the compaction synchronously"]
fn keyspace_ingest() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).worker_threads(0).open()?;
    let items = db.keyspace("items", KeyspaceCreateOptions::default)?;

    {
        let mut ingest = items.start_ingestion()?;

        for (k, v) in [0u8, 1, 2, 3, 4, 5]
            .into_iter()
            .map(|i| (UserKey::new(&i.to_be_bytes()), UserValue::empty()))
        {
            ingest.write(k, v)?;
        }

        ingest.finish()?;
    };
    assert_eq!(6, items.len()?);
    assert_eq!(1, items.table_count());

    {
        let mut ingest = items.start_ingestion()?;

        for (k, v) in [1u8, 6, 7, 8, 9]
            .into_iter()
            .map(|i| (UserKey::new(&i.to_be_bytes()), UserValue::empty()))
        {
            ingest.write(k, v)?;
        }

        ingest.finish()?;
    }
    assert_eq!(10, items.len()?);
    assert_eq!(2, items.table_count());

    {
        let mut ingest = items.start_ingestion()?;

        for (k, v) in [10u8, 11, 12]
            .into_iter()
            .map(|i| (UserKey::new(&i.to_be_bytes()), UserValue::empty()))
        {
            ingest.write(k, v)?;
        }

        ingest.finish()?;
    }
    assert_eq!(13, items.len()?);
    assert_eq!(3, items.table_count());

    {
        let mut ingest = items.start_ingestion()?;

        for (k, v) in [13u8, 14]
            .into_iter()
            .map(|i| (UserKey::new(&i.to_be_bytes()), UserValue::empty()))
        {
            ingest.write(k, v)?;
        }

        ingest.finish()?;
    }
    assert_eq!(15, items.len()?);
    assert_eq!(4, items.table_count());

    while !db.worker_pool.sender.is_empty() {}
    assert_eq!(1, items.table_count());

    Ok(())
}
