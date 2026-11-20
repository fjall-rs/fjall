use crate::{Database, KeyspaceCreateOptions, UserKey, UserValue};

#[test_log::test]
fn keyspace_ingest() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    // NOTE: No background workers, the compaction below runs synchronously
    let db = Database::builder(&folder)
        .worker_threads_unchecked(0)
        .open()?;

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

    // NOTE: 4 overlapping L0 tables reach the L0 threshold of the default (leveled) strategy
    items.compact()?;
    assert_eq!(1, items.table_count());
    assert_eq!(15, items.len()?);

    Ok(())
}
