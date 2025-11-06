use fjall::{Database, UserKey, UserValue};

#[test_log::test]
fn keyspace_ingest() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder)
        .flush_workers(0)
        .compaction_workers(1)
        .open()?;

    let items = db.keyspace("items", Default::default())?;

    items.ingest(
        [0u8, 1, 2, 3, 4, 5]
            .into_iter()
            .map(|i| (UserKey::new(&i.to_be_bytes()), UserValue::empty())),
    )?;
    assert_eq!(6, items.len()?);
    assert_eq!(1, items.table_count());

    items.ingest(
        [1u8, 6, 7, 8, 9]
            .into_iter()
            .map(|i| (UserKey::new(&i.to_be_bytes()), UserValue::empty())),
    )?;
    assert_eq!(10, items.len()?);
    assert_eq!(2, items.table_count());

    items.ingest(
        [10u8, 11, 12]
            .into_iter()
            .map(|i| (UserKey::new(&i.to_be_bytes()), UserValue::empty())),
    )?;
    assert_eq!(13, items.len()?);
    assert_eq!(3, items.table_count());

    items.ingest(
        [13u8, 14]
            .into_iter()
            .map(|i| (UserKey::new(&i.to_be_bytes()), UserValue::empty())),
    )?;
    assert_eq!(15, items.len()?);
    assert_eq!(4, items.table_count());

    while db.compaction_manager.len() > 0 {}
    assert_eq!(1, items.table_count());

    Ok(())
}
