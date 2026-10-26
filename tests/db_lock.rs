use fjall::{Database, DurabilityLevel::Buffer, KeyspaceCreateOptions};
use test_log::test;

#[test]
fn db_lock() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    tree.insert("asd", "def", Buffer)?;
    tree.insert("efg", "hgf", Buffer)?;
    tree.insert("hij", "wer", Buffer)?;

    drop(db);

    assert!(matches!(
        Database::builder(&folder).open(),
        Err(fjall::Error::Locked),
    ));

    Ok(())
}
