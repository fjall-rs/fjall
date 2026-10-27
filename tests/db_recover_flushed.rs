use fjall::{Database, DurabilityLevel::Buffer, KeyspaceCreateOptions};
use test_log::test;

#[test]
fn db_flush_all_smoke_test() -> fjall::Result<()> {
    let dir = tempfile::tempdir()?;

    {
        let db = Database::builder(&dir).worker_threads_unchecked(0).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        tree.insert("1", "abc", Buffer)?;
        assert_eq!(Some(b"abc".as_slice()), tree.get(b"1")?.as_deref());

        db.flush_all()?;
        assert_eq!(Some(b"abc".as_slice()), tree.get(b"1")?.as_deref());
    }

    {
        let db = Database::builder(&dir).worker_threads_unchecked(0).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert_eq!(Some(b"abc".as_slice()), tree.get(b"1")?.as_deref());
    }

    Ok(())
}
