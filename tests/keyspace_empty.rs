use fjall::{Database, KeyspaceCreateOptions};

// Smoke test to see that a Version is always found
#[test_log::test]
fn empty_keyspace() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder)
        .worker_threads_unchecked(0)
        .open()?;

    assert_eq!(0, db.next_seqno());
    assert_eq!(0, db.visible_seqno());

    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    assert!(tree.is_empty()?);
    assert!(tree.get(b"hello")?.is_none());

    assert_eq!(0, db.next_seqno());
    assert_eq!(0, db.visible_seqno());

    Ok(())
}
