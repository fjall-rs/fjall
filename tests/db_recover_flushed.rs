use fjall::{Database, DurabilityLevel::Buffer, KeyspaceCreateOptions, Readable};
use test_log::test;

#[test]
fn db_flush_all_smoke_test() -> fjall::Result<()> {
    let dir = tempfile::tempdir()?;

    let (seqno_before, visible_seqno_before) = {
        let db = Database::builder(&dir).worker_threads_unchecked(0).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        tree.insert("1", "abc", Buffer)?;
        assert_eq!(Some(b"abc".as_slice()), tree.get(b"1")?.as_deref());
        assert_eq!(1, db.next_seqno());
        assert_eq!(1, db.visible_seqno());

        db.flush_all()?;
        assert_eq!(Some(b"abc".as_slice()), tree.get(b"1")?.as_deref());
        assert_eq!(2, db.next_seqno());
        assert_eq!(2, db.visible_seqno());

        (db.next_seqno(), db.visible_seqno())
    };

    {
        let db = Database::builder(&dir).worker_threads_unchecked(0).open()?;

        assert_eq!(seqno_before, db.next_seqno());
        assert_eq!(visible_seqno_before, db.visible_seqno());

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert_eq!(Some(b"abc".as_slice()), tree.get(b"1")?.as_deref());

        // TODO: v4 fix seqno publishing in flush
        {
            let snapshot = db.snapshot();
            assert_eq!(
                Some(b"abc".as_slice()),
                snapshot.get(&tree, b"1")?.as_deref(),
            );
        }
    }

    Ok(())
}
