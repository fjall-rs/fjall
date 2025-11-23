use fjall::{Database, KeyspaceCreateOptions};
use test_log::test;

#[test]
fn keyspace_prefix_carl() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;

    let tree = db.keyspace("default", || {
        KeyspaceCreateOptions::default().with_kv_separation(Default::default())
    })?;

    tree.insert("a#1", "a".repeat(4_096))?;
    tree.insert("a#2", "b".repeat(4_096))?;
    tree.insert("a#3", "c".repeat(4_096))?;

    tree.rotate_memtable_and_wait()?;

    {
        let mut iter = tree.prefix("a#");

        let guard = iter.next().expect("should exist");
        let key = guard.key()?; // does not load blob
        assert_eq!(b"a#1", &*key);

        let guard = iter.next().expect("should exist");
        let (key, value) = guard.into_inner()?; // loads blob
        assert_eq!(
            (b"a#2" as &[u8], &b"b".repeat(4_096) as &[u8]),
            (&*key, &*value),
        );

        let guard = iter.next().expect("should exist");
        let (key, value) = guard.into_inner()?; // loads blob
        assert_eq!(
            (b"a#3" as &[u8], &b"c".repeat(4_096) as &[u8]),
            (&*key, &*value),
        );

        assert!(iter.next().is_none(), "iter should be consumed");
    }

    Ok(())
}
