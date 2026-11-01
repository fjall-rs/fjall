// Regression test for https://github.com/fjall-rs/fjall/issues/68

use fjall::{Database, DurabilityLevel::Buffer, KeyspaceCreateOptions, KvSeparationOptions};

#[test_log::test]
fn journal_recover_large_value() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let large_value = "a".repeat(128_000);

    let (seqno_before, visible_seqno_before) = {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        tree.insert("a", &large_value, Buffer)?;
        tree.insert("b", "b", Buffer)?;

        (db.next_seqno(), db.visible_seqno())
    };

    {
        let db = Database::builder(&folder).open()?;

        assert_eq!(seqno_before, db.next_seqno());
        assert_eq!(visible_seqno_before, db.visible_seqno());

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        assert_eq!(large_value.as_bytes(), &*tree.get("a")?.unwrap());
        assert_eq!(b"b", &*tree.get("b")?.unwrap());
    }

    Ok(())
}

#[test_log::test]
fn journal_recover_large_value_blob() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let large_value = "a".repeat(128_000);

    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(KvSeparationOptions::default()))
        })?;
        tree.insert("a", &large_value, Buffer)?;
        tree.insert("b", "b", Buffer)?;
    }

    {
        let db = Database::builder(&folder).open()?;
        let tree = db.keyspace("default", || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(KvSeparationOptions::default()))
        })?;
        assert_eq!(large_value.as_bytes(), &*tree.get("a")?.unwrap());
        assert_eq!(b"b", &*tree.get("b")?.unwrap());
    }

    Ok(())
}
