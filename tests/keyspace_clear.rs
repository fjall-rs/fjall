use fjall::{Database, DurabilityLevel::Buffer, KeyspaceCreateOptions};

#[test_log::test]
fn clear_recover() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        assert!(tree.is_empty()?);

        tree.insert("a", "a", Buffer)?;
        assert!(tree.contains_key("a")?);

        tree.clear()?;
        assert!(tree.is_empty()?);
    }

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert!(tree.is_empty()?);
    }

    Ok(())
}

#[test_log::test]
fn clear_recover_multi_tree() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        let other = db.keyspace("other", KeyspaceCreateOptions::default)?;
        assert!(tree.is_empty()?);

        tree.insert("a", "a", Buffer)?;
        assert!(tree.contains_key("a")?);

        tree.clear()?;
        assert!(tree.is_empty()?);

        other.insert("a", "z", Buffer)?;
    }

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        assert!(tree.is_empty()?);

        let other = db.keyspace("other", KeyspaceCreateOptions::default)?;
        other.clear()?;
        assert!(other.is_empty()?);
    }

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        let other = db.keyspace("other", KeyspaceCreateOptions::default)?;
        assert!(tree.is_empty()?);
        assert!(other.is_empty()?);

        tree.insert("a", "a", Buffer)?;
        other.clear()?;
    }

    {
        let db = Database::builder(&folder).open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        let other = db.keyspace("other", KeyspaceCreateOptions::default)?;
        assert!(tree.contains_key("a")?);
        assert!(other.is_empty()?);
    }

    Ok(())
}

#[test_log::test]
fn clear_smoke_test() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    let other = db.keyspace("other", KeyspaceCreateOptions::default)?;

    other.insert("a", "z", Buffer)?;

    // Clear memtable only
    tree.insert("a", "a", Buffer)?;
    tree.insert("b", "b", Buffer)?;
    assert_eq!(2, tree.len()?);

    let seqno_before = db.visible_seqno();
    tree.clear()?;
    assert!(db.visible_seqno() > seqno_before);
    assert!(tree.is_empty()?);
    assert_eq!(None, tree.get("a")?);

    // Clear tables + memtable
    tree.insert("a", "a", Buffer)?;
    tree.insert("b", "b", Buffer)?;
    db.flush_all()?;
    assert!(tree.table_count() > 0);

    tree.insert("c", "c", Buffer)?;
    assert_eq!(3, tree.len()?);

    let seqno_before = db.visible_seqno();
    tree.clear()?;
    assert!(db.visible_seqno() > seqno_before);
    assert!(tree.is_empty()?);
    assert_eq!(0, tree.table_count());
    assert_eq!(None, tree.get("a")?);
    assert_eq!(None, tree.get("c")?);
    assert_eq!(0, tree.iter().count());
    assert_eq!(0, tree.iter().rev().count());

    // Writes after clear are visible
    tree.insert("d", "d", Buffer)?;
    assert_eq!(1, tree.len()?);
    assert_eq!(Some("d".as_bytes().into()), tree.get("d")?);

    // Other keyspace is untouched
    assert_eq!(1, other.len()?);
    assert_eq!(Some("z".as_bytes().into()), other.get("a")?);

    Ok(())
}
