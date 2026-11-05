use fjall::{AbstractTree, Database, DurabilityLevel::Buffer, KeyspaceCreateOptions, Readable};

fn open(folder: &tempfile::TempDir) -> fjall::Result<(Database, fjall::Keyspace)> {
    let db = Database::builder(folder).worker_threads_unchecked(0).open()?;
    let ks = db.keyspace("default", KeyspaceCreateOptions::default)?;
    Ok((db, ks))
}

/// Without snapshots, old versions (and their memtables) are pruned after every new version
#[test_log::test]
fn versions_pruned_without_snapshots() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (_db, ks) = open(&folder)?;

    for round in 0..10u8 {
        ks.insert("a", [round], Buffer)?;
        ks.flush(true)?;

        // NOTE: The latest version, and the one before it (readable at the GC watermark)
        assert!(
            ks.tree.version_set_len() <= 2,
            "round {round}: {} versions",
            ks.tree.version_set_len(),
        );
    }

    ks.major_compact()?;
    assert!(ks.tree.version_set_len() <= 2);

    Ok(())
}

/// An open snapshot keeps the versions it can read, until it is dropped
#[test_log::test]
fn versions_kept_for_open_snapshot() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let (db, ks) = open(&folder)?;

    ks.insert("a", "old", Buffer)?;
    ks.flush(true)?;

    let snapshot = db.snapshot();

    for round in 0..5u8 {
        ks.insert("a", [round], Buffer)?;
        ks.flush(true)?;
    }
    ks.major_compact()?;

    // NOTE: The snapshot's version (and the old table) must still be there
    assert!(
        ks.tree.version_set_len() > 2,
        "{} versions",
        ks.tree.version_set_len(),
    );
    assert_eq!(Some(b"old".as_slice()), snapshot.get(&ks, "a")?.as_deref());

    drop(snapshot);

    // NOTE: Pruning happens when the next version is installed
    ks.insert("a", "new", Buffer)?;
    ks.flush(true)?;

    assert!(
        ks.tree.version_set_len() <= 2,
        "{} versions",
        ks.tree.version_set_len(),
    );
    assert_eq!(Some(b"new".as_slice()), ks.get("a")?.as_deref());

    Ok(())
}
