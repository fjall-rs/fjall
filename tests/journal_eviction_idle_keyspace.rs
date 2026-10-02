use fjall::{CompressionType, Database, Keyspace, KeyspaceCreateOptions};

const MIB: usize = 1_024 * 1_024;

// NOTE: Values are stored uncompressed, so 64 MiB of them fill a journal
fn open(folder: &tempfile::TempDir) -> fjall::Result<Database> {
    Database::builder(folder)
        .journal_compression(CompressionType::None)
        .open()
}

// NOTE: Large enough to take 64 MiB of writes without rotating on its own
fn heavy_options() -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default().max_memtable_size(128 * MIB as u64)
}

/// Writes enough into `heavy` for its flush to seal the active journal, then flushes it.
fn seal_journal(db: &Database, heavy: &Keyspace) -> fjall::Result<()> {
    for i in 0u32..64 {
        heavy.insert(i.to_be_bytes(), vec![0; MIB])?;
    }
    heavy.rotate_memtable_and_wait()?;
    assert_eq!(2, db.journal_count());
    Ok(())
}

/// Rotating a memtable runs journal maintenance before it returns.
fn run_journal_maintenance(keyspace: &Keyspace) -> fjall::Result<()> {
    keyspace.insert("maintenance", "")?;
    keyspace.rotate_memtable_and_wait()
}

/// Leaves `keyspace` with empty memtables and without `key`, through a flush,
/// a delete and a major compaction.
fn delete_and_compact(keyspace: &Keyspace, key: &str, other: &Keyspace) -> fjall::Result<()> {
    keyspace.rotate_memtable_and_wait()?;
    keyspace.remove(key)?;
    keyspace.rotate_memtable_and_wait()?;

    // NOTE: Lets the tombstone fall below the seqno that compaction may drop
    run_journal_maintenance(other)?;

    keyspace.major_compact()
}

#[test_log::test]
fn journal_eviction_after_clear() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = open(&folder)?;

    let cleared = db.keyspace("cleared", KeyspaceCreateOptions::default)?;
    let heavy = db.keyspace("heavy", heavy_options)?;

    cleared.insert("a", "a")?;
    seal_journal(&db, &heavy)?;

    cleared.clear()?;

    run_journal_maintenance(&heavy)?;
    assert_eq!(1, db.journal_count());

    Ok(())
}

#[test_log::test]
fn journal_eviction_after_compaction_drops_every_table() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = open(&folder)?;

    let compacted = db.keyspace("compacted", KeyspaceCreateOptions::default)?;
    let slow = db.keyspace("slow", KeyspaceCreateOptions::default)?;
    let heavy = db.keyspace("heavy", heavy_options)?;

    compacted.insert("a", "a")?;
    slow.insert("a", "a")?;
    seal_journal(&db, &heavy)?;

    // NOTE: `slow` holds the sealed journal meanwhile
    delete_and_compact(&compacted, "a", &heavy)?;
    assert_eq!(0, compacted.table_count());
    assert_eq!(2, db.journal_count());

    slow.rotate_memtable_and_wait()?;

    run_journal_maintenance(&heavy)?;
    assert_eq!(1, db.journal_count());

    Ok(())
}

#[test_log::test]
fn journal_eviction_after_compaction_drops_newest_entries() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = open(&folder)?;

    let compacted = db.keyspace("compacted", KeyspaceCreateOptions::default)?;
    let slow = db.keyspace("slow", KeyspaceCreateOptions::default)?;
    let heavy = db.keyspace("heavy", heavy_options)?;

    compacted.insert("kept", "kept")?;
    compacted.rotate_memtable_and_wait()?;

    compacted.insert("a", "a")?;
    slow.insert("a", "a")?;
    seal_journal(&db, &heavy)?;

    // NOTE: `slow` holds the sealed journal meanwhile
    delete_and_compact(&compacted, "a", &heavy)?;
    assert_eq!(1, compacted.table_count());
    assert!(compacted.contains_key("kept")?);
    assert_eq!(2, db.journal_count());

    slow.rotate_memtable_and_wait()?;

    run_journal_maintenance(&heavy)?;
    assert_eq!(1, db.journal_count());

    Ok(())
}
