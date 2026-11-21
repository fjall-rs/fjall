use fjall::{compaction::Leveled, Database, DurabilityLevel::Buffer, KeyspaceCreateOptions};
use std::sync::Arc;
use test_log::test;

/// A major compaction writes tables of the keyspace's configured target size
#[test]
fn major_compact_uses_table_target_size() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder)
        .worker_threads_unchecked(0)
        .open()?;

    let tree = db.keyspace("default", || {
        KeyspaceCreateOptions::default()
            .compaction_strategy(Arc::new(Leveled::default().with_table_target_size(4_096)))
    })?;

    // NOTE: ~100 KB of random (barely compressible) values
    for key in 0..1_000_u32 {
        tree.insert(key.to_be_bytes(), nanoid::nanoid!(100), Buffer)?;
    }
    tree.flush(true)?;

    tree.major_compact()?;

    assert!(
        tree.table_count() > 10,
        "should be split into tables of ~4 KiB, got {} tables",
        tree.table_count(),
    );
    assert_eq!(1_000, tree.len()?);

    Ok(())
}
