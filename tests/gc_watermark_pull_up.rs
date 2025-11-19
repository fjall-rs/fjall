use fjall::Database;
use std::time::Duration;
use test_log::test;

#[test]
#[ignore = "3.0.0"]
fn db_recover_empty() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", Default::default())?;

    for _ in 0..10_000 {
        tree.insert("a", "a")?;
    }

    // NOTE: Wait for monitor thread tick to kick in
    std::thread::sleep(Duration::from_secs(1));

    assert!(db.supervisor.snapshot_tracker.get_seqno_safe_to_gc() > 0);

    Ok(())
}
