use fjall::{Database, KeyspaceCreateOptions};
use test_log::test;

#[test]
fn write_during_read() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;

    let tree = db.keyspace(
        "default",
        KeyspaceCreateOptions::default().max_memtable_size(128_000),
    )?;

    for x in 0u64..50_000 {
        tree.insert(x.to_be_bytes(), x.to_be_bytes())?;
    }
    tree.rotate_memtable_and_wait()?;

    for guard in tree.iter() {
        let (k, v) = guard.into_inner()?;
        tree.insert(k, v.repeat(4))?;
    }

    Ok(())
}
