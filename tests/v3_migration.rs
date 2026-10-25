use fjall::{Database, KeyspaceCreateOptions};
use std::path::Path;
use test_log::test;

fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> std::io::Result<()> {
    std::fs::create_dir_all(&dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
        } else {
            std::fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
        }
    }
    Ok(())
}

// V3 -> V4 migration with journal-only data
#[test]
fn v3_migration_journal_only() -> fjall::Result<()> {
    let folder = "test_fixture/v3_keyspace";

    let dir = tempfile::tempdir()?;
    copy_dir_all(folder, &dir)?;

    let db = Database::builder(&dir).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    let tree2 = db.keyspace("default2", KeyspaceCreateOptions::default)?;

    /*
        tree.insert("hello", "world")?;
        tree2.insert("hello", "world")?;

        tree.clear()?;
        tree2.clear()?;

        tree.insert("a", "avalue")?;
        tree.insert("b", "bvalue")?;
        tree.insert("c", "cvalue")?;

        tree2.insert("d", "dvalue")?;
    */

    assert!(!tree.contains_key("hello")?);
    assert!(!tree2.contains_key("hello")?);

    assert_eq!(Some(b"avalue".as_slice()), tree.get("a")?.as_deref());
    assert_eq!(Some(b"bvalue".as_slice()), tree.get("b")?.as_deref());
    assert_eq!(Some(b"cvalue".as_slice()), tree.get("c")?.as_deref());

    assert_eq!(Some(b"dvalue".as_slice()), tree2.get("d")?.as_deref());

    Ok(())
}
