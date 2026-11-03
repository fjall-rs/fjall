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

    for i in 0..5 {
        log::info!("[{i}] recovering");

        let db = Database::builder(&dir).open()?;

        // NOTE: The highest seqno in the v3 fixture is 12,
        // so migrated data must be visible, and new writes must get higher seqnos
        assert!(
            db.visible_seqno() > 12,
            "visible seqno: {}",
            db.visible_seqno(),
        );
        assert!(db.next_seqno() > 12, "next seqno: {}", db.next_seqno());

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        let tree2 = db.keyspace("default2", KeyspaceCreateOptions::default)?;

        assert!(!tree.contains_key("hello")?);
        assert!(!tree2.contains_key("hello")?);

        assert_eq!(Some(b"avalue".as_slice()), tree.get("a")?.as_deref());
        assert_eq!(Some(b"bvalue".as_slice()), tree.get("b")?.as_deref());
        assert_eq!(Some(b"cvalue".as_slice()), tree.get("c")?.as_deref());

        assert_eq!(Some(b"dvalue".as_slice()), tree2.get("d")?.as_deref());
    }

    Ok(())
}
