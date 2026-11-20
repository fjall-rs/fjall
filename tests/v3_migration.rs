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

    assert_migrated(dir.path())
}

// A crashed migration attempt leaves files in the DB-level folders,
// e.g. tables written by the migration flush; migrating again must still work
#[test]
fn v3_migration_journal_only_rerun_after_crash() -> fjall::Result<()> {
    let folder = "test_fixture/v3_keyspace";

    let dir = tempfile::tempdir()?;
    copy_dir_all(folder, &dir)?;

    // NOTE: Covers every file ID the migration could use for this fixture
    for folder in ["tables", "blobs"] {
        let folder = dir.path().join(folder);
        std::fs::create_dir_all(&folder)?;

        for id in 0..32 {
            std::fs::write(
                folder.join(id.to_string()),
                "leftover of a crashed migration",
            )?;
        }
    }

    assert_migrated(dir.path())?;

    for folder in ["tables", "blobs"] {
        for dirent in std::fs::read_dir(dir.path().join(folder))? {
            let path = dirent?.path();

            assert_ne!(
                b"leftover of a crashed migration".as_slice(),
                std::fs::read(&path)?,
                "{} should have been removed",
                path.display(),
            );
        }
    }

    Ok(())
}

/// Migrates the `v3_keyspace` fixture (or recovers it as v4, if already migrated)
/// a few times, and checks its contents and seqnos every time
fn assert_migrated(dir: &Path) -> fjall::Result<()> {
    for i in 0..5 {
        log::info!("[{i}] recovering");

        let db = Database::builder(dir).open()?;

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
