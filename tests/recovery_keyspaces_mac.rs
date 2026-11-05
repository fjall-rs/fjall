use fjall::{Database, KeyspaceCreateOptions};
use test_log::test;

/// macOS may put `.DS_Store` and `._<file name>` (AppleDouble) files into any folder,
/// so recovery must ignore unknown files in the DB folders
fn recover_with_junk_files(names: &[&str]) -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let db = Database::builder(&folder)
            .worker_threads_unchecked(0)
            .open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        tree.insert("a", "a", fjall::DurabilityLevel::Buffer)?;
        db.flush_all()?;
        tree.insert("b", "b", fjall::DurabilityLevel::Buffer)?;
        db.persist(fjall::DurabilityLevel::SyncAll)?;
    }

    for subfolder in ["", "tables", "blobs"] {
        let subfolder = folder.path().join(subfolder);
        std::fs::create_dir_all(&subfolder)?;

        for name in names {
            std::fs::File::create(subfolder.join(name))?;
        }
    }

    {
        let db = Database::builder(&folder)
            .worker_threads_unchecked(0)
            .open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        assert_eq!(2, tree.len()?);
        assert_eq!(Some("a".as_bytes().into()), tree.get("a")?);
        assert_eq!(Some("b".as_bytes().into()), tree.get("b")?);
    }

    Ok(())
}

#[test]
fn db_keyspaces_recovery_mac_ds_store() -> fjall::Result<()> {
    recover_with_junk_files(&[".DS_Store"])
}

#[test]
fn db_keyspaces_recovery_mac_underscore() -> fjall::Result<()> {
    recover_with_junk_files(&["._0", "._v0", "._current", "._version", "._0.hwal"])
}
