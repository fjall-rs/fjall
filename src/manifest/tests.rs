use crate::{Database, DurabilityLevel::SyncAll, KeyspaceCreateOptions};
use std::path::Path;
use test_log::test;

/// Writes a database with a keyspace and some flushed + unflushed data
fn write_db(folder: &Path) -> crate::Result<()> {
    let db = Database::builder(folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    tree.insert("a", "flushed", SyncAll)?;
    tree.flush(true)?;
    tree.insert("b", "unflushed", SyncAll)?;

    Ok(())
}

fn assert_db(folder: &Path) -> crate::Result<()> {
    let db = Database::builder(folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    assert_eq!(Some(b"flushed".as_slice()), tree.get("a")?.as_deref());
    assert_eq!(Some(b"unflushed".as_slice()), tree.get("b")?.as_deref());
    assert_eq!(2, tree.len()?);

    Ok(())
}

fn set_current(folder: &Path, id: u64) -> std::io::Result<()> {
    std::fs::write(folder.join("current"), id.to_be_bytes())
}

/// A manifest rotation writes a full snapshot into the new fragment, then updates `current`,
/// so older fragments are stale and must be ignored (and removed)
#[test]
fn manifest_recovery_ignores_older_fragments() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_db(folder.path())?;

    // NOTE: hwal fragments don't contain their own ID, so a copy of v0 is a valid v1 (= the snapshot)
    std::fs::copy(folder.path().join("v0"), folder.path().join("v1"))?;
    std::fs::write(folder.path().join("v0"), b"garbage, must not be read")?;
    set_current(folder.path(), 1)?;

    assert_db(folder.path())?;

    assert!(
        !folder.path().join("v0").try_exists()?,
        "v0 should be removed",
    );
    assert!(folder.path().join("v1").try_exists()?, "v1 should be kept");

    assert_db(folder.path())?;

    Ok(())
}

/// A rotation that crashed before updating `current` leaves a newer fragment behind,
/// which was never committed and must be ignored (and removed)
#[test]
fn manifest_recovery_ignores_uncommitted_newer_fragment() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_db(folder.path())?;

    std::fs::write(folder.path().join("v1"), b"garbage, must not be read")?;

    assert_db(folder.path())?;

    assert!(folder.path().join("v0").try_exists()?, "v0 should be kept");
    assert!(
        !folder.path().join("v1").try_exists()?,
        "v1 should be removed",
    );

    Ok(())
}

/// If `current` names a fragment that does not exist, nothing may be removed
#[test]
fn manifest_recovery_missing_current_fragment() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_db(folder.path())?;

    set_current(folder.path(), 5)?;

    assert!(Database::builder(folder.path()).open().is_err());
    assert!(folder.path().join("v0").try_exists()?, "v0 should be kept");

    // NOTE: Fixing the marker makes the database usable again
    set_current(folder.path(), 0)?;
    assert_db(folder.path())?;

    Ok(())
}

/// A corrupted `current` marker is an error, nothing may be removed
#[test]
fn manifest_recovery_corrupted_current_marker() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_db(folder.path())?;

    std::fs::write(folder.path().join("current"), b"abc")?;

    assert!(Database::builder(folder.path()).open().is_err());
    assert!(folder.path().join("v0").try_exists()?, "v0 should be kept");

    Ok(())
}

/// Other files starting with `v` are not manifest fragments, so they must not be touched
#[test]
fn manifest_recovery_keeps_unrelated_files() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;
    write_db(folder.path())?;

    for name in ["v0.bak", "vfoo", "v", "v1x"] {
        std::fs::write(folder.path().join(name), b"not a manifest fragment")?;
    }

    assert_db(folder.path())?;

    for name in ["v0.bak", "vfoo", "v", "v1x", "version"] {
        assert!(
            folder.path().join(name).try_exists()?,
            "{name} should be kept",
        );
    }

    Ok(())
}

/// Returns the IDs of all manifest fragments in the folder
fn manifest_fragments(folder: &Path) -> std::io::Result<Vec<u64>> {
    let mut ids = vec![];

    for dirent in std::fs::read_dir(folder)? {
        let name = dirent?.file_name();
        if let Some(id) = super::ManifestNameStrategy::parse_id(&name) {
            ids.push(id);
        }
    }

    ids.sort_unstable();

    Ok(ids)
}

#[expect(clippy::expect_used)]
fn read_current(folder: &Path) -> std::io::Result<u64> {
    let bytes = std::fs::read(folder.join("current"))?;
    Ok(u64::from_be_bytes(
        bytes.try_into().expect("should be 8 bytes"),
    ))
}

/// Encoded configuration of every keyspace, by name
#[expect(clippy::type_complexity)]
fn configs(db: &Database) -> crate::Result<Vec<(String, Vec<(String, Vec<u8>)>)>> {
    let mut names = db.list_keyspace_names();
    names.sort();

    names
        .iter()
        .map(|name| {
            let ks = db.keyspace(name, KeyspaceCreateOptions::default)?;
            let kvs = ks
                .config
                .encode_kvs(ks.id)
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_vec()))
                .collect();
            Ok((name.to_string(), kvs))
        })
        .collect()
}

/// Several keyspaces with different configs, flushed + unflushed data, a compaction
fn write_rotation_db(db: &Database) -> crate::Result<()> {
    let plain = db.keyspace("plain", KeyspaceCreateOptions::default)?;
    let blobs = db.keyspace("blobs", || {
        KeyspaceCreateOptions::default().with_kv_separation(Some(
            crate::KvSeparationOptions::default().separation_threshold(1),
        ))
    })?;
    let leveled = db.keyspace("leveled", || {
        KeyspaceCreateOptions::default().compaction_strategy(std::sync::Arc::new(
            crate::compaction::Leveled::default().with_l0_threshold(2),
        ))
    })?;

    for ks in [&plain, &blobs, &leveled] {
        for round in 0..3u8 {
            ks.insert([round], "flushed", SyncAll)?;
            ks.flush(true)?;
        }
        ks.insert("unflushed", "value", SyncAll)?;
    }

    leveled.compact()?;
    plain.major_compact()?;

    Ok(())
}

/// Checks the keyspaces written by [`write_rotation_db`] (there may be more keyspaces)
fn assert_rotation_db(db: &Database) -> crate::Result<()> {
    for name in ["blobs", "leveled", "plain"] {
        assert!(db.keyspace_exists(name), "keyspace {name} should exist");

        let ks = db.keyspace(name, KeyspaceCreateOptions::default)?;

        for round in 0..3u8 {
            assert_eq!(Some(b"flushed".as_slice()), ks.get([round])?.as_deref());
        }
        assert_eq!(Some(b"value".as_slice()), ks.get("unflushed")?.as_deref());
    }

    assert!(db
        .keyspace("blobs", KeyspaceCreateOptions::default)?
        .is_kv_separated());

    Ok(())
}

#[test]
fn manifest_rotation_explicit() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    // NOTE: No background workers and no write stalls, so nothing waits for a compaction
    let open = || {
        Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .disable_write_stalls(true)
            .open()
    };

    let expected_configs = {
        let db = open()?;
        write_rotation_db(&db)?;

        let expected_configs = configs(&db)?;

        db.rotate_manifest()?;

        // NOTE: The previous fragment is removed once `current` names the new one
        assert_eq!(vec![1], manifest_fragments(folder.path())?);
        assert_eq!(1, read_current(folder.path())?);

        assert_rotation_db(&db)?;

        // NOTE: Edits after the rotation go to the new fragment
        db.keyspace("plain", KeyspaceCreateOptions::default)?
            .insert("after", "rotation", SyncAll)?;
        db.keyspace("plain", KeyspaceCreateOptions::default)?
            .flush(true)?;
        db.keyspace("late", KeyspaceCreateOptions::default)?;

        expected_configs
    };

    for _ in 0..2 {
        let db = open()?;
        assert_rotation_db(&db)?;
        assert!(db.keyspace_exists("late"));

        let plain = db.keyspace("plain", KeyspaceCreateOptions::default)?;
        assert_eq!(Some(b"rotation".as_slice()), plain.get("after")?.as_deref());

        // NOTE: The re-encoded configs must be exactly what was persisted before
        let configs = configs(&db)?
            .into_iter()
            .filter(|(name, _)| name != "late")
            .collect::<Vec<_>>();

        assert_eq!(expected_configs, configs);
    }

    Ok(())
}

#[test]
#[expect(clippy::indexing_slicing)]
fn manifest_rotation_automatic() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    // NOTE: No background workers and no write stalls, so nothing waits for a compaction
    let open = || {
        Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .disable_write_stalls(true)
            .manifest_rotation_threshold(1_024)
            .open()
    };

    {
        let db = open()?;
        write_rotation_db(&db)?;

        let plain = db.keyspace("plain", KeyspaceCreateOptions::default)?;
        for round in 0..50u8 {
            plain.insert("counter", [round], SyncAll)?;
            plain.flush(true)?;
        }

        // NOTE: The tiny threshold rotates many times, but only one fragment is ever left
        let fragments = manifest_fragments(folder.path())?;
        assert_eq!(1, fragments.len());
        assert!(
            fragments[0] > 1,
            "should have rotated several times: {fragments:?}",
        );
        assert_eq!(fragments[0], read_current(folder.path())?);
    }

    for _ in 0..2 {
        let db = open()?;
        assert_rotation_db(&db)?;

        let plain = db.keyspace("plain", KeyspaceCreateOptions::default)?;
        assert_eq!(Some([49u8].as_slice()), plain.get("counter")?.as_deref());
    }

    Ok(())
}
