// A worker that fails a flush or compaction poisons the database and exits.
// Writers waiting on that worker, and the close, must then return instead of
// waiting forever. The failure is forced by making the keyspace's table folder
// read-only, so these tests only run on Unix and skip themselves as root.
#![cfg(unix)]

use fjall::{AbstractTree, Database, KeyspaceCreateOptions};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

const DEADLINE: Duration = Duration::from_secs(10);

/// Makes a folder read-only, so the next table written into it fails, and
/// restores it on drop so the temporary directory can be removed.
struct ReadOnly(PathBuf);

impl ReadOnly {
    fn set(dir: &Path) -> std::io::Result<Self> {
        fs::set_permissions(dir, fs::Permissions::from_mode(0o555))?;
        Ok(Self(dir.to_path_buf()))
    }
}

impl Drop for ReadOnly {
    fn drop(&mut self) {
        let _ = fs::set_permissions(&self.0, fs::Permissions::from_mode(0o755));
    }
}

/// `false` where a read-only folder still accepts new files (running as root).
fn permissions_apply() -> std::io::Result<bool> {
    let dir = tempfile::tempdir()?;
    let _guard = ReadOnly::set(dir.path())?;
    Ok(fs::File::create(dir.path().join("probe")).is_err())
}

fn tables_folder(ks: &fjall::Keyspace) -> PathBuf {
    let path = ks.path().join("tables");
    assert!(path.is_dir(), "no table folder at {}", path.display());
    path
}

/// Writers waiting for sealed memtables to flush return `Poisoned` once the
/// only worker dies of a failed flush.
#[test_log::test]
fn writers_in_backpressure_return_when_the_flush_fails() -> fjall::Result<()> {
    if !permissions_apply()? {
        eprintln!("skipped: read-only folders are writable for this user");
        return Ok(());
    }

    let dir = tempfile::tempdir()?;
    let db = Database::builder(&dir).worker_threads(1).open()?;
    let ks = db.keyspace("items", KeyspaceCreateOptions::default)?;
    let tables = tables_folder(&ks);

    // Holding the flush lock keeps the worker from flushing, so the sealed
    // memtables pile up to the backpressure threshold.
    let flush_lock = ks.tree.get_flush_lock();

    for i in 0..4u32 {
        ks.insert("a", i.to_be_bytes())?;
        assert!(ks.rotate_memtable()?);
    }
    assert_eq!(4, ks.tree.sealed_memtable_count());

    let (tx, rx) = mpsc::channel();

    let writer = ks.clone();
    let insert_tx = tx.clone();
    thread::spawn(move || insert_tx.send(("insert", writer.insert("b", "b"))).unwrap());

    let mut batch = db.batch();
    batch.insert(&ks, "c", "c");
    thread::spawn(move || tx.send(("batch", batch.commit())).unwrap());

    assert!(
        rx.recv_timeout(Duration::from_millis(500)).is_err(),
        "writers should wait in backpressure while the flush is held",
    );

    let read_only = ReadOnly::set(&tables)?;
    drop(flush_lock);

    for _ in 0..2 {
        let (writer, result) = rx
            .recv_timeout(DEADLINE)
            .expect("a writer still waits in backpressure after the worker died");
        assert!(
            matches!(result, Err(fjall::Error::Poisoned)),
            "{writer} returned {result:?}",
        );
    }

    drop(read_only);
    Ok(())
}

/// A writer held at the L0 write halt returns `Poisoned` once the compaction
/// it requests fails and kills the worker.
#[test_log::test]
#[ignore = "needs #326: on main nothing requests a compaction at the write halt"]
fn writer_at_the_write_halt_returns_when_the_compaction_fails() -> fjall::Result<()> {
    if !permissions_apply()? {
        eprintln!("skipped: read-only folders are writable for this user");
        return Ok(());
    }

    let dir = tempfile::tempdir()?;
    let db = Database::builder(&dir).worker_threads(1).open()?;
    let ks = db.keyspace("items", KeyspaceCreateOptions::default)?;
    let tables = tables_folder(&ks);

    for i in 0..30u32 {
        // Keys at both ends make every table overlap, so each flush is its own run.
        ks.insert("a", i.to_be_bytes())?;
        ks.insert("z", i.to_be_bytes())?;
        ks.tree.flush_active_memtable(0)?; // adds a run without queueing a compaction
    }
    assert_eq!(30, ks.tree.l0_run_count());

    let read_only = ReadOnly::set(&tables)?;

    let (tx, rx) = mpsc::channel();
    let writer = ks.clone();
    thread::spawn(move || tx.send(writer.insert("final", "value")).unwrap());

    let result = rx
        .recv_timeout(DEADLINE)
        .expect("the writer still waits at the write halt after the worker died");
    assert!(
        matches!(result, Err(fjall::Error::Poisoned)),
        "the write returned {result:?}",
    );

    drop(read_only);
    Ok(())
}
