use fjall::{Database, KeyspaceCreateOptions};

#[test]
fn oversized_active_journal_recovers_with_bounded_write_buffer() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    const MEMTABLE_BUDGET: u64 = 64 * 1024;
    const ROWS: u64 = 8_000;
    let value = [b'x'; 256];

    // With no worker, writes leave a single active journal and memtable far
    // larger than the configured budget. This models a database left behind
    // by the old single-worker queue deadlock.
    {
        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;
        let keyspace = db.keyspace("oversized", || {
            KeyspaceCreateOptions::default().max_memtable_size(MEMTABLE_BUDGET)
        })?;
        for i in 0..ROWS {
            keyspace.insert(i.to_be_bytes(), value)?;
        }
        assert!(db.write_buffer_size() > MEMTABLE_BUDGET * 8);
    }

    let db = Database::builder(folder.path())
        .worker_threads_unchecked(0)
        .open()?;
    let keyspace = db.keyspace("oversized", KeyspaceCreateOptions::default)?;
    assert!(
        db.write_buffer_size() <= MEMTABLE_BUDGET * 2,
        "recovery retained {} bytes in memory",
        db.write_buffer_size()
    );
    for i in [0, ROWS / 2, ROWS - 1] {
        assert_eq!(
            keyspace.get(i.to_be_bytes())?.as_deref(),
            Some(value.as_slice())
        );
    }
    drop(keyspace);
    drop(db);

    // The original journal is intentionally retained until normal journal
    // maintenance can evict it. Replaying it over recovered tables must work.
    let reopened = Database::builder(folder.path())
        .worker_threads_unchecked(0)
        .open()?;
    let keyspace = reopened.keyspace("oversized", KeyspaceCreateOptions::default)?;
    assert!(reopened.write_buffer_size() <= MEMTABLE_BUDGET * 2);
    for i in [0, ROWS / 2, ROWS - 1] {
        assert_eq!(
            keyspace.get(i.to_be_bytes())?.as_deref(),
            Some(value.as_slice())
        );
    }
    Ok(())
}

#[test]
fn bounded_replay_preserves_clear_and_tombstones_across_reopens() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let value = [b'v'; 256];
    {
        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;
        let keyspace = db.keyspace("cleared", || {
            KeyspaceCreateOptions::default().max_memtable_size(64 * 1024)
        })?;
        for i in 0_u64..4_000 {
            keyspace.insert(i.to_be_bytes(), value)?;
        }
        keyspace.clear()?;
        for i in 4_000_u64..8_000 {
            keyspace.insert(i.to_be_bytes(), value)?;
        }
        for i in 4_000_u64..4_200 {
            keyspace.remove(i.to_be_bytes())?;
        }
    }

    for _ in 0..2 {
        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;
        let keyspace = db.keyspace("cleared", KeyspaceCreateOptions::default)?;
        assert!(keyspace.get(0_u64.to_be_bytes())?.is_none());
        assert!(keyspace.get(4_000_u64.to_be_bytes())?.is_none());
        assert_eq!(
            keyspace.get(7_999_u64.to_be_bytes())?.as_deref(),
            Some(value.as_slice())
        );
        assert!(db.write_buffer_size() <= 128 * 1024);
    }
    Ok(())
}

#[test]
fn recovery_flushes_only_after_a_multi_keyspace_batch() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let value = [b'b'; 1024];
    {
        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;
        let left = db.keyspace("left", || {
            KeyspaceCreateOptions::default().max_memtable_size(512)
        })?;
        let right = db.keyspace("right", || {
            KeyspaceCreateOptions::default().max_memtable_size(512)
        })?;
        let mut batch = db.batch();
        batch.insert(&left, "shared", value);
        batch.insert(&right, "shared", value);
        batch.commit()?;
    }

    let db = Database::builder(folder.path())
        .worker_threads_unchecked(0)
        .open()?;
    for name in ["left", "right"] {
        let keyspace = db.keyspace(name, KeyspaceCreateOptions::default)?;
        assert_eq!(keyspace.get("shared")?.as_deref(), Some(value.as_slice()));
    }
    Ok(())
}

#[test]
fn recovery_bounds_total_active_bytes_across_keyspaces() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let value = [b'm'; 256];
    {
        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;
        for name in ["one", "two", "three", "four"] {
            let keyspace = db.keyspace(name, || {
                KeyspaceCreateOptions::default().max_memtable_size(64 * 1024 * 1024)
            })?;
            for i in 0_u64..20_000 {
                keyspace.insert(i.to_be_bytes(), value)?;
            }
        }
        assert!(db.write_buffer_size() > 16 * 1024 * 1024);
    }

    let db = Database::builder(folder.path())
        .worker_threads_unchecked(0)
        .open()?;
    assert!(
        db.write_buffer_size() <= 16 * 1024 * 1024,
        "recovery retained {} bytes across keyspaces",
        db.write_buffer_size()
    );
    for name in ["one", "two", "three", "four"] {
        let keyspace = db.keyspace(name, KeyspaceCreateOptions::default)?;
        assert_eq!(
            keyspace.get(19_999_u64.to_be_bytes())?.as_deref(),
            Some(value.as_slice())
        );
    }
    Ok(())
}

#[test]
fn spilled_batch_replays_later_duplicate_key_after_chunk_flush() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let value = [b'b'; 1024];
    {
        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;
        let keyspace = db.keyspace("spilled", || {
            KeyspaceCreateOptions::default().max_memtable_size(64 * 1024 * 1024)
        })?;
        let mut batch = db.batch();
        batch.insert(&keyspace, "duplicate", "old");
        for i in 0_u64..9_000 {
            batch.insert(&keyspace, i.to_be_bytes(), value);
        }
        batch.insert(&keyspace, "duplicate", "new");
        batch.commit()?;
    }

    for _ in 0..2 {
        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;
        let keyspace = db.keyspace("spilled", KeyspaceCreateOptions::default)?;
        assert_eq!(
            keyspace.get("duplicate")?.as_deref(),
            Some(b"new".as_slice())
        );
        for i in [0_u64, 4_500, 8_999] {
            assert_eq!(
                keyspace.get(i.to_be_bytes())?.as_deref(),
                Some(value.as_slice())
            );
        }
        assert!(db.write_buffer_size() <= 8 * 1024 * 1024);
    }
    Ok(())
}

#[test]
#[ignore = "manual crash-recovery stress test"]
fn interrupted_replay_keeps_the_original_journal() -> fjall::Result<()> {
    if let Some(path) = std::env::var_os("FJALL_RECOVERY_INTERRUPT_CHILD") {
        let _db = Database::builder(path).worker_threads_unchecked(0).open()?;
        return Ok(());
    }

    let folder = tempfile::tempdir()?;
    let value = [b'i'; 256];
    const ROWS: u64 = 300_000;
    {
        let db = Database::builder(folder.path())
            .worker_threads_unchecked(0)
            .open()?;
        let keyspace = db.keyspace("interrupted", || {
            KeyspaceCreateOptions::default().max_memtable_size(8 * 1024 * 1024)
        })?;
        for i in 0..ROWS {
            keyspace.insert(i.to_be_bytes(), value)?;
        }
    }

    let mut child = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "interrupted_replay_keeps_the_original_journal",
            "--ignored",
        ])
        .env("FJALL_RECOVERY_INTERRUPT_CHILD", folder.path())
        .spawn()?;
    let tables = folder.path().join("keyspaces/1/tables");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if std::fs::read_dir(&tables)?.count() >= 2 {
            child.kill()?;
            child.wait()?;
            break;
        }
        if let Some(status) = child.try_wait()? {
            panic!("recovery child exited before interruption: {status}");
        }
        if std::time::Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            panic!("recovery did not flush two chunks before deadline");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    assert!(folder.path().join("0.jnl").exists());
    let db = Database::builder(folder.path())
        .worker_threads_unchecked(0)
        .open()?;
    let keyspace = db.keyspace("interrupted", KeyspaceCreateOptions::default)?;
    assert_eq!(
        keyspace.get(0_u64.to_be_bytes())?.as_deref(),
        Some(value.as_slice())
    );
    assert_eq!(
        keyspace.get((ROWS - 1).to_be_bytes())?.as_deref(),
        Some(value.as_slice())
    );
    assert!(db.write_buffer_size() <= 8 * 1024 * 1024);
    Ok(())
}
