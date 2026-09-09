use fjall::{Database, KeyspaceCreateOptions};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;
use test_log::test;

const MAX_MEMTABLE_SIZE: u64 = 128 * 1_024;
const INSERTS: u64 = 100_000;
const VALUE_LEN: usize = 64;
const MIN_FLUSHED_PCT: f64 = 50.0;
const ROTATE_BUDGET: Duration = Duration::from_secs(5);

fn flush_under_load(worker_threads: usize) -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder)
        .worker_threads(worker_threads)
        .open()?;

    let keyspace = db.keyspace("default", || {
        KeyspaceCreateOptions::default().max_memtable_size(MAX_MEMTABLE_SIZE)
    })?;

    let value = vec![b'v'; VALUE_LEN];
    for i in 0..INSERTS {
        keyspace.insert(i.to_be_bytes(), &value)?;
    }

    let (tx, rx) = mpsc::channel();
    std::thread::spawn({
        let keyspace = keyspace.clone();
        move || {
            let _ = tx.send(keyspace.rotate_memtable_and_wait());
        }
    });

    let rotate = match rx.recv_timeout(ROTATE_BUDGET) {
        Ok(result) => result,
        Err(RecvTimeoutError::Timeout) => {
            // `DatabaseInner::drop` waits for the worker counter to hit zero,
            // so dropping a stuck pool hangs the test.
            std::mem::forget(keyspace);
            std::mem::forget(db);
            std::mem::forget(folder);
            panic!("rotate_memtable_and_wait blocked for {ROTATE_BUDGET:?}");
        }
        Err(RecvTimeoutError::Disconnected) => {
            panic!("rotate_memtable_and_wait thread panicked")
        }
    };
    rotate?;

    let bytes_written = INSERTS * (8 + VALUE_LEN as u64);
    let segment_bytes = keyspace.disk_space();
    let flushed_pct = segment_bytes as f64 / bytes_written as f64 * 100.0;
    assert!(
        flushed_pct >= MIN_FLUSHED_PCT,
        "only {flushed_pct:.1}% of written bytes reached segments \
         ({segment_bytes} of {bytes_written})",
    );

    Ok(())
}

#[test]
fn flush_with_one_worker() -> fjall::Result<()> {
    flush_under_load(1)
}

#[test]
fn flush_with_two_workers() -> fjall::Result<()> {
    flush_under_load(2)
}
