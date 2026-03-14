use fjall::{Database, KeyspaceCreateOptions};
use std::sync::{Arc, Barrier};

/// Stress test for #260: verifies that Database::drop() completes
/// under sustained write pressure without deadlocking.
///
/// Uses a tiny memtable (1 KiB) to force frequent RotateMemtable/Flush/Compact
/// messages that saturate the bounded worker channel. Spawns multiple writer
/// threads, then drops the database while writers are active. A watchdog
/// timeout ensures the test fails fast instead of hanging on deadlock.
#[test]
fn drop_completes_under_write_pressure() {
    const WRITER_THREADS: usize = 8;
    const ITERATIONS: usize = 10;
    const WATCHDOG_SECS: u64 = 30;

    for iteration in 0..ITERATIONS {
        let folder = tempfile::tempdir().unwrap();
        // Explicitly use 4 worker threads so pool_size > 1 worker #0 behavior
        // is exercised even on single-core CI environments
        let db = Database::builder(&folder).worker_threads(4).open().unwrap();
        let keyspace = db
            .keyspace("default", || {
                KeyspaceCreateOptions::default().max_memtable_size(1_024)
            })
            .unwrap();

        let barrier = Arc::new(Barrier::new(WRITER_THREADS + 1));
        let mut handles = Vec::new();

        for t in 0..WRITER_THREADS {
            let ks = keyspace.clone();
            let b = barrier.clone();
            handles.push(std::thread::spawn(move || {
                b.wait();
                for i in 0..10_000 {
                    let key = format!("t{t}-k{i}");
                    // Errors (poisoned DB, closed channel) are expected once drop starts.
                    // We intentionally ignore them — this test validates that drop()
                    // completes without deadlock, not write correctness.
                    let _ = ks.insert(&key, b"value");
                }
            }));
        }

        // Let all writers start simultaneously
        barrier.wait();

        // Give writers a head start to fill the channel
        std::thread::sleep(std::time::Duration::from_millis(5));

        // Drop the database while writers are active — this must not deadlock
        let db_drop = std::thread::spawn(move || {
            drop(keyspace);
            drop(db);
        });

        // Watchdog: fail fast if drop doesn't complete within deadline
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(WATCHDOG_SECS);
        loop {
            if db_drop.is_finished() {
                break;
            }
            if std::time::Instant::now() > deadline {
                // Don't join (would hang). Abort the process so a deadlocked drop
                // thread cannot keep the test binary alive indefinitely.
                eprintln!(
                    "iteration {iteration}: drop did not complete within {WATCHDOG_SECS}s — likely deadlock; aborting process"
                );
                std::process::abort();
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        db_drop.join().expect("drop thread panicked");

        for (i, h) in handles.into_iter().enumerate() {
            if let Err(e) = h.join() {
                panic!("iteration {iteration}: writer thread {i} panicked: {e:?}");
            }
        }
    }
}
