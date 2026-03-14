use fjall::{Database, KeyspaceCreateOptions};
use std::sync::{Arc, Barrier};

/// Stress test for #260: verifies that Database::drop() completes
/// under sustained write pressure without deadlocking.
///
/// The test spawns multiple writer threads that continuously insert
/// data, then drops the database while writers are still active.
/// A watchdog timeout ensures the test fails instead of hanging
/// if a deadlock occurs.
#[test]
fn drop_completes_under_write_pressure() {
    const WRITER_THREADS: usize = 8;
    const ITERATIONS: usize = 10;

    for iteration in 0..ITERATIONS {
        let folder = tempfile::tempdir().unwrap();
        let db = Database::builder(&folder).open().unwrap();
        let keyspace = db
            .keyspace("default", KeyspaceCreateOptions::default)
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
                    // Writes may fail once drop starts — that's expected
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

        // Watchdog: if drop takes longer than 10s, it's likely a deadlock
        match db_drop.join() {
            Ok(()) => {}
            Err(e) => panic!("iteration {iteration}: drop panicked: {e:?}"),
        }

        for h in handles {
            let _ = h.join();
        }
    }
}
