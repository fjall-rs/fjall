//! Regression test for <https://github.com/fjall-rs/fjall/issues/287>.
//!
//! `Keyspace::clear` replaces the tree version while the compaction worker may already
//! have picked tables out of the previous one. Committing that stale choice panics the
//! worker inside `Version::with_moved` and poisons the compaction state mutex and the
//! version history lock, which takes the whole keyspace down with it.
//!
//! This is a race, so it is a stress test and not a deterministic one, and it is ignored
//! by default. Run it with:
//!
//! ```sh
//! cargo test --test clear_ingest_race -- --ignored --nocapture
//! ```
//!
//! The deterministic tests for the same defect live in lsm-tree, in
//! `src/compaction/worker.rs`.

use fjall::{Database, KeyspaceCreateOptions};
use std::time::{Duration, Instant};

const BUDGET: Duration = Duration::from_secs(30);

#[test]
#[ignore = "stress test, run manually with --ignored"]
fn clear_during_ingestion_does_not_kill_the_keyspace() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder)
        .max_journaling_size(64 * 1_024 * 1_024)
        .open()?;

    let deadline = Instant::now() + BUDGET;

    std::thread::scope(|spawner| {
        // NOTE: manual journal persist makes the race land much faster, but it is not
        // required for it, see the issue.
        let keyspace = db
            .keyspace("race", || {
                KeyspaceCreateOptions::default().manual_journal_persist(true)
            })
            .expect("open keyspace");

        let clearer = spawner.spawn({
            let keyspace = keyspace.clone();
            move || -> fjall::Result<u64> {
                let mut n = 0;
                while Instant::now() < deadline {
                    keyspace.clear()?;
                    n += 1;
                    std::thread::sleep(Duration::from_millis(10));
                }
                Ok(n)
            }
        });

        let ingester = spawner.spawn({
            let keyspace = keyspace.clone();
            move || -> fjall::Result<u64> {
                let mut n = 0;
                while Instant::now() < deadline {
                    let mut ingestion = keyspace.start_ingestion()?;
                    for i in 0..=10_240u32 {
                        ingestion.write(format!("key{i:09}"), [(i % 256) as u8; 64])?;
                    }
                    ingestion.finish()?;
                    n += 1;
                }
                Ok(n)
            }
        });

        // NOTE: A panicking worker shows up here as a poisoned lock on one of these two
        // threads, so joining both is what actually asserts the fix.
        let clears = clearer
            .join()
            .expect("clear thread panicked")
            .expect("clear failed");
        let ingests = ingester
            .join()
            .expect("ingest thread panicked")
            .expect("ingest failed");

        eprintln!("clears={clears} ingests={ingests}");
        assert!(clears > 0, "clear thread made no progress");
        assert!(ingests > 0, "ingest thread made no progress");
    });

    Ok(())
}
