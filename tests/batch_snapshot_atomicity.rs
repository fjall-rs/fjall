use fjall::{Database, KeyspaceCreateOptions, Readable};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};
use test_log::test;

const ITEMS_PER_BATCH: u64 = 1_000;
const TIME_LIMIT: Duration = Duration::from_secs(5);

/// Writers commit batches that set every key in two keyspaces to the batch's round
/// number while `background` keeps upgrading tree versions (flushes, compactions,
/// keyspace creation and deletion). A snapshot must see one round everywhere.
fn snapshots_see_whole_batches(
    background: impl Fn(&Database) -> fjall::Result<()> + Sync,
) -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;
    let db = Database::builder(&folder).worker_threads(2).open()?;
    let a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    let b = db.keyspace("b", KeyspaceCreateOptions::default)?;

    let stop = AtomicBool::new(false);
    let key = |i: u64| i.to_be_bytes();

    std::thread::scope(|scope| -> fjall::Result<()> {
        let (db, a, b, stop) = (&db, &a, &b, &stop);

        let writers = (0..2u64)
            .map(|writer| {
                scope.spawn(move || -> fjall::Result<()> {
                    let mut round = writer;
                    while !stop.load(Relaxed) {
                        round += 2;
                        let mut batch = db.batch();
                        for i in 0..ITEMS_PER_BATCH {
                            batch.insert(a, key(i), round.to_be_bytes());
                            batch.insert(b, key(i), round.to_be_bytes());
                        }
                        batch.commit()?;
                    }
                    Ok(())
                })
            })
            .collect::<Vec<_>>();

        let churn = scope.spawn(|| -> fjall::Result<()> {
            while !stop.load(Relaxed) {
                background(db)?;
            }
            Ok(())
        });

        let mut torn_snapshot = None;
        let result = (|| -> fjall::Result<()> {
            let start = Instant::now();
            let mut snapshots = 0;

            while start.elapsed() < TIME_LIMIT {
                let snapshot = db.snapshot();
                snapshots += 1;

                let mut rounds = std::collections::BTreeSet::new();
                for keyspace in [&a, &b] {
                    for kv in snapshot.iter(keyspace) {
                        rounds.insert(kv.value()?);
                    }
                }

                if rounds.len() > 1 {
                    torn_snapshot = Some(format!(
                        "snapshot #{snapshots} at seqno {} saw part of a batch: {} different rounds",
                        snapshot.seqno(),
                        rounds.len(),
                    ));
                    break;
                }
            }

            Ok(())
        })();

        stop.store(true, Relaxed);

        for writer in writers {
            writer.join().expect("writer panicked")?;
        }
        churn.join().expect("background thread panicked")?;
        result?;

        assert_eq!(None, torn_snapshot);

        Ok(())
    })
}

#[test]
fn batch_snapshot_atomicity_across_flushes() -> fjall::Result<()> {
    snapshots_see_whole_batches(|db| {
        let ks = db.keyspace("flushed", KeyspaceCreateOptions::default)?;
        ks.insert("a", "a")?;
        ks.rotate_memtable_and_wait()
    })
}

#[test]
fn batch_snapshot_atomicity_across_keyspace_churn() -> fjall::Result<()> {
    snapshots_see_whole_batches(|db| {
        let ks = db.keyspace("churned", KeyspaceCreateOptions::default)?;
        ks.insert("a", "a")?;
        db.delete_keyspace(ks)
    })
}
