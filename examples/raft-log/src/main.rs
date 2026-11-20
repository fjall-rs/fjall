use fjall::{Database, DurabilityLevel, KeyspaceCreateOptions};
use std::path::Path;

const BATCH_SIZE: u64 = 100;

/// Entries older than this are covered by a (pretend) snapshot, so they can be removed
const KEEP_ENTRIES: u64 = 1_000_000;

fn main() -> fjall::Result<()> {
    let path = Path::new(".fjall_data");

    if path.try_exists()? {
        std::fs::remove_dir_all(&path)?;
    }

    let db = Database::builder(path).temporary(true).open()?;
    let log = db.keyspace("log", || {
        KeyspaceCreateOptions::default().max_memtable_size(8_000_000)
    })?;

    // NOTE: Big-endian log indexes keep the entries sorted by index
    let mut next_index = 0u64;

    // NOTE: Everything below this index is truncated (and may or may not be on disk anymore)
    let mut first_index = 0u64;

    loop {
        // Append entries
        for _ in 0..BATCH_SIZE {
            let entry = format!("entry#{next_index}");
            log.insert(next_index.to_be_bytes(), entry, DurabilityLevel::Visible)?;
            next_index += 1;
        }
        db.persist(DurabilityLevel::SyncAll)?;

        // Random access read
        let index = next_index - BATCH_SIZE / 2;
        let entry = log.get(index.to_be_bytes())?.expect("entry should exist");
        assert_eq!(format!("entry#{index}").as_bytes(), &*entry);

        // Garbage collect the tail of the log
        //
        // NOTE: Only tables that are fully contained in the range are dropped,
        // so some entries below `first_index` may still be readable, which is why
        // the log keeps track of `first_index` itself
        first_index = first_index.max(next_index.saturating_sub(KEEP_ENTRIES));
        log.drop_range(..first_index.to_be_bytes())?;

        let oldest = log
            .first_key_value()
            .expect("log should not be empty")
            .key()?;

        let oldest = u64::from_be_bytes(oldest[..8].try_into().expect("key should be a u64"));

        println!(
            "log: [{first_index}, {next_index}) - oldest entry on disk: {oldest} - # tables: {} - # wals: {}",
            log.table_count(),
            db.wal_count(),
        );
    }

    Ok(())
}
