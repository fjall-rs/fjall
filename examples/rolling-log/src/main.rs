use fjall::{Database, KeyspaceCreateOptions};
use std::path::Path;

const LIMIT: u64 = 10_000_000;

fn main() -> fjall::Result<()> {
    let path = Path::new(".fjall_data");

    let db = Database::builder(path)
        .temporary(true)
        .max_write_buffer_size(4_000_000)
        .open()?;

    let log = db.keyspace(
        "log",
        KeyspaceCreateOptions::default()
            fjall::compaction::Fifo::new(LIMIT, None),
        )),
            .compaction_strategy(Arc::new(fjall::compaction::Fifo::new(LIMIT, None))),
    )?;

    for x in 0u64..2_500_000 {
        log.insert(x.to_be_bytes(), x.to_be_bytes())?;

        if x % 100_000 == 0 {
            let (min_key, _) = log.first_key_value()?.unwrap();
            let (max_key, _) = log.last_key_value()?.unwrap();

            let mut buf = [0; 8];
            buf.copy_from_slice(&min_key[0..8]);
            let min_key = u64::from_be_bytes(buf);

            buf.copy_from_slice(&max_key[0..8]);
            let max_key = u64::from_be_bytes(buf);

            let disk_space = log.disk_space();

            println!(
                "key range: [{min_key}, {max_key}] - disk space used: {} MiB - # tables: {}",
                disk_space / 1_024 / 1_024,
                log.table_count(),
            );

            assert!(disk_space < (LIMIT as f64 * 1.5) as u64);
        }
    }

    Ok(())
}
