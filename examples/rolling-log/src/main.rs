use fjall::{Config, PartitionCreateOptions};
use std::{path::Path, sync::Arc};

const LIMIT: u64 = 16_000_000;

fn main() -> fjall::Result<()> {
    let path = Path::new(".fjall_data");

    if path.try_exists()? {
        std::fs::remove_dir_all(path)?;
    }

    let keyspace = Config::new(path).max_write_buffer_size(4_000_000).open()?;
    let log = keyspace.open_partition("log", PartitionCreateOptions::default())?;
    log.set_compaction_strategy(Arc::new(fjall::compaction::Fifo::new(LIMIT, None)));

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
                "key range: [{min_key}, {max_key}] - disk space used: {} MiB - # segments: {}",
                disk_space / 1_024 / 1_024,
                log.segment_count()
            );

            assert!(disk_space < LIMIT);
        }
    }

    Ok(())
}
