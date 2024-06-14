fn get_split_name(no: usize) -> String {
    format!("split_{no}")
}

const SPLITS: usize = 1_000;
const ITEMS_PER_SPLIT: usize = 1_000;

fn main() -> fjall::Result<()> {
    let path = std::path::Path::new(".fjall_data");

    if path.try_exists()? {
        std::fs::remove_dir_all(path)?;
    }

    let keyspace = fjall::Config::new(path).open()?;

    let start = std::time::Instant::now();

    for no in 0..SPLITS {
        let split_name = get_split_name(no);
        eprintln!("writing into {split_name:?}");
        let split = keyspace.open_partition(&split_name, Default::default())?;

        let before = std::time::Instant::now();
        for _ in 0..ITEMS_PER_SPLIT {
            split.insert(
                scru128::new_string(),
                random_string::generate(50, random_string::charsets::ALPHANUMERIC),
            )?;
        }

        keyspace.persist(fjall::PersistMode::SyncData)?;

        // NOTE: Flush memtable because partition becomes immutable
        // This simplifies things for the journal GC, making everything a bit faster
        split.rotate_memtable()?;

        eprintln!(
            "writing {ITEMS_PER_SPLIT} took {}ms, journal size: {} MiB - KS.memory_usage={} MiB",
            before.elapsed().as_millis(),
            keyspace.journal_disk_space() / 1_024 / 1_024,
            keyspace.memory_usage() / 1_024 / 1_024,
        );
    }

    let elapsed = start.elapsed();

    eprintln!(
        "written {} ({} MiB) in {}s, {}µs per item",
        SPLITS * ITEMS_PER_SPLIT,
        keyspace.disk_space() / 1_024 / 1_024,
        elapsed.as_secs_f32(),
        elapsed.as_micros() / (SPLITS as u128 * ITEMS_PER_SPLIT as u128)
    );

    assert_eq!(SPLITS, keyspace.partition_count());

    Ok(())
}
