use fjall::{BlockCache, Config, PersistMode};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
    time::Instant,
};

fn main() -> fjall::Result<()> {
    let path = Path::new(".fjall_data");

    let keyspace = Config::new(path).temporary(true).open()?;
    let items = keyspace.open_partition("items", Default::default())?;

    // To search suffixes of keys, we store a secondary index that stores the reversed key
    // which will allow a .prefix() search over that key, resulting in a suffix search.
    let items_rev = keyspace.open_partition("items_rev", Default::default())?;

    if items.is_empty()? {
        println!("Ingesting test data");

        let line_reader = BufReader::new(File::open("english_words.txt")?);

        for (idx, line) in line_reader.lines().enumerate() {
            let line = line?;

            // We use a write batch to keep both partitions synchronized
            let mut batch = keyspace.batch();
            batch.insert(&items, &line, &line);
            batch.insert(&items_rev, line.chars().rev().collect::<String>(), line);
            batch.commit()?;

            if idx % 50_000 == 0 {
                println!("Loaded {idx} words");
            }
        }
    }

    keyspace.persist(PersistMode::SyncAll)?;

    let suffix = "west";
    let test_runs = 5;

    let count = items.len()?;

    for i in 0..test_runs {
        let before = Instant::now();
        let mut found_count = 0;

        if i == 0 {
            println!("\n[SLOW] Scanning all items for suffix {suffix:?}:");
        }

        for kv in items_rev.iter() {
            let (_, value) = kv?;

            if value.ends_with(suffix.as_bytes()) {
                if i == 0 {
                    println!("  -> {}", std::str::from_utf8(&value).unwrap());
                }

                found_count += 1;
            }
        }

        println!(
            "Found {found_count:?} in {count:?} words in {:?}",
            before.elapsed(),
        );

        assert_eq!(40, found_count);
    }
    println!("===============================================");

    for i in 0..test_runs {
        let before = Instant::now();
        let mut found_count = 0;

        if i == 0 {
            println!("\n[FAST] Finding all items by suffix {suffix:?}:");
        }

        // Uses prefix, so generally faster than table scan
        // `------------------v
        for kv in items_rev.prefix(suffix.chars().rev().collect::<String>()) {
            let (_, value) = kv?;

            if i == 0 {
                println!("  -> {}", std::str::from_utf8(&value).unwrap());
            }

            found_count += 1;
        }

        println!(
            "Found {found_count:?} in {count:?} words in {:?}",
            before.elapsed(),
        );

        assert_eq!(40, found_count);
    }
    println!("===============================================");

    Ok(())
}
