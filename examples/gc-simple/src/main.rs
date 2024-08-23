use fjall::{Config, Gc, PartitionCreateOptions};
use std::time::Instant;

const BLOB_SIZE: usize = 10_000;

fn main() -> fjall::Result<()> {
    let keyspace = Config::default().temporary(true).open()?;
    let blobs = keyspace.open_partition(
        "blobs",
        PartitionCreateOptions::default().use_kv_separation(true),
    )?;

    for _ in 0..10 {
        blobs.insert("a", "a".repeat(BLOB_SIZE))?;

        // NOTE: This is just used to force the data into the value log
        blobs.rotate_memtable_and_wait()?;
    }

    eprintln!("Running GC for partition {:?}", blobs.name);
    let start = Instant::now();

    let report = Gc::scan(&blobs)?;
    assert_eq!(10.0, report.space_amp());
    assert_eq!(0.9, report.stale_ratio());
    assert_eq!(9, report.stale_blobs);
    assert_eq!(9 * BLOB_SIZE as u64, report.stale_bytes);
    assert_eq!(9, report.stale_segment_count);

    let freed_bytes = Gc::with_space_amp_target(&blobs, 1.0)?;

    eprintln!("GC done in {:?}, freed {freed_bytes}B", start.elapsed());
    assert_eq!(
        freed_bytes,
        /* NOTE: freed_bytes is the amount of bytes freed on disk, not uncompressed data */
        9 * 55
    );

    let report = Gc::scan(&blobs)?;
    assert_eq!(1.0, report.space_amp());
    assert_eq!(0.0, report.stale_ratio());
    assert_eq!(0, report.stale_blobs);
    assert_eq!(0, report.stale_bytes);
    assert_eq!(0, report.stale_segment_count);

    Ok(())
}
