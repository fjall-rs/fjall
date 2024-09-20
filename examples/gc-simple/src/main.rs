use fjall::{Config, GarbageCollection, KvSeparationOptions, PartitionCreateOptions};
use std::time::Instant;

const BLOB_SIZE: usize = 10_000;

fn main() -> fjall::Result<()> {
    let keyspace = Config::default().temporary(true).open()?;
    let blobs = keyspace.open_partition(
        "blobs",
        PartitionCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
    )?;

    for _ in 0..10 {
        blobs.insert("a", "a".repeat(BLOB_SIZE))?;

        // NOTE: This is just used to force the data into the value log
        blobs.rotate_memtable_and_wait()?;
    }

    eprintln!("Running GC for partition {:?}", blobs.name);
    let start = Instant::now();

    let report = blobs.gc_scan()?;
    assert_eq!(10.0, report.space_amp());
    assert_eq!(0.9, report.stale_ratio());
    assert_eq!(9, report.stale_blobs);
    assert_eq!(9 * BLOB_SIZE as u64, report.stale_bytes);
    assert_eq!(9, report.stale_segment_count);

    let freed_bytes = blobs.gc_with_space_amp_target(1.0)?;

    eprintln!("GC done in {:?}, freed {freed_bytes}B", start.elapsed());
    assert!(
        // TODO: needs to be fixed in value-log
        /* NOTE: freed_bytes is the amount of bytes freed on disk, not uncompressed data */
        freed_bytes > 0
    );

    let report = blobs.gc_scan()?;
    assert_eq!(1.0, report.space_amp());
    assert_eq!(0.0, report.stale_ratio());
    assert_eq!(0, report.stale_blobs);
    assert_eq!(0, report.stale_bytes);
    assert_eq!(0, report.stale_segment_count);

    Ok(())
}
