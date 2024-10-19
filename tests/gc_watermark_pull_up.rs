use std::time::Duration;

use fjall::Config;
use test_log::test;

#[test]
fn keyspace_recover_empty() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = Config::new(&folder).open()?;
    let partition = keyspace.open_partition("default", Default::default())?;

    for _ in 0..10_000 {
        partition.insert("a", "a")?;
    }

    // NOTE: Wait for monitor thread tick to kick in
    std::thread::sleep(Duration::from_secs(1));

    assert!(keyspace.snapshot_tracker.get_seqno_safe_to_gc() > 0);

    Ok(())
}
