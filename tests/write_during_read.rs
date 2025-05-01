use fjall::{Config, PartitionCreateOptions};
use test_log::test;

#[test]
fn write_during_read() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = Config::new(&folder).open()?;

    let tree = keyspace.open_partition(
        "default",
        PartitionCreateOptions::default().max_memtable_size(128_000),
    )?;

    for x in 0u64..50_000 {
        tree.insert(x.to_be_bytes(), x.to_be_bytes())?;
    }
    tree.rotate_memtable_and_wait()?;

    for kv in tree.iter() {
        let (k, v) = kv?;
        tree.insert(k, v.repeat(4))?;
    }

    Ok(())
}
