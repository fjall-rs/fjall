use fjall::{Config, Keyspace, PartitionCreateOptions};
use test_log::test;

// TODO: 3.0.0 remove in v3
#[test]
fn v2_awkward_sealed_journal_recovery() -> fjall::Result<()> {
    let config = Config::new("test_fixture/v2_sealed_journal_shenanigans")
        .flush_workers(0)
        .compaction_workers(0);

    let keyspace = Keyspace::create_or_recover(config)?;

    let tree = keyspace.open_partition(
        "default",
        PartitionCreateOptions::default().max_memtable_size(1_000),
    )?;

    /* tree.insert("a", "a")?;
    tree.rotate_memtable()?;

    tree.insert("b", "b")?;
    tree.rotate_memtable()?;

    tree.insert("c", "c")?;
    tree.rotate_memtable()?; */

    assert_eq!(4, keyspace.journal_count());
    assert_eq!(3, tree.len()?);

    Ok(())
}
