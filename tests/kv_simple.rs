use fjall::{Config, PartitionCreateOptions};
use lsm_tree::AbstractTree;
use test_log::test;

#[test]
fn blob_kv_simple() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = Config::new(folder).open()?;
    let partition = keyspace.open_partition(
        "default",
        PartitionCreateOptions::default().use_kv_separation(),
    )?;

    assert_eq!(partition.len()?, 0);
    partition.insert("1", "oxygen".repeat(128_000))?;
    partition.insert("3", "abc")?;
    partition.insert("5", "abc")?;
    assert_eq!(partition.len()?, 3);

    partition.rotate_memtable()?;
    std::thread::sleep(std::time::Duration::from_millis(100));

    if let fjall::AnyTree::Blob(tree) = &partition.tree {
        assert!(tree.index.disk_space() < 200);

        // NOTE: The data is compressed quite well, so it's way less than 128k
        assert!(tree.disk_space() > 1_000);
    } else {
        panic!("nope");
    }

    Ok(())
}
