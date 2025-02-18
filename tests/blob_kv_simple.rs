use fjall::{Config, KvSeparationOptions, PartitionCreateOptions};
use lsm_tree::AbstractTree;
use test_log::test;

#[test]
fn blob_kv_simple() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = Config::new(folder).open()?;
    let partition = keyspace.open_partition(
        "default",
        PartitionCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
    )?;

    assert_eq!(partition.len()?, 0);
    partition.insert("1", "oxygen".repeat(1_000_000))?;
    partition.insert("3", "abc")?;
    partition.insert("5", "abc")?;
    assert_eq!(partition.len()?, 3);

    partition.rotate_memtable_and_wait()?;

    if let fjall::AnyTree::Blob(tree) = &partition.tree {
        assert!(tree.index.disk_space() < 200);

        // NOTE: The data is compressed quite well, so it's way less than 1M
        assert!(tree.disk_space() > 5_000);

        assert!(tree.blobs.manifest.disk_space_used() > 5_000);
        assert_eq!(1, tree.blobs.segment_count());
    } else {
        panic!("nope");
    }

    Ok(())
}
