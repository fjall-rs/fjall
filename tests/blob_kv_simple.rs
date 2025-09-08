use fjall::{Database, KeyspaceCreateOptions, KvSeparationOptions};
use lsm_tree::AbstractTree;
use test_log::test;

#[test]
fn blob_kv_simple() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace(
        "default",
        KeyspaceCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
    )?;

    assert_eq!(tree.len()?, 0);
    tree.insert("1", "oxygen".repeat(1_000_000))?;
    tree.insert("3", "abc")?;
    tree.insert("5", "abc")?;
    assert_eq!(tree.len()?, 3);

    tree.rotate_memtable_and_wait()?;

    if let fjall::AnyTree::Blob(tree) = &tree.tree {
        assert!(tree.index.disk_space() < 200);

        // NOTE: The data is compressed quite well, so it's way less than 1M
        assert!(tree.disk_space() > 5_000);

        assert!(tree.blobs.manifest.disk_space_used() > 5_000);
        assert_eq!(1, tree.blob_file_count());
    } else {
        panic!("nope");
    }

    Ok(())
}
