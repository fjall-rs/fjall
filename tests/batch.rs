use fjall::{Database, KeyspaceCreateOptions};
use test_log::test;

#[test]
fn batch_simple() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    let mut batch = db.batch();

    assert_eq!(tree.len()?, 0);
    batch.insert(&tree, "1", "abc");
    batch.insert(&tree, "3", "abc");
    batch.insert(&tree, "5", "abc");
    assert_eq!(tree.len()?, 0);

    batch.commit()?;
    assert_eq!(tree.len()?, 3);

    Ok(())
}

#[test]
#[ignore = "restore 3.0.0"]
fn blob_batch_simple() -> fjall::Result<()> {
    todo!()

    // let folder = tempfile::tempdir()?;

    // let db = Database::builder(&folder).open()?;
    // let tree = db.keyspace(
    //     "default",
    //     KeyspaceCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
    // )?;

    // let blob = "oxygen".repeat(128_000);

    // let mut batch = db.batch();

    // assert_eq!(tree.len()?, 0);
    // batch.insert(&tree, "1", &blob);
    // batch.insert(&tree, "3", "abc");
    // batch.insert(&tree, "5", "abc");
    // assert_eq!(tree.len()?, 0);

    // batch.commit()?;
    // assert_eq!(tree.len()?, 3);

    // assert_eq!(&*tree.get("1")?.unwrap(), blob.as_bytes());

    // Ok(())
}
