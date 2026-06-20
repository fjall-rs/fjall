use fjall::{Database, KeyspaceCreateOptions, KvSeparationOptions};
use test_log::test;

#[test]
fn batch_simple() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
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
fn blob_batch_simple() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", || {
        KeyspaceCreateOptions::default().with_kv_separation(Some(KvSeparationOptions::default()))
    })?;

    let blob = "oxygen".repeat(128_000);

    let mut batch = db.batch();

    assert_eq!(tree.len()?, 0);
    batch.insert(&tree, "1", &blob);
    batch.insert(&tree, "3", "abc");
    batch.insert(&tree, "5", "abc");
    assert_eq!(tree.len()?, 0);

    batch.commit()?;
    assert_eq!(tree.len()?, 3);

    assert_eq!(&*tree.get("1")?.unwrap(), blob.as_bytes());

    Ok(())
}

#[test]
fn batch_multi_keys() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
    let mut batch = db.batch();

    assert_eq!(tree.len()?, 0);
    batch.insert(&tree, "1", "abc");
    batch.insert(&tree, "1", "def");
    batch.insert(&tree, "1", "ghi");
    assert_eq!(tree.len()?, 0);

    batch.commit()?;
    assert_eq!(tree.len()?, 1);
    assert_eq!(&*tree.get("1")?.unwrap(), b"ghi");

    Ok(())
}

#[test]
fn batch_multi_keyspace_commits_atomically() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;
    let ks_a = db.keyspace("a", KeyspaceCreateOptions::default)?;
    let ks_b = db.keyspace("b", KeyspaceCreateOptions::default)?;
    let ks_c = db.keyspace("c", KeyspaceCreateOptions::default)?;

    let mut batch = db.batch();

    // Interleave keyspaces and repeatedly touch the same keyspace within the
    // batch: exercises the linear-dedup stall set (one keyspace appears many
    // times) and the apply loop running without the keyspaces read-lock.
    batch.insert(&ks_a, "1", "a1");
    batch.insert(&ks_b, "1", "b1");
    batch.insert(&ks_a, "2", "a2");
    batch.insert(&ks_c, "1", "c1");
    batch.insert(&ks_a, "3", "a3");
    batch.insert(&ks_b, "2", "b2");

    // Nothing is visible until commit.
    assert_eq!(ks_a.len()?, 0);
    assert_eq!(ks_b.len()?, 0);
    assert_eq!(ks_c.len()?, 0);

    batch.commit()?;

    assert_eq!(ks_a.len()?, 3);
    assert_eq!(ks_b.len()?, 2);
    assert_eq!(ks_c.len()?, 1);

    assert_eq!(&*ks_a.get("1")?.unwrap(), b"a1");
    assert_eq!(&*ks_a.get("2")?.unwrap(), b"a2");
    assert_eq!(&*ks_a.get("3")?.unwrap(), b"a3");
    assert_eq!(&*ks_b.get("1")?.unwrap(), b"b1");
    assert_eq!(&*ks_b.get("2")?.unwrap(), b"b2");
    assert_eq!(&*ks_c.get("1")?.unwrap(), b"c1");

    Ok(())
}

#[test]
fn batch_commit_concurrent_with_keyspace_creation() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    // A second thread creates keyspaces (which takes keyspaces.write()) while
    // the main thread commits batches (which used to hold keyspaces.read()
    // across its apply loop). This guards against a deadlock or lost writes
    // after dropping that read-lock.
    //
    // A barrier releases both threads together so the keyspace creation
    // deterministically overlaps the commit loop, actually exercising the
    // contention path rather than relying on scheduling luck.
    let start = std::sync::Arc::new(std::sync::Barrier::new(2));

    let creator = std::thread::spawn({
        let db = db.clone();
        let start = std::sync::Arc::clone(&start);
        move || -> fjall::Result<()> {
            start.wait();
            for i in 0..50 {
                let ks = db.keyspace(&format!("ks_{i}"), KeyspaceCreateOptions::default)?;
                ks.insert("k", "v")?;
            }
            Ok(())
        }
    });

    start.wait();
    for i in 0..200 {
        let mut batch = db.batch();
        batch.insert(&tree, format!("key-{i}"), format!("val-{i}"));
        batch.commit()?;
    }

    creator.join().expect("creator thread should not panic")?;

    // Every batched write landed.
    assert_eq!(tree.len()?, 200);
    for i in 0..200 {
        assert_eq!(
            &*tree.get(format!("key-{i}"))?.unwrap(),
            format!("val-{i}").as_bytes(),
        );
    }

    // Every concurrently-created keyspace is intact.
    for i in 0..50 {
        let ks = db.keyspace(&format!("ks_{i}"), KeyspaceCreateOptions::default)?;
        assert_eq!(&*ks.get("k")?.unwrap(), b"v");
    }

    Ok(())
}
