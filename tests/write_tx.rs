use fjall::{KeyspaceCreateOptions, Readable, SingleWriterTxDatabase};
use test_log::test;

#[test]
fn write_tx_multi_keys() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = SingleWriterTxDatabase::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    let mut wtx = db.write_tx();
    wtx.insert(&tree, "1", "abc");
    wtx.insert(&tree, "1", "def");
    wtx.insert(&tree, "1", "ghi");
    assert_eq!(&*wtx.get(&tree, "1")?.unwrap(), b"ghi");
    wtx.commit()?;
    assert_eq!(&*tree.get("1")?.unwrap(), b"ghi");

    Ok(())
}
