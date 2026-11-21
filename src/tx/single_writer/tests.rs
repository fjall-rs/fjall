use super::*;
use crate::Readable;
use test_log::test;

/// A write transaction can be moved to (and committed on) another thread,
/// while other write transactions wait for it
#[test]
fn write_tx_commit_on_other_thread() -> crate::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = SingleWriterTxDatabase::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    let mut first = db.write_tx();
    first.insert(&tree, "a", "first");

    let waiting = std::thread::spawn(move || -> crate::Result<_> {
        // NOTE: Blocks until the first tx is committed, so it sees its write
        let wtx = db.write_tx();
        wtx.get(&tree, "a")
    });

    std::thread::spawn(move || first.commit())
        .join()
        .expect("thread should not panic")?;

    let seen = waiting.join().expect("thread should not panic")?;
    assert_eq!(Some(b"first".as_slice()), seen.as_deref());

    Ok(())
}
