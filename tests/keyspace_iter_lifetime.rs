use fjall::{Database, KeyspaceCreateOptions};
use test_log::test;

struct Counter<I: DoubleEndedIterator<Item = fjall::Result<fjall::KvPair>>> {
    iter: I,
}

impl<I: DoubleEndedIterator<Item = fjall::Result<fjall::KvPair>>> Counter<I> {
    pub fn execute(self) -> usize {
        self.iter.count()
    }
}

#[test]
fn keyspace_iter_lifetime() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let db = Database::builder(&folder).open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    assert_eq!(0, db.write_buffer_size());

    tree.insert("asd", "def")?;
    tree.insert("efg", "hgf")?;
    tree.insert("hij", "wer")?;

    {
        let iter = tree.iter();
        let counter = Counter { iter };
        assert_eq!(3, counter.execute());
    }

    Ok(())
}
