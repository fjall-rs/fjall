use fjall::{Database, KeyspaceCreateOptions};
use lsm_tree::Guard;
use test_log::test;

// TODO: currently guards return an lsm_tree error, not fjall...

struct Counter<'a, I: DoubleEndedIterator<Item = lsm_tree::Result<fjall::KvPair>>> {
    iter: &'a mut I,
}

impl<'a, I: DoubleEndedIterator<Item = lsm_tree::Result<fjall::KvPair>>> Counter<'a, I> {
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
        let mut iter = tree.iter().map(|guard| guard.into_inner());
        let counter = Counter { iter: &mut iter };
        assert_eq!(3, counter.execute());
    }

    Ok(())
}
