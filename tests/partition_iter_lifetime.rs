use fjall::{Config, PartitionCreateOptions};
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
fn partition_iter_lifetime() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let keyspace = Config::new(&folder).open()?;

    let tree = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    assert_eq!(0, keyspace.write_buffer_size());

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
