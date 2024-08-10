use fjall::{UserKey, UserValue};
use std::path::Path;

fn iterrr(
    path: &Path,
) -> fjall::Result<impl DoubleEndedIterator<Item = fjall::Result<(UserKey, UserValue)>>> {
    let keyspace = fjall::Config::new(path).open()?;
    let tree = keyspace.open_partition("default", Default::default())?;

    for x in 0..100u32 {
        let x = x.to_be_bytes();
        tree.insert(x, x)?;
    }

    Ok(tree.iter())
}

#[test_log::test]
fn partition_iter_lifetime() -> fjall::Result<()> {
    let folder = tempfile::tempdir().unwrap();
    assert_eq!(100, iterrr(folder.path())?.count());
    Ok(())
}
