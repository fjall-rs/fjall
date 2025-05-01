use fjall::{Config, TxKeyspace, TxPartition};
use std::path::Path;

#[derive(Debug)]
pub enum Error {
    Storage(fjall::Error),
    UniqueConstraintFailed,
}

impl From<fjall::Error> for Error {
    fn from(value: fjall::Error) -> Self {
        Self::Storage(value)
    }
}

fn maybe_create_item(
    keyspace: &TxKeyspace,
    items: &TxPartition,
    uniq: &TxPartition,
    id: &str,
    name: &str,
) -> Result<(), Error> {
    let mut tx = keyspace.write_tx();

    if uniq.contains_key(name)? {
        return Err(Error::UniqueConstraintFailed);
    }

    tx.insert(items, id, name);
    tx.insert(uniq, name, id);

    tx.commit()?;

    Ok(())
}

fn main() -> Result<(), Error> {
    let path = Path::new(".fjall_data");

    let keyspace = Config::new(path).temporary(true).open_transactional()?;
    let items = keyspace.open_partition("items", Default::default())?;
    let uniq = keyspace.open_partition("uniq_idx", Default::default())?;

    maybe_create_item(&keyspace, &items, &uniq, "a", "Item A")?;
    maybe_create_item(&keyspace, &items, &uniq, "b", "Item B")?;
    maybe_create_item(&keyspace, &items, &uniq, "c", "Item C")?;

    assert!(matches!(
        maybe_create_item(&keyspace, &items, &uniq, "d", "Item A"),
        Err(Error::UniqueConstraintFailed),
    ));

    println!("Listing all unique values and their owners");

    let mut found_count = 0;

    for kv in keyspace.read_tx().iter(&uniq) {
        let (k, v) = kv?;

        println!(
            "unique value: {:?} -> {:?}",
            std::str::from_utf8(&k).unwrap(),
            std::str::from_utf8(&v).unwrap(),
        );

        found_count += 1;
    }

    assert_eq!(3, found_count);

    Ok(())
}
