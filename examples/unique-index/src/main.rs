use fjall::{Guard, TxDatabase, TxKeyspace};
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
    db: &TxDatabase,
    items: &TxKeyspace,
    uniq: &TxKeyspace,
    id: &str,
    name: &str,
) -> Result<(), Error> {
    let mut tx = db.write_tx();

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

    let db = TxDatabase::builder(path).temporary(true).open()?;

    let items = db.keyspace("items", Default::default())?;
    let uniq = db.keyspace("uniq_idx", Default::default())?;

    maybe_create_item(&db, &items, &uniq, "a", "Item A")?;
    maybe_create_item(&db, &items, &uniq, "b", "Item B")?;
    maybe_create_item(&db, &items, &uniq, "c", "Item C")?;

    assert!(matches!(
        maybe_create_item(&db, &items, &uniq, "d", "Item A"),
        Err(Error::UniqueConstraintFailed),
    ));

    println!("Listing all unique values and their owners");

    let mut found_count = 0;

    for kv in db.read_tx().iter(&uniq) {
        // TODO: 3.0.0: error needs to be fjall::Error, not lsm_tree
        let (k, v) = kv.into_inner().map_err(|e| fjall::Error::Storage(e))?;

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
