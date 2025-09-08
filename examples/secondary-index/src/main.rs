use fjall::{Batch, BlockCache, Database, Keyspace};
use format_bytes::format_bytes;
use nanoid::nanoid;
use std::path::Path;

fn create_item(
    batch: &mut Batch,
    table: &Keyspace,
    index: &Keyspace,
    name: &str,
    year: u64,
) -> fjall::Result<()> {
    let id = nanoid!();
    batch.insert(table, &id, format!("{name} [{year}]"));

    let ts_bytes = year.to_be_bytes();
    let key = format_bytes!(b"{}\0{}", ts_bytes, id.as_bytes());

    batch.insert(index, &key, "");

    Ok(())
}

fn main() -> fjall::Result<()> {
    let path = Path::new(".fjall_data");

    let db = Database::builder(path).temporary(true).open()?;
    let items = db.keyspace("items", Default::default())?;
    let sec = db.keyspace("sec_idx", Default::default())?;

    let mut batch = db.batch();
    create_item(&mut batch, &items, &sec, "Remain in Light", 1_980)?;
    create_item(&mut batch, &items, &sec, "Seventeen Seconds", 1_980)?;
    create_item(&mut batch, &items, &sec, "Power, Corruption & Lies", 1_983)?;
    create_item(&mut batch, &items, &sec, "Hounds of Love", 1_985)?;
    create_item(&mut batch, &items, &sec, "Black Celebration", 1_986)?;
    create_item(&mut batch, &items, &sec, "Disintegration", 1_989)?;
    create_item(&mut batch, &items, &sec, "Violator", 1_990)?;
    create_item(&mut batch, &items, &sec, "Wish", 1_991)?;
    create_item(&mut batch, &items, &sec, "Loveless", 1_991)?;
    create_item(&mut batch, &items, &sec, "Dummy", 1_994)?;
    create_item(&mut batch, &items, &sec, "When The Pawn...", 1_999)?;
    create_item(&mut batch, &items, &sec, "Kid A", 2_000)?;
    create_item(&mut batch, &items, &sec, "Have You In My Wilderness", 2_015)?;

    batch.commit()?;
    db.persist(fjall::PersistMode::SyncAll)?;

    // Get items from 1990 to 2000 (exclusive)
    let lo = 1_990_u64;
    let hi = 1_999_u64;

    println!("Searching for [{lo} - {hi}]");

    let mut found_count = 0;

    for kv in sec.range(lo.to_be_bytes()..(hi + 1).to_be_bytes()) {
        let (k, _) = kv?;

        // Get ID
        let primary_key = &k[std::mem::size_of::<u64>() + 1..];

        // Get from primary index
        let item = items.get(primary_key)?.unwrap();

        println!("found: {}", std::str::from_utf8(&item).unwrap());

        found_count += 1;
    }

    assert_eq!(5, found_count);

    Ok(())
}
