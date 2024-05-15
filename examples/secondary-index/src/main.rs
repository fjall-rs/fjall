use fjall::{BlockCache, Config, Keyspace, PartitionHandle};
use format_bytes::format_bytes;
use nanoid::nanoid;
use std::path::Path;

fn create_item(
    ks: &Keyspace,
    table: &PartitionHandle,
    index: &PartitionHandle,
    name: &str,
    year: u64,
) -> fjall::Result<()> {
    let mut batch = ks.batch();

    let id = nanoid!();
    batch.insert(table, &id, format!("{name} [{year}]"));

    let ts_bytes = year.to_be_bytes();
    let key = format_bytes!(b"{}#{}", ts_bytes, id.as_bytes());

    batch.insert(index, &key, "");

    batch.commit()?;

    Ok(())
}

fn main() -> fjall::Result<()> {
    let path = Path::new(".fjall_data");

    if path.try_exists()? {
        std::fs::remove_dir_all(path)?;
    }

    let keyspace = Config::new(path)
        .block_cache(BlockCache::with_capacity_bytes(10 * 1_024).into())
        .open()?;
    let items = keyspace.open_partition("items", Default::default())?;
    let sec = keyspace.open_partition("sec_idx", Default::default())?;

    create_item(&keyspace, &items, &sec, "Remain in Light", 1_980)?;
    create_item(&keyspace, &items, &sec, "Power, Corruption & Lies", 1_983)?;
    create_item(&keyspace, &items, &sec, "Hounds of Love", 1_985)?;
    create_item(&keyspace, &items, &sec, "Black Celebration", 1_986)?;
    create_item(&keyspace, &items, &sec, "Disintegration", 1_989)?;
    create_item(&keyspace, &items, &sec, "Violator", 1_990)?;
    create_item(&keyspace, &items, &sec, "Wish", 1_991)?;
    create_item(&keyspace, &items, &sec, "Loveless", 1_991)?;
    create_item(&keyspace, &items, &sec, "Dummy", 1_994)?;
    create_item(&keyspace, &items, &sec, "When The Pawn...", 1_999)?;
    create_item(&keyspace, &items, &sec, "Kid A", 2_000)?;
    create_item(&keyspace, &items, &sec, "Have You In My Wilderness", 2_015)?;

    // Get items from 1990 to 2000 (exclusive)
    let lo = 1_990_u64;
    let hi = 1_999_u64;

    eprintln!("Searching for [{lo} - {hi}]");

    for item in &sec.range(lo.to_be_bytes()..(hi + 1).to_be_bytes()) {
        let (k, _) = item?;

        let primary_key = k.split(|&c| c == b'#').nth(1).unwrap();

        let item = items.get(primary_key)?.unwrap();

        eprintln!("found: {}", std::str::from_utf8(&item).unwrap());
    }

    keyspace.persist(fjall::FlushMode::SyncAll)?;

    Ok(())
}
