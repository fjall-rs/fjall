use lsm_tree::Config;
use test_log::test;

const ITEM_COUNT: usize = 1_000_000;

#[test]
fn tree_timing() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let db = Config::new(folder).open()?;
    let start = std::time::Instant::now();

    for key in (0u64..ITEM_COUNT as u64).map(u64::to_be_bytes) {
        let value = nanoid::nanoid!();
        db.insert(key, value)?;
    }

    let total_micros = start.elapsed().as_micros();
    let avg = (total_micros as f64) / (ITEM_COUNT as f64);
    dbg!("Insert rate in micros", avg);
    assert!(avg < 2.5, "Insert rate too low");

    assert_eq!(db.len()?, ITEM_COUNT);

    Ok(())
}
