fn main() -> fjall::Result<()> {
    let db = fjall::Database::builder(".fjall_data").open()?;
    let items = db.keyspace("items", Default::default())?;

    assert_eq!(0, items.len()?);

    println!("OK");

    Ok(())
}
