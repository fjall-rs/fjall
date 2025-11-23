fn main() -> fjall::Result<()> {
    let db = fjall::Database::builder(".fjall_data").open()?;
    let items = db.keyspace("items", fjall::KeyspaceCreateOptions::default)?;

    assert_eq!(0, items.len()?);

    println!("OK");

    Ok(())
}
