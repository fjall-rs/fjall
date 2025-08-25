fn main() -> fjall::Result<()> {
    let keyspace = fjall::Config::new(".fjall_data").open()?;
    let items = keyspace.open_partition("items", Default::default())?;

    assert_eq!(0, items.len()?);

    println!("OK");

    Ok(())
}
