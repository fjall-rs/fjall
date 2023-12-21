use fjall::Config;

pub fn main() -> fjall::Result<()> {
    env_logger::init();

    eprintln!("hello");

    let keyspace = Config::new(".data").open()?;

    let items = keyspace.open_partition("default" /* PartitionConfig {} */)?;

    items.insert("hello world-0", "this is fjall")?;
    items.insert("hello world-1", "this is fjall")?;
    items.insert("hello world-2", "this is fjall")?;
    items.insert("hello world-3", "this is fjall")?;
    items.insert("hello world-4", "this is fjall")?;
    items.insert("hello world-5", "this is fjall")?;
    items.insert("hello world-6", "this is fjall")?;
    items.insert("hello world-7", "this is fjall")?;
    items.insert("hello world-8", "this is fjall")?;
    items.insert("hello world-9", "this is fjall")?;

    assert!(items.get("hello world-0")?.is_some());
    assert!(items.get("hello world-1")?.is_some());
    assert!(items.get("hello world-2")?.is_some());
    assert!(items.get("hello world-3")?.is_some());
    assert!(items.get("hello world-4")?.is_some());
    assert!(items.get("hello world-5")?.is_some());
    assert!(items.get("hello world-6")?.is_some());
    assert!(items.get("hello world-7")?.is_some());
    assert!(items.get("hello world-8")?.is_some());
    assert!(items.get("hello world-9")?.is_some());

    items
        .tree
        .flush_active_memtable()
        .unwrap()
        .join()
        .unwrap()?;

    assert!(items.get("hello world-0")?.is_some());
    assert!(items.get("hello world-1")?.is_some());
    assert!(items.get("hello world-2")?.is_some());
    assert!(items.get("hello world-3")?.is_some());
    assert!(items.get("hello world-4")?.is_some());
    assert!(items.get("hello world-5")?.is_some());
    assert!(items.get("hello world-6")?.is_some());
    assert!(items.get("hello world-7")?.is_some());
    assert!(items.get("hello world-8")?.is_some());
    assert!(items.get("hello world-9")?.is_some());

    items.remove("hello world-0")?;
    items.remove("hello world-1")?;
    items.remove("hello world-2")?;
    items.remove("hello world-3")?;
    items.remove("hello world-4")?;
    items.remove("hello world-5")?;
    items.remove("hello world-6")?;
    items.remove("hello world-7")?;
    items.remove("hello world-8")?;
    items.remove("hello world-9")?;

    assert!(items.get("hello world-0")?.is_none());
    assert!(items.get("hello world-1")?.is_none());
    assert!(items.get("hello world-2")?.is_none());
    assert!(items.get("hello world-3")?.is_none());
    assert!(items.get("hello world-4")?.is_none());
    assert!(items.get("hello world-5")?.is_none());
    assert!(items.get("hello world-6")?.is_none());
    assert!(items.get("hello world-7")?.is_none());
    assert!(items.get("hello world-8")?.is_none());
    assert!(items.get("hello world-9")?.is_none());

    items
        .tree
        .flush_active_memtable()
        .unwrap()
        .join()
        .unwrap()?;

    assert!(items.get("hello world-0")?.is_none());
    assert!(items.get("hello world-1")?.is_none());
    assert!(items.get("hello world-2")?.is_none());
    assert!(items.get("hello world-3")?.is_none());
    assert!(items.get("hello world-4")?.is_none());
    assert!(items.get("hello world-5")?.is_none());
    assert!(items.get("hello world-6")?.is_none());
    assert!(items.get("hello world-7")?.is_none());
    assert!(items.get("hello world-8")?.is_none());
    assert!(items.get("hello world-9")?.is_none());

    eprintln!("hello partition");

    Ok(())
}
