use fjall::Config;
use std::time::Instant;

pub fn main() -> fjall::Result<()> {
    env_logger::init();

    eprintln!("hello");

    let keyspace = Config::new(".data")
        .max_journaling_size(24_000_000)
        .open()?;

    let items = keyspace.open_partition("default" /* PartitionConfig {} */)?;

    eprintln!("hello partition");

    let start = Instant::now();

    for x in 0.. {
        items.insert(format!("hello world-{x}"), "this is fjall")?;
        // keyspace.persist()?;
    }

    eprintln!("Took {}s", start.elapsed().as_secs_f32());

    assert_eq!(1_000, items.len()?);

    for x in 0..1_000 {
        assert!(items.get(format!("hello world-{x}"))?.is_some());
    }

    /*  assert!(items.get("hello world-0")?.is_some());
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
    assert!(items.get("hello world-9")?.is_none()); */

    Ok(())
}
