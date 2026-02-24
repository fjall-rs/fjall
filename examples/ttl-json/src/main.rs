use fjall::compaction::filter::*;
use serde::{Deserialize, Serialize};
use std::sync::{atomic::AtomicU64, Arc};

#[derive(Debug, Deserialize, Serialize)]
struct Record {
    id: u64,
    ttl: u64,
}

struct MyFilter(u64);

impl CompactionFilter for MyFilter {
    fn filter_item(&mut self, item: ItemAccessor<'_>, _ctx: &Context) -> CompactionFilterResult {
        let i = item.value()?;

        let Ok(item) = serde_json::from_slice::<Record>(&i) else {
            // Compaction filters should not panic, so if we cannot deserialize, just return Keep
            return Ok(Verdict::Keep);
        };

        if self.0 >= item.ttl {
            eprintln!(
                "Drop stale value: {}",
                match std::str::from_utf8(&i) {
                    Ok(s) => s,
                    Err(_) => "<invalid json>", // should not be possible
                },
            );
            Ok(Verdict::Remove)
        } else {
            Ok(Verdict::Keep)
        }
    }
}

struct MyFactory(Arc<AtomicU64>);

impl Factory for MyFactory {
    fn make_filter(&self, _ctx: &Context) -> Box<dyn CompactionFilter> {
        Box::new(MyFilter(self.0.load(std::sync::atomic::Ordering::Relaxed)))
    }

    fn name(&self) -> &str {
        "ttl"
    }
}

fn main() -> fjall::Result<()> {
    let ttl_watermark = Arc::new(AtomicU64::default());

    let db = fjall::Database::builder(".fjall_data")
        .with_compaction_filter_factories({
            let ttl_watermark = ttl_watermark.clone();

            Arc::new(move |keyspace| match keyspace {
                "items" => Some(Arc::new(MyFactory(ttl_watermark.clone()))),
                _ => None,
            })
        })
        .temporary(true)
        .open()?;

    let items = db.keyspace("items", fjall::KeyspaceCreateOptions::default)?;

    items.insert(
        "a",
        serde_json::to_string(&Record { id: 1, ttl: 1 }).unwrap(),
    )?;
    items.insert(
        "b",
        serde_json::to_string(&Record { id: 2, ttl: 10 }).unwrap(),
    )?;
    items.insert(
        "c",
        serde_json::to_string(&Record { id: 3, ttl: 15 }).unwrap(),
    )?;
    items.insert("d", "dsadasdasd")?;
    items.insert("e", &[255])?;
    items.rotate_memtable_and_wait()?;

    items.major_compact()?;
    assert!(items.contains_key("a")?);
    assert!(items.contains_key("b")?);
    assert!(items.contains_key("c")?);

    ttl_watermark.store(1, std::sync::atomic::Ordering::Relaxed);
    eprintln!("TTL now <= 1");
    items.major_compact()?;
    assert!(!items.contains_key("a")?);
    assert!(items.contains_key("b")?);
    assert!(items.contains_key("c")?);

    ttl_watermark.store(9, std::sync::atomic::Ordering::Relaxed);
    eprintln!("\nTTL now <= 9");
    items.major_compact()?;
    assert!(!items.contains_key("a")?);
    assert!(items.contains_key("b")?);
    assert!(items.contains_key("c")?);

    ttl_watermark.store(10, std::sync::atomic::Ordering::Relaxed);
    eprintln!("\nTTL now <= 10");
    items.major_compact()?;
    assert!(!items.contains_key("a")?);
    assert!(!items.contains_key("b")?);
    assert!(items.contains_key("c")?);

    ttl_watermark.store(50, std::sync::atomic::Ordering::Relaxed);
    eprintln!("\nTTL now <= 50");
    items.major_compact()?;
    assert!(!items.contains_key("a")?);
    assert!(!items.contains_key("b")?);
    assert!(!items.contains_key("c")?);

    Ok(())
}
