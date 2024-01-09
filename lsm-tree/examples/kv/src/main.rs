mod wal;

use lsm_tree::{Config, SequenceNumberCounter, Tree, Value};
use nanoid::nanoid;
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use wal::Wal;

/// Single-writer-only JSON-based KV-store.
#[derive(Clone)]
pub struct KvStore {
    tree: Tree,
    wal: Wal,
    seqno: SequenceNumberCounter,
}

impl KvStore {
    pub fn open<P: AsRef<Path>>(path: P) -> lsm_tree::Result<Self> {
        let start = Instant::now();
        let tree = Config::new(&path).open()?;
        eprintln!("Recovered LSM-tree in {}s", start.elapsed().as_secs_f32());

        let start = Instant::now();
        let (wal, memtable) = Wal::open(&path)?;
        eprintln!(
            "Recovered WAL + memtable in {}s",
            start.elapsed().as_secs_f32()
        );

        let seqno = SequenceNumberCounter::new(memtable.get_lsn().unwrap_or_default());

        tree.set_active_memtable(memtable);

        let kv = Self { tree, wal, seqno };

        {
            let tree = kv.tree.clone();

            // Run garbage collection on interval
            //
            // Could use something like a semaphore that
            // gets incremented after flushing instead
            std::thread::spawn(move || {
                loop {
                    eprintln!("Maybe compact");
                    let strategy = lsm_tree::compaction::Levelled::default();
                    tree.compact(Arc::new(strategy))?;
                    std::thread::sleep(Duration::from_secs(1));
                }
                Ok::<_, lsm_tree::Error>(())
            });
        }

        {
            let kv = kv.clone();

            // Keep data durable up to 1 second into the past
            // Could also call wal.sync() after every insert
            // But that makes inserts really slow
            std::thread::spawn(move || {
                loop {
                    std::thread::sleep(Duration::from_secs(1));
                    kv.wal.sync()?;
                }
                Ok::<_, lsm_tree::Error>(())
            });
        }

        Ok(kv)
    }

    pub fn insert<K: AsRef<str>, V: AsRef<str>>(
        &mut self,
        key: K,
        value: V,
    ) -> lsm_tree::Result<()> {
        let key = key.as_ref().as_bytes();
        let value = value.as_ref().as_bytes();
        let seqno = self.seqno.next();

        self.wal.write(Value {
            key: key.into(),
            value: value.into(),
            seqno,
            value_type: lsm_tree::ValueType::Value,
        })?;

        let (_, memtable_size) = self.tree.insert(key, value, seqno);
        self.maintenance(memtable_size)?;

        Ok(())
    }

    pub fn remove<K: AsRef<str>>(&mut self, key: K) -> lsm_tree::Result<()> {
        let key = key.as_ref().as_bytes();
        let seqno = self.seqno.next();

        self.wal.write(Value {
            key: key.into(),
            value: [].into(),
            seqno,
            value_type: lsm_tree::ValueType::Tombstone,
        })?;

        let (_, memtable_size) = self.tree.remove(key, seqno);
        self.maintenance(memtable_size)?;

        Ok(())
    }

    pub fn force_flush(&self) -> lsm_tree::Result<()> {
        eprintln!("Flushing memtable");
        self.tree.flush_active_memtable()?;
        Ok(())
    }

    pub fn maintenance(&mut self, memtable_size: u32) -> lsm_tree::Result<()> {
        // 8 MiB limit
        if memtable_size > 8 * 1_024 * 1_024 {
            self.force_flush()?;

            // NOTE: This is not safe and should not be used in a real implementation:
            // If the system crashes >here<, the WAL will still exist, and on
            // recovery we will have the flushed data on disk AND in the memtable
            // only to get flushed again
            //
            // You can try and think of a way of how to solve this ;)

            // NOTE: Because we are doing synchronous flushing, we can safely
            // truncate the log, as we now know all data is flushed to segments.
            self.wal.truncate()?;
        }

        if self.tree.first_level_segment_count() > 16 {
            eprintln!("Stalling writes...");
            std::thread::sleep(Duration::from_millis(100));
        }

        while self.tree.first_level_segment_count() > 20 {
            eprintln!("Halting writes until L0 is cleared up...");
        }

        Ok(())
    }

    pub fn get<K: AsRef<str>>(&self, key: K) -> lsm_tree::Result<Option<Arc<str>>> {
        Ok(self.tree.get(key.as_ref())?.map(|bytes| {
            std::str::from_utf8(&bytes)
                .expect("should be valid utf-8")
                .into()
        }))
    }

    pub fn contains_key<K: AsRef<str>>(&self, key: K) -> lsm_tree::Result<bool> {
        self.tree.contains_key(key.as_ref())
    }

    pub fn is_empty(&self) -> lsm_tree::Result<bool> {
        self.tree.is_empty()
    }

    pub fn len(&self) -> lsm_tree::Result<usize> {
        self.tree.len()
    }
}

const ITEM_COUNT: usize = 1_000_000;

fn main() -> lsm_tree::Result<()> {
    let mut kv = KvStore::open(".data")?;

    eprintln!("Counting items");
    eprintln!("Recovered LSM-tree with {} items", kv.len()?);

    if !kv.contains_key("my-key-1")? {
        kv.insert("my-key-1", "my-value-1")?;
    }
    if !kv.contains_key("my-key-2")? {
        kv.insert("my-key-2", "my-value-2")?;
    }
    if !kv.contains_key("my-key-3")? {
        kv.insert("my-key-3", "my-value-3")?;
    }

    eprintln!("Getting items");

    assert_eq!(Some("my-value-1"), kv.get("my-key-1")?.as_deref());
    assert_eq!(Some("my-value-2"), kv.get("my-key-2")?.as_deref());
    assert_eq!(Some("my-value-3"), kv.get("my-key-3")?.as_deref());

    eprintln!("Remove 3 items");
    kv.remove("my-key-1")?;
    kv.remove("my-key-2")?;
    kv.remove("my-key-3")?;

    assert!(!kv.contains_key("my-key-1")?);
    assert!(!kv.contains_key("my-key-2")?);
    assert!(!kv.contains_key("my-key-3")?);

    eprintln!("Counting items");
    let remaining_item_count = ITEM_COUNT - kv.len()?;

    eprintln!("Bulk loading {remaining_item_count} items");
    let start = Instant::now();

    for idx in 0..remaining_item_count {
        kv.insert(nanoid!(), nanoid!())?;

        if idx % 1_000_000 == 0 {
            eprintln!("Written {idx} items");
        }
    }
    eprintln!("Took: {}s", start.elapsed().as_secs_f32());

    eprintln!("Counting items");
    assert_eq!(ITEM_COUNT, kv.len()?);

    while kv.tree.is_compacting() {
        eprintln!("Waiting for compaction...");
        std::thread::sleep(Duration::from_secs(1));
    }

    eprintln!("Counting items");
    assert_eq!(ITEM_COUNT, kv.len()?);

    eprintln!("All good");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_kv() -> lsm_tree::Result<()> {
        if Path::new(".test-data").try_exists()? {
            std::fs::remove_dir_all(".test-data")?;
        }

        // Write some data
        {
            let mut kv = KvStore::open(".test-data")?;

            kv.insert("my-key-1", "my-value-1")?;
            kv.insert("my-key-2", "my-value-2")?;
            kv.insert("my-key-3", "my-value-3")?;

            assert_eq!(Some("my-value-1"), kv.get("my-key-1")?.as_deref());
            assert_eq!(Some("my-value-2"), kv.get("my-key-2")?.as_deref());
            assert_eq!(Some("my-value-3"), kv.get("my-key-3")?.as_deref());

            assert_eq!(3, kv.len()?);

            kv.remove("my-key-2")?;

            kv.force_flush()?;
        }

        // Recover from disk
        {
            let kv = KvStore::open(".test-data")?;

            assert_eq!(Some("my-value-1"), kv.get("my-key-1")?.as_deref());
            assert_eq!(None, kv.get("my-key-2")?);
            assert_eq!(Some("my-value-3"), kv.get("my-key-3")?.as_deref());

            assert_eq!(2, kv.len()?);
        }

        Ok(())
    }
}
