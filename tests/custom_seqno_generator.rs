use fjall::{Database, KeyspaceCreateOptions, SeqNo, SequenceNumberGenerator};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

/// A tracking generator that records how many times each method was called.
#[derive(Debug)]
struct TrackingGenerator {
    inner: AtomicU64,
    next_count: AtomicU64,
    get_count: AtomicU64,
    set_count: AtomicU64,
    fetch_max_count: AtomicU64,
}

impl TrackingGenerator {
    fn new() -> Self {
        Self {
            inner: AtomicU64::new(0),
            next_count: AtomicU64::new(0),
            get_count: AtomicU64::new(0),
            set_count: AtomicU64::new(0),
            fetch_max_count: AtomicU64::new(0),
        }
    }
}

impl SequenceNumberGenerator for TrackingGenerator {
    fn next(&self) -> SeqNo {
        self.next_count.fetch_add(1, Ordering::Relaxed);
        self.inner.fetch_add(1, Ordering::AcqRel)
    }

    fn get(&self) -> SeqNo {
        self.get_count.fetch_add(1, Ordering::Relaxed);
        self.inner.load(Ordering::Acquire)
    }

    fn set(&self, value: SeqNo) {
        self.set_count.fetch_add(1, Ordering::Relaxed);
        self.inner.store(value, Ordering::Release);
    }

    fn fetch_max(&self, value: SeqNo) {
        self.fetch_max_count.fetch_add(1, Ordering::Relaxed);
        self.inner.fetch_max(value, Ordering::AcqRel);
    }
}

#[test]
fn custom_generator_basic() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let generator = Arc::new(TrackingGenerator::new());
    let db = Database::builder(&folder)
        .seqno_generator(generator.clone())
        .open()?;

    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    tree.insert("a", "hello")?;
    tree.insert("b", "world")?;

    assert!(
        generator.next_count.load(Ordering::Relaxed) >= 2,
        "Custom generator should be called for write operations",
    );

    assert_eq!(b"hello", &*tree.get("a")?.unwrap());
    assert_eq!(b"world", &*tree.get("b")?.unwrap());

    Ok(())
}

#[test]
fn custom_generator_recovery() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    // Write with custom generator
    {
        let generator = Arc::new(TrackingGenerator::new());
        let db = Database::builder(&folder)
            .seqno_generator(generator.clone())
            .open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
        tree.insert("key1", "value1")?;
        tree.insert("key2", "value2")?;

        db.persist(fjall::PersistMode::SyncAll)?;
    }

    // Recover with a new custom generator — recovery calls .set()/.fetch_max()
    {
        let generator = Arc::new(TrackingGenerator::new());
        let db = Database::builder(&folder)
            .seqno_generator(generator.clone())
            .open()?;

        let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

        // Data survived recovery
        assert_eq!(b"value1", &*tree.get("key1")?.unwrap());
        assert_eq!(b"value2", &*tree.get("key2")?.unwrap());

        // Recovery used the generator (fetch_max or get)
        let fetch_max_calls = generator.fetch_max_count.load(Ordering::Relaxed);
        let get_calls = generator.get_count.load(Ordering::Relaxed);
        assert!(
            fetch_max_calls > 0 || get_calls > 0,
            "Recovery should use the custom generator",
        );

        // Can write after recovery
        tree.insert("key3", "value3")?;
        assert_eq!(b"value3", &*tree.get("key3")?.unwrap());
    }

    Ok(())
}

#[test]
fn default_generator_unchanged() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    // Without custom generator, everything works as before
    let db = Database::builder(&folder).open()?;
    let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;

    tree.insert("a", "1")?;
    tree.insert("b", "2")?;

    assert_eq!(b"1", &*tree.get("a")?.unwrap());
    assert_eq!(b"2", &*tree.get("b")?.unwrap());

    Ok(())
}
