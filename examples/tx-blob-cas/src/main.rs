use fjall::{
    GarbageCollection, KeyspaceCreateOptions, KvSeparationOptions, PersistMode, TxDatabase,
    TxKeyspace, UserValue, WriteTransaction,
};
use format_bytes::format_bytes;
use sha2::Digest;
use std::path::Path;

// TODO: 3.0.0 restore

/// Content-addressable store
struct Cas {
    db: TxDatabase,
    items: TxKeyspace,
    blobs: TxKeyspace,
}

impl Cas {
    pub fn new<P: AsRef<Path>>(path: P) -> fjall::Result<Self> {
        let db = TxDatabase::builder(path).open()?;

        let items = db.keyspace("items", Default::default())?;
        let blobs = db.keyspace(
            "blobs",
            // IMPORTANT: Use KV-separation
            KeyspaceCreateOptions::default()
                .with_kv_separation(KvSeparationOptions::default())
                .max_memtable_size(32_000_000),
        )?;

        Ok(Self { db, items, blobs })
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> fjall::Result<Option<UserValue>> {
        let key = key.as_ref();

        let tx = self.db.read_tx();

        let Some(content_hash) = tx.get(&self.items, format_bytes!(b"k#{}", key))? else {
            return Ok(None);
        };

        tx.get(&self.blobs, content_hash)
    }

    pub fn has_content<V: AsRef<[u8]>>(&self, value: V) -> fjall::Result<bool> {
        let value = value.as_ref();

        let content_hash = Self::get_content_hash(value);

        self.db.read_tx().contains_key(&self.blobs, content_hash)
    }

    fn get_content_hash(value: &[u8]) -> Vec<u8> {
        let mut content_hash = sha2::Sha256::new();
        content_hash.update(value);
        content_hash.finalize().to_vec()
    }

    fn increment_ref_count(
        &self,
        tx: &mut WriteTransaction,
        content_hash: &[u8],
    ) -> fjall::Result<u64> {
        let key = format_bytes!(b"rc#{}", &content_hash);
        let curr_rc = tx.get(&self.items, &key)?;

        let higher_rc = curr_rc
            .map(|prev_bytes| {
                let mut buf = [0; 8];
                buf.copy_from_slice(&prev_bytes);
                u64::from_be_bytes(buf)
            })
            .unwrap_or_default()
            + 1;

        tx.insert(&self.items, key, higher_rc.to_be_bytes());
        Ok(higher_rc)
    }

    fn decrement_ref_count(
        &self,
        tx: &mut WriteTransaction,
        content_hash: &[u8],
    ) -> fjall::Result<u64> {
        let key = format_bytes!(b"rc#{}", &content_hash);
        let curr_bytes = tx.get(&self.items, &key)?.expect("RC should exist");

        let mut buf = [0; 8];
        buf.copy_from_slice(&curr_bytes);
        let curr_rc = u64::from_be_bytes(buf);

        assert!(curr_rc >= 1);

        let prev_rc = curr_rc - 1;

        if prev_rc == 0 {
            tx.remove(&self.items, key);
        } else {
            tx.insert(&self.items, key, prev_rc.to_be_bytes());
        }

        Ok(prev_rc)
    }

    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> fjall::Result<()> {
        let key = key.as_ref();
        let value = value.as_ref();

        let content_hash = Self::get_content_hash(value);

        let mut tx = self.db.write_tx();

        let item_key = format_bytes!(b"k#{}", key);

        if let Some(prev_content_hash) = tx.get(&self.items, &item_key)? {
            if *prev_content_hash == content_hash {
                return Ok(());
            }

            let rc = self.decrement_ref_count(&mut tx, &prev_content_hash)?;
            if rc == 0 {
                // No more references
                tx.remove(&self.blobs, prev_content_hash);
            }
        }

        // Insert new item
        tx.insert(&self.items, &item_key, &content_hash);
        self.increment_ref_count(&mut tx, &content_hash)?;

        if !tx.contains_key(&self.blobs, &content_hash)? {
            tx.insert(&self.blobs, &content_hash, value);
        }

        tx.commit()?;
        self.db.persist(PersistMode::SyncAll)?;

        Ok(())
    }

    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> fjall::Result<()> {
        let key = key.as_ref();

        let mut tx = self.db.write_tx();

        let item_key = format_bytes!(b"k#{}", key);

        if let Some(content_hash) = tx.get(&self.items, &item_key)? {
            tx.remove(&self.items, item_key);

            let rc = self.decrement_ref_count(&mut tx, &content_hash)?;
            if rc == 0 {
                // No more references
                tx.remove(&self.blobs, content_hash);
            }

            tx.commit()?;
            self.db.persist(PersistMode::SyncAll)?;
        }

        Ok(())
    }
}

fn main() -> fjall::Result<()> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    let path = Path::new(".fjall_data");

    if path.try_exists()? {
        std::fs::remove_dir_all(path)?;
    }

    let big_blob_1 = "hellotest1".repeat(1_000_000).as_bytes().to_vec();
    let big_blob_2 = "hellotest2".repeat(1_000_000).as_bytes().to_vec();
    let big_blob_3 = "anothervalue".repeat(1_000_000).as_bytes().to_vec();

    let cas = Cas::new(path)?;

    assert!(!cas.has_content(&big_blob_1)?);
    assert!(!cas.has_content(&big_blob_2)?);
    assert!(!cas.has_content(&big_blob_3)?);

    assert_eq!(0, cas.db.read_tx().len(&cas.items)?);
    assert_eq!(0, cas.db.read_tx().len(&cas.blobs)?);

    cas.insert(b"a", &big_blob_1)?;

    assert!(cas.has_content(&big_blob_1)?);
    assert!(!cas.has_content(&big_blob_2)?);
    assert!(!cas.has_content(&big_blob_3)?);

    assert_eq!(1 + 1, cas.db.read_tx().len(&cas.items)?);
    assert_eq!(1, cas.db.read_tx().len(&cas.blobs)?);

    cas.insert(b"b", &big_blob_2)?;

    assert!(cas.has_content(&big_blob_1)?);
    assert!(cas.has_content(&big_blob_2)?);
    assert!(!cas.has_content(&big_blob_3)?);

    assert_eq!(&*cas.get(b"a")?.unwrap(), big_blob_1);
    assert_eq!(&*cas.get(b"b")?.unwrap(), big_blob_2);

    assert_eq!(2 + 2, cas.db.read_tx().len(&cas.items)?);
    assert_eq!(2, cas.db.read_tx().len(&cas.blobs)?);

    cas.remove(b"a")?;

    assert!(!cas.has_content(&big_blob_1)?);
    assert!(cas.has_content(&big_blob_2)?);
    assert!(!cas.has_content(&big_blob_3)?);

    assert_eq!(1 + 1, cas.db.read_tx().len(&cas.items)?);
    assert_eq!(1, cas.db.read_tx().len(&cas.blobs)?);

    cas.remove(b"b")?;

    assert!(!cas.has_content(&big_blob_1)?);
    assert!(!cas.has_content(&big_blob_2)?);
    assert!(!cas.has_content(&big_blob_3)?);

    assert_eq!(0, cas.db.read_tx().len(&cas.items)?);
    assert_eq!(0, cas.db.read_tx().len(&cas.blobs)?);

    cas.insert(b"b", &big_blob_3)?;

    assert!(!cas.has_content(&big_blob_1)?);
    assert!(!cas.has_content(&big_blob_2)?);
    assert!(cas.has_content(&big_blob_3)?);

    assert_eq!(1 + 1, cas.db.read_tx().len(&cas.items)?);
    assert_eq!(1, cas.db.read_tx().len(&cas.blobs)?);

    cas.remove(b"b")?;

    assert!(!cas.has_content(&big_blob_1)?);
    assert!(!cas.has_content(&big_blob_2)?);
    assert!(!cas.has_content(&big_blob_3)?);

    assert_eq!(0, cas.db.read_tx().len(&cas.items)?);
    assert_eq!(0, cas.db.read_tx().len(&cas.blobs)?);

    cas.insert(b"b", &big_blob_3)?;
    cas.insert(b"c", &big_blob_3)?;

    assert!(!cas.has_content(&big_blob_1)?);
    assert!(!cas.has_content(&big_blob_2)?);
    assert!(cas.has_content(&big_blob_3)?);

    assert_eq!(2 + 1, cas.db.read_tx().len(&cas.items)?);
    assert_eq!(1, cas.db.read_tx().len(&cas.blobs)?);

    cas.remove(b"c")?;

    assert!(!cas.has_content(&big_blob_1)?);
    assert!(!cas.has_content(&big_blob_2)?);
    assert!(cas.has_content(&big_blob_3)?);

    assert_eq!(1 + 1, cas.db.read_tx().len(&cas.items)?);
    assert_eq!(1, cas.db.read_tx().len(&cas.blobs)?);

    cas.remove(b"b")?;

    assert!(!cas.has_content(&big_blob_1)?);
    assert!(!cas.has_content(&big_blob_2)?);
    assert!(!cas.has_content(&big_blob_3)?);

    assert_eq!(0, cas.db.read_tx().len(&cas.items)?);
    assert_eq!(0, cas.db.read_tx().len(&cas.blobs)?);

    cas.insert(b"a", &big_blob_1)?;
    assert_eq!(1, cas.db.read_tx().len(&cas.blobs)?);
    cas.insert(b"a", &big_blob_2)?;
    assert_eq!(1, cas.db.read_tx().len(&cas.blobs)?);
    cas.insert(b"a", &big_blob_3)?;
    assert_eq!(1, cas.db.read_tx().len(&cas.blobs)?);

    assert!(!cas.has_content(&big_blob_1)?);
    assert!(!cas.has_content(&big_blob_2)?);
    assert!(cas.has_content(&big_blob_3)?);

    cas.remove(b"a")?;

    assert!(!cas.has_content(&big_blob_1)?);
    assert!(!cas.has_content(&big_blob_2)?);
    assert!(!cas.has_content(&big_blob_3)?);

    assert_eq!(0, cas.db.read_tx().len(&cas.items)?);
    assert_eq!(0, cas.db.read_tx().len(&cas.blobs)?);

    // example

    log::info!(">>> Storing HTML page for https://google.com");
    cas.insert(
        "com.google",
        "<html>hello google</html>".repeat(1_000_000).as_bytes(),
    )?;

    assert_eq!(1, cas.db.read_tx().len(&cas.blobs)?);
    log::info!("Stored blobs: {}", cas.db.read_tx().len(&cas.blobs)?);

    log::info!(">>> Storing HTML page for https://github.com");
    cas.insert(
        "com.github",
        "<html>hello github</html>".repeat(1_000_000).as_bytes(),
    )?;

    assert_eq!(2, cas.db.read_tx().len(&cas.blobs)?);
    log::info!("Stored blobs: {}", cas.db.read_tx().len(&cas.blobs)?);

    assert!(cas.get("com.google")?.is_some());
    log::info!(
        "Do I have https://google.com? {}",
        cas.get("com.google")?.is_some()
    );

    assert!(cas.get("com.github")?.is_some());
    log::info!(
        "Do I have https://www.github.com? {}",
        cas.get("com.github")?.is_some()
    );

    log::info!(">>> Storing HTML page for https://www.google.com");
    cas.insert(
        "com.google.www",
        "<html>hello google</html>".repeat(1_000_000).as_bytes(),
    )?;

    assert!(cas.get("com.google.www")?.is_some());
    log::info!(
        "Do I have https://www.google.com? {}",
        cas.get("com.google.www")?.is_some()
    );

    assert_eq!(2, cas.db.read_tx().len(&cas.blobs)?);
    log::info!("Stored blobs: {}", cas.db.read_tx().len(&cas.blobs)?);

    assert_eq!(cas.get("com.google.www")?, cas.get("com.google")?);
    log::info!(
        "www.google should be same as google: {}",
        cas.get("com.google.www")? == cas.get("com.google")?
    );

    log::info!(">>> Removing https://github.com");
    cas.remove("com.github")?;

    log::info!(
        "Do I have https://www.github.com? {}",
        cas.get("com.github")?.is_some()
    );

    assert_eq!(1, cas.db.read_tx().len(&cas.blobs)?);
    log::info!("Stored blobs: {}", cas.db.read_tx().len(&cas.blobs)?);

    log::info!(">>> Removing https://google.com");
    cas.remove("com.google")?;

    assert!(cas.get("com.google")?.is_none());
    log::info!(
        "Do I have https://google.com? {}",
        cas.get("com.google")?.is_some()
    );

    assert_eq!(1, cas.db.read_tx().len(&cas.blobs)?);
    log::info!("Stored blobs: {}", cas.db.read_tx().len(&cas.blobs)?);

    log::info!(">>> Removing https://www.google.com");
    cas.remove("com.google.www")?;

    assert!(cas.get("com.google.www")?.is_none());
    log::info!(
        "Do I have https://www.google.com? {}",
        cas.get("www.com.google")?.is_some()
    );

    assert_eq!(0, cas.db.read_tx().len(&cas.blobs)?);
    log::info!("Stored blobs: {}", cas.db.read_tx().len(&cas.blobs)?);

    let report = cas.blobs.gc_scan()?;
    eprintln!("{report}");

    cas.blobs.gc_drop_stale_segments()?;

    let report = cas.blobs.gc_scan()?;
    eprintln!("{report}");

    Ok(())
}
