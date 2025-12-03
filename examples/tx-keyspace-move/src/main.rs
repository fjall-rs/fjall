use fjall::{PersistMode, Readable, SingleWriterTxDatabase};
use std::path::Path;

const ITEM_COUNT: u64 = 200;

fn main() -> fjall::Result<()> {
    let path = Path::new(".fjall_data");

    let db = SingleWriterTxDatabase::builder(path)
        .temporary(true)
        .open()?;

    let src = db.keyspace("src", fjall::KeyspaceCreateOptions::default)?;
    let dst = db.keyspace("dst", fjall::KeyspaceCreateOptions::default)?;

    for _ in 0..ITEM_COUNT {
        src.insert(scru128::new_string(), "")?;
    }

    let movers = (0..4)
        .map(|idx| {
            let db = db.clone();
            let src = src.clone();
            let dst = dst.clone();

            std::thread::spawn(move || {
                use rand::Rng;

                let mut rng = rand::thread_rng();

                loop {
                    let mut tx = db.write_tx();

                    // TODO: NOTE:
                    // Tombstones will add up over time, making first KV slower
                    // Something like SingleDelete https://github.com/facebook/rocksdb/wiki/Single-Delete
                    // would be good for this type of workload
                    if let Some(kv) = tx.first_key_value(&src).map(|x| x.into_inner()) {
                        let (key, value) = kv?;

                        let task_id = std::str::from_utf8(&key).unwrap().to_owned();

                        tx.remove(&src, key.clone());
                        tx.insert(&dst, key, value);

                        tx.commit()?;

                        println!("consumer {idx} moved {task_id}");

                        let ms = rng.gen_range(10..100);
                        std::thread::sleep(std::time::Duration::from_millis(ms));
                    } else {
                        return Ok::<_, fjall::Error>(());
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    for t in movers {
        t.join().unwrap()?;
    }

    assert_eq!(ITEM_COUNT, db.read_tx().len(dst.inner())? as u64);
    assert!(db.read_tx().is_empty(src.inner())?);

    Ok(())
}
