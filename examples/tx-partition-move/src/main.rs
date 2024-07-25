use fjall::{Config, PersistMode};
use std::path::Path;

const ITEM_COUNT: u64 = 200;

fn main() -> fjall::Result<()> {
    let path = Path::new(".fjall_data");

    let keyspace = Config::new(path).temporary(true).open_transactional()?;
    let src = keyspace.open_partition("src", Default::default())?;
    let dst = keyspace.open_partition("dst", Default::default())?;

    for _ in 0..ITEM_COUNT {
        src.insert(scru128::new_string(), "")?;
    }

    let movers = (0..4)
        .map(|idx| {
            let keyspace = keyspace.clone();
            let src = src.clone();
            let dst = dst.clone();

            std::thread::spawn(move || {
                use rand::Rng;

                let mut rng = rand::thread_rng();

                loop {
                    let mut tx = keyspace.write_tx();

                    // TODO: NOTE:
                    // Tombstones will add up over time, making first KV slower
                    // Something like SingleDelete https://github.com/facebook/rocksdb/wiki/Single-Delete
                    // would be good for this type of workload
                    if let Some((key, value)) = tx.first_key_value(&src)? {
                        tx.remove(&src, &key);
                        tx.insert(&dst, &key, &value);

                        tx.commit()?;
                        keyspace.persist(PersistMode::Buffer)?;

                        let task_id = std::str::from_utf8(&key).unwrap();
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

    assert_eq!(ITEM_COUNT, keyspace.read_tx().len(&dst)? as u64);
    assert!(keyspace.read_tx().is_empty(&src)?);

    Ok(())
}
