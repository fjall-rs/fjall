use fjall::{Config, PersistMode};
use std::path::Path;

fn main() -> fjall::Result<()> {
    let path = Path::new(".fjall_data");

    if path.try_exists()? {
        std::fs::remove_dir_all(path)?;
    }

    let keyspace = Config::new(path).open_transactional()?;
    let tasks = keyspace.open_partition("tasks", Default::default())?;

    let producers = (0_u8..4)
        .map(|idx| {
            let keyspace = keyspace.clone();
            let tasks = tasks.clone();

            std::thread::spawn(move || {
                use rand::Rng;

                let mut rng = rand::thread_rng();

                for _ in 0..100 {
                    let task_id = scru128::new_string();

                    tasks.insert(&task_id, &task_id)?;
                    keyspace.persist(PersistMode::Buffer)?;

                    println!("producer {idx} created task {task_id}");

                    let ms = rng.gen_range(10..100);
                    std::thread::sleep(std::time::Duration::from_millis(ms));
                }

                Ok::<_, fjall::Error>(())
            })
        })
        .collect::<Vec<_>>();

    let consumers = (0_u8..4)
        .map(|idx| {
            let keyspace = keyspace.clone();
            let tasks = tasks.clone();

            std::thread::spawn(move || {
                use rand::Rng;

                let mut rng = rand::thread_rng();

                loop {
                    let mut tx = keyspace.write_tx();

                    // TODO: NOTE:
                    // Tombstones will add up over time, making first KV slower
                    // Something like SingleDelete https://github.com/facebook/rocksdb/wiki/Single-Delete
                    // would be good for this type of workload
                    if let Some((key, _)) = tx.first_key_value(&tasks)? {
                        let task_id = std::str::from_utf8(&key).unwrap();

                        tx.remove(&tasks, task_id);

                        tx.commit()?;
                        keyspace.persist(PersistMode::Buffer)?;

                        println!("consumer {idx} completed task {task_id}");

                        let ms = rng.gen_range(100..200);
                        std::thread::sleep(std::time::Duration::from_millis(ms));
                    } else {
                        println!("consumer {idx} exited");
                        return Ok::<_, fjall::Error>(());
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    for t in producers {
        t.join().unwrap()?;
    }

    for t in consumers {
        t.join().unwrap()?;
    }

    let read_tx = keyspace.read_tx();
    assert!(read_tx.is_empty(&tasks)?);

    Ok(())
}
