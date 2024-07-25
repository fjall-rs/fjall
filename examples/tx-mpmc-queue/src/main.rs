use fjall::{Config, PersistMode};
use std::path::Path;
use std::sync::{
    atomic::{AtomicUsize, Ordering::Relaxed},
    Arc,
};

const PRODUCER_COUNT: usize = 4;
const PRODUCING_COUNT: usize = 100;

const EXPECTED_COUNT: usize = PRODUCER_COUNT * PRODUCING_COUNT;

fn main() -> fjall::Result<()> {
    let path = Path::new(".fjall_data");

    let keyspace = Config::new(path).temporary(true).open_transactional()?;
    let tasks = keyspace.open_partition("tasks", Default::default())?;

    let counter = Arc::new(AtomicUsize::default());

    let producers = (0..PRODUCER_COUNT)
        .map(|idx| {
            let keyspace = keyspace.clone();
            let tasks = tasks.clone();

            std::thread::spawn(move || {
                use rand::Rng;

                let mut rng = rand::thread_rng();

                for _ in 0..PRODUCING_COUNT {
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

    let consumers = (0..4)
        .map(|idx| {
            let keyspace = keyspace.clone();
            let tasks = tasks.clone();
            let counter = counter.clone();

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
                        tx.remove(&tasks, &key);

                        tx.commit()?;
                        keyspace.persist(PersistMode::Buffer)?;

                        let task_id = std::str::from_utf8(&key).unwrap();
                        println!("consumer {idx} completed task {task_id}");

                        counter.fetch_add(1, Relaxed);

                        let ms = rng.gen_range(50..200);
                        std::thread::sleep(std::time::Duration::from_millis(ms));
                    } else if counter.load(Relaxed) == EXPECTED_COUNT {
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

    assert_eq!(EXPECTED_COUNT, counter.load(Relaxed));

    Ok(())
}
