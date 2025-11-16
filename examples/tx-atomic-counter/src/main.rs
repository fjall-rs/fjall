use fjall::{PersistMode, Readable, SingleWriterTxDatabase};
use std::path::Path;

const LIMIT: u64 = 100;

fn main() -> fjall::Result<()> {
    let path = Path::new(".fjall_data");

    let db = SingleWriterTxDatabase::builder(path)
        .temporary(true)
        .open()?;

    let counters = db.keyspace("counters", Default::default())?;

    counters.insert("c1", 0_u64.to_be_bytes())?;

    let workers = (0_u8..4)
        .map(|idx| {
            let db = db.clone();
            let counters = counters.clone();

            std::thread::spawn(move || {
                use rand::Rng;

                let mut rng = rand::thread_rng();

                loop {
                    let mut write_tx = db.write_tx();

                    let item = write_tx.get(&counters, "c1")?.unwrap();

                    let mut bytes = [0; 8];
                    bytes.copy_from_slice(&item);
                    let prev = u64::from_be_bytes(bytes);

                    if prev >= LIMIT {
                        return Ok::<_, fjall::Error>(());
                    }

                    let next = prev + 1;

                    write_tx.insert(&counters, "c1", next.to_be_bytes());
                    write_tx.commit()?;

                    println!("worker {idx} incremented to {next}");

                    let ms = rng.gen_range(10..400);
                    std::thread::sleep(std::time::Duration::from_millis(ms));
                }
            })
        })
        .collect::<Vec<_>>();

    for worker in workers {
        worker.join().unwrap()?;
    }

    assert_eq!(&*counters.get("c1").unwrap().unwrap(), LIMIT.to_be_bytes());

    Ok(())
}
