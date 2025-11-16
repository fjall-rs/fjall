use fjall::Readable;
use std::time::{Duration, Instant};

fn main() -> fjall::Result<()> {
    let db = fjall::SingleWriterTxDatabase::builder(".fjall_data")
        .temporary(true)
        .open()?;

    let items = db.keyspace("items", Default::default())?;

    let start = Instant::now();

    let t1 = {
        let db = db.clone();
        let items = items.clone();

        std::thread::spawn(move || {
            let mut wtx = db.write_tx();
            println!("Started tx1");
            std::thread::sleep(Duration::from_secs(3));
            wtx.insert(&items, "a", "a");
            wtx.commit()
        })
    };

    let t2 = {
        let db = db.clone();
        let items = items.clone();

        std::thread::spawn(move || {
            let mut wtx = db.write_tx();
            println!("Started tx2");
            std::thread::sleep(Duration::from_secs(3));
            wtx.insert(&items, "b", "b");
            wtx.commit()
        })
    };

    t1.join().expect("should join")?;

    t2.join().expect("should join")?;

    // NOTE: We would expect a single writer tx implementation to finish in
    // ~6 seconds
    println!("Done in {:?}, items.len={}", start.elapsed(), {
        let rtx = db.read_tx();
        rtx.len(items.inner())?
    });

    Ok(())
}
