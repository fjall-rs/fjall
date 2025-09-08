use std::time::{Duration, Instant};

fn main() -> fjall::Result<()> {
    let db = fjall::Database::builder(".fjall_data")
        .temporary(true)
        .open_transactional()?;

    let items = db.keyspace("items", Default::default())?;

    let start = Instant::now();

    let t1 = {
        let db = db.clone();
        let items = items.clone();

        std::thread::spawn(move || {
            let mut wtx = db.write_tx().unwrap();
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
            let mut wtx = db.write_tx().unwrap();
            println!("Started tx2");
            std::thread::sleep(Duration::from_secs(3));
            wtx.insert(&items, "b", "b");
            wtx.commit()
        })
    };

    t1.join()
        .expect("should join")?
        .expect("tx should not fail");

    t2.join()
        .expect("should join")?
        .expect("tx should not fail");

    // NOTE: We would expect a single writer tx implementation to finish in
    // ~6 seconds
    println!("Done in {:?}, items.len={}", start.elapsed(), {
        let rtx = db.read_tx();
        rtx.len(&items)?
    });

    Ok(())
}
