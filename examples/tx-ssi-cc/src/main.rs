use std::time::{Duration, Instant};

fn main() -> fjall::Result<()> {
    let keyspace = fjall::Config::new(".fjall_data")
        .temporary(true)
        .open_transactional()?;
    let items = keyspace.open_partition("items", Default::default())?;

    let start = Instant::now();

    let t1 = {
        let keyspace = keyspace.clone();
        let items = items.clone();

        std::thread::spawn(move || {
            let mut wtx = keyspace.write_tx().unwrap();
            println!("Started tx1");
            std::thread::sleep(Duration::from_secs(3));
            wtx.insert(&items, "a", "a");
            wtx.commit()
        })
    };

    let t2 = {
        let keyspace = keyspace.clone();
        let items = items.clone();

        std::thread::spawn(move || {
            let mut wtx = keyspace.write_tx().unwrap();
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
        let rtx = keyspace.read_tx();
        rtx.len(&items)?
    });

    Ok(())
}
