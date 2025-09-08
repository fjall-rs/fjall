use tokio::task::spawn_blocking;

#[tokio::main]
async fn main() -> fjall::Result<()> {
    let db = fjall::Database::builder(".fjall_data").open()?;
    let items = db.keyspace("items", Default::default())?;

    {
        let items = items.clone();
        spawn_blocking(move || items.insert("hello", "world"))
            .await
            .expect("join failed")?;
    }

    let item = {
        let items = items.clone();
        spawn_blocking(move || items.get("hello"))
            .await
            .expect("join failed")?
    };

    let item = item.expect("should exist");

    assert_eq!(b"world", &*item);

    println!("OK");

    Ok(())
}
