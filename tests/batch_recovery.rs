use fjall::Database;
use test_log::test;

#[test]
fn batch_recovery() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    for i in 0_u128..25 {
        let db = Database::create_or_recover(Database::builder(folder.path()).into_config())?;

        let tree = db.keyspace("default", Default::default)?;
        let tree2 = db.keyspace("default2", Default::default)?;

        let mut batch = db.batch();
        batch.insert(&tree, i.to_be_bytes(), i.to_be_bytes());
        batch.insert(&tree2, i.to_be_bytes(), i.to_be_bytes());
        batch.commit()?;
    }

    Ok(())
}
