use fjall::{Database, Readable, Slice};
use test_log::test;

#[test]
fn ingest_recovery() -> fjall::Result<()> {
    let path = tempfile::tempdir()?;

    let ks = "default";
    let key: Slice = b"abc".into();
    let value: Slice = b"zzz".into();

    {
        let db = Database::builder(&path).open()?;
        let keyspace = db.keyspace(ks, Default::default)?;
        let mut ing = keyspace.start_ingestion()?;
        ing.write(key.clone(), value.clone())?;
        ing.finish()?;
        assert_eq!(keyspace.get(key.clone())?, Some(value.clone())); // ok
    }

    {
        let db = Database::builder(&path).open()?;
        let keyspace = db.keyspace(ks, Default::default)?;
        assert_eq!(keyspace.get(key.clone())?, Some(value.clone())); // ok
    }

    {
        let db = Database::builder(&path).open()?;
        let keyspace = db.keyspace(ks, Default::default)?;
        let snapshot = db.snapshot();
        assert_eq!(snapshot.get(&keyspace, key.clone())?, Some(value.clone())); // snapshot - not ok
    }

    Ok(())
}
