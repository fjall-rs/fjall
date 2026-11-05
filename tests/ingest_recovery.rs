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

/// An unflushed write to the same key is older than the ingested item, so it must not shadow it,
/// neither in the memtable nor when it is replayed from the WAL after a reopen
#[test]
fn ingest_overwrites_unflushed_item() -> fjall::Result<()> {
    for kv_separation in [false, true] {
        let path = tempfile::tempdir()?;

        let open = || -> fjall::Result<(Database, fjall::Keyspace)> {
            let db = Database::builder(&path).worker_threads_unchecked(0).open()?;
            let keyspace = db.keyspace("default", || {
                let opts = fjall::KeyspaceCreateOptions::default();

                if kv_separation {
                    opts.with_kv_separation(Some(
                        fjall::KvSeparationOptions::default().separation_threshold(1),
                    ))
                } else {
                    opts
                }
            })?;
            Ok((db, keyspace))
        };

        {
            let (_db, keyspace) = open()?;

            keyspace.insert("a", "old", fjall::DurabilityLevel::SyncAll)?;
            keyspace.insert("b", "unflushed", fjall::DurabilityLevel::SyncAll)?;

            let mut ingestion = keyspace.start_ingestion()?;
            ingestion.write("a", "ingested")?;
            ingestion.finish()?;

            assert_eq!(Some(b"ingested".as_slice()), keyspace.get("a")?.as_deref());
            assert_eq!(Some(b"unflushed".as_slice()), keyspace.get("b")?.as_deref());
        }

        {
            let (_db, keyspace) = open()?;
            assert_eq!(Some(b"ingested".as_slice()), keyspace.get("a")?.as_deref());
            assert_eq!(Some(b"unflushed".as_slice()), keyspace.get("b")?.as_deref());
        }
    }

    Ok(())
}
