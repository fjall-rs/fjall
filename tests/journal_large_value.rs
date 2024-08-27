// Regression test for https://github.com/fjall-rs/fjall/issues/68

use fjall::{Config, PartitionOptions};

#[test_log::test]
fn journal_recover_large_value() -> fjall::Result<()> {
    let folder = ".test";

    let large_value = "a".repeat(128_000);

    {
        let keyspace = Config::new(&folder).open()?;
        let partition = keyspace.open_partition("default", PartitionOptions::default())?;
        partition.insert("a", &large_value)?;
        partition.insert("b", "b")?;
    }

    {
        let keyspace = Config::new(&folder).open()?;
        let partition = keyspace.open_partition("default", PartitionOptions::default())?;
        assert_eq!(large_value.as_bytes(), &*partition.get("a")?.unwrap());
        assert_eq!(b"b", &*partition.get("b")?.unwrap());
    }

    Ok(())
}

#[test_log::test]
fn journal_recover_large_value_blob() -> fjall::Result<()> {
    let folder = tempfile::tempdir()?;

    let large_value = "a".repeat(128_000);

    {
        let keyspace = Config::new(&folder).open()?;
        let partition = keyspace.open_partition(
            "default",
            PartitionOptions::default().use_kv_separation(true),
        )?;
        partition.insert("a", &large_value)?;
        partition.insert("b", "b")?;
    }

    {
        let keyspace = Config::new(&folder).open()?;
        let partition = keyspace.open_partition(
            "default",
            PartitionOptions::default().use_kv_separation(true),
        )?;
        assert_eq!(large_value.as_bytes(), &*partition.get("a")?.unwrap());
        assert_eq!(b"b", &*partition.get("b")?.unwrap());
    }

    Ok(())
}
