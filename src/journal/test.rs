use super::*;
use crate::batch::item::Item as BatchItem;
use entry::Entry;
use lsm_tree::ValueType;
use std::io::Write;
use tempfile::tempdir;
use test_log::test;

impl PartialEq<BatchItem> for crate::journal::batch_reader::ReadBatchItem {
    fn eq(&self, other: &BatchItem) -> bool {
        self.keyspace_id == other.keyspace.id
            && self.key == other.key
            && self.value == other.value
            && self.value_type == other.value_type
    }
}

impl PartialEq<crate::journal::batch_reader::ReadBatchItem> for BatchItem {
    fn eq(&self, other: &crate::journal::batch_reader::ReadBatchItem) -> bool {
        other.eq(self)
    }
}

#[test]
#[expect(clippy::redundant_clone)]
fn journal_rotation() -> crate::Result<()> {
    let dir1 = tempdir()?;
    let db = crate::Database::builder(&dir1).open()?;
    let keyspace = db.keyspace("default", Default::default)?;

    let dir2 = tempdir()?;
    let path = dir2.path().join("0.jnl");
    let next_path = dir2.path().join("1.jnl");

    {
        let journal = Journal::create_new(&path)?;
        let mut writer = journal.get_writer();

        writer.write_batch(
            [
                BatchItem::new(keyspace.clone(), *b"a", *b"a", ValueType::Value),
                BatchItem::new(keyspace.clone(), *b"b", *b"b", ValueType::Value),
            ]
            .iter(),
            2,
            0,
        )?;
        writer.rotate()?;
    }

    assert!(path.try_exists()?);
    assert!(next_path.try_exists()?);

    Ok(())
}

#[test]
#[expect(clippy::redundant_clone)]
fn journal_recovery_active() -> crate::Result<()> {
    let dir1 = tempdir()?;
    let db = crate::Database::builder(&dir1).open()?;
    let keyspace0 = db.keyspace("default", Default::default)?;
    let keyspace1 = db.keyspace("default1", Default::default)?;
    let keyspace2 = db.keyspace("default2", Default::default)?;

    let dir2 = tempdir()?;
    let path = dir2.path().join("0.jnl");
    let next_path = dir2.path().join("1.jnl");
    let next_next_path = dir2.path().join("2.jnl");

    {
        let journal = Journal::create_new(&path)?;
        let mut writer = journal.get_writer();

        writer.write_batch(
            [
                BatchItem::new(keyspace0.clone(), *b"a", *b"a", ValueType::Value),
                BatchItem::new(keyspace0.clone(), *b"b", *b"b", ValueType::Value),
            ]
            .iter(),
            2,
            0,
        )?;
        writer.rotate()?;

        writer.write_batch(
            [
                BatchItem::new(keyspace1.clone(), *b"c", *b"c", ValueType::Value),
                BatchItem::new(keyspace1.clone(), *b"d", *b"d", ValueType::Value),
            ]
            .iter(),
            2,
            1,
        )?;
        writer.rotate()?;

        writer.write_batch(
            [
                BatchItem::new(keyspace2.clone(), *b"c", *b"c", ValueType::Value),
                BatchItem::new(keyspace2.clone(), *b"d", *b"d", ValueType::Value),
            ]
            .iter(),
            2,
            1,
        )?;
    }

    assert!(path.try_exists()?);
    assert!(next_path.try_exists()?);
    assert!(next_next_path.try_exists()?);

    let journal_recovered = Journal::recover(dir2, CompressionType::None, 0)?;
    assert_eq!(journal_recovered.active.path(), next_next_path);
    assert_eq!(journal_recovered.sealed, &[(0, path), (1, next_path)]);

    Ok(())
}

#[test]
#[cfg(feature = "lz4")]
#[expect(clippy::redundant_clone)]
fn journal_recovery_active_lz4() -> crate::Result<()> {
    let dir1 = tempdir()?;
    let db = crate::Database::builder(&dir1).open()?;
    let keyspace0 = db.keyspace("default", Default::default)?;
    let keyspace1 = db.keyspace("default1", Default::default)?;
    let keyspace2 = db.keyspace("default2", Default::default)?;

    let dir2 = tempdir()?;
    let path = dir2.path().join("0.jnl");
    let next_path = dir2.path().join("1.jnl");
    let next_next_path = dir2.path().join("2.jnl");

    {
        let journal = Journal::create_new(&path)?.with_compression(CompressionType::Lz4, 1);
        let mut writer = journal.get_writer();

        writer.write_batch(
            [
                BatchItem::new(keyspace0.clone(), *b"a", *b"a", ValueType::Value),
                BatchItem::new(keyspace0.clone(), *b"b", *b"b", ValueType::Value),
            ]
            .iter(),
            2,
            0,
        )?;
        writer.rotate()?;

        writer.write_batch(
            [
                BatchItem::new(keyspace1.clone(), *b"c", *b"c", ValueType::Value),
                BatchItem::new(keyspace1.clone(), *b"d", *b"d", ValueType::Value),
            ]
            .iter(),
            2,
            1,
        )?;
        writer.rotate()?;

        writer.write_batch(
            [
                BatchItem::new(keyspace2.clone(), *b"c", *b"c", ValueType::Value),
                BatchItem::new(keyspace2.clone(), *b"d", *b"d", ValueType::Value),
            ]
            .iter(),
            2,
            1,
        )?;
    }

    assert!(path.try_exists()?);
    assert!(next_path.try_exists()?);
    assert!(next_next_path.try_exists()?);

    let journal_recovered = Journal::recover(dir2, CompressionType::None, 0)?;
    assert_eq!(journal_recovered.active.path(), next_next_path);
    assert_eq!(journal_recovered.sealed, &[(0, path), (1, next_path)]);

    Ok(())
}

#[test]
#[expect(clippy::redundant_clone)]
fn journal_recovery_no_active() -> crate::Result<()> {
    let dir1 = tempdir()?;
    let db = crate::Database::builder(&dir1).open()?;
    let keyspace = db.keyspace("default", Default::default)?;

    let dir2 = tempdir()?;
    let path = dir2.path().join("0.jnl");
    let next_path = dir2.path().join("1.jnl");

    {
        let journal = Journal::create_new(&path)?;

        {
            let mut writer = journal.get_writer();

            writer.write_batch(
                [
                    BatchItem::new(keyspace.clone(), *b"a", *b"a", ValueType::Value),
                    BatchItem::new(keyspace.clone(), *b"b", *b"b", ValueType::Value),
                ]
                .iter(),
                2,
                0,
            )?;
            writer.rotate()?;
        }

        // NOTE: Delete the new, active journal -> old journal will be
        // reused as active on next recovery
        std::fs::remove_file(&next_path)?;
    }

    assert!(path.try_exists()?);
    assert!(!next_path.try_exists()?);

    let journal_recovered = Journal::recover(dir2, CompressionType::None, 0)?;
    assert_eq!(journal_recovered.active.path(), path);
    assert_eq!(journal_recovered.sealed, &[]);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, clippy::redundant_clone)]
fn journal_truncation_corrupt_bytes() -> crate::Result<()> {
    let dir1 = tempdir()?;
    let db = crate::Database::builder(&dir1).open()?;
    let keyspace = db.keyspace("default", Default::default)?;

    let dir2 = tempdir()?;
    let path = dir2.path().join("0.jnl");

    let values = [
        BatchItem::new(keyspace.clone(), *b"abc", *b"def", ValueType::Value),
        BatchItem::new(keyspace.clone(), *b"yxc", *b"ghj", ValueType::Value),
    ];

    {
        let journal = Journal::create_new(&path)?;
        journal
            .get_writer()
            .write_batch(values.iter(), values.len(), 0)?;
    }

    {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let collected = reader.flatten().collect::<Vec<_>>();
        assert_eq!(values.to_vec(), collected.first().unwrap().items);
    }

    // Mangle journal
    {
        let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
        file.write_all(b"09pmu35w3a9mp53bao9upw3ab5up")?;
        file.sync_all()?;
    }

    for _ in 0..10 {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let collected = reader.flatten().collect::<Vec<_>>();
        assert_eq!(values.to_vec(), collected.first().unwrap().items);
    }

    // Mangle journal
    for _ in 0..5 {
        let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
        file.write_all(b"09pmu35w3a9mp53bao9upw3ab5up")?;
        file.sync_all()?;
    }

    for _ in 0..10 {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let collected = reader.flatten().collect::<Vec<_>>();
        assert_eq!(values.to_vec(), collected.first().unwrap().items);
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, clippy::redundant_clone)]
fn journal_truncation_repeating_start_marker() -> crate::Result<()> {
    let dir1 = tempdir()?;
    let db = crate::Database::builder(&dir1).open()?;
    let keyspace = db.keyspace("default", Default::default)?;

    let dir2 = tempdir()?;
    let path = dir2.path().join("0.jnl");

    let values = [
        BatchItem::new(keyspace.clone(), *b"abc", *b"def", ValueType::Value),
        BatchItem::new(keyspace.clone(), *b"yxc", *b"ghj", ValueType::Value),
    ];

    {
        let journal = Journal::create_new(&path)?;
        journal
            .get_writer()
            .write_batch(values.iter(), values.len(), 0)?;
    }

    {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let collected = reader.flatten().collect::<Vec<_>>();
        assert_eq!(values.to_vec(), collected.first().unwrap().items);
    }

    // Mangle journal
    {
        let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
        Entry::Start {
            item_count: 2,
            seqno: 64,
        }
        .encode_into(&mut file)?;
        file.sync_all()?;
    }

    for _ in 0..10 {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let collected = reader.flatten().collect::<Vec<_>>();
        assert_eq!(values.to_vec(), collected.first().unwrap().items);
    }

    // Mangle journal
    for _ in 0..5 {
        let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
        Entry::Start {
            item_count: 2,
            seqno: 64,
        }
        .encode_into(&mut file)?;
        file.sync_all()?;
    }

    for _ in 0..10 {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let collected = reader.flatten().collect::<Vec<_>>();
        assert_eq!(values.to_vec(), collected.first().unwrap().items);
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, clippy::redundant_clone)]
fn journal_truncation_repeating_end_marker() -> crate::Result<()> {
    let dir1 = tempdir()?;
    let db = crate::Database::builder(&dir1).open()?;
    let keyspace = db.keyspace("default", Default::default)?;

    let dir2 = tempdir()?;
    let path = dir2.path().join("0.jnl");

    let values = [
        BatchItem::new(keyspace.clone(), *b"abc", *b"def", ValueType::Value),
        BatchItem::new(keyspace.clone(), *b"yxc", *b"ghj", ValueType::Value),
    ];

    {
        let journal = Journal::create_new(&path)?;
        journal
            .get_writer()
            .write_batch(values.iter(), values.len(), 0)?;
    }

    {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let collected = reader.flatten().collect::<Vec<_>>();
        assert_eq!(values.to_vec(), collected.first().unwrap().items);
    }

    // Mangle journal
    {
        let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
        Entry::End(5432).encode_into(&mut file)?;
        file.sync_all()?;
    }

    for _ in 0..10 {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let collected = reader.flatten().collect::<Vec<_>>();
        assert_eq!(values.to_vec(), collected.first().unwrap().items);
    }

    // Mangle journal
    for _ in 0..5 {
        let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
        Entry::End(5432).encode_into(&mut file)?;
        file.sync_all()?;
    }

    for _ in 0..10 {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let collected = reader.flatten().collect::<Vec<_>>();
        assert_eq!(values.to_vec(), collected.first().unwrap().items);
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, clippy::redundant_clone)]
fn journal_truncation_repeating_item_marker() -> crate::Result<()> {
    let dir1 = tempdir()?;
    let db = crate::Database::builder(&dir1).open()?;
    let keyspace = db.keyspace("default", Default::default)?;

    let dir2 = tempdir()?;
    let path = dir2.path().join("0.jnl");

    let values = [
        BatchItem::new(keyspace.clone(), *b"abc", *b"def", ValueType::Value),
        BatchItem::new(keyspace.clone(), *b"yxc", *b"ghj", ValueType::Value),
    ];

    {
        let journal = Journal::create_new(&path)?;
        journal
            .get_writer()
            .write_batch(values.iter(), values.len(), 0)?;
    }

    {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let collected = reader.flatten().collect::<Vec<_>>();
        assert_eq!(values.to_vec(), collected.first().unwrap().items);
    }

    // Mangle journal
    {
        let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
        Entry::Item {
            keyspace_id: 0,
            key: (*b"zzz").into(),
            value: (*b"").into(),
            value_type: ValueType::Tombstone,
            compression: lsm_tree::CompressionType::None,
        }
        .encode_into(&mut file)?;

        file.sync_all()?;
    }

    for _ in 0..10 {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let collected = reader.flatten().collect::<Vec<_>>();
        assert_eq!(values.to_vec(), collected.first().unwrap().items);
    }

    // Mangle journal
    for _ in 0..5 {
        let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
        Entry::Item {
            keyspace_id: 0,
            key: (*b"zzz").into(),
            value: (*b"").into(),
            value_type: ValueType::Tombstone,
            compression: lsm_tree::CompressionType::None,
        }
        .encode_into(&mut file)?;

        file.sync_all()?;
    }

    for _ in 0..10 {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let collected = reader.flatten().collect::<Vec<_>>();
        assert_eq!(values.to_vec(), collected.first().unwrap().items);
    }

    Ok(())
}
