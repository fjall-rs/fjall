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

fn assert_batch_items(
    expected: &[BatchItem],
    mut collected: Vec<batch_reader::Batch>,
) -> crate::Result<()> {
    let batch = collected.remove(0);
    let items = batch.items.collect::<crate::Result<Vec<_>>>()?;
    assert_eq!(expected.to_vec(), items);
    Ok(())
}

#[test]
fn large_journal_batch_streams_after_checksum_verification() -> crate::Result<()> {
    let dir = tempdir()?;
    let db = crate::Database::builder(dir.path().join("db"))
        .worker_threads_unchecked(0)
        .open()?;
    let keyspace = db.keyspace("spilled", Default::default)?;
    let path = dir.path().join("batch.jnl");
    let values: Vec<_> = (0_u64..9_000)
        .map(|i| {
            BatchItem::new(
                keyspace.clone(),
                i.to_be_bytes(),
                vec![b's'; 1024],
                ValueType::Value,
            )
        })
        .collect();
    let journal = Journal::create_new(&path)?;
    #[cfg(feature = "lz4")]
    journal
        .get_writer()?
        .set_compression(lsm_tree::CompressionType::Lz4, 1);
    journal
        .get_writer()?
        .write_batch(values.iter(), values.len(), 7)?;
    journal.get_writer()?.persist(PersistMode::Buffer)?;

    let batch = journal
        .get_reader()?
        .next()
        .ok_or(crate::Error::Unrecoverable)??;
    assert!(batch.items.is_streaming());
    let recovered = batch.items.collect::<crate::Result<Vec<_>>>()?;
    assert_eq!(recovered.len(), values.len());
    assert_eq!(
        recovered.first().unwrap().key.as_ref(),
        values.first().unwrap().key.as_ref()
    );
    assert_eq!(
        recovered.last().unwrap().key.as_ref(),
        values.last().unwrap().key.as_ref()
    );
    Ok(())
}

#[test]
fn streaming_batch_rejects_bad_checksum() -> crate::Result<()> {
    use std::io::{Seek, SeekFrom};
    let dir = tempdir()?;
    let db = crate::Database::builder(dir.path().join("db"))
        .worker_threads_unchecked(0)
        .open()?;
    let keyspace = db.keyspace("corrupt", Default::default)?;
    let path = dir.path().join("batch.jnl");
    let values: Vec<_> = (0_u64..9_000)
        .map(|i| {
            BatchItem::new(
                keyspace.clone(),
                i.to_be_bytes(),
                vec![b'v'; 1024],
                ValueType::Value,
            )
        })
        .collect();
    let journal = Journal::create_new(&path)?;
    journal
        .get_writer()?
        .write_batch(values.iter(), values.len(), 1)?;
    journal.get_writer()?.persist(PersistMode::Buffer)?;

    let mut file = std::fs::OpenOptions::new().write(true).open(&path)?;
    file.seek(SeekFrom::Start(40))?;
    file.write_all(b"X")?;
    assert!(matches!(
        journal.get_reader()?.next(),
        Some(Err(crate::Error::JournalRecovery(
            crate::JournalRecoveryError::ChecksumMismatch
        )))
    ));
    Ok(())
}

#[test]
fn tiny_items_exceed_item_count_budget() -> crate::Result<()> {
    let dir = tempdir()?;
    let db = crate::Database::builder(dir.path().join("db"))
        .worker_threads_unchecked(0)
        .open()?;
    let keyspace = db.keyspace("tiny", Default::default)?;
    let path = dir.path().join("batch.jnl");
    let values: Vec<_> = (0_u64..100_000)
        .map(|i| BatchItem::new(keyspace.clone(), i.to_be_bytes(), [], ValueType::Value))
        .collect();
    let journal = Journal::create_new(&path)?;
    journal
        .get_writer()?
        .write_batch(values.iter(), values.len(), 7)?;
    journal.get_writer()?.persist(PersistMode::Buffer)?;

    let batch = journal
        .get_reader()?
        .next()
        .ok_or(crate::Error::Unrecoverable)??;
    assert!(batch.items.is_streaming());
    let mut items = batch.items;
    assert_eq!(
        items
            .next()
            .ok_or(crate::Error::Unrecoverable)??
            .key
            .as_ref(),
        0_u64.to_be_bytes()
    );
    let remaining = items.try_fold(0_usize, |count, item| {
        item?;
        Ok::<_, crate::Error>(count + 1)
    })?;
    assert_eq!(remaining, values.len() - 1);
    Ok(())
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
        let mut writer = journal.get_writer()?;

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
        let mut writer = journal.get_writer()?;

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
    assert_eq!(journal_recovered.active.path()?, next_next_path);
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
        let mut writer = journal.get_writer()?;

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
    assert_eq!(journal_recovered.active.path()?, next_next_path);
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
            let mut writer = journal.get_writer()?;

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
    assert_eq!(journal_recovered.active.path()?, path);
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
            .get_writer()?
            .write_batch(values.iter(), values.len(), 0)?;
    }

    {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let collected = reader.flatten().collect::<Vec<_>>();
        assert_batch_items(&values, collected)?;
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
        assert_batch_items(&values, collected)?;
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
        assert_batch_items(&values, collected)?;
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
            .get_writer()?
            .write_batch(values.iter(), values.len(), 0)?;
    }

    {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let collected = reader.flatten().collect::<Vec<_>>();
        assert_batch_items(&values, collected)?;
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
        assert_batch_items(&values, collected)?;
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
        assert_batch_items(&values, collected)?;
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
            .get_writer()?
            .write_batch(values.iter(), values.len(), 0)?;
    }

    {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let collected = reader.flatten().collect::<Vec<_>>();
        assert_batch_items(&values, collected)?;
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
        assert_batch_items(&values, collected)?;
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
        assert_batch_items(&values, collected)?;
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
            .get_writer()?
            .write_batch(values.iter(), values.len(), 0)?;
    }

    {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let collected = reader.flatten().collect::<Vec<_>>();
        assert_batch_items(&values, collected)?;
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
        assert_batch_items(&values, collected)?;
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
        assert_batch_items(&values, collected)?;
    }

    Ok(())
}
