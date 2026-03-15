use super::*;
use crate::batch::item::Item as BatchItem;
use crate::file::MAGIC_BYTES;
use entry::Entry;
use lsm_tree::ValueType;
use std::io::{Read, Seek, SeekFrom, Write};
use tempfile::tempdir;
use test_log::test;

#[test]
#[expect(clippy::unwrap_used, clippy::redundant_clone)]
fn journal_single_item_write_and_recovery() -> crate::Result<()> {
    let dir1 = tempdir()?;
    let db = crate::Database::builder(&dir1).open()?;
    let keyspace = db.keyspace("default", Default::default)?;

    let dir2 = tempdir()?;
    let path = dir2.path().join("0.jnl");

    {
        let journal = Journal::create_new(&path)?;
        let mut writer = journal.get_writer();

        // Write single items via write_raw (uses compact SingleItem format)
        writer.write_raw(keyspace.id, b"key1", b"val1", ValueType::Value, 0)?;
        writer.write_raw(keyspace.id, b"key2", b"val2", ValueType::Value, 1)?;
        writer.write_raw(keyspace.id, b"key3", b"", ValueType::Tombstone, 2)?;
    }

    // Recover and verify
    let journal = Journal::from_file(&path)?;
    let reader = journal.get_reader()?;
    let batches: Vec<_> = reader.flatten().collect();

    assert_eq!(batches.len(), 3);

    assert_eq!(batches[0].seqno, 0);
    assert_eq!(batches[0].items.len(), 1);
    assert_eq!(batches[0].items[0].key.as_ref(), b"key1");
    assert_eq!(batches[0].items[0].value.as_ref(), b"val1");

    assert_eq!(batches[1].seqno, 1);
    assert_eq!(batches[1].items[0].key.as_ref(), b"key2");

    assert_eq!(batches[2].seqno, 2);
    assert_eq!(batches[2].items[0].key.as_ref(), b"key3");
    assert_eq!(batches[2].items[0].value_type, ValueType::Tombstone);

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, clippy::redundant_clone)]
fn journal_mixed_single_and_batch() -> crate::Result<()> {
    let dir1 = tempdir()?;
    let db = crate::Database::builder(&dir1).open()?;
    let keyspace = db.keyspace("default", Default::default)?;

    let dir2 = tempdir()?;
    let path = dir2.path().join("0.jnl");

    {
        let journal = Journal::create_new(&path)?;
        let mut writer = journal.get_writer();

        // Single item (compact format)
        writer.write_raw(keyspace.id, b"single1", b"v1", ValueType::Value, 0)?;

        // Multi-item batch (standard format)
        let batch_items = [
            BatchItem::new(keyspace.clone(), *b"batch_a", *b"ba", ValueType::Value),
            BatchItem::new(keyspace.clone(), *b"batch_b", *b"bb", ValueType::Value),
        ];
        writer.write_batch(batch_items.iter(), 2, 1)?;

        // Another single item (compact format)
        writer.write_raw(keyspace.id, b"single2", b"v2", ValueType::Value, 2)?;
    }

    // Recover and verify all 3 batches
    let journal = Journal::from_file(&path)?;
    let reader = journal.get_reader()?;
    let batches: Vec<_> = reader.flatten().collect();

    assert_eq!(batches.len(), 3);

    // First: single item
    assert_eq!(batches[0].seqno, 0);
    assert_eq!(batches[0].items.len(), 1);
    assert_eq!(batches[0].items[0].key.as_ref(), b"single1");

    // Second: multi-item batch
    assert_eq!(batches[1].seqno, 1);
    assert_eq!(batches[1].items.len(), 2);
    assert_eq!(batches[1].items[0].key.as_ref(), b"batch_a");
    assert_eq!(batches[1].items[1].key.as_ref(), b"batch_b");

    // Third: single item
    assert_eq!(batches[2].seqno, 2);
    assert_eq!(batches[2].items.len(), 1);
    assert_eq!(batches[2].items[0].key.as_ref(), b"single2");

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, clippy::redundant_clone)]
fn journal_truncation_corrupt_single_item() -> crate::Result<()> {
    let dir1 = tempdir()?;
    let db = crate::Database::builder(&dir1).open()?;
    let keyspace = db.keyspace("default", Default::default)?;

    let dir2 = tempdir()?;
    let path = dir2.path().join("0.jnl");

    {
        let journal = Journal::create_new(&path)?;
        let mut writer = journal.get_writer();

        writer.write_raw(keyspace.id, b"good", b"data", ValueType::Value, 0)?;
    }

    // Verify initial recovery
    {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let batches: Vec<_> = reader.flatten().collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].items[0].key.as_ref(), b"good");
    }

    // Append corrupt bytes after valid single item
    {
        let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
        file.write_all(b"corrupt_garbage_data_here")?;
        file.sync_all()?;
    }

    // Recovery should still yield the valid single item
    for _ in 0..10 {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let batches: Vec<_> = reader.flatten().collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].items[0].key.as_ref(), b"good");
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, clippy::redundant_clone)]
fn journal_single_item_checksum_mismatch() -> crate::Result<()> {
    let dir1 = tempdir()?;
    let db = crate::Database::builder(&dir1).open()?;
    let keyspace = db.keyspace("default", Default::default)?;

    let dir2 = tempdir()?;
    let path = dir2.path().join("0.jnl");

    {
        let journal = Journal::create_new(&path)?;
        let mut writer = journal.get_writer();

        writer.write_raw(keyspace.id, b"good", b"data", ValueType::Value, 0)?;
    }

    // Verify initial recovery succeeds
    {
        let journal = Journal::from_file(&path)?;
        let reader = journal.get_reader()?;
        let batches: Vec<_> = reader.flatten().collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].items[0].key.as_ref(), b"good");
    }

    // Corrupt a byte in the payload while keeping the trailer intact
    {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        // Find actual entry end by locating MAGIC_BYTES trailer.
        // The file is pre-allocated to 64MB, so buf.len() != entry size.
        // Entry layout: tag(1) + seqno(8) + payload(...) + checksum(8) + magic(4)
        let entry_end = buf
            .windows(MAGIC_BYTES.len())
            .position(|w| w == MAGIC_BYTES)
            .expect("should find magic bytes")
            + MAGIC_BYTES.len();

        let header_len: usize = 1 + 8; // tag + seqno
        let trailer_len: usize = 8 + 4; // checksum + magic
        let payload_start = header_len;
        let payload_end = entry_end - trailer_len;

        assert!(
            payload_end > payload_start,
            "entry too small to have a payload"
        );

        // Corrupt the last byte of the value data (avoids hitting length
        // fields which would trigger debug_assert before checksum check).
        let flip_index = payload_end - 1;
        buf[flip_index] ^= 0xFF;

        file.seek(SeekFrom::Start(0))?;
        file.write_all(&buf)?;
        file.sync_all()?;
    }

    // Recovery should fail due to checksum mismatch rather than silently truncating
    let journal = Journal::from_file(&path)?;
    let reader = journal.get_reader()?;
    let batches: Vec<_> = reader.collect();

    // The batch reader should surface the specific checksum error
    let has_checksum_mismatch = batches.iter().any(|b| {
        matches!(
            b,
            Err(crate::Error::JournalRecovery(
                crate::JournalRecoveryError::ChecksumMismatch
            ))
        )
    });

    assert!(
        has_checksum_mismatch,
        "expected ChecksumMismatch error, but got: {batches:?}",
    );

    Ok(())
}

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
