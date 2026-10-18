use super::*;
use crate::batch::item::Item as BatchItem;
use crate::file::MAGIC_BYTES;
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

    let journal_recovered =
        Journal::recover(dir2, CompressionType::None, 0, RecoveryMode::default())?;
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

    let journal_recovered =
        Journal::recover(dir2, CompressionType::None, 0, RecoveryMode::default())?;
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

    let journal_recovered =
        Journal::recover(dir2, CompressionType::None, 0, RecoveryMode::default())?;
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
            .get_writer()?
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
            .get_writer()?
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
            .get_writer()?
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

#[test]
#[expect(clippy::unwrap_used, clippy::redundant_clone)]
fn journal_mid_journal_corruption_recovery_modes() -> crate::Result<()> {
    let keyspace_dir = tempdir()?;
    let db = crate::Database::builder(&keyspace_dir).open()?;
    let keyspace = db.keyspace("default", Default::default)?;

    let items = [BatchItem::new(
        keyspace.clone(),
        *b"abc",
        *b"def",
        ValueType::Value,
    )];

    // One intact batch, then garbage where a batch header belongs, then another
    // intact batch: the corruption is not at the tail, so recovering quietly would
    // drop the second batch.
    let build = |dir: &tempfile::TempDir| -> crate::Result<(std::path::PathBuf, u64)> {
        let path = dir.path().join("0.jnl");

        {
            let journal = Journal::create_new(&path)?;
            journal
                .get_writer()?
                .write_batch(items.iter(), items.len(), 0)?;
        }

        let intact_len = std::fs::metadata(&path)?.len();

        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
            file.write_all(b"09pmu35w3a9mp53bao9upw3ab5up")?;
            // the magic is only 4 bytes, so an unframed one can sit inside the corrupt
            // region without being the trailer of the batch that follows it
            file.write_all(MAGIC_BYTES)?;
            file.sync_all()?;
        }

        {
            let journal = Journal::from_file(&path)?;
            journal
                .get_writer()?
                .write_batch(items.iter(), items.len(), 1)?;
        }

        Ok((path, intact_len))
    };

    // Strict: a completed batch follows the unreadable region, so it is reported and
    // the file is left untouched
    {
        let dir = tempdir()?;
        let (path, _) = build(&dir)?;
        let len_before = std::fs::metadata(&path)?.len();

        let journal = Journal::from_file(&path)?.with_recovery_mode(RecoveryMode::Strict);
        let collected = journal.get_reader()?.collect::<Vec<_>>();

        assert_eq!(2, collected.len());
        assert!(matches!(
            collected.last(),
            Some(Err(crate::Error::JournalRecovery(
                crate::JournalRecoveryError::MidJournalCorruption
            )))
        ));
        assert_eq!(
            len_before,
            std::fs::metadata(&path)?.len(),
            "strict recovery must not truncate the journal"
        );
    }

    // Default: the same file is truncated down to the last intact batch, as before
    {
        let dir = tempdir()?;
        let (path, len_before) = build(&dir)?;

        let journal = Journal::from_file(&path)?;
        let collected = journal.get_reader()?.collect::<Vec<_>>();

        assert_eq!(1, collected.len());
        assert!(collected.first().unwrap().is_ok());
        assert!(
            std::fs::metadata(&path)?.len() < len_before,
            "default recovery truncates the journal to the last intact batch"
        );
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used, clippy::redundant_clone)]
fn journal_strict_recovery_tolerates_a_corrupt_tail() -> crate::Result<()> {
    let keyspace_dir = tempdir()?;
    let db = crate::Database::builder(&keyspace_dir).open()?;
    let keyspace = db.keyspace("default", Default::default)?;

    let dir = tempdir()?;
    let path = dir.path().join("0.jnl");

    let items = [BatchItem::new(
        keyspace.clone(),
        *b"abc",
        *b"def",
        ValueType::Value,
    )];

    {
        let journal = Journal::create_new(&path)?;
        journal
            .get_writer()?
            .write_batch(items.iter(), items.len(), 0)?;
    }

    // Nothing after the garbage: a torn tail, which strict mode still discards
    {
        let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
        file.write_all(b"09pmu35w3a9mp53bao9upw3ab5up")?;
        file.sync_all()?;
    }

    let journal = Journal::from_file(&path)?.with_recovery_mode(RecoveryMode::Strict);
    let collected = journal.get_reader()?.collect::<crate::Result<Vec<_>>>()?;

    assert_eq!(1, collected.len());

    Ok(())
}

/// The trailer magic can occur inside a corrupt key or value, so only a magic that is
/// preceded by `Tag::End` is a completed batch trailer.
#[test]
#[expect(clippy::unwrap_used, clippy::redundant_clone)]
fn journal_strict_recovery_rejects_unframed_magic_in_a_torn_tail() -> crate::Result<()> {
    let keyspace_dir = tempdir()?;
    let db = crate::Database::builder(&keyspace_dir).open()?;
    let keyspace = db.keyspace("default", Default::default)?;

    let items = [BatchItem::new(keyspace, *b"abc", *b"def", ValueType::Value)];

    // 0xf0 is not a journal tag, so decoding fails right there; nine bytes in front of the
    // magic is where a trailer would carry its `Tag::End`
    let torn_tail = |tag_before_magic: u8| {
        let mut bytes = vec![0xf0, tag_before_magic];
        bytes.extend_from_slice(&[0u8; 8]);
        bytes.extend_from_slice(MAGIC_BYTES);
        bytes
    };

    for tag_before_magic in [0x00, u8::from(entry::Tag::Item)] {
        let dir = tempdir()?;
        let path = dir.path().join("0.jnl");

        {
            let journal = Journal::create_new(&path)?;
            journal
                .get_writer()?
                .write_batch(items.iter(), items.len(), 0)?;
        }

        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
            file.write_all(&torn_tail(tag_before_magic))?;
            file.sync_all()?;
        }

        let len_before = std::fs::metadata(&path)?.len();

        let journal = Journal::from_file(&path)?.with_recovery_mode(RecoveryMode::Strict);
        let collected = journal.get_reader()?.collect::<crate::Result<Vec<_>>>()?;

        assert_eq!(
            1,
            collected.len(),
            "an unframed magic in a torn tail is not a completed batch"
        );
        assert!(
            std::fs::metadata(&path)?.len() < len_before,
            "strict recovery truncates a torn tail like the default mode"
        );
    }

    Ok(())
}

/// A trailer that a chunk boundary splits apart is still seen.
#[test]
#[expect(clippy::unwrap_used, clippy::redundant_clone)]
fn journal_strict_recovery_finds_a_trailer_across_a_scan_chunk_boundary() -> crate::Result<()> {
    let keyspace_dir = tempdir()?;
    let db = crate::Database::builder(&keyspace_dir).open()?;
    let keyspace = db.keyspace("default", Default::default)?;

    let dir = tempdir()?;
    let path = dir.path().join("0.jnl");

    let items = [BatchItem::new(keyspace, *b"abc", *b"def", ValueType::Value)];

    {
        let journal = Journal::create_new(&path)?;
        journal
            .get_writer()?
            .write_batch(items.iter(), items.len(), 0)?;
    }

    // Place the trailer so that its last two bytes fall into the next scan chunk
    {
        let trailer = Entry::End(1234).encode_into_vec();
        let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
        file.write_all(&vec![0xf0; reader::SCAN_CHUNK_LEN - trailer.len() + 2])?;
        file.write_all(&trailer)?;
        file.sync_all()?;
    }

    let journal = Journal::from_file(&path)?.with_recovery_mode(RecoveryMode::Strict);
    let collected = journal.get_reader()?.collect::<Vec<_>>();

    assert_eq!(2, collected.len());
    assert!(matches!(
        collected.last(),
        Some(Err(crate::Error::JournalRecovery(
            crate::JournalRecoveryError::MidJournalCorruption
        )))
    ));

    Ok(())
}
