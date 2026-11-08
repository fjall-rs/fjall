// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    db::Keyspaces,
    keyspace::{InternalKeyspaceId, KeyspaceKey},
    Database, HashMap,
};
use byteorder::ReadBytesExt;
use lsm_tree::{coding::Decode, CompressionType, SeqNo, UserKey, UserValue, ValueType};
use std::io::Read;
use varint_rs::VarintReader;

#[derive(Debug)]
struct DecodedItem {
    keyspace_id: InternalKeyspaceId,
    key: UserKey,
    value: UserValue,
    value_type: ValueType,
    seqno: SeqNo,
}

struct BatchIterator<'a, R: Read> {
    reader: &'a mut R,
    remaining: u64,
    batch_seqno: SeqNo,
}

impl<R: Read> Iterator for BatchIterator<'_, R> {
    type Item = crate::Result<DecodedItem>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        self.remaining -= 1;
        Some(decode_item(self.reader, self.batch_seqno))
    }
}

enum DecodedRecord<'a, R: Read> {
    Single(DecodedItem),
    Full(BatchIterator<'a, R>),
}

fn decode_item<R: Read>(reader: &mut R, batch_seqno: SeqNo) -> crate::Result<DecodedItem> {
    let keyspace_id = reader.read_u64_varint()?;

    let key_len = usize::from(reader.read_u16_varint()?);
    let key = UserKey::from_reader(reader, key_len)?;

    let value_len = reader.read_u32_varint()? as usize;

    let compression = CompressionType::decode_from(reader)?;

    let value = match compression {
        CompressionType::None => UserValue::from_reader(reader, value_len)?,

        #[cfg(feature = "lz4")]
        CompressionType::Lz4 => {
            let compressed_value_len = reader.read_u32_varint()? as usize;

            let compressed_value = UserValue::from_reader(reader, compressed_value_len)?;

            #[expect(unsafe_code)]
            let mut uncompressed_value = unsafe { UserValue::builder_unzeroed(value_len) };

            let uncompressed_len =
                lz4_flex::decompress_into(&compressed_value, &mut uncompressed_value).map_err(
                    |e| {
                        log::error!("LZ4 decompression failed: {e}");
                        crate::Error::Decompress(CompressionType::Lz4)
                    },
                )?;

            if uncompressed_len != uncompressed_value.len() {
                log::error!("Decompressed size does not match expected value size");
                return Err(crate::Error::Decompress(CompressionType::Lz4));
            }

            UserValue::from(uncompressed_value.freeze())
        }
    };

    let value_type = reader.read_u8()?;
    let value_type = value_type
        .try_into()
        .map_err(|()| crate::Error::InvalidTag(("ValueType", value_type)))?;

    Ok(DecodedItem {
        keyspace_id,
        key,
        value,
        value_type,
        seqno: batch_seqno,
    })
}

fn decode_wal_record<R: Read>(reader: &mut R) -> crate::Result<DecodedRecord<'_, R>> {
    let record_type = match reader.read_u8()? {
        value if value == super::RecordType::Single as u8 => super::RecordType::Single,
        value if value == super::RecordType::Full as u8 => super::RecordType::Full,
        value => return Err(crate::Error::InvalidTag(("WalRecordType", value))),
    };

    let batch_seqno = reader.read_u64_varint()?;

    match record_type {
        super::RecordType::Single => Ok(DecodedRecord::Single(decode_item(reader, batch_seqno)?)),
        super::RecordType::Full => {
            let remaining = reader.read_u64_varint()?;

            Ok(DecodedRecord::Full(BatchIterator {
                reader,
                remaining,
                batch_seqno,
            }))
        }
    }
}

pub type Watermarks = HashMap<hwal::FragmentId, HashMap<InternalKeyspaceId, SeqNo>>;

struct WalRecovery<'a, F> {
    keyspaces: &'a Keyspaces,
    get_keyspace_name: F,
    watermarks: Watermarks,

    /// Flush watermark of each keyspace, as recovered from the manifest.
    ///
    /// Every WAL item up to that seqno is already contained in the keyspace's
    /// tables (flush), or was removed on purpose (compaction, drop range, clear),
    /// so it must not be replayed.
    ///
    /// NOTE: This is not the version seqno, which is used for snapshots.
    ///
    /// See <https://github.com/fjall-rs/fjall/pull/332>
    flush_watermarks: HashMap<InternalKeyspaceId, Option<SeqNo>>,

    replayed_bytes: u64,
}

impl<'a, F> WalRecovery<'a, F>
where
    F: Fn(InternalKeyspaceId) -> Option<KeyspaceKey>,
{
    fn new(keyspaces: &'a Keyspaces, get_keyspace_name: F) -> Self {
        use crate::AbstractTree;

        let flush_watermarks = keyspaces
            .values()
            .map(|ks| (ks.id(), ks.tree.get_flush_watermark()))
            .collect();

        Self {
            keyspaces,
            get_keyspace_name,
            watermarks: HashMap::default(),
            flush_watermarks,
            replayed_bytes: 0,
        }
    }

    fn replay_item(&mut self, fragment_id: hwal::FragmentId, item: DecodedItem) {
        use crate::AbstractTree;

        if let Some(Some(flush_watermark)) = self.flush_watermarks.get(&item.keyspace_id) {
            if item.seqno <= *flush_watermark {
                return;
            }
        }

        // NOTE: Items of deleted keyspaces are skipped
        let Some(keyspace_name) = (self.get_keyspace_name)(item.keyspace_id) else {
            return;
        };

        if let Some(keyspace) = self.keyspaces.get(&keyspace_name) {
            let (item_size, _) = match item.value_type {
                ValueType::Value => keyspace.tree.insert(item.key, item.value, item.seqno),
                ValueType::Tombstone => keyspace.tree.remove(item.key, item.seqno),
                ValueType::WeakTombstone => keyspace.tree.remove_weak(item.key, item.seqno),
                ValueType::Indirection => unreachable!(),
            };

            self.replayed_bytes += item_size;

            self.watermarks
                .entry(fragment_id)
                .or_default()
                .entry(item.keyspace_id)
                .and_modify(|prev| *prev = (*prev).max(item.seqno))
                .or_insert(item.seqno);
        }
    }

    fn into_watermarks(self) -> HashMap<hwal::FragmentId, HashMap<InternalKeyspaceId, SeqNo>> {
        self.watermarks
    }
}

/// Recovers the WAL and replays its records into the available keyspaces.
pub fn recover_wal(
    db: &Database,
    keyspaces: &Keyspaces,
    get_keyspace_name: impl Fn(InternalKeyspaceId) -> Option<KeyspaceKey>,
) -> crate::Result<SeqNo> {
    log::info!("Recovering WAL at {}", db.path().display());

    let mut ledger = db.supervisor.ledger()?;

    let wal = &mut ledger.wal;

    let mut recovery = wal.inner.recover()?;
    let mut find_a_name = WalRecovery::new(keyspaces, get_keyspace_name);

    let mut highest_seqno = SeqNo::default();

    for record in recovery.by_ref() {
        let record = record?;

        let wal_fragment_id = record.fragment_id();
        let payload = record.payload();

        match decode_wal_record(&mut &payload[..])? {
            DecodedRecord::Single(item) => {
                highest_seqno = highest_seqno.max(item.seqno);
                find_a_name.replay_item(wal_fragment_id, item);
            }
            DecodedRecord::Full(batch) => {
                highest_seqno = highest_seqno.max(batch.batch_seqno);

                for item in batch {
                    find_a_name.replay_item(wal_fragment_id, item?);
                }
            }
        }
    }

    // NOTE: Replayed items now sit in memtables, so they count towards the write buffer
    db.supervisor
        .write_buffer_size
        .allocate(find_a_name.replayed_bytes);

    let watermarks = find_a_name.into_watermarks();
    log::trace!("WAL watermarks: {watermarks:?}");

    // IMPORTANT: Finish recovery
    recovery.finish()?;

    // NOTE: The replayed items are in memtables now, so sealed fragments are kept until their
    // keyspaces are flushed; fragments that only contain flushed items are pruned right away
    wal.set_recovered_watermarks(watermarks);
    wal.prune(keyspaces)?;

    Ok(highest_seqno)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::encoder::Encoder;
    use test_log::test;

    #[test]
    fn wal_kv_coding_roundtrip() -> crate::Result<()> {
        let mut encoder = Encoder::default();
        encoder.encode_kv(0, b"hello", b"world", ValueType::Value)?;
        let encoded_bytes = encoder.to_vec();

        let decoded = decode_item(&mut encoded_bytes.as_slice(), 5)?;
        assert_eq!(b"hello", &*decoded.key);
        assert_eq!(b"world", &*decoded.value);
        assert_eq!(5, decoded.seqno);
        assert_eq!(0, decoded.keyspace_id);
        assert_eq!(ValueType::Value, decoded.value_type);

        Ok(())
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn wal_kv_coding_roundtrip_compressed_lz4() -> crate::Result<()> {
        use lsm_tree::CompressionType::Lz4;

        let mut encoder = Encoder::default().with_compression(Lz4, 0);
        encoder.encode_kv(0, b"hello", b"world", ValueType::Value)?;
        let encoded_bytes = encoder.to_vec();

        let decoded = decode_item(&mut encoded_bytes.as_slice(), 5)?;
        assert_eq!(b"hello", &*decoded.key);
        assert_eq!(b"world", &*decoded.value);
        assert_eq!(5, decoded.seqno);
        assert_eq!(0, decoded.keyspace_id);
        assert_eq!(ValueType::Value, decoded.value_type);

        Ok(())
    }
}
