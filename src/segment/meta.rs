use super::writer::Writer;
use crate::{
    file::SEGMENT_METADATA_FILE,
    time::unix_timestamp,
    value::{SeqNo, UserKey},
    version::Version,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CompressionType {
    Lz4,
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "lz4")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Metadata {
    pub version: Version,

    /// Path of segment folder
    pub path: PathBuf,

    /// Segment ID
    pub id: String,

    /// Creation time as unix timestamp (in µs)
    pub created_at: u128,

    /// Number of items in the segment
    ///
    /// This may include tombstones
    pub item_count: u64,

    /// Block size (uncompressed)
    pub block_size: u32,

    /// Number of written blocks
    pub block_count: u32,

    /// What type of compression is used
    pub compression: CompressionType,

    /// compressed size in bytes (on disk)
    pub file_size: u64,

    /// true size in bytes (if no compression were used)
    pub uncompressed_size: u64,

    /// Key range
    pub key_range: (UserKey, UserKey),

    /// Sequence number range
    pub seqnos: (SeqNo, SeqNo),

    /// Number of tombstones
    pub tombstone_count: u64,
}

impl Metadata {
    /// Consumes a writer and its metadata to create the segment metadata
    pub fn from_writer(id: String, writer: Writer) -> crate::Result<Self> {
        Ok(Self {
            id,
            version: Version::V0,
            path: writer.opts.path,
            block_count: writer.block_count as u32,
            block_size: writer.opts.block_size,

            // NOTE: Using seconds is not granular enough
            // But because millis already returns u128, might as well use micros :)
            created_at: unix_timestamp().as_micros(),

            file_size: writer.file_pos,
            compression: CompressionType::Lz4,
            item_count: writer.item_count as u64,
            key_range: (
                writer
                    .first_key
                    .expect("should have written at least 1 item"),
                writer
                    .last_key
                    .expect("should have written at least 1 item"),
            ),
            seqnos: (writer.lowest_seqno, writer.highest_seqno),
            tombstone_count: writer.tombstone_count as u64,
            uncompressed_size: writer.uncompressed_size,
        })
    }

    pub(crate) fn key_range_contains<K: AsRef<[u8]>>(&self, key: K) -> bool {
        let key = key.as_ref();
        key >= &self.key_range.0 && key <= &self.key_range.1
    }

    /// Stores segment metadata in a file
    ///
    /// Will be stored as JSON
    pub fn write_to_file(&self) -> std::io::Result<()> {
        let mut writer = OpenOptions::new()
            .truncate(true)
            .create(true)
            .write(true)
            .open(self.path.join(SEGMENT_METADATA_FILE))?;

        writer.write_all(
            serde_json::to_string_pretty(self)
                .expect("Failed to serialize to JSON")
                .as_bytes(),
        )?;
        writer.flush()?;
        writer.sync_all()?;

        // fsync folder
        let folder = std::fs::File::open(&self.path)?;
        folder.sync_all()?;

        Ok(())
    }

    /// Reads and parses a Segment metadata file
    pub fn from_disk<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file_content = std::fs::read_to_string(path)?;
        let item = serde_json::from_str(&file_content)?;
        Ok(item)
    }
}
