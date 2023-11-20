use serde::{Deserialize, Serialize};
use std::{
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

use crate::{time::unix_timestamp, value::SeqNo};

use super::writer::Writer;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Metadata {
    /// Path of segment folder, relative to tree base path
    pub path: PathBuf,

    /// Segment ID
    pub id: String,

    /// Creation time as unix timestamp
    pub created_at: u64,

    /// Number of items in the segment
    ///
    /// This may include tombstones
    pub item_count: u64,

    /// Block size (uncompressed)
    pub block_size: u32,

    /// Number of written blocks
    pub block_count: u32,

    /// Whether LZ4 is used
    ///
    /// Is always true
    pub is_compressed: bool,

    /// compressed size in bytes (on disk)
    pub file_size: u64,

    /// true size in bytes (if no compression were used)
    pub uncompressed_size: u64,

    /// Key range
    pub key_range: (Vec<u8>, Vec<u8>),

    /// Sequence number range
    pub seqnos: (SeqNo, SeqNo),

    /// Number of tombstones
    pub tombstone_count: u64,
}

impl Metadata {
    /// Consumes a writer and its metadata to create the segment metadata
    pub fn from_writer<P: Into<PathBuf>>(id: String, writer: Writer, path: P) -> Self {
        Self {
            id,
            path: path.into(),
            block_count: writer.block_count as u32,
            block_size: writer.opts.block_size,
            created_at: unix_timestamp().as_secs(),
            file_size: writer.file_pos,
            is_compressed: true,
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
        }
    }

    pub(crate) fn key_range_contains<K: AsRef<[u8]>>(&self, key: K) -> bool {
        let key = key.as_ref();
        key >= &self.key_range.0 && key <= &self.key_range.1
    }

    /// Stores segment metadata in a file
    ///
    /// Will be stored as JSON
    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()> {
        log::debug!("Writing segment meta file to {}", path.as_ref().display());

        let mut writer = OpenOptions::new()
            .truncate(true)
            .create(true)
            .write(true)
            .open(&path)?;

        writer.write_all(
            serde_json::to_string_pretty(self)
                .expect("Failed to serialize to JSON")
                .as_bytes(),
        )?;
        writer.sync_all()?;

        Ok(())
    }

    /// Reads and parses a Segment metadata file
    pub fn from_disk<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file_content = std::fs::read_to_string(path)?;
        let item = serde_json::from_str(&file_content)?;
        Ok(item)
    }
}
