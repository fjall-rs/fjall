use serde::{Deserialize, Serialize};
use std::{
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

use crate::value::SeqNo;

use super::writer::Writer;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Metadata {
    pub path: PathBuf,
    pub id: String,
    pub created_at: u128,
    pub item_count: u64,
    pub block_size: u32,
    pub block_count: u32,

    pub is_compressed: bool,

    /// compressed size in bytes (on disk)
    pub file_size: u64,

    /// true size in bytes (if no compression were used)
    pub uncompressed_size: u64,

    pub key_range: (Vec<u8>, Vec<u8>),
    pub seqnos: (SeqNo, SeqNo),

    pub tombstone_count: u64,
}

impl Metadata {
    /// Consumes a writer and its metadata to create the segment metadata
    pub fn from_writer(id: String, writer: Writer) -> Self {
        Self {
            id,
            path: writer.opts.path,
            block_count: writer.block_count as u32,
            block_size: writer.opts.block_size,
            created_at: 0, /* TODO: */
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
    pub fn write<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()> {
        // TODO: atomic rewrite

        let mut writer = OpenOptions::new()
            .truncate(true)
            .create(true)
            .write(true)
            .open(path.as_ref().join("meta.json"))?;

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
