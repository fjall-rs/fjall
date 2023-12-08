mod level;

#[cfg(feature = "segment_history")]
mod segment_history;

#[cfg(feature = "segment_history")]
use crate::time::unix_timestamp;
#[cfg(feature = "segment_history")]
use serde_json::json;

use self::level::{Level, ResolvedLevel};
use crate::{file::rewrite_atomic, segment::Segment};
use std::{
    collections::{HashMap, HashSet},
    fs::{self},
    path::{Path, PathBuf},
    sync::Arc,
};

pub type HiddenSet = HashSet<String>;
pub type ResolvedView = Vec<ResolvedLevel>;

/// Represents the levels of a log-structured merge tree.
pub struct Levels {
    path: PathBuf,

    /// Amount of levels of the LSM tree
    ///
    /// RocksDB has 7 by default
    level_count: u8,

    segments: HashMap<String, Arc<Segment>>,
    levels: Vec<Level>,

    //writer: BufWriter<File>,
    /// Set of segment IDs that are masked
    ///
    /// While consuming segments (because of compaction) they will not appear in the list of segments
    /// as to not cause conflicts between multiple compaction threads (compacting the same segments)
    hidden_set: HiddenSet,

    #[cfg(feature = "segment_history")]
    segment_history_writer: segment_history::Writer,
}

impl Levels {
    pub(crate) fn contains_id(&self, id: &str) -> bool {
        self.levels.iter().any(|lvl| lvl.contains_id(id))
    }

    pub(crate) fn list_ids(&self) -> Vec<String> {
        let items = self.levels.iter().map(|f| &**f).cloned();
        items.flatten().collect()
    }

    pub(crate) fn is_compacting(&self) -> bool {
        !self.hidden_set.is_empty()
    }

    pub(crate) fn create_new<P: AsRef<Path>>(level_count: u8, path: P) -> crate::Result<Self> {
        assert!(level_count > 1, "level_count should be > 1");

        let levels = (0..level_count)
            .map(|_| Level::default())
            .collect::<Vec<_>>();

        let mut levels = Self {
            path: path.as_ref().to_path_buf(),
            segments: HashMap::new(),
            level_count,
            levels,
            hidden_set: HashSet::new(),

            #[cfg(feature = "segment_history")]
            segment_history_writer: segment_history::Writer::new()?,
        };
        levels.write_to_disk()?;

        #[cfg(feature = "segment_history")]
        levels.write_segment_history_entry("create_new")?;

        Ok(levels)
    }

    #[cfg(feature = "segment_history")]
    fn write_segment_history_entry(&mut self, event: &str) -> crate::Result<()> {
        let segment_map = self.get_all_segments();
        let ts = unix_timestamp();

        let line = serde_json::to_string(&json!({
            "time_unix": ts.as_secs(),
            "time_ms": ts.as_millis(),
            "event": event,
            "levels": self.levels.iter().map(|level| {
                let segments = level.iter().map(|seg_id| segment_map[seg_id].clone()).collect::<Vec<_>>();

                segments
                .iter()
                .map(|segment| json!({
                        "path": segment.metadata.path.clone(),
                        "metadata": segment.metadata.clone(),
                        "hidden": self.hidden_set.contains(&segment.metadata.id)
                    }))
                    .collect::<Vec<_>>()
            }).collect::<Vec<_>>()
        }))
        .expect("Segment history write failed");

        self.segment_history_writer.write(&line)
    }

    pub(crate) fn recover<P: AsRef<Path>>(
        path: P,
        segments: HashMap<String, Arc<Segment>>,
    ) -> crate::Result<Self> {
        let level_manifest = fs::read_to_string(&path)?;
        let levels: Vec<_> = serde_json::from_str(&level_manifest).expect("deserialize error");

        // NOTE: There are never that many levels
        // so it's fine to just truncate it
        #[allow(clippy::cast_possible_truncation)]
        let level_count = levels.len() as u8;

        // NOTE: See segment_history feature
        #[allow(unused_mut)]
        let mut levels = Self {
            segments,
            level_count,
            levels,
            hidden_set: HashSet::new(),
            path: path.as_ref().to_path_buf(),

            #[cfg(feature = "segment_history")]
            segment_history_writer: segment_history::Writer::new()?,
        };

        #[cfg(feature = "segment_history")]
        levels.write_segment_history_entry("load_from_disk")?;

        Ok(levels)
    }

    pub(crate) fn write_to_disk(&mut self) -> crate::Result<()> {
        log::trace!("Writing level manifest");

        // NOTE: Serialization can't fail here
        #[allow(clippy::expect_used)]
        let json = serde_json::to_string_pretty(&self.levels).expect("should serialize");

        // NOTE: Compaction threads don't have concurrent access to the level manifest
        // because it is behind a mutex
        // *However*, the file still needs to be rewritten atomically, because
        // the system could crash at any moment, so
        //
        // a) truncating is not an option, because for a short moment, the file is empty
        // b) just overwriting corrupts the file content
        rewrite_atomic(&self.path, json.as_bytes())?;

        Ok(())
    }

    pub(crate) fn add(&mut self, segment: Arc<Segment>) {
        self.insert_into_level(0, segment);
    }

    pub(crate) fn add_id(&mut self, segment_id: String) {
        self.levels
            .first_mut()
            .expect("should have at least one level")
            .push(segment_id);
    }

    pub(crate) fn sort_levels(&mut self) {
        for level in &mut self.levels {
            level.sort_by(|a, b| {
                let seg_a = self.segments.get(a).expect("where's the segment at");
                let seg_b = self.segments.get(b).expect("where's the segment at");
                seg_b.metadata.created_at.cmp(&seg_a.metadata.created_at)
            });
        }
    }

    pub(crate) fn insert_into_level(&mut self, level_no: u8, segment: Arc<Segment>) {
        let last_level_index = self.level_count - 1;
        let index = level_no.clamp(0, last_level_index);

        let level = self
            .levels
            .get_mut(index as usize)
            .expect("level should exist");

        level.push(segment.metadata.id.clone());
        self.segments.insert(segment.metadata.id.clone(), segment);

        self.sort_levels();

        #[cfg(feature = "segment_history")]
        self.write_segment_history_entry("insert").ok();
    }

    pub(crate) fn remove(&mut self, segment_id: &String) {
        for level in &mut self.levels {
            level.retain(|x| segment_id != x);
        }
        self.segments.remove(segment_id);

        #[cfg(feature = "segment_history")]
        self.write_segment_history_entry("remove").ok();
    }

    /// Returns `true` if there are no segments
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the amount of levels in the tree
    #[must_use]
    pub fn depth(&self) -> u8 {
        self.levels.len() as u8
    }

    /// Returns the amount of levels in the tree
    #[must_use]
    pub fn last_level_index(&self) -> u8 {
        self.depth() - 1
    }

    /// Returns the amount of segments, summed over all levels
    #[must_use]
    pub fn len(&self) -> usize {
        self.levels.iter().fold(0, |sum, level| sum + level.len())
    }

    pub fn busy_levels(&self) -> HashSet<u8> {
        let mut output = HashSet::with_capacity(self.len());

        for (idx, level) in self.levels.iter().enumerate() {
            for segment_id in level.iter() {
                if self.hidden_set.contains(segment_id) {
                    output.insert(idx as u8);
                }
            }
        }

        output
    }

    /// Returns a view into the levels
    /// hiding all segments that currently are being compacted
    #[must_use]
    pub fn resolved_view(&self) -> ResolvedView {
        let mut output = Vec::new();

        for raw_level in &self.levels {
            output.push(ResolvedLevel::new(
                raw_level,
                &self.hidden_set,
                &self.segments,
            ));
        }

        output
    }

    pub(crate) fn get_all_segments_flattened(&self) -> Vec<Arc<Segment>> {
        let mut output = Vec::with_capacity(self.len());

        for level in &self.levels {
            for segment_id in level.iter() {
                output.push(
                    self.segments
                        .get(segment_id)
                        .cloned()
                        .expect("where's the segment at?"),
                );
            }
        }

        output
    }

    pub(crate) fn get_all_segments(&self) -> HashMap<String, Arc<Segment>> {
        let mut output = HashMap::new();

        for segment in self.get_all_segments_flattened() {
            output.insert(segment.metadata.id.clone(), segment);
        }

        output
    }

    pub(crate) fn get_segments(&self) -> HashMap<String, Arc<Segment>> {
        self.get_all_segments()
            .into_iter()
            .filter(|(key, _)| !self.hidden_set.contains(key))
            .collect()
    }

    pub(crate) fn show_segments(&mut self, keys: &[String]) {
        for key in keys {
            self.hidden_set.remove(key);
        }

        #[cfg(feature = "segment_history")]
        self.write_segment_history_entry("show").ok();
    }

    pub(crate) fn hide_segments(&mut self, keys: &[String]) {
        for key in keys {
            self.hidden_set.insert(key.to_string());
        }

        #[cfg(feature = "segment_history")]
        self.write_segment_history_entry("hide").ok();
    }
}

#[cfg(test)]
mod tests {
    use super::ResolvedLevel;
    use crate::{
        block_cache::BlockCache,
        descriptor_table::FileDescriptorTable,
        segment::{index::BlockIndex, meta::Metadata, Segment},
        value::UserKey,
    };
    use std::sync::Arc;

    fn fixture_segment(id: String, key_range: (UserKey, UserKey)) -> Arc<Segment> {
        let block_cache = Arc::new(BlockCache::with_capacity_blocks(0));

        Arc::new(Segment {
            // NOTE: It's just a test
            #[allow(clippy::expect_used)]
            descriptor_table: Arc::new(
                FileDescriptorTable::new("Cargo.toml").expect("should open"),
            ),
            block_index: Arc::new(
                // NOTE: It's just a test
                #[allow(clippy::expect_used)]
                BlockIndex::new(id.clone(), block_cache.clone()),
            ),
            metadata: Metadata {
                path: ".".into(),
                version: crate::version::Version::V0,
                block_count: 0,
                block_size: 0,
                created_at: 0,
                id,
                file_size: 0,
                compression: crate::segment::meta::CompressionType::Lz4,
                item_count: 0,
                key_range,
                tombstone_count: 0,
                uncompressed_size: 0,
                seqnos: (0, 0),
            },
            block_cache,
        })
    }

    #[test]
    fn level_overlaps() {
        let seg0 = fixture_segment("1".into(), (b"c".to_vec().into(), b"k".to_vec().into()));
        let seg1 = fixture_segment("2".into(), (b"l".to_vec().into(), b"z".to_vec().into()));

        let level = ResolvedLevel(vec![seg0, seg1]);

        assert_eq!(
            Vec::<&str>::new(),
            level.get_overlapping_segments(b"a".to_vec().into(), b"b".to_vec().into()),
        );

        assert_eq!(
            vec!["1"],
            level.get_overlapping_segments(b"d".to_vec().into(), b"k".to_vec().into()),
        );

        assert_eq!(
            vec!["1", "2"],
            level.get_overlapping_segments(b"f".to_vec().into(), b"x".to_vec().into()),
        );
    }
}
