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

pub type HiddenSet = HashSet<Arc<str>>;
pub type ResolvedView = Vec<ResolvedLevel>;

/// Represents the levels of a log-structured merge tree.
pub struct Levels {
    path: PathBuf,

    segments: HashMap<Arc<str>, Arc<Segment>>,
    levels: Vec<Level>,

    /// Set of segment IDs that are masked
    ///
    /// While consuming segments (because of compaction) they will not appear in the list of segments
    /// as to not cause conflicts between multiple compaction threads (compacting the same segments)
    hidden_set: HiddenSet,

    #[cfg(feature = "segment_history")]
    segment_history_writer: segment_history::Writer,
}

impl Levels {
    pub(crate) fn is_compacting(&self) -> bool {
        !self.hidden_set.is_empty()
    }

    pub(crate) fn create_new<P: AsRef<Path>>(level_count: u8, path: P) -> crate::Result<Self> {
        assert!(level_count > 0, "level_count should be >= 1");

        let levels = (0..level_count)
            .map(|_| Level::default())
            .collect::<Vec<_>>();

        let mut levels = Self {
            path: path.as_ref().to_path_buf(),
            segments: HashMap::with_capacity(100),
            levels,
            hidden_set: HashSet::with_capacity(10),

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

    pub(crate) fn recover_ids<P: AsRef<Path>>(path: P) -> crate::Result<Vec<Arc<str>>> {
        let level_manifest = fs::read_to_string(&path)?;
        let levels: Vec<Level> = serde_json::from_str(&level_manifest).expect("deserialize error");
        Ok(levels.iter().flat_map(|f| &**f).cloned().collect())
    }

    pub(crate) fn recover<P: AsRef<Path>>(
        path: P,
        segments: Vec<Arc<Segment>>,
    ) -> crate::Result<Self> {
        let level_manifest = fs::read_to_string(&path)?;
        let levels: Vec<_> = serde_json::from_str(&level_manifest).expect("deserialize error");

        let segments = segments
            .into_iter()
            .map(|seg| (seg.metadata.id.clone(), seg))
            .collect();

        // NOTE: See segment_history feature
        #[allow(unused_mut)]
        let mut levels = Self {
            segments,
            levels,
            hidden_set: HashSet::with_capacity(10),
            path: path.as_ref().to_path_buf(),

            #[cfg(feature = "segment_history")]
            segment_history_writer: segment_history::Writer::new()?,
        };

        #[cfg(feature = "segment_history")]
        levels.write_segment_history_entry("load_from_disk")?;

        Ok(levels)
    }

    pub(crate) fn write_to_disk(&mut self) -> crate::Result<()> {
        log::trace!("Writing level manifest to {}", self.path.display());

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

    /// Sorts all levels from newest to oldest
    ///
    /// This will make segments with highest seqno get checked first,
    /// so if there are two versions of an item, the fresher one is seen first:
    ///
    /// segment a   segment b
    /// [key:asd:2] [key:asd:1]
    ///
    /// point read ----------->
    pub(crate) fn sort_levels(&mut self) {
        for level in &mut self.levels {
            level.sort_by(|a, b| {
                let seg_a = self.segments.get(a).expect("where's the segment at");
                let seg_b = self.segments.get(b).expect("where's the segment at");
                seg_b.metadata.seqnos.1.cmp(&seg_a.metadata.seqnos.1)
            });
        }
    }

    pub(crate) fn insert_into_level(&mut self, level_no: u8, segment: Arc<Segment>) {
        let last_level_index = self.depth() - 1;
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

    pub(crate) fn remove(&mut self, segment_id: &Arc<str>) {
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

    pub fn first_level_segment_count(&self) -> usize {
        self.levels.first().expect("L0 should always exist").len()
    }

    /// Returns the amount of levels in the tree
    #[must_use]
    pub fn last_level_index(&self) -> u8 {
        self.depth() - 1
    }

    /// Returns the amount of segments, summed over all levels
    #[must_use]
    pub fn len(&self) -> usize {
        self.levels.iter().map(|level| level.len()).sum()
    }

    /// Returns the (compressed) size of all segments
    #[must_use]
    pub fn size(&self) -> u64 {
        self.get_all_segments_flattened()
            .iter()
            .map(|s| s.metadata.file_size)
            .sum()
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
        let mut output = Vec::with_capacity(self.len());

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

    pub(crate) fn get_all_segments(&self) -> HashMap<Arc<str>, Arc<Segment>> {
        let mut output = HashMap::new();

        for segment in self.get_all_segments_flattened() {
            output.insert(segment.metadata.id.clone(), segment);
        }

        output
    }

    pub(crate) fn get_segments(&self) -> HashMap<Arc<str>, Arc<Segment>> {
        self.get_all_segments()
            .into_iter()
            .filter(|(key, _)| !self.hidden_set.contains(key))
            .collect()
    }

    pub(crate) fn show_segments(&mut self, keys: &[Arc<str>]) {
        for key in keys {
            self.hidden_set.remove(key);
        }

        #[cfg(feature = "segment_history")]
        self.write_segment_history_entry("show").ok();
    }

    pub(crate) fn hide_segments(&mut self, keys: &[Arc<str>]) {
        for key in keys {
            self.hidden_set.insert(key.clone());
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

    #[cfg(feature = "bloom")]
    use crate::bloom::BloomFilter;

    #[allow(clippy::expect_used)]
    fn fixture_segment(id: Arc<str>, key_range: (UserKey, UserKey)) -> Arc<Segment> {
        let block_cache = Arc::new(BlockCache::with_capacity_bytes(u64::MAX));

        Arc::new(Segment {
            descriptor_table: Arc::new(FileDescriptorTable::new(512, 1)),
            block_index: Arc::new(BlockIndex::new(id.clone(), block_cache.clone())),
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
                key_count: 0,
                key_range,
                tombstone_count: 0,
                uncompressed_size: 0,
                seqnos: (0, 0),
            },
            block_cache,

            #[cfg(feature = "bloom")]
            bloom_filter: BloomFilter::with_fp_rate(1, 0.1),
        })
    }

    #[test]
    fn level_overlaps() {
        let seg0 = fixture_segment("1".into(), (b"c".to_vec().into(), b"k".to_vec().into()));
        let seg1 = fixture_segment("2".into(), (b"l".to_vec().into(), b"z".to_vec().into()));

        let level = ResolvedLevel(vec![seg0, seg1]);

        assert_eq!(
            Vec::<Arc<str>>::new(),
            level.get_overlapping_segments(b"a".to_vec().into(), b"b".to_vec().into()),
        );

        assert_eq!(
            vec![Arc::<str>::from("1")],
            level.get_overlapping_segments(b"d".to_vec().into(), b"k".to_vec().into()),
        );

        assert_eq!(
            vec![Arc::<str>::from("1"), Arc::<str>::from("2")],
            level.get_overlapping_segments(b"f".to_vec().into(), b"x".to_vec().into()),
        );
    }
}
