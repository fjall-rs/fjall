use crate::segment::Segment;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{BufWriter, Seek, Write},
    ops::{Bound, DerefMut},
    path::Path,
    sync::Arc,
};

/* #[cfg(feature = "segment_history")]
use crate::time::unix_timestamp;
#[cfg(feature = "segment_history")]
use serde_json::json;
#[cfg(feature = "segment_history")]
use std::io::Write;
 */
pub type HiddenSet = HashSet<String>;
pub type ResolvedView = Vec<ResolvedLevel>;

#[derive(Serialize, Deserialize)]
pub struct Level(Vec<String>);

impl std::ops::Deref for Level {
    type Target = Vec<String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Level {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Default for Level {
    fn default() -> Self {
        Self(Vec::with_capacity(20))
    }
}

/* #[cfg(feature = "segment_history")]
const SEGMENT_HISTORY_PATH: &str = "./segment_history.jsonl";
 */
pub struct ResolvedLevel(Vec<Arc<Segment>>);

impl ResolvedLevel {
    pub fn new(
        level: &Level,
        hidden_set: &HiddenSet,
        segments: &HashMap<String, Arc<Segment>>,
    ) -> Self {
        let mut new_level = Vec::new();

        for segment_id in level.iter() {
            if !hidden_set.contains(segment_id) {
                new_level.push(
                    segments
                        .get(segment_id)
                        .cloned()
                        .expect("where's the segment at?"),
                );
            }
        }

        Self(new_level)
    }

    pub fn get_overlapping_segments(&self, start: Vec<u8>, end: Vec<u8>) -> Vec<&String> {
        let bounds = (Bound::Included(start), Bound::Included(end));

        self.0
            .iter()
            .filter(|x| Segment::check_key_range_overlap(x, &bounds))
            .map(|x| &x.metadata.id)
            .collect()
    }
}

impl std::ops::Deref for ResolvedLevel {
    type Target = Vec<Arc<Segment>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ResolvedLevel {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Represents the levels of a log-structured merge tree.
pub struct Levels {
    /// Amount of levels of the LSM tree
    ///
    /// RocksDB has 7 by default
    level_count: u8,

    segments: HashMap<String, Arc<Segment>>,
    levels: Vec<Level>,

    writer: BufWriter<File>,

    /// Set of segment IDs that are masked
    ///
    /// While consuming segments (because of compaction) they will not appear in the list of segments
    /// as to not cause conflicts between multiple compaction threads (compacting the same segments)
    hidden_set: HiddenSet,
}

/* #[cfg(feature = "segment_history")]
fn write_segment_history_entry(event: String, levels: &Levels) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(SEGMENT_HISTORY_PATH)
        .expect("Segment history write failed");

    let segment_map = levels.get_all_segments();
    let ts = unix_timestamp();

    let line = serde_json::to_string(&json!({
        "time_unix": ts.as_secs(),
        "time_ms": ts.as_millis(),
        "event": event,
        "levels": levels.levels.iter().map(|level| {
            let segments = level.iter().map(|seg_id| segment_map[seg_id]).collect::<Vec<_>>();
            segments
            .iter()
            .map(|segment| json!({
                    "path": segment.path.clone(),
                    "metadata": segment.metadata.clone(),
                    "hidden": levels.hidden_set.contains(&segment.metadata.id)
                }))
                .collect::<Vec<_>>()
        }).collect::<Vec<_>>()
    }))
    .expect("Segment history write failed");

    writeln!(file, "{line}").expect("Segment history write failed");
}
 */
impl Levels {
    pub(crate) fn is_compacting(&self) -> bool {
        !self.hidden_set.is_empty()
    }

    pub(crate) fn create_new<P: AsRef<Path>>(level_count: u8, path: P) -> std::io::Result<Self> {
        assert!(level_count > 1, "level_count should be > 1");

        let levels = (0..level_count)
            .map(|_| Level::default())
            .collect::<Vec<_>>();

        let mut levels = Self {
            segments: HashMap::new(),
            level_count,
            levels,
            hidden_set: HashSet::new(),
            writer: BufWriter::new(OpenOptions::new().write(true).create_new(true).open(path)?),
        };
        levels.write_to_disk()?;

        /*  #[cfg(feature = "segment_history")]
        write_segment_history_entry("create_new".into(), &obj); */

        Ok(levels)
    }

    pub(crate) fn from_disk<P: AsRef<Path>>(
        path: &P,
        segments: HashMap<String, Arc<Segment>>,
    ) -> crate::Result<Self> {
        let level_manifest = fs::read_to_string(path)?;
        let levels: Vec<_> = serde_json::from_str(&level_manifest).expect("deserialize error");

        // NOTE: There are never that many levels
        // so it's fine to just truncate it
        #[allow(clippy::cast_possible_truncation)]
        let level_count = levels.len() as u8;

        let levels = Self {
            segments,
            level_count,
            levels,
            hidden_set: HashSet::new(),
            writer: BufWriter::new(OpenOptions::new().write(true).open(path)?),
        };

        /* #[cfg(feature = "segment_history")]
        write_segment_history_entry("load_from_disk".into(), &obj); */

        Ok(levels)
    }

    pub(crate) fn write_to_disk(&mut self) -> std::io::Result<()> {
        log::trace!("Writing level manifest");

        self.writer.seek(std::io::SeekFrom::Start(0))?;
        self.writer.get_mut().set_len(0)?;
        serde_json::to_writer_pretty(&mut self.writer, &self.levels).expect("should serialize");

        // fsync levels manifest
        self.writer.flush()?;
        self.writer.get_mut().sync_all()?;

        Ok(())
    }

    pub(crate) fn add(&mut self, segment: Arc<Segment>) {
        self.insert_into_level(0, segment);
    }

    pub(crate) fn insert_into_level(&mut self, level_no: u8, segment: Arc<Segment>) {
        let last_level_index = self.level_count - 1;
        let index = level_no.clamp(0, last_level_index);

        let level = self.levels.get_mut(index as usize).unwrap();
        level.push(segment.metadata.id.clone());
        self.segments.insert(segment.metadata.id.clone(), segment);

        for level in &mut self.levels {
            level.sort_by(|a, b| {
                let seg_a = self.segments.get(a).expect("where's the segment at");
                let seg_b = self.segments.get(b).expect("where's the segment at");
                seg_b.metadata.created_at.cmp(&seg_a.metadata.created_at)
            });
        }

        /* #[cfg(feature = "segment_history")]
        write_segment_history_entry("insert".into(), self); */
    }

    pub(crate) fn remove(&mut self, segment_id: &String) {
        for level in &mut self.levels {
            level.retain(|x| segment_id != x);
        }
        self.segments.remove(segment_id);

        /*  #[cfg(feature = "segment_history")]
        write_segment_history_entry("remove".into(), self); */
    }

    /// Returns true if there are no segments
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the amount of levels in the tree
    #[must_use]
    pub fn depth(&self) -> usize {
        self.levels.len()
    }

    /// Returns the amount of segments, summed over all levels
    #[must_use]
    pub fn len(&self) -> usize {
        self.levels.iter().fold(0, |sum, level| sum + level.len())
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

    pub(crate) fn show_segments(&mut self, keys: &Vec<String>) {
        for key in keys {
            self.hidden_set.remove(key);
        }

        #[cfg(feature = "segment_history")]
        write_segment_history_entry("show".into(), self);
    }

    pub(crate) fn hide_segments(&mut self, keys: &Vec<String>) {
        for key in keys {
            self.hidden_set.insert(key.to_string());
        }

        #[cfg(feature = "segment_history")]
        write_segment_history_entry("hide".into(), self);
    }
}

#[cfg(test)]
mod tests {
    /* use super::ResolvedLevel;
    use crate::{
        bloom::BloomFilter,
        segment::{meta::Metadata, BlockCache, BlockIndex, DescriptorTable, Segment},
    };
    use std::{collections::BTreeMap, sync::Arc}; */

    /* fn fixture_segment(id: String, key_range: (Vec<u8>, Vec<u8>)) -> Segment {
        Segment {
            path: ".".into(),
            block_index: Arc::new(BlockIndex::new(BTreeMap::new())),
            metadata: Metadata {
                block_count: 0,
                block_size: 0,
                created_at: 0,
                id,
                file_size: 0,
                is_compressed: true,
                item_count: 0,
                key_range,
                tombstone_count: 0,
                uncompressed_size: 0,
            },
            block_cache: Arc::new(BlockCache::with_capacity(1)),
            descriptor_table: DescriptorTable::new(&".").unwrap(),
            bloom_filter: BloomFilter::new(1),
        }
    } */

    /*  #[test]
    fn level_overlaps() {
        let seg0 = fixture_segment("1".into(), (b"c".to_vec(), b"k".to_vec()));
        let seg1 = fixture_segment("2".into(), (b"l".to_vec(), b"z".to_vec()));

        let level = ResolvedLevel(vec![&seg0, &seg1]);

        assert_eq!(
            Vec::<&str>::new(),
            level.get_overlapping_segments(b"a".to_vec(), b"b".to_vec()),
        );

        assert_eq!(
            vec!["1"],
            level.get_overlapping_segments(b"d".to_vec(), b"k".to_vec()),
        );

        assert_eq!(
            vec!["1", "2"],
            level.get_overlapping_segments(b"f".to_vec(), b"x".to_vec()),
        );
    } */
}
