use super::{Choice, CompactionStrategy};
use crate::{config::PersistedConfig, levels::Levels, segment::Segment};
use std::{ops::Deref, sync::Arc};

const L0_SEGMENT_CAP: usize = 24;

/// FIFO-style compaction.
///
/// Limits the tree size to roughly `limit` bytes, deleting the oldest segment(s)
/// when the threshold is reached.
///
/// Will also merge segments if the amount of segments in level 0 grows too much, which
/// could cause write stalls.
///
/// ###### Caution
///
/// Only use it for specific workloads where:
///
/// 1) You only want to store recent data (unimportant logs, ...)
/// 2) Your keyspace grows monotonically (time series)
/// 3) You only insert new data
///
/// More info here: <https://github.com/facebook/rocksdb/wiki/FIFO-compaction-style>
pub struct Strategy {
    limit: u64,
}

impl Strategy {
    /// Configures a new `Fifo` compaction strategy
    #[must_use]
    pub fn new(limit: u64) -> Self {
        Self { limit }
    }
}

/// Choose a run of segments that has the least file size sum
///
/// This minimizes the compaction time (+ write amp) for a amount of segments we
/// want to get rid of
fn choose_least_effort_compaction(segments: &[Arc<Segment>], n: usize) -> Vec<Arc<str>> {
    let num_segments = segments.len();

    // Ensure that n is not greater than the number of segments
    assert!(
        n <= num_segments,
        "N must be less than or equal to the number of segments"
    );

    let windows = segments.windows(n);

    let window = windows
        .min_by_key(|window| window.iter().map(|s| s.metadata.file_size).sum::<u64>())
        .expect("should have at least one window");

    window.iter().map(|x| x.metadata.id.clone()).collect()
}

impl CompactionStrategy for Strategy {
    fn choose(&self, levels: &Levels, _: &PersistedConfig) -> Choice {
        let resolved_view = levels.resolved_view();

        let mut first_level = resolved_view
            .first()
            .expect("L0 should always exist")
            .deref()
            .clone();

        let db_size = levels
            .get_all_segments()
            .values()
            .map(|x| x.metadata.file_size)
            .sum::<u64>();

        if db_size > self.limit {
            let mut bytes_to_delete = db_size - self.limit;

            // Sort the level by creation date
            first_level.sort_by(|a, b| a.metadata.created_at.cmp(&b.metadata.created_at));

            let mut ids = vec![];

            for segment in first_level {
                if bytes_to_delete == 0 {
                    break;
                }

                bytes_to_delete = bytes_to_delete.saturating_sub(segment.metadata.file_size);

                ids.push(segment.metadata.id.clone());
            }

            Choice::DeleteSegments(ids)
        } else if first_level.len() > L0_SEGMENT_CAP {
            // NOTE: +1 because two will merge into one
            // So if we have 18 segments, and merge two, we'll have 17, not 16
            let segments_to_merge = first_level.len() - L0_SEGMENT_CAP + 1;

            // Sort the level by creation date
            first_level.sort_by(|a, b| a.metadata.created_at.cmp(&b.metadata.created_at));

            let segment_ids = choose_least_effort_compaction(&first_level, segments_to_merge);

            Choice::DoCompact(super::Input {
                dest_level: 0,
                segment_ids,
                target_size: u64::MAX,
            })
        } else {
            Choice::DoNothing
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Strategy;
    use crate::{
        block_cache::BlockCache,
        compaction::{Choice, CompactionStrategy},
        config::PersistedConfig,
        descriptor_table::FileDescriptorTable,
        file::LEVELS_MANIFEST_FILE,
        levels::Levels,
        segment::{index::BlockIndex, meta::Metadata, Segment},
    };
    use std::sync::Arc;
    use test_log::test;

    #[allow(clippy::expect_used)]
    fn fixture_segment(id: Arc<str>, created_at: u128) -> Arc<Segment> {
        let block_cache = Arc::new(BlockCache::with_capacity_bytes(u64::MAX));

        Arc::new(Segment {
            descriptor_table: Arc::new(
                FileDescriptorTable::new("Cargo.toml").expect("should open"),
            ),
            block_index: Arc::new(BlockIndex::new(id.clone(), block_cache.clone())),
            metadata: Metadata {
                path: ".".into(),
                version: crate::version::Version::V0,
                block_count: 0,
                block_size: 0,
                created_at,
                id,
                file_size: 1,
                compression: crate::segment::meta::CompressionType::Lz4,
                item_count: 0,
                key_count: 0,
                key_range: (vec![].into(), vec![].into()),
                tombstone_count: 0,
                uncompressed_size: 0,
                seqnos: (0, 0),
            },
            block_cache,
        })
    }

    #[test]
    fn empty_levels() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::new(1);

        let levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        assert_eq!(
            compactor.choose(&levels, &PersistedConfig::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn below_limit() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::new(4);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.add(fixture_segment("1".into(), 1));
        assert_eq!(
            compactor.choose(&levels, &PersistedConfig::default()),
            Choice::DoNothing
        );

        levels.add(fixture_segment("2".into(), 2));
        assert_eq!(
            compactor.choose(&levels, &PersistedConfig::default()),
            Choice::DoNothing
        );

        levels.add(fixture_segment("3".into(), 3));
        assert_eq!(
            compactor.choose(&levels, &PersistedConfig::default()),
            Choice::DoNothing
        );

        levels.add(fixture_segment("4".into(), 4));
        assert_eq!(
            compactor.choose(&levels, &PersistedConfig::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn more_than_limit() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::new(2);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.add(fixture_segment("1".into(), 1));
        levels.add(fixture_segment("2".into(), 2));
        levels.add(fixture_segment("3".into(), 3));
        levels.add(fixture_segment("4".into(), 4));

        assert_eq!(
            compactor.choose(&levels, &PersistedConfig::default()),
            Choice::DeleteSegments(vec!["1".into(), "2".into()])
        );

        Ok(())
    }
}
