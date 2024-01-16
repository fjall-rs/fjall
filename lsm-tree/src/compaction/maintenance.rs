use super::{Choice, CompactionStrategy};
use crate::{config::PersistedConfig, levels::Levels, segment::Segment};
use std::{ops::Deref, sync::Arc};

const L0_SEGMENT_CAP: usize = 20;

/// Maintenance compactor
///
/// This is a hidden compaction strategy that may be called by other strategies.
///
/// It cleans up L0 if it grows too large.
#[derive(Default)]
pub struct Strategy;

// TODO: add test case

/// Choose a run of segments that has the least file size sum.
///
/// This minimizes the compaction time (+ write amp) for a amount of segments we
/// want to get rid of.
pub fn choose_least_effort_compaction(segments: &[Arc<Segment>], n: usize) -> Vec<Arc<str>> {
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

        if first_level.len() > L0_SEGMENT_CAP {
            // NOTE: +1 because two will merge into one
            // So if we have 18 segments, and merge two, we'll have 17, not 16
            let segments_to_merge = first_level.len() - L0_SEGMENT_CAP + 1;

            // Sort the level by oldest to newest
            first_level.sort_by(|a, b| a.metadata.seqnos.0.cmp(&b.metadata.seqnos.0));

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
    use super::*;
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

    #[cfg(feature = "bloom")]
    use crate::bloom::BloomFilter;

    #[allow(clippy::expect_used)]
    fn fixture_segment(id: Arc<str>, created_at: u128) -> Arc<Segment> {
        let block_cache = Arc::new(BlockCache::with_capacity_bytes(u64::MAX));

        Arc::new(Segment {
            descriptor_table: Arc::new(FileDescriptorTable::new(512, 1)),
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

            #[cfg(feature = "bloom")]
            bloom_filter: BloomFilter::with_fp_rate(1, 0.1),
        })
    }

    #[test]
    fn empty_level() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy;

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
        let compactor = Strategy;

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        for id in 0..5 {
            levels.add(fixture_segment(id.to_string().into(), id));
        }

        assert_eq!(
            compactor.choose(&levels, &PersistedConfig::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn l0_too_large() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy;

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        for id in 0..(L0_SEGMENT_CAP + 2) {
            levels.add(fixture_segment(id.to_string().into(), id as u128));
        }

        assert_eq!(
            compactor.choose(&levels, &PersistedConfig::default()),
            Choice::DoCompact(crate::compaction::Input {
                dest_level: 0,
                segment_ids: vec!["0".into(), "1".into(), "2".into()],
                target_size: u64::MAX
            })
        );

        Ok(())
    }
}
