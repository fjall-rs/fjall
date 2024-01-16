use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{config::PersistedConfig, levels::Levels, segment::Segment, value::UserKey};
use std::{ops::Deref, sync::Arc};

/// Levelled compaction strategy (LCS)
///
/// If a level reaches some threshold size, parts of it are merged into overlapping segments in the next level.
///
/// Each level Ln for n >= 1 can have up to ratio^n segments.
///
/// LCS suffers from high write amplification, but decent read & space amplification.
///
/// More info here: <https://opensource.docs.scylladb.com/stable/cql/compaction.html#leveled-compaction-strategy-lcs>
pub struct Strategy {
    /// When the number of segments in L0 reaches this threshold,
    /// they are merged into L1
    ///
    /// Default = 4
    ///
    /// Same as `level0_file_num_compaction_trigger` in RocksDB
    pub l0_threshold: u8,

    /// Target segment size (compressed)
    ///
    /// Default = 64 MiB
    ///
    /// Same as `target_file_size_base` in RocksDB
    pub target_size: u32,
}

impl Default for Strategy {
    fn default() -> Self {
        Self {
            l0_threshold: 4,
            target_size: 64 * 1_024 * 1_024,
        }
    }
}

fn get_key_range(segments: &[Arc<Segment>]) -> (UserKey, UserKey) {
    let (mut min, mut max) = segments
        .first()
        .expect("segment should always exist")
        .metadata
        .key_range
        .clone();

    for other in segments.iter().skip(1) {
        if other.metadata.key_range.0 < min {
            min = other.metadata.key_range.0.clone();
        }
        if other.metadata.key_range.1 > max {
            max = other.metadata.key_range.1.clone();
        }
    }

    (min, max)
}

fn desired_level_size_in_bytes(level_idx: u8, ratio: u8, target_size: u32) -> usize {
    (ratio as usize).pow(u32::from(level_idx)) * (target_size as usize)
}

// TODO: test with timeseries workload
// TODO: time series are disjunct, so it should be possible to just move segments down
// TODO: instead of rewriting

impl CompactionStrategy for Strategy {
    fn choose(&self, levels: &Levels, config: &PersistedConfig) -> Choice {
        let resolved_view = levels.resolved_view();

        // If there are any levels that already have a compactor working on it
        // we can't touch those, because that could cause a race condition
        // violating the levelled compaction invariance of having a single sorted
        // run per level
        //
        // TODO: However, this can probably improved by checking two compaction
        // workers just don't cross key ranges
        // If so, we should sort the level(s), because if multiple compaction workers
        // wrote to the same level at the same time, we couldn't guarantee that the levels
        // are sorted in ascending keyspace order (current they are because we write the
        // segments from left to right, so lower key bound + creation date match up)
        let busy_levels = levels.busy_levels();

        for (curr_level_index, level) in resolved_view
            .iter()
            .enumerate()
            .map(|(idx, lvl)| (idx as u8, lvl))
            .skip(1)
            .take(resolved_view.len() - 2)
            .rev()
        {
            let next_level_index = curr_level_index + 1;

            if level.is_empty() {
                continue;
            }

            if busy_levels.contains(&curr_level_index) || busy_levels.contains(&next_level_index) {
                continue;
            }

            let curr_level_bytes = level.size();

            let desired_bytes =
                desired_level_size_in_bytes(curr_level_index, config.level_ratio, self.target_size);

            let mut overshoot = curr_level_bytes.saturating_sub(desired_bytes as u64) as usize;

            if overshoot > 0 {
                let mut segments_to_compact = vec![];

                let mut level = level.deref().clone();
                level.sort_by(|a, b| a.metadata.key_range.0.cmp(&b.metadata.key_range.0));

                for segment in level.iter().take(config.level_ratio.into()).cloned() {
                    if overshoot == 0 {
                        break;
                    }

                    overshoot = overshoot.saturating_sub(segment.metadata.file_size as usize);
                    segments_to_compact.push(segment);
                }

                let (min, max) = get_key_range(&segments_to_compact);

                let Some(next_level) = &resolved_view.get(next_level_index as usize) else {
                    break;
                };

                let overlapping_segment_ids = next_level.get_overlapping_segments(min, max);

                let mut segment_ids: Vec<_> = segments_to_compact
                    .iter()
                    .map(|x| &x.metadata.id)
                    .cloned()
                    .collect();

                segment_ids.extend(overlapping_segment_ids);

                return Choice::DoCompact(CompactionInput {
                    segment_ids,
                    dest_level: next_level_index,
                    target_size: u64::from(self.target_size),
                });
            }
        }

        {
            let Some(first_level) = resolved_view.first() else {
                return Choice::DoNothing;
            };

            if first_level.len() >= self.l0_threshold.into()
                && !busy_levels.contains(&0)
                && !busy_levels.contains(&1)
            {
                let mut first_level_segments = first_level.deref().clone();
                first_level_segments
                    .sort_by(|a, b| a.metadata.key_range.0.cmp(&b.metadata.key_range.0));

                let (min, max) = get_key_range(&first_level_segments);

                let Some(next_level) = &resolved_view.get(1) else {
                    return Choice::DoNothing;
                };

                let overlapping_segment_ids = next_level.get_overlapping_segments(min, max);

                let mut segment_ids = first_level_segments
                    .iter()
                    .map(|x| &x.metadata.id)
                    .cloned()
                    .collect::<Vec<_>>();

                segment_ids.extend(overlapping_segment_ids);

                return Choice::DoCompact(CompactionInput {
                    segment_ids,
                    dest_level: 1,
                    target_size: u64::from(self.target_size),
                });
            }
        }

        Choice::DoNothing
    }
}

#[cfg(test)]
mod tests {
    use super::{Choice, Strategy};
    use crate::{
        block_cache::BlockCache,
        compaction::{CompactionStrategy, Input as CompactionInput},
        descriptor_table::FileDescriptorTable,
        file::LEVELS_MANIFEST_FILE,
        levels::Levels,
        segment::{index::BlockIndex, meta::Metadata, Segment},
        time::unix_timestamp,
        value::UserKey,
        Config,
    };
    use std::sync::Arc;
    use test_log::test;

    #[cfg(feature = "bloom")]
    use crate::bloom::BloomFilter;

    #[allow(clippy::expect_used)]
    fn fixture_segment(id: Arc<str>, key_range: (UserKey, UserKey), size: u64) -> Arc<Segment> {
        let block_cache = Arc::new(BlockCache::with_capacity_bytes(u64::MAX));

        Arc::new(Segment {
            descriptor_table: Arc::new(FileDescriptorTable::new(512, 1)),
            block_index: Arc::new(BlockIndex::new(id.clone(), block_cache.clone())),
            metadata: Metadata {
                path: ".".into(),
                version: crate::version::Version::V0,
                block_count: 0,
                block_size: 0,
                created_at: unix_timestamp().as_nanos(),
                id,
                file_size: size,
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
    fn empty_levels() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 128 * 1_024 * 1_024,
            ..Default::default()
        };

        let levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        assert_eq!(
            compactor.choose(&levels, &Config::default().inner),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn default_l0() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 128 * 1_024 * 1_024,
            ..Default::default()
        };

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.add(fixture_segment(
            "1".into(),
            ("a".as_bytes().into(), "z".as_bytes().into()),
            128 * 1_024 * 1_024,
        ));
        assert_eq!(
            compactor.choose(&levels, &Config::default().inner),
            Choice::DoNothing
        );

        levels.add(fixture_segment(
            "2".into(),
            ("a".as_bytes().into(), "z".as_bytes().into()),
            128 * 1_024 * 1_024,
        ));
        assert_eq!(
            compactor.choose(&levels, &Config::default().inner),
            Choice::DoNothing
        );

        levels.add(fixture_segment(
            "3".into(),
            ("a".as_bytes().into(), "z".as_bytes().into()),
            128 * 1_024 * 1_024,
        ));
        assert_eq!(
            compactor.choose(&levels, &Config::default().inner),
            Choice::DoNothing
        );

        levels.add(fixture_segment(
            "4".into(),
            ("a".as_bytes().into(), "z".as_bytes().into()),
            128 * 1_024 * 1_024,
        ));

        assert_eq!(
            compactor.choose(&levels, &Config::default().inner),
            Choice::DoCompact(CompactionInput {
                dest_level: 1,
                segment_ids: vec!["1".into(), "2".into(), "3".into(), "4".into()],
                target_size: 128 * 1024 * 1024
            })
        );

        levels.hide_segments(&["4".into()]);
        assert_eq!(
            compactor.choose(&levels, &Config::default().inner),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn more_than_min_no_overlap() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 128 * 1_024 * 1_024,
            ..Default::default()
        };

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.add(fixture_segment(
            "1".into(),
            ("h".as_bytes().into(), "t".as_bytes().into()),
            128 * 1_024 * 1_024,
        ));
        levels.add(fixture_segment(
            "2".into(),
            ("h".as_bytes().into(), "t".as_bytes().into()),
            128 * 1_024 * 1_024,
        ));
        levels.add(fixture_segment(
            "3".into(),
            ("h".as_bytes().into(), "t".as_bytes().into()),
            128 * 1_024 * 1_024,
        ));
        levels.add(fixture_segment(
            "4".into(),
            ("h".as_bytes().into(), "t".as_bytes().into()),
            128 * 1_024 * 1_024,
        ));

        levels.insert_into_level(
            1,
            fixture_segment(
                "5".into(),
                ("a".as_bytes().into(), "g".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );
        levels.insert_into_level(
            1,
            fixture_segment(
                "6".into(),
                ("a".as_bytes().into(), "g".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );
        levels.insert_into_level(
            1,
            fixture_segment(
                "7".into(),
                ("y".as_bytes().into(), "z".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );
        levels.insert_into_level(
            1,
            fixture_segment(
                "8".into(),
                ("y".as_bytes().into(), "z".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );

        assert_eq!(
            compactor.choose(&levels, &Config::default().inner),
            Choice::DoCompact(CompactionInput {
                dest_level: 1,
                segment_ids: vec!["1".into(), "2".into(), "3".into(), "4".into()],
                target_size: 128 * 1024 * 1024
            })
        );

        Ok(())
    }

    #[test]
    fn more_than_min_with_overlap() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 128 * 1_024 * 1_024,
            ..Default::default()
        };

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.add(fixture_segment(
            "1".into(),
            ("a".as_bytes().into(), "g".as_bytes().into()),
            128 * 1_024 * 1_024,
        ));
        levels.add(fixture_segment(
            "2".into(),
            ("h".as_bytes().into(), "t".as_bytes().into()),
            128 * 1_024 * 1_024,
        ));
        levels.add(fixture_segment(
            "3".into(),
            ("i".as_bytes().into(), "t".as_bytes().into()),
            128 * 1_024 * 1_024,
        ));
        levels.add(fixture_segment(
            "4".into(),
            ("j".as_bytes().into(), "t".as_bytes().into()),
            128 * 1_024 * 1_024,
        ));

        levels.insert_into_level(
            1,
            fixture_segment(
                "5".into(),
                ("a".as_bytes().into(), "g".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );
        levels.insert_into_level(
            1,
            fixture_segment(
                "6".into(),
                ("a".as_bytes().into(), "g".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );
        levels.insert_into_level(
            1,
            fixture_segment(
                "7".into(),
                ("y".as_bytes().into(), "z".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );
        levels.insert_into_level(
            1,
            fixture_segment(
                "8".into(),
                ("y".as_bytes().into(), "z".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );

        assert_eq!(
            compactor.choose(&levels, &Config::default().inner),
            Choice::DoCompact(CompactionInput {
                dest_level: 1,
                segment_ids: vec![
                    "1".into(),
                    "2".into(),
                    "3".into(),
                    "4".into(),
                    "5".into(),
                    "6".into()
                ],
                target_size: 128 * 1024 * 1024
            })
        );

        levels.hide_segments(&["5".into()]);
        assert_eq!(
            compactor.choose(&levels, &Config::default().inner),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn deeper_level_with_overlap() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 128 * 1_024 * 1_024,
            ..Default::default()
        };
        let config = Config::default().level_ratio(2);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.insert_into_level(
            2,
            fixture_segment(
                "4".into(),
                ("f".as_bytes().into(), "l".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );
        assert_eq!(compactor.choose(&levels, &config.inner), Choice::DoNothing);

        levels.insert_into_level(
            1,
            fixture_segment(
                "1".into(),
                ("a".as_bytes().into(), "g".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );
        assert_eq!(compactor.choose(&levels, &config.inner), Choice::DoNothing);

        levels.insert_into_level(
            1,
            fixture_segment(
                "2".into(),
                ("h".as_bytes().into(), "t".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );

        levels.insert_into_level(
            1,
            fixture_segment(
                "3".into(),
                ("h".as_bytes().into(), "t".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );

        assert_eq!(
            compactor.choose(&levels, &config.inner),
            Choice::DoCompact(CompactionInput {
                dest_level: 2,
                segment_ids: vec!["1".into(), "4".into()],
                target_size: 128 * 1024 * 1024
            })
        );

        Ok(())
    }

    #[test]
    fn last_level_with_overlap() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 128 * 1_024 * 1_024,
            ..Default::default()
        };
        let config = Config::default().level_ratio(2);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.insert_into_level(
            3,
            fixture_segment(
                "5".into(),
                ("f".as_bytes().into(), "l".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );
        assert_eq!(compactor.choose(&levels, &config.inner), Choice::DoNothing);

        levels.insert_into_level(
            2,
            fixture_segment(
                "1".into(),
                ("a".as_bytes().into(), "g".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );
        assert_eq!(compactor.choose(&levels, &config.inner), Choice::DoNothing);

        levels.insert_into_level(
            2,
            fixture_segment(
                "2".into(),
                ("a".as_bytes().into(), "g".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );
        assert_eq!(compactor.choose(&levels, &config.inner), Choice::DoNothing);

        levels.insert_into_level(
            2,
            fixture_segment(
                "3".into(),
                ("a".as_bytes().into(), "g".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );
        assert_eq!(compactor.choose(&levels, &config.inner), Choice::DoNothing);

        levels.insert_into_level(
            2,
            fixture_segment(
                "4".into(),
                ("a".as_bytes().into(), "g".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );

        levels.insert_into_level(
            2,
            fixture_segment(
                "6".into(),
                ("y".as_bytes().into(), "z".as_bytes().into()),
                128 * 1_024 * 1_024,
            ),
        );

        assert_eq!(
            compactor.choose(&levels, &config.inner),
            Choice::DoCompact(CompactionInput {
                dest_level: 3,
                segment_ids: vec!["1".into(), "5".into()],
                target_size: 128 * 1024 * 1024
            })
        );

        Ok(())
    }
}
