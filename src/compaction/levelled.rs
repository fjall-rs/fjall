use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{levels::Levels, segment::Segment, value::UserKey};
use std::sync::Arc;

/// Levelled compaction strategy (LCS)
///
/// If a level reaches a threshold, parts of it are merged into overlapping segments in the next level
///
/// LCS suffers from high write amplification, but decent read & space amplification
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
    pub target_size: u64,

    /// Level size factor
    ///
    /// Each level Ln for n >= 1 can have up to ratio^n segments
    ///
    /// Default = 10
    ///
    /// Same as `fanout_size` in Cassandra
    pub ratio: u8,
}

impl Default for Strategy {
    fn default() -> Self {
        Self {
            l0_threshold: 4,
            target_size: 64 * 1024 * 1024,
            ratio: 10,
        }
    }
}

fn get_key_range(segments: &[Arc<Segment>]) -> (UserKey, UserKey) {
    let (mut min, mut max) = segments[0].metadata.key_range.clone();

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

fn level_desired_size(level_idx: u8, ratio: u8) -> usize {
    (ratio as usize).pow(u32::from(level_idx))
}

impl CompactionStrategy for Strategy {
    fn choose(&self, levels: &Levels) -> Choice {
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

        for (level_index, level) in resolved_view
            .iter()
            .enumerate()
            .skip(1)
            .take(resolved_view.len() - 2)
            .rev()
        {
            let level_index = level_index as u8;
            let next_level_index = level_index + 1;

            if busy_levels.contains(&level_index) || busy_levels.contains(&next_level_index) {
                continue;
            }

            // The amount of segments that should be compacted down
            // because they cause the level to be larger than the desired size
            let level_size_overshoot =
                (level.len() as isize) - (level_desired_size(level_index, self.ratio) as isize);

            if level_size_overshoot > 0 {
                let current_level_segments: Vec<_> = level
                    .iter()
                    .rev()
                    .take(level_size_overshoot as usize)
                    .cloned()
                    .collect();

                let (min, max) = get_key_range(&current_level_segments);

                let next_level = &resolved_view[next_level_index as usize];
                let overlapping_segment_ids = next_level.get_overlapping_segments(min, max);

                let mut segment_ids: Vec<_> = current_level_segments
                    .iter()
                    .map(|x| &x.metadata.id)
                    .cloned()
                    .collect();

                segment_ids.extend(overlapping_segment_ids.into_iter().cloned());

                return Choice::DoCompact(CompactionInput {
                    segment_ids,
                    dest_level: next_level_index,
                    target_size: self.target_size,
                });
            }
        }

        if busy_levels.contains(&0) || busy_levels.contains(&1) {
            return Choice::DoNothing;
        }

        let Some(first_level) = &resolved_view.get(0) else {
            return Choice::DoNothing;
        };

        if first_level.len() >= 2 && first_level.len() >= self.l0_threshold.into() {
            let first_level_segments: Vec<_> = first_level.iter().cloned().collect();

            let (min, max) = get_key_range(&first_level_segments);

            let next_level = &resolved_view[1];
            let overlapping_segment_ids = next_level.get_overlapping_segments(min, max);

            let mut segment_ids: Vec<_> = first_level_segments
                .iter()
                .map(|x| &x.metadata.id)
                .cloned()
                .collect();

            segment_ids.extend(overlapping_segment_ids.into_iter().cloned());

            Choice::DoCompact(CompactionInput {
                segment_ids,
                dest_level: 1,
                target_size: self.target_size,
            })
        } else {
            Choice::DoNothing
        }
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
    };
    use std::sync::Arc;
    use test_log::test;

    fn fixture_segment(id: String, key_range: (UserKey, UserKey)) -> Arc<Segment> {
        let block_cache = Arc::new(BlockCache::new(0));

        Arc::new(Segment {
            // NOTE: It's just a test
            #[allow(clippy::expect_used)]
            descriptor_table: Arc::new(
                FileDescriptorTable::new("Cargo.toml").expect("should open"),
            ),
            // NOTE: It's just a test
            #[allow(clippy::expect_used)]
            block_index: Arc::new(
                BlockIndex::new(id.clone(), block_cache.clone()).expect("should create index"),
            ),
            metadata: Metadata {
                path: ".".into(),
                version: crate::version::Version::V0,
                block_count: 0,
                block_size: 0,
                created_at: unix_timestamp().as_nanos(),
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
    fn empty_levels() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::default();

        let levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        Ok(())
    }

    #[test]
    fn default_l0() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::default();

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.add(fixture_segment(
            "1".into(),
            ("a".as_bytes().into(), "z".as_bytes().into()),
        ));
        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        levels.add(fixture_segment(
            "2".into(),
            ("a".as_bytes().into(), "z".as_bytes().into()),
        ));
        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        levels.add(fixture_segment(
            "3".into(),
            ("a".as_bytes().into(), "z".as_bytes().into()),
        ));
        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        levels.add(fixture_segment(
            "4".into(),
            ("a".as_bytes().into(), "z".as_bytes().into()),
        ));

        assert_eq!(
            compactor.choose(&levels),
            Choice::DoCompact(CompactionInput {
                dest_level: 1,
                segment_ids: vec!["4".into(), "3".into(), "2".into(), "1".into()],
                target_size: 64 * 1024 * 1024
            })
        );

        levels.hide_segments(&["4".into()]);
        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        Ok(())
    }

    #[test]
    fn more_than_min_no_overlap() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::default();

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.add(fixture_segment(
            "1".into(),
            ("h".as_bytes().into(), "t".as_bytes().into()),
        ));
        levels.add(fixture_segment(
            "2".into(),
            ("h".as_bytes().into(), "t".as_bytes().into()),
        ));
        levels.add(fixture_segment(
            "3".into(),
            ("h".as_bytes().into(), "t".as_bytes().into()),
        ));
        levels.add(fixture_segment(
            "4".into(),
            ("h".as_bytes().into(), "t".as_bytes().into()),
        ));

        levels.insert_into_level(
            1,
            fixture_segment("5".into(), ("a".as_bytes().into(), "g".as_bytes().into())),
        );
        levels.insert_into_level(
            1,
            fixture_segment("6".into(), ("a".as_bytes().into(), "g".as_bytes().into())),
        );
        levels.insert_into_level(
            1,
            fixture_segment("7".into(), ("y".as_bytes().into(), "z".as_bytes().into())),
        );
        levels.insert_into_level(
            1,
            fixture_segment("8".into(), ("y".as_bytes().into(), "z".as_bytes().into())),
        );

        assert_eq!(
            compactor.choose(&levels),
            Choice::DoCompact(CompactionInput {
                dest_level: 1,
                segment_ids: vec!["4".into(), "3".into(), "2".into(), "1".into()],
                target_size: 64 * 1024 * 1024
            })
        );

        Ok(())
    }

    #[test]
    fn more_than_min_with_overlap() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::default();

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.add(fixture_segment(
            "1".into(),
            ("a".as_bytes().into(), "g".as_bytes().into()),
        ));
        levels.add(fixture_segment(
            "2".into(),
            ("h".as_bytes().into(), "t".as_bytes().into()),
        ));
        levels.add(fixture_segment(
            "3".into(),
            ("h".as_bytes().into(), "t".as_bytes().into()),
        ));
        levels.add(fixture_segment(
            "4".into(),
            ("h".as_bytes().into(), "t".as_bytes().into()),
        ));

        levels.insert_into_level(
            1,
            fixture_segment("5".into(), ("a".as_bytes().into(), "g".as_bytes().into())),
        );
        levels.insert_into_level(
            1,
            fixture_segment("6".into(), ("a".as_bytes().into(), "g".as_bytes().into())),
        );
        levels.insert_into_level(
            1,
            fixture_segment("7".into(), ("y".as_bytes().into(), "z".as_bytes().into())),
        );
        levels.insert_into_level(
            1,
            fixture_segment("8".into(), ("y".as_bytes().into(), "z".as_bytes().into())),
        );

        assert_eq!(
            compactor.choose(&levels),
            Choice::DoCompact(CompactionInput {
                dest_level: 1,
                segment_ids: vec![
                    "4".into(),
                    "3".into(),
                    "2".into(),
                    "1".into(),
                    "6".into(),
                    "5".into()
                ],
                target_size: 64 * 1024 * 1024
            })
        );

        levels.hide_segments(&["5".into()]);
        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        Ok(())
    }

    #[test]
    fn deeper_level_with_overlap() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            ratio: 2,
            ..Default::default()
        };

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.insert_into_level(
            2,
            fixture_segment("4".into(), ("f".as_bytes().into(), "l".as_bytes().into())),
        );
        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        levels.insert_into_level(
            1,
            fixture_segment("1".into(), ("a".as_bytes().into(), "g".as_bytes().into())),
        );
        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        levels.insert_into_level(
            1,
            fixture_segment("2".into(), ("h".as_bytes().into(), "t".as_bytes().into())),
        );

        levels.insert_into_level(
            1,
            fixture_segment("3".into(), ("h".as_bytes().into(), "t".as_bytes().into())),
        );

        assert_eq!(
            compactor.choose(&levels),
            Choice::DoCompact(CompactionInput {
                dest_level: 2,
                segment_ids: vec!["1".into(), "4".into()],
                target_size: 64 * 1024 * 1024
            })
        );

        Ok(())
    }

    #[test]
    fn last_level_with_overlap() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            ratio: 2,
            ..Default::default()
        };

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.insert_into_level(
            3,
            fixture_segment("5".into(), ("f".as_bytes().into(), "l".as_bytes().into())),
        );
        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        levels.insert_into_level(
            2,
            fixture_segment("1".into(), ("a".as_bytes().into(), "g".as_bytes().into())),
        );
        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        levels.insert_into_level(
            2,
            fixture_segment("2".into(), ("a".as_bytes().into(), "g".as_bytes().into())),
        );
        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        levels.insert_into_level(
            2,
            fixture_segment("3".into(), ("a".as_bytes().into(), "g".as_bytes().into())),
        );
        assert_eq!(compactor.choose(&levels), Choice::DoNothing);

        levels.insert_into_level(
            2,
            fixture_segment("4".into(), ("a".as_bytes().into(), "g".as_bytes().into())),
        );

        levels.insert_into_level(
            2,
            fixture_segment("6".into(), ("y".as_bytes().into(), "z".as_bytes().into())),
        );

        assert_eq!(
            compactor.choose(&levels),
            Choice::DoCompact(CompactionInput {
                dest_level: 3,
                segment_ids: vec!["1".into(), "5".into()],
                target_size: 64 * 1024 * 1024
            })
        );

        Ok(())
    }
}
