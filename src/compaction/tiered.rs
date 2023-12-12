use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{levels::Levels, Config};

#[derive(Default)]
/// Size-tiered compaction strategy (STCS)
///
/// If a level reaches a threshold, it is merged into a larger segment to the next level
///
/// STCS suffers from high read and temporary space amplification, but decent write amplification
///
/// More info here: <https://opensource.docs.scylladb.com/stable/cql/compaction.html#size-tiered-compaction-strategy-stcs>
pub struct Strategy {
    /*  min_threshold: usize,
    max_threshold: usize, */
}

impl Strategy {
    /*  /// Configures a new `SizeTiered` compaction strategy
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::{Config, compaction::SizeTiered};
    ///
    /// let config = Config::default().compaction_strategy(SizeTiered::new(4, 8));
    /// ```
    ///
    /// # Panics
    ///
    /// Panics, if `min_threshold` is equal to 0 or larger than `max_threshold`
    #[must_use]
    pub fn new(min_threshold: usize, max_threshold: usize) -> Arc<Self> {
        assert!(min_threshold > 0, "SizeTiered::new: invalid thresholds");

        assert!(
            min_threshold <= max_threshold,
            "SizeTiered::new: invalid thresholds"
        );

        Arc::new(Self {
            min_threshold,
            max_threshold,
        })
    } */
}

/* impl Default for Strategy {
    fn default() -> Self {
        Self {} /* {
                    min_threshold: 4,
                    max_threshold: 8,
                } */
    }
} */

impl CompactionStrategy for Strategy {
    fn choose(&self, levels: &Levels, config: &Config) -> Choice {
        let resolved_view = levels.resolved_view();

        for (level_index, level) in resolved_view
            .iter()
            .enumerate()
            .take(resolved_view.len() - 1)
        {
            if level.len() >= config.level_ratio.into() {
                // NOTE: There are never that many levels
                // so it's fine to just truncate it
                #[allow(clippy::cast_possible_truncation)]
                let next_level_index = (level_index + 1) as u8;

                return Choice::DoCompact(CompactionInput {
                    segment_ids: level
                        .iter()
                        .take(config.level_ratio.into())
                        .map(|x| &x.metadata.id)
                        .cloned()
                        .collect(),
                    dest_level: next_level_index,
                    target_size: u64::MAX,
                });
            }
        }

        Choice::DoNothing
    }
}

#[cfg(test)]
mod tests {
    use super::Strategy;
    use crate::{
        block_cache::BlockCache,
        compaction::{Choice, CompactionStrategy, Input as CompactionInput},
        descriptor_table::FileDescriptorTable,
        file::LEVELS_MANIFEST_FILE,
        levels::Levels,
        segment::{index::BlockIndex, meta::Metadata, Segment},
        Config,
    };
    use std::sync::Arc;
    use test_log::test;

    #[allow(clippy::expect_used)]
    fn fixture_segment(id: String) -> Arc<Segment> {
        let block_cache = Arc::new(BlockCache::with_capacity_blocks(0));

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
                created_at: 0,
                id,
                file_size: 0,
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
        let compactor = Strategy::default();

        let levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn default_l0() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::default();
        let config = Config::default().level_ratio(4);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.add(fixture_segment("1".into()));
        assert_eq!(compactor.choose(&levels, &config), Choice::DoNothing);

        levels.add(fixture_segment("2".into()));
        assert_eq!(compactor.choose(&levels, &config), Choice::DoNothing);

        levels.add(fixture_segment("3".into()));
        assert_eq!(compactor.choose(&levels, &config), Choice::DoNothing);

        levels.add(fixture_segment("4".into()));
        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::DoCompact(CompactionInput {
                dest_level: 1,
                segment_ids: vec!["1".into(), "2".into(), "3".into(), "4".into()],
                target_size: u64::MAX,
            })
        );

        Ok(())
    }

    #[test]
    fn more_than_min() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::default(/* 2, 8 */);
        let config = Config::default().level_ratio(4);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.add(fixture_segment("1".into()));
        levels.add(fixture_segment("2".into()));
        levels.add(fixture_segment("3".into()));
        levels.add(fixture_segment("4".into()));

        levels.insert_into_level(1, fixture_segment("5".into()));
        levels.insert_into_level(1, fixture_segment("6".into()));
        levels.insert_into_level(1, fixture_segment("7".into()));
        levels.insert_into_level(1, fixture_segment("8".into()));

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::DoCompact(CompactionInput {
                dest_level: 1,
                segment_ids: vec!["1".into(), "2".into(), "3".into(), "4".into()],
                target_size: u64::MAX,
            })
        );

        Ok(())
    }

    #[test]
    fn many_segments() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::default(/* 2, 2 */);
        let config = Config::default().level_ratio(2);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.add(fixture_segment("1".into()));
        levels.add(fixture_segment("2".into()));
        levels.add(fixture_segment("3".into()));
        levels.add(fixture_segment("4".into()));

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::DoCompact(CompactionInput {
                dest_level: 1,
                segment_ids: vec!["1".into(), "2".into()],
                target_size: u64::MAX,
            })
        );

        Ok(())
    }

    #[test]
    fn deeper_level() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::default(/* 2, 4 */);
        let config = Config::default().level_ratio(2);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.add(fixture_segment("1".into()));

        levels.insert_into_level(1, fixture_segment("2".into()));
        levels.insert_into_level(1, fixture_segment("3".into()));

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::DoCompact(CompactionInput {
                dest_level: 2,
                segment_ids: vec!["2".into(), "3".into()],
                target_size: u64::MAX,
            })
        );

        let tempdir = tempfile::tempdir()?;
        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.insert_into_level(2, fixture_segment("2".into()));
        levels.insert_into_level(2, fixture_segment("3".into()));

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::DoCompact(CompactionInput {
                dest_level: 3,
                segment_ids: vec!["2".into(), "3".into()],
                target_size: u64::MAX,
            })
        );

        Ok(())
    }

    #[test]
    fn last_level() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::default(/* 2, 4 */);
        let config = Config::default().level_ratio(2);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.insert_into_level(3, fixture_segment("2".into()));
        levels.insert_into_level(3, fixture_segment("3".into()));

        assert_eq!(compactor.choose(&levels, &config), Choice::DoNothing);

        Ok(())
    }
}
