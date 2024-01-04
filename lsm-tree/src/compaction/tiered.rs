use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{config::PersistedConfig, levels::Levels};

/// Size-tiered compaction strategy (STCS)
///
/// If a level reaches a threshold, it is merged into a larger segment to the next level
///
/// STCS suffers from high read and temporary space amplification, but decent write amplification
///
/// More info here: <https://opensource.docs.scylladb.com/stable/cql/compaction.html#size-tiered-compaction-strategy-stcs>
#[derive(Default)]
pub struct Strategy;

impl CompactionStrategy for Strategy {
    fn choose(&self, levels: &Levels, config: &PersistedConfig) -> Choice {
        let resolved_view = levels.resolved_view();

        for (level_index, level) in resolved_view
            .iter()
            .enumerate()
            .take(resolved_view.len() - 1)
            .rev()
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
        config::PersistedConfig,
        descriptor_table::FileDescriptorTable,
        file::LEVELS_MANIFEST_FILE,
        levels::Levels,
        segment::{index::BlockIndex, meta::Metadata, Segment},
        Config,
    };
    use std::sync::Arc;
    use test_log::test;

    #[allow(clippy::expect_used)]
    fn fixture_segment(id: Arc<str>) -> Arc<Segment> {
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
        let compactor = Strategy;

        let levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        assert_eq!(
            compactor.choose(&levels, &PersistedConfig::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn default_l0() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy;
        let config = Config::default().level_ratio(4);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.add(fixture_segment("1".into()));
        assert_eq!(compactor.choose(&levels, &config.inner), Choice::DoNothing);

        levels.add(fixture_segment("2".into()));
        assert_eq!(compactor.choose(&levels, &config.inner), Choice::DoNothing);

        levels.add(fixture_segment("3".into()));
        assert_eq!(compactor.choose(&levels, &config.inner), Choice::DoNothing);

        levels.add(fixture_segment("4".into()));
        assert_eq!(
            compactor.choose(&levels, &config.inner),
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
        let compactor = Strategy;
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
            compactor.choose(&levels, &config.inner),
            Choice::DoCompact(CompactionInput {
                dest_level: 2,
                segment_ids: vec!["5".into(), "6".into(), "7".into(), "8".into()],
                target_size: u64::MAX,
            })
        );

        Ok(())
    }

    #[test]
    fn many_segments() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy;
        let config = Config::default().level_ratio(2);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.add(fixture_segment("1".into()));
        levels.add(fixture_segment("2".into()));
        levels.add(fixture_segment("3".into()));
        levels.add(fixture_segment("4".into()));

        assert_eq!(
            compactor.choose(&levels, &config.inner),
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
        let compactor = Strategy;
        let config = Config::default().level_ratio(2);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.add(fixture_segment("1".into()));

        levels.insert_into_level(1, fixture_segment("2".into()));
        levels.insert_into_level(1, fixture_segment("3".into()));

        assert_eq!(
            compactor.choose(&levels, &config.inner),
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
            compactor.choose(&levels, &config.inner),
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
        let compactor = Strategy;
        let config = Config::default().level_ratio(2);

        let mut levels = Levels::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.insert_into_level(3, fixture_segment("2".into()));
        levels.insert_into_level(3, fixture_segment("3".into()));

        assert_eq!(compactor.choose(&levels, &config.inner), Choice::DoNothing);

        Ok(())
    }
}
