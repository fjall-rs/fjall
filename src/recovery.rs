use crate::{
    descriptor_table::FileDescriptorTable,
    file::{
        BLOCKS_FILE, FLUSH_MARKER, JOURNALS_FOLDER, LEVELS_MANIFEST_FILE, LSM_MARKER,
        PARTITIONS_FOLDER, SEGMENTS_FOLDER,
    },
    id::generate_segment_id,
    journal::Journal,
    levels::Levels,
    memtable::MemTable,
    partition::Partition,
    segment::{self, Segment},
    stop_signal::StopSignal,
    tree_inner::TreeInner,
    version::Version,
    BlockCache, Config, Tree,
};
use std::{
    collections::HashMap,
    path::Path,
    sync::{
        atomic::{AtomicU32, AtomicU64},
        Arc, RwLock,
    },
};
use std_semaphore::Semaphore;

#[allow(clippy::type_complexity)]
pub fn recover_active_journal(
    config: &Config,
) -> crate::Result<Option<(Journal, HashMap<Arc<str>, MemTable>)>> {
    let mut active_journal = None;

    for dirent in std::fs::read_dir(config.path.join(JOURNALS_FOLDER))? {
        let dirent = dirent?;
        let journal_path = dirent.path();

        assert!(journal_path.is_dir());

        // TODO: replace fs extra with Journal::disk_space
        let journal_size = fs_extra::dir::get_size(&journal_path)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "fs_extra error"))?;

        if journal_size == 0 {
            std::fs::remove_dir_all(&journal_path)?;
            continue;
        }

        // If the marker does not exist, it's the active journal
        if !journal_path.join(FLUSH_MARKER).exists() {
            // TODO: handle this
            assert!(active_journal.is_none(), "Second active journal found :(");

            if journal_size < config.max_memtable_size.into() {
                log::info!("Setting {} as active journal", journal_path.display());

                let (recovered_journal, memtables) = Journal::recover(journal_path.clone())?;

                active_journal = Some((recovered_journal, memtables));

                continue;
            }

            log::info!(
                "Flushing active journal because it is too large: {}",
                dirent.path().to_string_lossy()
            );

            // Journal is too large to be continued to be used
            // Just flush it
        } else {
            log::info!(
                "Flushing orphaned journal {} to segment",
                dirent.path().to_string_lossy()
            );
        }

        // TODO: optimize this

        let (recovered_journal, mut memtables) = Journal::recover(journal_path.clone())?;
        log::trace!("Recovered old journal");
        drop(recovered_journal);

        let segment_id = dirent
            .file_name()
            .to_str()
            .expect("invalid journal folder name")
            .to_string();

        for partition_marker_dirent in std::fs::read_dir(dirent.path())? {
            let partition_marker_dirent = partition_marker_dirent?;
            let file_name = partition_marker_dirent.file_name();
            let file_name = file_name.to_string_lossy();

            if file_name.starts_with(".pt_") {
                let partition_name = file_name.to_string().replace(".pt_", "");

                let partition_folder = config.path.join(PARTITIONS_FOLDER).join(&partition_name);

                // Load previous levels manifest
                // Add all flushed segments to it, then recover properly
                let mut levels =
                    Levels::recover(partition_folder.join(LEVELS_MANIFEST_FILE), HashMap::new())?;

                let memtable = memtables
                    .remove(&*partition_name)
                    .expect("memtable should exist");

                let segment_folder = partition_folder.join(SEGMENTS_FOLDER).join(&segment_id);

                if !levels.contains_id(&segment_id) {
                    // The level manifest does not contain the segment
                    // If the segment is maybe half written, clean it up here
                    // and then write it
                    if segment_folder.exists() {
                        std::fs::remove_dir_all(&segment_folder)?;
                    }

                    let mut segment_writer =
                        segment::writer::Writer::new(segment::writer::Options {
                            partition: partition_name.clone().into(),
                            path: segment_folder.clone(),
                            evict_tombstones: false,
                            block_size: config.block_size,
                        })?;

                    for (key, value) in memtable.items {
                        segment_writer.write(crate::Value::from((key, value)))?;
                    }

                    segment_writer.finish()?;

                    if segment_writer.item_count > 0 {
                        let metadata = segment::meta::Metadata::from_writer(
                            segment_id.clone().into(),
                            segment_writer,
                        )?;
                        metadata.write_to_file()?;

                        log::info!(
                            "Written segment {:?} from orphaned journal to partition {partition_name:?}",
                            metadata.id
                        );

                        levels.add_id(metadata.id);
                        levels.write_to_disk()?;

                        log::debug!(
                            "Deleting partition marker for old journal: {} -> {partition_name}",
                            journal_path.display()
                        );
                        std::fs::remove_file(partition_marker_dirent.path())?;
                    }
                }
            }
        }

        std::fs::remove_dir_all(journal_path)?;
    }

    Ok(active_journal)
}

pub fn recover_segments<P: AsRef<Path>>(
    folder: P,
    block_cache: &Arc<BlockCache>,
) -> crate::Result<HashMap<Arc<str>, Arc<Segment>>> {
    let folder = folder.as_ref();

    // NOTE: First we load the level manifest without any
    // segments just to get the IDs
    // Then we recover the segments and build the actual level manifest
    let levels = Levels::recover(folder.join(LEVELS_MANIFEST_FILE), HashMap::new())?;
    let segment_ids_to_recover = levels.list_ids();

    let mut segments = HashMap::new();

    for dirent in std::fs::read_dir(folder.join(SEGMENTS_FOLDER))? {
        let dirent = dirent?;
        let path = dirent.path();

        assert!(path.is_dir());

        let segment_id = dirent
            .file_name()
            .to_str()
            .expect("invalid segment folder name")
            .to_owned()
            .into();

        log::debug!("Recovering segment from {}", path.display());

        if segment_ids_to_recover.contains(&segment_id) {
            let segment = Segment::recover(
                &path,
                Arc::clone(block_cache),
                Arc::new(FileDescriptorTable::new(path.join(BLOCKS_FILE))?),
            )?;
            segments.insert(segment.metadata.id.clone(), Arc::new(segment));
            log::debug!("Recovered segment from {}", path.display());
        } else {
            log::debug!(
                "Deleting unfinished segment (not part of level manifest): {}",
                path.to_string_lossy()
            );
            std::fs::remove_dir_all(path)?;
        }
    }

    if segments.len() < segment_ids_to_recover.len() {
        log::error!("Expected segments : {segment_ids_to_recover:?}");
        log::error!(
            "Recovered segments: {:?}",
            segments.keys().collect::<Vec<_>>()
        );

        // TODO: no panic here
        panic!("Some segments were not recovered")
    }

    Ok(segments)
}

pub fn recover_tree(config: Config) -> crate::Result<Tree> {
    log::info!("Recovering tree from {}", config.path.display());

    let start = std::time::Instant::now();

    log::info!("Checking tree version");
    let version_bytes = std::fs::read(config.path.join(LSM_MARKER))?;
    let version = Version::parse_file_header(&version_bytes);
    assert!(version.is_some(), "Invalid LSM-tree version");

    log::info!("Restoring journal");
    let active_journal = crate::recovery::recover_active_journal(&config)?;

    let (journal, memtables) = if let Some(active_journal) = active_journal {
        log::debug!("Restored {} memtables", active_journal.1.len());
        active_journal
    } else {
        let next_journal_path = config
            .path
            .join(JOURNALS_FOLDER)
            .join(&*generate_segment_id());

        log::debug!(
            "No active journal found, creating new one at {}",
            next_journal_path.display()
        );

        (Journal::create_new(next_journal_path)?, HashMap::default())
    };

    let block_cache = Arc::clone(&config.block_cache);

    // Finalize Tree
    log::debug!("Loading level manifest");

    let compaction_threads = 4; // TODO: config
    let flush_threads = config.flush_threads.into();

    let journal = Arc::new(journal);

    // TODO: need to track journal size separately from memtables
    /* // TODO: replace fs extra with Journal::disk_space
    let active_journal_size = fs_extra::dir::get_size(&journal.path)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "fs_extra error"))?; */

    let next_lsn = memtables.values().map(MemTable::get_lsn).max().unwrap_or(0);
    let next_lsn = Arc::new(AtomicU64::new(next_lsn));

    let mut partitions = memtables
        .into_iter()
        .map(|(name, table)| {
            Ok((
                name.clone(),
                Partition::recover(
                    config.path.join(PARTITIONS_FOLDER).join(&*name),
                    name.clone(),
                    config.clone(),
                    table,
                    journal.clone(),
                    next_lsn.clone(),
                )?,
            ))
        })
        .collect::<crate::Result<HashMap<_, _>>>()?;

    for entry in std::fs::read_dir(config.path.join(PARTITIONS_FOLDER))? {
        let partition_name = entry?;
        let partition_name = partition_name.file_name();
        let partition_name: Arc<str> = partition_name.to_string_lossy().to_string().into();

        if !partitions.contains_key(&partition_name) {
            partitions.insert(
                partition_name.clone(),
                Partition::recover(
                    config.path.join(PARTITIONS_FOLDER).join(&*partition_name),
                    partition_name,
                    config.clone(),
                    MemTable::default(),
                    journal.clone(),
                    next_lsn.clone(),
                )?,
            );
        }
    }

    let partition_highest_lsn = partitions
        .values()
        .map(Partition::get_lsn)
        .max()
        .unwrap_or(0);

    next_lsn.store(
        next_lsn
            .load(std::sync::atomic::Ordering::Acquire)
            .max(partition_highest_lsn),
        std::sync::atomic::Ordering::Release,
    );

    let inner = TreeInner {
        config,
        journal,
        block_cache,
        next_lsn,
        partitions: Arc::new(RwLock::new(partitions)),
        flush_semaphore: Arc::new(Semaphore::new(flush_threads)),
        compaction_semaphore: Arc::new(Semaphore::new(compaction_threads)),
        open_snapshots: Arc::new(AtomicU32::new(0)),
        stop_signal: StopSignal::default(),
    };

    let tree = Tree(Arc::new(inner));

    // TODO:
    /* log::debug!("Starting {compaction_threads} compaction threads");
    for _ in 0..compaction_threads {
        start_compaction_thread(&tree);
    } */

    log::info!("Tree loaded in {}s", start.elapsed().as_secs_f32());

    Ok(tree)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{file::FLUSH_MARKER, get_default_partition_key};
    use test_log::test;

    #[test]
    fn tree_flush_on_recover() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let path = folder.path();

        {
            let tree = crate::Config::new(path).open()?;
            tree.insert("abc", "def")?;
            tree.flush()?;
        }

        let journal_dir = std::fs::read_dir(path.join(JOURNALS_FOLDER))?
            .next()
            .expect("should exist")?
            .path();

        let marker = std::fs::File::create(journal_dir.join(FLUSH_MARKER))?;
        marker.sync_all()?;

        let marker = std::fs::File::create(
            journal_dir.join(format!(".pt_{}", get_default_partition_key())),
        )?;
        marker.sync_all()?;

        assert!(journal_dir.join(FLUSH_MARKER).exists());
        assert!(journal_dir
            .join(format!(".pt_{}", get_default_partition_key()))
            .exists());

        {
            let tree = crate::Config::new(path).open()?;
            assert!(tree.get("abc")?.is_some());
        }

        assert!(!journal_dir.exists());

        Ok(())
    }

    #[test]
    fn tree_flush_with_partition_on_recover() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let path = folder.path();

        {
            let tree = crate::Config::new(path).open()?;
            tree.insert("abc", "def")?;
            tree.partition("another_one")?.insert("abc", "xyz")?;
            tree.flush()?;
        }

        let journal_dir = std::fs::read_dir(path.join(JOURNALS_FOLDER))?
            .next()
            .expect("should exist")?
            .path();

        let marker = std::fs::File::create(journal_dir.join(FLUSH_MARKER))?;
        marker.sync_all()?;

        let marker = std::fs::File::create(
            journal_dir.join(format!(".pt_{}", get_default_partition_key())),
        )?;
        marker.sync_all()?;

        let marker = std::fs::File::create(journal_dir.join(".pt_another_one"))?;
        marker.sync_all()?;

        assert!(journal_dir.join(FLUSH_MARKER).exists());
        assert!(journal_dir
            .join(format!(".pt_{}", get_default_partition_key()))
            .exists());

        {
            let tree = crate::Config::new(path).open()?;
            assert_eq!(Some("def".as_bytes().into()), tree.get("abc")?);
        }

        {
            let tree = crate::Config::new(path).open()?;
            assert_eq!(
                Some("xyz".as_bytes().into()),
                tree.partition("another_one")?.get("abc")?
            );
        }

        assert!(!journal_dir.exists());

        Ok(())
    }
}
