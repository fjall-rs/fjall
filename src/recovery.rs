use std_semaphore::Semaphore;

use crate::{
    compaction::worker::start_compaction_thread,
    descriptor_table::FileDescriptorTable,
    file::{BLOCKS_FILE, JOURNALS_FOLDER, LEVELS_MANIFEST_FILE, SEGMENTS_FOLDER},
    id::generate_segment_id,
    journal::Journal,
    levels::Levels,
    memtable::MemTable,
    segment::{self, Segment},
    stop_signal::StopSignal,
    tree_inner::TreeInner,
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

pub fn recover_active_journal(config: &Config) -> crate::Result<Option<(Journal, MemTable)>> {
    // Load previous levels manifest
    // Add all flushed segments to it, then recover properly
    let mut levels = Levels::recover(config.path.join(LEVELS_MANIFEST_FILE), HashMap::new())?;

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

        if !journal_path.join(".flush").exists() {
            // TODO: handle this
            assert!(active_journal.is_none(), "Second active journal found :(");

            if journal_size < config.max_memtable_size.into() {
                log::info!("Setting {} as active journal", journal_path.display());

                let (recovered_journal, memtable) = Journal::recover(journal_path.clone())?;
                active_journal = Some((recovered_journal, memtable));

                continue;
            }

            log::info!(
                "Flushing active journal because it is too large: {}",
                dirent.path().to_string_lossy()
            );

            // Journal is too large to be continued to be used
            // Just flush it
        }

        log::info!(
            "Flushing orphaned journal {} to segment",
            dirent.path().to_string_lossy()
        );

        // TODO: optimize this

        let (recovered_journal, memtable) = Journal::recover(journal_path.clone())?;
        log::trace!("Recovered old journal");
        drop(recovered_journal);

        let segment_id = dirent
            .file_name()
            .to_str()
            .expect("invalid journal folder name")
            .to_string();
        let segment_folder = config.path.join(SEGMENTS_FOLDER).join(&segment_id);

        if !levels.contains_id(&segment_id) {
            // The level manifest does not contain the segment
            // If the segment is maybe half written, clean it up here
            // and then write it
            if segment_folder.exists() {
                std::fs::remove_dir_all(&segment_folder)?;
            }

            let mut segment_writer = segment::writer::Writer::new(segment::writer::Options {
                path: segment_folder.clone(),
                evict_tombstones: false,
                block_size: config.block_size,
            })?;

            for (key, value) in memtable.items {
                segment_writer.write(crate::Value::from((key, value)))?;
            }

            segment_writer.finish()?;

            if segment_writer.item_count > 0 {
                let metadata = segment::meta::Metadata::from_writer(segment_id, segment_writer)?;
                metadata.write_to_file()?;

                log::info!("Written segment from orphaned journal: {:?}", metadata.id);

                levels.add_id(metadata.id);
                levels.write_to_disk()?;
            }
        }

        std::fs::remove_dir_all(journal_path)?;
    }

    Ok(active_journal)
}

pub fn recover_segments<P: AsRef<Path>>(
    folder: P,
    block_cache: &Arc<BlockCache>,
) -> crate::Result<HashMap<String, Arc<Segment>>> {
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
            .to_owned();
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
            log::info!("Deleting unfinished segment: {}", path.to_string_lossy());
            std::fs::remove_dir_all(path)?;
        }
    }

    if segments.len() < segment_ids_to_recover.len() {
        log::error!("Expected segments : {segment_ids_to_recover:?}");
        log::error!(
            "Recovered segments: {:?}",
            segments.keys().collect::<Vec<_>>()
        );

        panic!("Some segments were not recovered")
    }

    Ok(segments)
}

pub fn recover_tree(config: Config) -> crate::Result<Tree> {
    log::info!("Recovering tree from {}", config.path.display());

    let start = std::time::Instant::now();

    log::info!("Restoring journal");
    let active_journal = crate::recovery::recover_active_journal(&config)?;

    log::info!("Restoring memtable");

    let (journal, memtable) = if let Some(active_journal) = active_journal {
        active_journal
    } else {
        let next_journal_path = config
            .path
            .join(JOURNALS_FOLDER)
            .join(generate_segment_id());
        (Journal::create_new(next_journal_path)?, MemTable::default())
    };

    // TODO: optimize this... do on journal load...
    let lsn = memtable
        .items
        .iter()
        .map(|x| {
            let key = x.key();
            key.seqno + 1
        })
        .max()
        .unwrap_or(0);

    // Load segments
    log::info!("Restoring segments");

    let block_cache = Arc::clone(&config.block_cache);

    let segments = crate::recovery::recover_segments(&config.path, &block_cache)?;

    // Check if a segment has a higher seqno and then take it
    let lsn = lsn.max(
        segments
            .values()
            .map(|x| x.metadata.seqnos.1 + 1)
            .max()
            .unwrap_or(0),
    );

    // Finalize Tree
    log::debug!("Loading level manifest");

    let mut levels = Levels::recover(config.path.join(LEVELS_MANIFEST_FILE), segments)?;
    levels.sort_levels();

    let compaction_threads = 4; // TODO: config
    let flush_threads = config.flush_threads.into();

    // TODO: replace fs extra with Journal::disk_space
    let active_journal_size = fs_extra::dir::get_size(&journal.path)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "fs_extra error"))?;

    let inner = TreeInner {
        config,
        journal: Arc::new(journal),
        active_memtable: Arc::new(RwLock::new(memtable)),
        immutable_memtables: Arc::default(),
        block_cache,
        next_lsn: AtomicU64::new(lsn),
        levels: Arc::new(RwLock::new(levels)),
        flush_semaphore: Arc::new(Semaphore::new(flush_threads)),
        compaction_semaphore: Arc::new(Semaphore::new(compaction_threads)),
        approx_active_memtable_size: AtomicU32::new(active_journal_size as u32),
        open_snapshots: Arc::new(AtomicU32::new(0)),
        stop_signal: StopSignal::default(),
    };

    let tree = Tree(Arc::new(inner));

    log::debug!("Starting {compaction_threads} compaction threads");
    for _ in 0..compaction_threads {
        start_compaction_thread(&tree);
    }

    log::info!("Tree loaded in {}s", start.elapsed().as_secs_f32());

    Ok(tree)
}
