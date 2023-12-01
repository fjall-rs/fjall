use crate::{
    compaction::Choice,
    levels::Levels,
    merge::MergeIterator,
    segment::{index::MetaIndex, writer::MultiWriter, Segment},
    Tree,
};
use std::{
    fs::File,
    io::BufReader,
    sync::{Arc, Mutex, RwLockWriteGuard},
    time::Instant,
};

pub(crate) fn do_compaction(
    tree: &Tree,
    payload: &crate::compaction::Options,
    mut segments_lock: RwLockWriteGuard<'_, Levels>,
) -> crate::Result<()> {
    let start = Instant::now();

    log::debug!(
        "Chosen {} segments to compact into a single new segment at level {}",
        payload.segment_ids.len(),
        payload.dest_level
    );

    let merge_iter = {
        let to_merge: Vec<Arc<Segment>> = {
            let segments = segments_lock.get_segments();
            payload
                .segment_ids
                .iter()
                .filter_map(|x| segments.get(x))
                .cloned()
                .collect()
        };

        MergeIterator::from_segments(&to_merge)?
            .evict_old_versions(false /* TODO: evict if there are no open snapshots */)
    };

    segments_lock.hide_segments(&payload.segment_ids);
    drop(segments_lock);
    log::trace!("Freed segment lock");

    // NOTE: Only evict tombstones when reaching the last level,
    // That way we don't resurrect data beneath the tombstone
    let should_evict_tombstones = payload.dest_level == (tree.config.levels - 1);

    let mut segment_writer = MultiWriter::new(
        payload.target_size,
        crate::segment::writer::Options {
            block_size: tree.config.block_size,
            evict_tombstones: should_evict_tombstones,
            path: tree.path().join("segments"),
        },
    )?;

    for item in merge_iter {
        segment_writer.write(item?)?;
    }

    let created_segments = segment_writer.finish()?;

    for metadata in &created_segments {
        metadata.write_to_file()?;
    }

    let created_segments = created_segments
        .into_iter()
        .map(|metadata| -> crate::Result<Segment> {
            let segment_id = metadata.id.clone();
            let path = metadata.path.clone();

            Ok(Segment {
                file: Mutex::new(BufReader::new(File::open(path.join("blocks"))?)),
                metadata,
                block_cache: Arc::clone(&tree.block_cache),
                block_index: MetaIndex::from_file(segment_id, path, Arc::clone(&tree.block_cache))?
                    .into(),
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;

    log::trace!("Acquiring segment lock");
    let mut segments_lock = tree.levels.write().expect("lock poisoned");

    log::debug!(
        "Compacted in {}ms ({} segments created)",
        start.elapsed().as_millis(),
        created_segments.len()
    );

    // NOTE: Write lock memtable, otherwise segments may get deleted while a range read is happening
    log::trace!("Acquiring memtable lock");
    let memtable_lock = tree.immutable_memtables.write().expect("lock poisoned");

    for segment in created_segments {
        log::trace!("Persisting segment {}", segment.metadata.id);
        segments_lock.insert_into_level(payload.dest_level, segment.into());
    }

    for key in &payload.segment_ids {
        log::trace!("Removing segment {}", key);
        segments_lock.remove(key);
    }

    for key in &payload.segment_ids {
        log::trace!("rm -rf segment folder {}", key);
        std::fs::remove_dir_all(tree.path().join("segments").join(key))?;
    }

    segments_lock.write_to_disk()?;
    segments_lock.show_segments(&payload.segment_ids);

    drop(memtable_lock);
    drop(segments_lock);
    log::debug!("Compaction successful");

    Ok(())
}

pub fn compaction_worker(tree: &Tree) -> crate::Result<()> {
    loop {
        log::trace!("Acquiring segment lock");
        let segments_lock = tree.levels.write().expect("lock poisoned");

        match tree.config.compaction_strategy.choose(&segments_lock) {
            Choice::DoCompact(payload) => {
                do_compaction(tree, &payload, segments_lock)?;
            }
            Choice::DoNothing => {
                log::trace!("Compactor chose to do nothing");
                drop(segments_lock);
                log::trace!("Compaction: Freed segment lock");
                return Ok(());
            }
        }
    }
}

pub fn start_compaction_thread(tree: &Tree) -> std::thread::JoinHandle<crate::Result<()>> {
    let tree = tree.clone();

    std::thread::spawn(move || {
        // TODO: maybe change compaction semaphore to atomic u8
        // TODO: when there are already N compaction threads running, just don't spawn a thread
        tree.compaction_semaphore.acquire();

        log::debug!("Compaction thread received event: New segment flushed");
        compaction_worker(&tree)?;

        log::trace!("Post compaction semaphore");
        tree.compaction_semaphore.release();

        Ok(())
    })
}
