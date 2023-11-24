use crate::{
    commit_log::CommitLog,
    memtable::MemTable,
    segment::{index::MetaIndex, meta::Metadata, writer::Writer, Segment},
    time::unix_timestamp,
    Tree,
};
use std::{
    path::Path,
    sync::{Arc, MutexGuard, RwLockWriteGuard},
};

fn flush_worker(
    tree: &Tree,
    old_memtable: &Arc<MemTable>,
    segment_id: &str,
    old_commit_log_path: &Path,
) -> crate::Result<()> {
    let segment_folder = tree.config.path.join(format!("segments/{segment_id}"));

    let mut segment_writer = Writer::new(crate::segment::writer::Options {
        path: segment_folder.clone(),
        evict_tombstones: false,
        block_size: tree.config.block_size,
    })?;

    log::debug!(
        "Flushing memtable -> {}",
        segment_writer.opts.path.display()
    );

    // TODO: this clone hurts
    for value in old_memtable.items.values().cloned() {
        segment_writer.write(value)?;
    }

    segment_writer.finish()?;
    log::debug!("Finalized segment write");

    let metadata = Metadata::from_writer(segment_id.to_string(), segment_writer);
    metadata.write_to_file()?;

    match MetaIndex::from_file(
        segment_id.into(),
        &segment_folder,
        Arc::clone(&tree.block_cache),
    )
    .map(Arc::new)
    {
        Ok(meta_index) => {
            let created_segment = Segment {
                block_index: meta_index,
                block_cache: Arc::clone(&tree.block_cache),
                metadata,
            };

            let mut levels = tree.levels.write().expect("should lock");
            levels.add(Arc::new(created_segment));
            levels.write_to_disk()?;
            drop(levels);

            log::trace!("Destroying old memtable");
            let mut memtable_lock = tree.immutable_memtables.write().expect("lock poisoned");
            memtable_lock.remove(segment_id);
            drop(memtable_lock);
        }
        Err(error) => {
            log::error!("Flush error: {:?}", error);
        }
    }

    tree.flush_semaphore.release();
    std::fs::remove_file(old_commit_log_path)?;

    log::debug!("Flush done");

    Ok(())
}

pub fn start(
    tree: &Tree,
    mut commit_log_lock: MutexGuard<CommitLog>,
    mut memtable_lock: RwLockWriteGuard<MemTable>,
) -> crate::Result<std::thread::JoinHandle<crate::Result<()>>> {
    log::debug!("Preparing memtable flush thread");
    tree.flush_semaphore.acquire();

    let segment_id = unix_timestamp().as_micros().to_string();
    let old_commit_log_path = tree.config.path.join(format!("logs/{segment_id}"));

    std::fs::rename(tree.config.path.join("log"), old_commit_log_path.clone())?;
    *commit_log_lock = CommitLog::new(tree.config.path.join("log"))?;
    drop(commit_log_lock);

    let old_memtable = Arc::new(std::mem::take(&mut *memtable_lock));
    let mut immutable_memtables = tree.immutable_memtables.write().expect("should lock");
    immutable_memtables.insert(segment_id.clone(), Arc::clone(&old_memtable));
    drop(memtable_lock);

    let tree = tree.clone();

    Ok(std::thread::spawn(move || {
        flush_worker(&tree, &old_memtable, &segment_id, &old_commit_log_path)
    }))
}
