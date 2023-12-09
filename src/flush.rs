use crate::{
    compaction::worker::start_compaction_thread,
    descriptor_table::FileDescriptorTable,
    file::{BLOCKS_FILE, JOURNALS_FOLDER, SEGMENTS_FOLDER},
    id::generate_segment_id,
    journal::Journal,
    memtable::MemTable,
    segment::{index::BlockIndex, meta::Metadata, writer::Writer, Segment},
    Tree,
};
use std::{fs::File, path::Path, sync::Arc};

fn flush_worker(
    tree: &Tree,
    old_memtable: &Arc<MemTable>,
    segment_id: &str,
    old_journal_folder: &Path,
) -> crate::Result<()> {
    let segment_folder = tree.config.path.join(SEGMENTS_FOLDER).join(segment_id);

    let mut segment_writer = Writer::new(crate::segment::writer::Options {
        path: segment_folder.clone(),
        evict_tombstones: false,
        block_size: tree.config.block_size,
    })?;

    log::debug!(
        "Flushing memtable -> {}",
        segment_writer.opts.path.display()
    );

    for entry in &old_memtable.items {
        let key = entry.key();
        let value = entry.value();
        segment_writer.write(crate::Value::from(((key.clone()), value.clone())))?;
    }

    segment_writer.finish()?;
    log::debug!("Finalized segment write");

    let metadata = Metadata::from_writer(segment_id.to_string(), segment_writer)?;
    metadata.write_to_file()?;

    let descriptor_table = Arc::new(FileDescriptorTable::new(metadata.path.join(BLOCKS_FILE))?);

    match BlockIndex::from_file(
        segment_id.into(),
        Arc::clone(&descriptor_table),
        &segment_folder,
        Arc::clone(&tree.block_cache),
    )
    .map(Arc::new)
    {
        Ok(block_index) => {
            log::debug!("Read BlockIndex");

            let created_segment = Segment {
                descriptor_table,
                block_index,
                block_cache: Arc::clone(&tree.block_cache),
                metadata,
            };

            log::debug!("flush: acquiring levels manifest");
            let mut levels = tree.levels.write().expect("lock is poisoned");
            levels.add(Arc::new(created_segment));
            levels.write_to_disk()?;
            drop(levels);

            log::debug!("flush: acquiring immu memtables write lock");
            let mut memtable_lock = tree.immutable_memtables.write().expect("lock is poisoned");
            memtable_lock.remove(segment_id);
            drop(memtable_lock);

            log::debug!(
                "Deleting old journal folder: {}",
                old_journal_folder.display()
            );
            std::fs::remove_dir_all(old_journal_folder)?;
        }
        Err(error) => {
            log::error!("Flush worker error: {:?}", error);
        }
    }

    log::debug!("Flush done");

    Ok(())
}

pub fn start(tree: &Tree) -> crate::Result<std::thread::JoinHandle<crate::Result<()>>> {
    log::debug!("Acquiring flush semaphore");
    tree.flush_semaphore.acquire();
    log::trace!("Got flush semaphore");

    log::debug!("flush: acquiring journal full lock");
    let mut journal_lock = tree.journal.shards.full_lock().expect("lock is poisoned");

    log::debug!("flush: acquiring memtable write lock");
    let mut memtable_lock = tree.active_memtable.write().expect("lock is poisoned");

    if memtable_lock.items.is_empty() {
        log::debug!("MemTable is empty (so another thread beat us to it) - aborting flush");

        // TODO: this is a bit stupid, change it
        return Ok(std::thread::spawn(|| Ok(())));
    }

    let old_journal_folder = journal_lock
        .first()
        .expect("journal should have shard")
        .path
        .parent()
        .expect("journal shard should have parent folder")
        .to_path_buf();

    let segment_id = old_journal_folder
        .file_name()
        .expect("invalid journal folder name")
        .to_str()
        .expect("invalid journal folder name")
        .to_string();

    let old_memtable = std::mem::take(&mut *memtable_lock);
    let old_memtable = Arc::new(old_memtable);

    log::debug!("flush: acquiring immu memtable write lock");
    let mut immutable_memtables = tree.immutable_memtables.write().expect("lock is poisoned");
    immutable_memtables.insert(segment_id.clone(), Arc::clone(&old_memtable));

    drop(memtable_lock);

    log::trace!(
        "Marking journal {} as flushable",
        old_journal_folder.display()
    );

    let marker = File::create(old_journal_folder.join(".flush"))?;
    marker.sync_all()?;

    let folder = File::open(&old_journal_folder)?;
    folder.sync_all()?;

    let new_journal_path = tree
        .config
        .path
        .join(JOURNALS_FOLDER)
        .join(generate_segment_id());

    Journal::rotate(new_journal_path, &mut journal_lock)?;

    tree.approx_active_memtable_size
        .store(0, std::sync::atomic::Ordering::Relaxed);

    drop(immutable_memtables);
    drop(journal_lock);

    let tree = tree.clone();

    Ok(std::thread::spawn(move || {
        log::debug!("Starting flush worker");

        if let Err(error) = flush_worker(&tree, &old_memtable, &segment_id, &old_journal_folder) {
            log::error!("Flush thread error: {error:?}");
        };

        log::trace!("Post flush semaphore");
        tree.flush_semaphore.release();

        // Flush done, so notify compaction that a segment was created
        start_compaction_thread(&tree);

        Ok(())
    }))
}
