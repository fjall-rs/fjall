use crate::{
    compaction::worker::start_compaction_thread,
    descriptor_table::FileDescriptorTable,
    file::{BLOCKS_FILE, FLUSH_MARKER, JOURNALS_FOLDER, PARTITIONS_FOLDER, SEGMENTS_FOLDER},
    id::generate_segment_id,
    journal::Journal,
    memtable::MemTable,
    partition::Partition,
    segment::{index::BlockIndex, meta::Metadata, writer::Writer, Segment},
    time::unix_timestamp,
    Tree,
};
use std::{fs::File, io::Write, path::Path, sync::Arc};

fn flush_worker(
    tree: &Tree,
    old_memtable: &Arc<MemTable>,
    journal_id: &str,
    old_journal_folder: &Path,
    partition: &Partition,
) -> crate::Result<()> {
    let segment_id: Arc<str> = Arc::from(journal_id);
    let segment_folder = tree
        .config
        .path
        .join(PARTITIONS_FOLDER)
        .join(&*partition.name)
        .join(SEGMENTS_FOLDER)
        .join(&*segment_id);

    let mut segment_writer = Writer::new(crate::segment::writer::Options {
        partition: partition.name.clone(),
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

    let metadata = Metadata::from_writer(segment_id.clone(), segment_writer)?;
    metadata.write_to_file()?;

    let descriptor_table = Arc::new(FileDescriptorTable::new(metadata.path.join(BLOCKS_FILE))?);

    /* // TODO:: Don't use global block cache for L0 segments maybe
    // similar to RocksDB's `pin_l0_filter_and_index_blocks_in_cache`
    // Benchmarking didn't yield great results yet though
    let block_cache = Arc::new(BlockCache::with_capacity_blocks(
        metadata.block_count as usize + 512,
    )); */

    match BlockIndex::from_file(
        segment_id.clone(),
        Arc::clone(&descriptor_table),
        &segment_folder,
        Arc::clone(&tree.block_cache),
    )
    .map(Arc::new)
    {
        Ok(block_index) => {
            /* log::debug!("Preloading BlockIndex");
            block_index.preload()?; */

            let created_segment = Segment {
                descriptor_table,
                metadata,
                block_index,
                block_cache: Arc::clone(&tree.block_cache),
            };

            log::debug!("flush: acquiring levels manifest write lock");
            let mut levels = partition.levels.write().expect("lock is poisoned");
            levels.add(Arc::new(created_segment));
            levels.write_to_disk()?;

            log::debug!("flush: acquiring immu memtables write lock");
            let mut memtable_lock = partition
                .immutable_memtables
                .write()
                .expect("lock is poisoned");
            memtable_lock.remove(&segment_id);

            drop(memtable_lock);
            drop(levels);

            log::debug!(
                "Deleting partition marker for old journal: {} -> {}",
                old_journal_folder.display(),
                partition.name
            );
            std::fs::remove_file(old_journal_folder.join(format!(".pt_{}", partition.name)))?;
        }
        Err(error) => {
            log::error!("Flush worker error: {:?}", error);
        }
    }

    log::debug!("Flush done");

    Ok(())
}

pub fn start(
    tree: &Tree,
    partition: &Partition,
) -> crate::Result<std::thread::JoinHandle<crate::Result<()>>> {
    log::debug!("Acquiring flush semaphore");
    tree.flush_semaphore.acquire();
    log::trace!("Got flush semaphore");

    log::debug!("flush: acquiring journal full lock");
    let mut journal_lock = tree.journal.shards.full_lock().expect("lock is poisoned");

    log::debug!("flush: acquiring memtable write lock");
    let mut memtable_lock = partition.active_memtable.write().expect("lock is poisoned");
    let partition_lock = tree.partitions.write().expect("lock is poisoned");

    if memtable_lock.items.is_empty() {
        log::debug!("MemTable is empty (so another thread beat us to it) - aborting flush");

        // TODO: this is a bit stupid, change it
        return Ok(std::thread::spawn(|| Ok(())));
    }

    let old_memtable = std::mem::take(&mut *memtable_lock);
    let old_memtable = Arc::new(old_memtable);

    let old_journal_folder = journal_lock
        .first()
        .expect("journal should have shard")
        .path
        .parent()
        .expect("journal shard should have parent folder")
        .to_path_buf();

    let journal_id: Arc<str> = old_journal_folder
        .file_name()
        .expect("invalid journal folder name")
        .to_str()
        .expect("invalid journal folder name")
        .to_string()
        .into();

    let old_memtable = Arc::new(old_memtable);

    log::debug!("flush: acquiring immu memtable write lock");
    let mut immutable_memtables = partition
        .immutable_memtables
        .write()
        .expect("lock is poisoned");

    immutable_memtables.insert(journal_id.clone(), Arc::clone(&old_memtable));

    log::trace!(
        "Marking journal {} as flushable",
        old_journal_folder.display()
    );

    let marker = File::create(old_journal_folder.join(FLUSH_MARKER))?;
    marker.sync_all()?;

    let mut marker = File::create(old_journal_folder.join(".created_at"))?;
    marker.write_all(unix_timestamp().as_secs().to_string().as_bytes())?;
    marker.sync_all()?;

    for partition in partition_lock.keys() {
        let marker = File::create(old_journal_folder.join(format!(".pt_{partition}")))?;
        marker.sync_all()?;
    }

    #[cfg(not(target_os = "windows"))]
    {
        // Fsync folder on Unix
        let folder = File::open(&old_journal_folder)?;
        folder.sync_all()?;
    }

    drop(partition_lock);
    drop(memtable_lock);

    let new_journal_path = tree
        .config
        .path
        .join(JOURNALS_FOLDER)
        .join(&*generate_segment_id());

    Journal::rotate(new_journal_path, &mut journal_lock)?;

    drop(immutable_memtables);
    drop(journal_lock);

    let tree = tree.clone();
    let partition = partition.clone();

    Ok(std::thread::spawn(move || {
        log::debug!("Starting flush worker");

        if let Err(error) = flush_worker(
            &tree,
            &old_memtable,
            &journal_id,
            &old_journal_folder,
            &partition,
        ) {
            log::error!("Flush thread error: {error:?}");
        };

        log::trace!("Post flush semaphore");
        tree.flush_semaphore.release();

        // Flush done, so notify compaction that a segment was created
        start_compaction_thread(&tree, partition.clone());

        Ok(())
    }))
}
