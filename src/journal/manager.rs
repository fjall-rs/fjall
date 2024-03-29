use super::shard::JournalShard;
use crate::{
    batch::PartitionKey,
    file::{FLUSH_MARKER, FLUSH_PARTITIONS_LIST},
    journal::Journal,
    PartitionHandle,
};
use lsm_tree::{id::generate_segment_id, SeqNo};
use std::{collections::HashMap, fs::File, io::Write, path::PathBuf, sync::RwLockWriteGuard};

pub struct PartitionSeqNo {
    pub(crate) partition: PartitionHandle,
    pub(crate) lsn: SeqNo,
}

impl std::fmt::Debug for PartitionSeqNo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.lsn)
    }
}

pub struct Item {
    pub(crate) path: PathBuf,
    pub(crate) size_in_bytes: u64,
    pub(crate) partition_seqnos: HashMap<PartitionKey, PartitionSeqNo>,
}

impl std::fmt::Debug for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "JournalManagerItem {} => {:#?}",
            self.path.display(),
            self.partition_seqnos
        )
    }
}

// TODO: accessing journal manager shouldn't take RwLock... but changing its internals should

/// The [`JournalManager`] keeps track of sealed journals that are being flushed.
///
/// Each journal may contain items of different partitions.
#[allow(clippy::module_name_repetitions)]
pub struct JournalManager {
    active_path: PathBuf,
    items: Vec<Item>,
    disk_space_in_bytes: u64,
}

impl JournalManager {
    pub(crate) fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            active_path: path.into(),
            items: Vec::with_capacity(10),
            disk_space_in_bytes: 0,
        }
    }

    pub(crate) fn enqueue(&mut self, item: Item) {
        self.disk_space_in_bytes += item.size_in_bytes;
        self.items.push(item);
    }

    /// Returns the amount of journals
    pub(crate) fn journal_count(&self) -> usize {
        // NOTE: + 1 = active journal
        self.sealed_journal_count() + 1
    }

    /// Returns the amount of sealed journals
    pub(crate) fn sealed_journal_count(&self) -> usize {
        self.items.len()
    }

    /// Returns the amount of bytes used on disk by journals
    pub(crate) fn disk_space_used(&self) -> u64 {
        self.disk_space_in_bytes
    }

    /// Enqueues partitions to be flushed so that the oldest journal can be safely evicted
    pub(crate) fn get_partitions_to_flush_for_oldest_journal_eviction(
        &self,
    ) -> Vec<PartitionHandle> {
        let mut items = vec![];

        if let Some(item) = self.items.first() {
            for item in item.partition_seqnos.values() {
                let Some(partition_seqno) = item.partition.tree.get_segment_lsn() else {
                    items.push(item.partition.clone());
                    continue;
                };

                if partition_seqno < item.lsn {
                    items.push(item.partition.clone());
                }
            }
        }

        items
    }

    /// Performs maintenance, maybe deleting some old journals
    pub(crate) fn maintenance(&mut self) -> crate::Result<()> {
        // NOTE: Walk backwards because of shifting indices
        'outer: for idx in (0..self.items.len()).rev() {
            let Some(item) = &self.items.get(idx) else {
                continue 'outer;
            };

            // TODO: unit test: check deleted partition does not prevent journal eviction
            for item in item.partition_seqnos.values() {
                // Only check partition seqno if not deleted
                if !item
                    .partition
                    .is_deleted
                    .load(std::sync::atomic::Ordering::Acquire)
                {
                    let Some(partition_seqno) = item.partition.tree.get_segment_lsn() else {
                        continue 'outer;
                    };

                    if partition_seqno < item.lsn {
                        continue 'outer;
                    }
                }
            }

            // NOTE: Once the LSN of *every* partition's segments [1] is higher than the journal's stored partition seqno,
            // it can be deleted from disk, as we know the entire journal has been flushed to segments [2].
            //
            // [1] We cannot use the partition's max seqno, because the memtable will get writes, which increase the seqno.
            // We *need* to check the disk segments specifically, they are the source of truth for flushed data.
            //
            // [2] Checking the seqno is safe because the queues inside the flush manager are FIFO.
            //
            // IMPORTANT: On recovery, the journals need to be flushed from oldest to newest.
            log::trace!("Removing fully flushed journal at {}", item.path.display());
            std::fs::remove_dir_all(&item.path)?;

            self.disk_space_in_bytes -= item.size_in_bytes;
            self.items.remove(idx);
        }

        Ok(())
    }

    pub(crate) fn rotate_journal(
        &mut self,
        journal_lock: &mut [RwLockWriteGuard<'_, JournalShard>],
        seqnos: HashMap<PartitionKey, PartitionSeqNo>,
    ) -> crate::Result<()> {
        let old_journal_path = self.active_path.clone();

        log::debug!("Sealing journal at {}", old_journal_path.display());

        let mut file = File::create(old_journal_path.join(FLUSH_PARTITIONS_LIST))?;

        for (name, item) in &seqnos {
            writeln!(file, "{name}:{}", item.lsn)?;
        }
        file.sync_all()?;

        let marker = File::create(old_journal_path.join(FLUSH_MARKER))?;
        marker.sync_all()?;

        #[cfg(not(target_os = "windows"))]
        {
            // fsync folder on Unix
            let folder = File::open(&old_journal_path)?;
            folder.sync_all()?;
        }

        let new_journal_path = old_journal_path
            .parent()
            .expect("should have parent")
            .join(&*generate_segment_id());

        log::trace!("journal manager: acquiring journal full lock");
        Journal::rotate(&new_journal_path, journal_lock)?;

        self.active_path = new_journal_path;

        // TODO: Journal::disk_space

        let journal_size = fs_extra::dir::get_size(&old_journal_path)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{:?}", e.kind)))?;

        self.enqueue(Item {
            path: old_journal_path,
            partition_seqnos: seqnos,
            size_in_bytes: journal_size,
        });

        Ok(())
    }
}
