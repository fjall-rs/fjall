use crate::batch::PartitionKey;
use core::ops::Bound;
use lsm_tree::Slice;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::RangeBounds,
    sync::Mutex,
};

#[derive(Clone, Debug)]
enum Read {
    Single(Slice),
    Range {
        start: Bound<Slice>,
        end: Bound<Slice>,
    },
    All,
}

#[derive(Default, Debug)]
pub struct ConflictManager {
    reads: Mutex<BTreeMap<PartitionKey, Vec<Read>>>,
    conflict_keys: Mutex<BTreeMap<PartitionKey, BTreeSet<Slice>>>,
}

impl ConflictManager {
    fn push_read(&self, partition: &PartitionKey, read: Read) {
        let mut map = self.reads.lock().expect("poisoned reads lock");
        if let Some(tbl) = map.get_mut(partition) {
            tbl.push(read);
        } else {
            map.entry(partition.clone()).or_default().push(read);
        }
    }

    pub fn mark_read(&self, partition: &PartitionKey, key: &Slice) {
        self.push_read(partition, Read::Single(key.clone()));
    }

    pub fn mark_conflict(&self, partition: &PartitionKey, key: &Slice) {
        let mut map = self
            .conflict_keys
            .lock()
            .expect("poisoned conflict keys lock");
        if let Some(tbl) = map.get_mut(partition) {
            tbl.insert(key.clone());
        } else {
            map.entry(partition.clone())
                .or_default()
                .insert(key.clone());
        }
    }

    pub fn mark_range(&self, partition: &PartitionKey, range: impl RangeBounds<Slice>) {
        let start = match range.start_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end = match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let read = if start == Bound::Unbounded && end == Bound::Unbounded {
            Read::All
        } else {
            Read::Range { start, end }
        };

        self.push_read(partition, read);
    }
}

#[derive(Debug)]
pub struct ConflictChecker {
    reads: BTreeMap<PartitionKey, Vec<Read>>,
    conflict_keys: BTreeMap<PartitionKey, BTreeSet<Slice>>,
}

impl ConflictChecker {
    #[allow(clippy::too_many_lines)]
    pub fn has_conflict(&self, other: &Self) -> bool {
        if self.reads.is_empty() {
            return false;
        }

        for (partition, keys) in &self.reads {
            if let Some(other_conflict_keys) = other.conflict_keys.get(partition) {
                for ro in keys {
                    match ro {
                        Read::Single(k) => {
                            if other_conflict_keys.contains(k) {
                                return true;
                            }
                        }
                        Read::Range { start, end } => match (start, end) {
                            (Bound::Included(start), Bound::Included(end)) => {
                                if other_conflict_keys
                                    .range::<Slice, _>((
                                        Bound::Included(start),
                                        Bound::Included(end),
                                    ))
                                    .next()
                                    .is_some()
                                {
                                    return true;
                                }
                            }
                            (Bound::Included(start), Bound::Excluded(end)) => {
                                if other_conflict_keys
                                    .range::<Slice, _>((
                                        Bound::Included(start),
                                        Bound::Excluded(end),
                                    ))
                                    .next()
                                    .is_some()
                                {
                                    return true;
                                }
                            }
                            (Bound::Included(start), Bound::Unbounded) => {
                                if other_conflict_keys
                                    .range::<Slice, _>((Bound::Included(start), Bound::Unbounded))
                                    .next()
                                    .is_some()
                                {
                                    return true;
                                }
                            }
                            (Bound::Excluded(start), Bound::Included(end)) => {
                                if other_conflict_keys
                                    .range::<Slice, _>((
                                        Bound::Excluded(start),
                                        Bound::Included(end),
                                    ))
                                    .next()
                                    .is_some()
                                {
                                    return true;
                                }
                            }
                            (Bound::Excluded(start), Bound::Excluded(end)) => {
                                if other_conflict_keys
                                    .range::<Slice, _>((
                                        Bound::Excluded(start),
                                        Bound::Excluded(end),
                                    ))
                                    .next()
                                    .is_some()
                                {
                                    return true;
                                }
                            }
                            (Bound::Excluded(start), Bound::Unbounded) => {
                                if other_conflict_keys
                                    .range::<Slice, _>((Bound::Excluded(start), Bound::Unbounded))
                                    .next()
                                    .is_some()
                                {
                                    return true;
                                }
                            }
                            (Bound::Unbounded, Bound::Included(end)) => {
                                let range = ..=end;
                                for write in other_conflict_keys {
                                    if range.contains(&write) {
                                        return true;
                                    }
                                }
                            }
                            (Bound::Unbounded, Bound::Excluded(end)) => {
                                let range = ..end;
                                for write in other_conflict_keys {
                                    if range.contains(&write) {
                                        return true;
                                    }
                                }
                            }
                            (Bound::Unbounded, Bound::Unbounded) => unreachable!(),
                        },
                        Read::All => {
                            if !other_conflict_keys.is_empty() {
                                return true;
                            }
                        }
                    }
                }
            }
        }

        false
    }
}

impl From<ConflictManager> for ConflictChecker {
    fn from(value: ConflictManager) -> Self {
        Self {
            reads: std::mem::take(&mut value.reads.lock().expect("reads lock poisoned")),
            conflict_keys: std::mem::take(
                &mut value
                    .conflict_keys
                    .lock()
                    .expect("conflict_keys lock poisoned"),
            ),
        }
    }
}

impl From<ConflictChecker> for ConflictManager {
    fn from(mut value: ConflictChecker) -> Self {
        Self {
            reads: Mutex::new(std::mem::take(&mut value.reads)),
            conflict_keys: Mutex::new(std::mem::take(&mut value.conflict_keys)),
        }
    }
}
