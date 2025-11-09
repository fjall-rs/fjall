use crate::keyspace::InternalKeyspaceId;
use core::ops::Bound;
use lsm_tree::Slice;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::RangeBounds,
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
    reads: BTreeMap<InternalKeyspaceId, Vec<Read>>,
    conflict_keys: BTreeMap<InternalKeyspaceId, BTreeSet<Slice>>,
}

impl ConflictManager {
    fn push_read(&mut self, keyspace_id: InternalKeyspaceId, read: Read) {
        if let Some(tbl) = self.reads.get_mut(&keyspace_id) {
            tbl.push(read);
        } else {
            self.reads.entry(keyspace_id).or_default().push(read);
        }
    }

    pub fn mark_read(&mut self, keyspace_id: InternalKeyspaceId, key: Slice) {
        self.push_read(keyspace_id, Read::Single(key));
    }

    pub fn mark_conflict(&mut self, keyspace_id: InternalKeyspaceId, key: Slice) {
        if let Some(tbl) = self.conflict_keys.get_mut(&keyspace_id) {
            tbl.insert(key);
        } else {
            self.conflict_keys
                .entry(keyspace_id)
                .or_default()
                .insert(key);
        }
    }

    pub fn mark_range(&mut self, keyspace_id: InternalKeyspaceId, range: impl RangeBounds<Slice>) {
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

        self.push_read(keyspace_id, read);
    }

    #[allow(clippy::too_many_lines)]
    pub fn has_conflict(&self, other: &Self) -> bool {
        if self.reads.is_empty() {
            return false;
        }

        for (keyspace_name, keys) in &self.reads {
            if let Some(other_conflict_keys) = other.conflict_keys.get(keyspace_name) {
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
