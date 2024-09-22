use core::ops::Bound;

use lsm_tree::Slice;
use std::{
    collections::{
        // BTreeMap,
        BTreeMap,
        BTreeSet,
    },
    ops::RangeBounds,
};

use crate::batch::PartitionKey;

#[derive(Clone, Debug)]
enum Read {
    Single(Slice),
    Range {
        start: Bound<Slice>,
        end: Bound<Slice>,
    },
    All,
}

/// A [`Cm`] conflict manager implementation that based on the [`BTreeSet`](std::collections::BTreeSet).
#[derive(Default, Debug, Clone)]
pub struct BTreeCm {
    reads: BTreeMap<PartitionKey, Vec<Read>>,
    conflict_keys: BTreeMap<PartitionKey, BTreeSet<Slice>>,
}

impl BTreeCm {
    #[inline]
    pub fn mark_read(&mut self, partition: &PartitionKey, key: &Slice) {
        self.reads
            .entry(partition.clone())
            .or_default()
            .push(Read::Single(key.clone()));
    }

    #[inline]
    pub fn mark_conflict(&mut self, partition: &PartitionKey, key: &Slice) {
        self.conflict_keys
            .entry(partition.clone())
            .or_default()
            .insert(key.clone());
    }

    #[inline]
    pub fn has_conflict(&self, other: &Self) -> bool {
        if self.reads.is_empty() {
            return false;
        }

        for (partition, keys) in &self.reads {
            if let Some(other_conflict_keys) = other.conflict_keys.get(partition) {
                for ro in keys.iter() {
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
                                for write in other_conflict_keys.iter() {
                                    if range.contains(&write) {
                                        return true;
                                    }
                                }
                            }
                            (Bound::Unbounded, Bound::Excluded(end)) => {
                                let range = ..end;
                                for write in other_conflict_keys.iter() {
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

    pub fn mark_range(&mut self, partition: &PartitionKey, range: impl RangeBounds<Slice>) {
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

        let reads = self.reads.entry(partition.clone()).or_default();

        if start == Bound::Unbounded && end == Bound::Unbounded {
            reads.push(Read::All);
            return;
        }

        reads.push(Read::Range { start, end });
    }
}

// #[cfg(test)]
// mod test {
//     use super::{BTreeCm, Cm};

//     #[test]
//     fn test_btree_cm() {
//         let mut cm = BTreeCm::<u64>::new(()).unwrap();
//         cm.mark_read(&1);
//         cm.mark_read(&2);
//         cm.mark_conflict(&2);
//         cm.mark_conflict(&3);
//         let cm2 = cm.clone();
//         assert!(cm.has_conflict(&cm2));
//     }
// }
