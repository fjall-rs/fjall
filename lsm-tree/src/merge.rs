use crate::{segment::Segment, value::SeqNo, Value};
use min_max_heap::MinMaxHeap;
use std::sync::Arc;

// TODO: use (ParsedInternalKey, UserValue) instead of Value...

pub type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = crate::Result<Value>> + 'a>;

type IteratorIndex = usize;

#[derive(Debug)]
struct IteratorValue((IteratorIndex, Value));

impl std::ops::Deref for IteratorValue {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.0 .1
    }
}

impl PartialEq for IteratorValue {
    fn eq(&self, other: &Self) -> bool {
        self.0 .1 == other.0 .1
    }
}
impl Eq for IteratorValue {}

impl PartialOrd for IteratorValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.0 .1.cmp(&other.0 .1))
    }
}

impl Ord for IteratorValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0 .1.cmp(&other.0 .1)
    }
}

/// This iterator can iterate through N iterators simultaneously in order
/// This is achieved by advancing the iterators that yield the lowest/highest item
/// and merging using a simple k-way merge algorithm
///
/// If multiple iterators yield the same key value, the freshest one (by seqno) will be picked
#[allow(clippy::module_name_repetitions)]
pub struct MergeIterator<'a> {
    iterators: Vec<BoxedIterator<'a>>,
    heap: MinMaxHeap<IteratorValue>,
    evict_old_versions: bool,
    seqno: Option<SeqNo>,
}

impl<'a> MergeIterator<'a> {
    /// Initializes a new merge iterator
    pub fn new(iterators: Vec<BoxedIterator<'a>>) -> Self {
        Self {
            iterators,
            heap: MinMaxHeap::new(),
            evict_old_versions: false,
            seqno: None,
        }
    }

    pub fn evict_old_versions(mut self, v: bool) -> Self {
        self.evict_old_versions = v;
        self
    }

    pub fn snapshot_seqno(mut self, v: SeqNo) -> Self {
        self.seqno = Some(v);
        self
    }

    pub fn from_segments(segments: &[Arc<Segment>]) -> MergeIterator<'a> {
        let mut iter_vec: Vec<Box<dyn DoubleEndedIterator<Item = crate::Result<Value>>>> =
            Vec::with_capacity(segments.len());

        for segment in segments {
            let iter = Box::new(segment.iter(false));
            iter_vec.push(iter);
        }

        MergeIterator::new(iter_vec)
    }

    fn advance_iter(&mut self, idx: usize) -> crate::Result<()> {
        let iterator = self.iterators.get_mut(idx).expect("iter should exist");

        if let Some(value) = iterator.next() {
            self.heap.push(IteratorValue((idx, value?)));
        }

        Ok(())
    }

    fn advance_iter_backwards(&mut self, idx: usize) -> crate::Result<()> {
        let iterator = self.iterators.get_mut(idx).expect("iter should exist");

        if let Some(value) = iterator.next_back() {
            self.heap.push(IteratorValue((idx, value?)));
        }

        Ok(())
    }

    fn push_next(&mut self) -> crate::Result<()> {
        for idx in 0..self.iterators.len() {
            self.advance_iter(idx)?;
        }

        Ok(())
    }

    fn push_next_back(&mut self) -> crate::Result<()> {
        for idx in 0..self.iterators.len() {
            self.advance_iter_backwards(idx)?;
        }

        Ok(())
    }
}

impl<'a> Iterator for MergeIterator<'a> {
    type Item = crate::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() {
            if let Err(e) = self.push_next() {
                return Some(Err(e));
            };
        }

        while let Some(mut head) = self.heap.pop_min() {
            let (iter_idx_consumed, _) = head.0;
            if let Err(e) = self.advance_iter(iter_idx_consumed) {
                return Some(Err(e));
            }

            if head.is_tombstone() || self.evict_old_versions {
                // Tombstone marker OR we want to GC old versions
                // As long as items beneath tombstone are the same key, ignore them
                while let Some(next) = self.heap.pop_min() {
                    if next.key == head.key {
                        let (iter_idx_consumed, _) = next.0;
                        if let Err(e) = self.advance_iter(iter_idx_consumed) {
                            return Some(Err(e));
                        }

                        // If the head is outside our snapshot
                        // we should take the next version
                        if let Some(seqno) = self.seqno {
                            if head.seqno >= seqno {
                                head = next;
                            }
                        }
                    } else {
                        // Reached next user key now
                        // Push back non-conflicting item and exit
                        self.heap.push(next);

                        break;
                    }
                }
            }

            if let Some(seqno) = self.seqno {
                if head.seqno >= seqno {
                    // Filter out seqnos that are too high
                    continue;
                }
            }

            return Some(Ok(head.clone()));
        }

        None
    }
}

impl<'a> DoubleEndedIterator for MergeIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() {
            if let Err(e) = self.push_next_back() {
                return Some(Err(e));
            };
        }

        while let Some(mut head) = self.heap.pop_max() {
            let (iter_idx_consumed, _) = head.0;
            if let Err(e) = self.advance_iter_backwards(iter_idx_consumed) {
                return Some(Err(e));
            }

            let mut reached_tombstone = false;

            if self.evict_old_versions {
                while let Some(next) = self.heap.pop_max() {
                    if next.key == head.key {
                        if reached_tombstone {
                            continue;
                        }

                        let (iter_idx_consumed, _) = next.0;
                        if let Err(e) = self.advance_iter_backwards(iter_idx_consumed) {
                            return Some(Err(e));
                        }

                        if next.is_tombstone() {
                            if let Some(seqno) = self.seqno {
                                if next.seqno < seqno {
                                    reached_tombstone = true;
                                }
                            } else {
                                reached_tombstone = true;
                            }
                        }

                        if let Some(seqno) = self.seqno {
                            if next.seqno < seqno {
                                head = next;
                            }
                        } else {
                            // Keep popping off heap until we reach the next key
                            // Because the seqno's are stored in descending order
                            // The next item will definitely have a higher seqno, so
                            // we can just take it
                            head = next;
                        }
                    } else {
                        // Reached next user key now
                        // Push back non-conflicting item and exit
                        self.heap.push(next);

                        break;
                    }
                }
            }

            if let Some(seqno) = self.seqno {
                if head.seqno >= seqno {
                    // Filter out seqnos that are too high
                    continue;
                }
            }

            if reached_tombstone {
                continue;
            }

            return Some(Ok(head.clone()));
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::ValueType;
    use test_log::test;

    #[test]
    fn test_snapshot_iter() -> crate::Result<()> {
        let vec0 = vec![
            crate::Value::new(1u64.to_be_bytes(), *b"old", 0, ValueType::Value),
            crate::Value::new(2u64.to_be_bytes(), *b"old", 0, ValueType::Value),
            crate::Value::new(3u64.to_be_bytes(), *b"old", 0, ValueType::Value),
        ];

        let vec1 = vec![
            crate::Value::new(1u64.to_be_bytes(), *b"new", 1, ValueType::Value),
            crate::Value::new(2u64.to_be_bytes(), *b"new", 1, ValueType::Value),
            crate::Value::new(3u64.to_be_bytes(), *b"new", 1, ValueType::Value),
        ];

        {
            let iter0 = Box::new(vec0.iter().cloned().map(Ok));
            let iter1 = Box::new(vec1.iter().cloned().map(Ok));

            let merge_iter = MergeIterator::new(vec![iter0, iter1])
                .evict_old_versions(true)
                // NOTE: "1" because the seqno starts at 0
                // When we insert an item, the tree LSN is at 1
                // So the snapshot to get all items with seqno = 0 should have seqno = 1
                .snapshot_seqno(1);
            let items = merge_iter.collect::<crate::Result<Vec<_>>>()?;

            assert_eq!(
                items,
                vec![
                    crate::Value::new(1u64.to_be_bytes(), *b"old", 0, ValueType::Value,),
                    crate::Value::new(2u64.to_be_bytes(), *b"old", 0, ValueType::Value,),
                    crate::Value::new(3u64.to_be_bytes(), *b"old", 0, ValueType::Value,),
                ]
            );
        }

        {
            let iter0 = Box::new(vec0.iter().cloned().map(Ok));
            let iter1 = Box::new(vec1.iter().cloned().map(Ok));

            let merge_iter = MergeIterator::new(vec![iter0, iter1])
                .evict_old_versions(true)
                // NOTE: "1" because the seqno starts at 0
                // When we insert an item, the tree LSN is at 1
                // So the snapshot to get all items with seqno = 0 should have seqno = 1
                .snapshot_seqno(1);
            let items = merge_iter.rev().collect::<crate::Result<Vec<_>>>()?;

            assert_eq!(
                items,
                vec![
                    crate::Value::new(3u64.to_be_bytes(), *b"old", 0, ValueType::Value,),
                    crate::Value::new(2u64.to_be_bytes(), *b"old", 0, ValueType::Value,),
                    crate::Value::new(1u64.to_be_bytes(), *b"old", 0, ValueType::Value,),
                ]
            );
        }

        Ok(())
    }

    #[test]
    fn test_non_overlapping() -> crate::Result<()> {
        let iter0 =
            (0u64..5).map(|x| crate::Value::new(x.to_be_bytes(), *b"old", 0, ValueType::Value));
        let iter1 =
            (5u64..10).map(|x| crate::Value::new(x.to_be_bytes(), *b"new", 3, ValueType::Value));
        let iter2 = (10u64..15)
            .map(|x| crate::Value::new(x.to_be_bytes(), *b"asd", 1, ValueType::Tombstone));
        let iter3 = (15u64..20)
            .map(|x| crate::Value::new(x.to_be_bytes(), *b"qwe", 2, ValueType::Tombstone));

        let iter0 = Box::new(iter0.map(Ok));
        let iter1 = Box::new(iter1.map(Ok));
        let iter2 = Box::new(iter2.map(Ok));
        let iter3 = Box::new(iter3.map(Ok));

        let merge_iter = MergeIterator::new(vec![iter0, iter1, iter2, iter3]);

        for (idx, item) in merge_iter.enumerate() {
            let item = item?;
            assert_eq!(item.key, (idx as u64).to_be_bytes().into());
            //assert_eq!(b"new", &*item.value);
        }

        Ok(())
    }

    #[test]
    fn test_mixed() -> crate::Result<()> {
        let vec0 = vec![
            crate::Value::new(1u64.to_be_bytes(), *b"old", 0, ValueType::Value),
            crate::Value::new(2u64.to_be_bytes(), *b"new", 2, ValueType::Value),
            crate::Value::new(3u64.to_be_bytes(), *b"old", 0, ValueType::Value),
        ];

        let vec1 = vec![
            crate::Value::new(1u64.to_be_bytes(), *b"new", 1, ValueType::Value),
            crate::Value::new(2u64.to_be_bytes(), *b"old", 0, ValueType::Value),
            crate::Value::new(3u64.to_be_bytes(), *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let merge_iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);
        let items = merge_iter.collect::<crate::Result<Vec<_>>>()?;

        assert_eq!(
            items,
            vec![
                crate::Value::new(1u64.to_be_bytes(), *b"new", 1, ValueType::Value,),
                crate::Value::new(2u64.to_be_bytes(), *b"new", 2, ValueType::Value,),
                crate::Value::new(3u64.to_be_bytes(), *b"new", 1, ValueType::Value,),
            ]
        );

        Ok(())
    }

    #[test]
    fn test_forward_merge() -> crate::Result<()> {
        let vec0 = vec![
            crate::Value::new(1u64.to_be_bytes(), *b"old", 0, ValueType::Value),
            crate::Value::new(2u64.to_be_bytes(), *b"old", 0, ValueType::Value),
            crate::Value::new(3u64.to_be_bytes(), *b"old", 0, ValueType::Value),
        ];

        let vec1 = vec![
            crate::Value::new(1u64.to_be_bytes(), *b"new", 1, ValueType::Value),
            crate::Value::new(2u64.to_be_bytes(), *b"new", 1, ValueType::Value),
            crate::Value::new(3u64.to_be_bytes(), *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let merge_iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);
        let items = merge_iter.collect::<crate::Result<Vec<_>>>()?;

        assert_eq!(
            items,
            vec![
                crate::Value::new(1u64.to_be_bytes(), *b"new", 1, ValueType::Value,),
                crate::Value::new(2u64.to_be_bytes(), *b"new", 1, ValueType::Value,),
                crate::Value::new(3u64.to_be_bytes(), *b"new", 1, ValueType::Value,),
            ]
        );

        Ok(())
    }

    #[test]
    fn test_forward_tombstone_shadowing() -> crate::Result<()> {
        let vec0 = vec![
            crate::Value::new(1u64.to_be_bytes(), *b"old", 0, ValueType::Value),
            crate::Value::new(2u64.to_be_bytes(), *b"old", 0, ValueType::Value),
            crate::Value::new(3u64.to_be_bytes(), *b"old", 0, ValueType::Value),
        ];

        let vec1 = vec![
            crate::Value::new(1u64.to_be_bytes(), *b"", 1, ValueType::Tombstone),
            crate::Value::new(2u64.to_be_bytes(), *b"", 1, ValueType::Tombstone),
            crate::Value::new(3u64.to_be_bytes(), *b"", 1, ValueType::Tombstone),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let merge_iter = MergeIterator::new(vec![iter0, iter1]);
        let items = merge_iter.collect::<crate::Result<Vec<_>>>()?;

        assert_eq!(
            items,
            vec![
                crate::Value::new(1u64.to_be_bytes(), *b"", 1, ValueType::Tombstone,),
                crate::Value::new(2u64.to_be_bytes(), *b"", 1, ValueType::Tombstone,),
                crate::Value::new(3u64.to_be_bytes(), *b"", 1, ValueType::Tombstone,),
            ]
        );

        Ok(())
    }

    #[test]
    fn test_rev_merge() -> crate::Result<()> {
        let vec0 = vec![
            crate::Value::new(1u64.to_be_bytes(), *b"old", 0, ValueType::Value),
            crate::Value::new(2u64.to_be_bytes(), *b"old", 0, ValueType::Value),
            crate::Value::new(3u64.to_be_bytes(), *b"old", 0, ValueType::Value),
        ];

        let vec1 = vec![
            crate::Value::new(1u64.to_be_bytes(), *b"new", 1, ValueType::Value),
            crate::Value::new(2u64.to_be_bytes(), *b"new", 1, ValueType::Value),
            crate::Value::new(3u64.to_be_bytes(), *b"new", 1, ValueType::Value),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let merge_iter = MergeIterator::new(vec![iter0, iter1]).evict_old_versions(true);
        let items = merge_iter.rev().collect::<crate::Result<Vec<_>>>()?;

        assert_eq!(
            items,
            vec![
                crate::Value::new(3u64.to_be_bytes(), *b"new", 1, ValueType::Value,),
                crate::Value::new(2u64.to_be_bytes(), *b"new", 1, ValueType::Value,),
                crate::Value::new(1u64.to_be_bytes(), *b"new", 1, ValueType::Value,),
            ]
        );

        Ok(())
    }
}
