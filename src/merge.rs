use crate::{segment::Segment, Value};
use min_max_heap::MinMaxHeap;
use std::sync::Arc;

type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = crate::Result<Value>> + 'a>;

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
pub struct MergeIterator<'a> {
    iterators: Vec<BoxedIterator<'a>>,
    heap: MinMaxHeap<IteratorValue>,
}

impl<'a> MergeIterator<'a> {
    /// Initializes a new merge iterator
    pub fn new(iterators: Vec<BoxedIterator<'a>>) -> Self {
        Self {
            iterators,
            heap: MinMaxHeap::new(),
        }
    }

    pub fn from_segments(segments: &[Arc<Segment>]) -> crate::Result<Box<MergeIterator<'a>>> {
        let mut iter_vec: Vec<Box<dyn DoubleEndedIterator<Item = crate::Result<Value>>>> =
            Vec::new();

        for segment in segments {
            let iter = Box::new(segment.iter()?);
            iter_vec.push(iter);
        }

        Ok(Box::new(MergeIterator::new(iter_vec)))
    }

    fn advance_iter(&mut self, idx: usize) -> crate::Result<()> {
        let iterator = self.iterators.get_mut(idx).unwrap();

        if let Some(value) = iterator.next() {
            self.heap.push(IteratorValue((idx, value?)));
        }

        Ok(())
    }

    fn advance_iter_backwards(&mut self, idx: usize) -> crate::Result<()> {
        let iterator = self.iterators.get_mut(idx).unwrap();

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

        if let Some(mut head) = self.heap.pop_min() {
            let (iter_idx_consumed, _) = head.0;
            if let Err(e) = self.advance_iter(iter_idx_consumed) {
                return Some(Err(e));
            }

            while let Some(next) = self.heap.pop_min() {
                if head.key == next.key {
                    let (iter_idx_consumed, _) = next.0;
                    if let Err(e) = self.advance_iter(iter_idx_consumed) {
                        return Some(Err(e));
                    }

                    head = if head.seqno > next.seqno { head } else { next };
                } else {
                    // Push back the non-conflicting item.
                    self.heap.push(next);
                    break;
                }
            }

            Some(Ok(head.clone()))
        } else {
            None
        }
    }
}

impl<'a> DoubleEndedIterator for MergeIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() {
            if let Err(e) = self.push_next_back() {
                return Some(Err(e));
            };
        }

        if let Some(mut head) = self.heap.pop_max() {
            let (iter_idx_consumed, _) = head.0;
            if let Err(e) = self.advance_iter_backwards(iter_idx_consumed) {
                return Some(Err(e));
            }

            while let Some(next) = self.heap.pop_max() {
                if head.key == next.key {
                    let (iter_idx_consumed, _) = next.0;
                    if let Err(e) = self.advance_iter_backwards(iter_idx_consumed) {
                        return Some(Err(e));
                    }

                    head = if head.seqno > next.seqno { head } else { next };
                } else {
                    // Push back the non-conflicting item.
                    self.heap.push(next);
                    break;
                }
            }

            Some(Ok(head.clone()))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn test_non_overlapping() -> crate::Result<()> {
        let iter0 = (0u64..5).map(|x| crate::Value::new(x.to_be_bytes(), "old", false, 0));
        let iter1 = (5u64..10).map(|x| crate::Value::new(x.to_be_bytes(), "new", false, 3));
        let iter2 = (10u64..15).map(|x| crate::Value::new(x.to_be_bytes(), "asd", true, 1));
        let iter3 = (15u64..20).map(|x| crate::Value::new(x.to_be_bytes(), "qwe", true, 2));

        let iter0 = Box::new(iter0.map(Ok));
        let iter1 = Box::new(iter1.map(Ok));
        let iter2 = Box::new(iter2.map(Ok));
        let iter3 = Box::new(iter3.map(Ok));

        let start = std::time::Instant::now();

        let merge_iter = MergeIterator::new(vec![iter0, iter1, iter2, iter3]);

        for (idx, item) in merge_iter.enumerate() {
            let item = item?;
            assert_eq!(item.key, (idx as u64).to_be_bytes());
            //assert_eq!(b"new", &*item.value);
        }

        eprintln!("took {}ms", start.elapsed().as_millis());

        Ok(())
    }

    #[test]
    fn test_mixed() -> crate::Result<()> {
        let vec0 = vec![
            crate::Value::new(1u64.to_be_bytes(), "old", false, 0),
            crate::Value::new(2u64.to_be_bytes(), "new", false, 2),
            crate::Value::new(3u64.to_be_bytes(), "old", false, 0),
        ];

        let vec1 = vec![
            crate::Value::new(1u64.to_be_bytes(), "new", false, 1),
            crate::Value::new(2u64.to_be_bytes(), "old", false, 0),
            crate::Value::new(3u64.to_be_bytes(), "new", false, 1),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let merge_iter = MergeIterator::new(vec![iter0, iter1]);
        let items = merge_iter.collect::<crate::Result<Vec<_>>>()?;

        assert_eq!(
            items,
            vec![
                crate::Value::new(1u64.to_be_bytes(), "new", false, 1),
                crate::Value::new(2u64.to_be_bytes(), "new", false, 2),
                crate::Value::new(3u64.to_be_bytes(), "new", false, 1),
            ]
        );

        Ok(())
    }

    #[test]
    fn test_forward_merge() -> crate::Result<()> {
        let vec0 = vec![
            crate::Value::new(1u64.to_be_bytes(), "old", false, 0),
            crate::Value::new(2u64.to_be_bytes(), "old", false, 0),
            crate::Value::new(3u64.to_be_bytes(), "old", false, 0),
        ];

        let vec1 = vec![
            crate::Value::new(1u64.to_be_bytes(), "new", false, 1),
            crate::Value::new(2u64.to_be_bytes(), "new", false, 1),
            crate::Value::new(3u64.to_be_bytes(), "new", false, 1),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let merge_iter = MergeIterator::new(vec![iter0, iter1]);
        let items = merge_iter.collect::<crate::Result<Vec<_>>>()?;

        assert_eq!(
            items,
            vec![
                crate::Value::new(1u64.to_be_bytes(), "new", false, 1),
                crate::Value::new(2u64.to_be_bytes(), "new", false, 1),
                crate::Value::new(3u64.to_be_bytes(), "new", false, 1),
            ]
        );

        Ok(())
    }

    #[test]
    fn test_rev_merge() -> crate::Result<()> {
        let vec0 = vec![
            crate::Value::new(1u64.to_be_bytes(), "old", false, 0),
            crate::Value::new(2u64.to_be_bytes(), "old", false, 0),
            crate::Value::new(3u64.to_be_bytes(), "old", false, 0),
        ];

        let vec1 = vec![
            crate::Value::new(1u64.to_be_bytes(), "new", false, 1),
            crate::Value::new(2u64.to_be_bytes(), "new", false, 1),
            crate::Value::new(3u64.to_be_bytes(), "new", false, 1),
        ];

        let iter0 = Box::new(vec0.iter().cloned().map(Ok));
        let iter1 = Box::new(vec1.iter().cloned().map(Ok));

        let merge_iter = MergeIterator::new(vec![iter0, iter1]);
        let items = merge_iter.rev().collect::<crate::Result<Vec<_>>>()?;

        assert_eq!(
            items,
            vec![
                crate::Value::new(3u64.to_be_bytes(), "new", false, 1),
                crate::Value::new(2u64.to_be_bytes(), "new", false, 1),
                crate::Value::new(1u64.to_be_bytes(), "new", false, 1),
            ]
        );

        Ok(())
    }
}
