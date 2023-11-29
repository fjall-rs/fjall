use crate::{segment::Segment, Value};
use min_max_heap::MinMaxHeap;
use std::sync::Arc;

type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = crate::Result<Value>> + 'a>;

/// This iterator can iterate through N iterators simultaneously in order
/// This is achieved by advancing the iterators that yield the lowest/highest item
/// and merging using a simple k-way merge algorithm
///
/// If multiple iterators yield the same key value, the freshest one (by seqno) will be picked
pub struct MergeIterator<'a> {
    iterators: Vec<BoxedIterator<'a>>,
    heap: MinMaxHeap<Value>,
}

impl<'a> MergeIterator<'a> {
    /// Initializes a new merge iterator
    pub fn new(iterators: Vec<BoxedIterator<'a>>) -> Self {
        Self {
            iterators,
            heap: MinMaxHeap::new(),
        }
    }

    pub fn from_segments(segments: &Vec<Arc<Segment>>) -> crate::Result<Box<MergeIterator<'a>>> {
        let mut iter_vec: Vec<Box<dyn DoubleEndedIterator<Item = crate::Result<Value>>>> =
            Vec::new();

        for segment in segments {
            let iter = Box::new(segment.iter()?);
            iter_vec.push(iter);
        }

        Ok(Box::new(MergeIterator::new(iter_vec)))
    }

    fn push_next(&mut self) -> crate::Result<()> {
        for iterator in &mut self.iterators {
            if let Some(result) = iterator.next() {
                let value = result?;
                self.heap.push(value);
            }
        }

        Ok(())
    }

    fn push_next_back(&mut self) -> crate::Result<()> {
        for iterator in &mut self.iterators {
            if let Some(result) = iterator.next_back() {
                let value = result?;
                self.heap.push(value);
            }
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
            while let Some(next) = self.heap.pop_min() {
                if head.key == next.key {
                    head = if head.seqno > next.seqno { head } else { next };
                } else {
                    // Push back the non-conflicting item.
                    self.heap.push(next);
                    break;
                }
            }

            if let Err(e) = self.push_next() {
                return Some(Err(e));
            };

            Some(Ok(head))
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
            while let Some(next) = self.heap.pop_max() {
                if head.key == next.key {
                    head = if head.seqno > next.seqno { head } else { next };
                } else {
                    // Push back the non-conflicting item.
                    self.heap.push(next);
                    break;
                }
            }

            if let Err(e) = self.push_next_back() {
                return Some(Err(e));
            };

            Some(Ok(head))
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
    fn test_big() -> crate::Result<()> {
        let iter0 = (000u64..100).map(|x| crate::Value::new(x.to_be_bytes(), "old", false, 0));
        let iter1 = (100u64..200).map(|x| crate::Value::new(x.to_be_bytes(), "new", false, 3));
        let iter2 = (200u64..300).map(|x| crate::Value::new(x.to_be_bytes(), "asd", true, 1));
        let iter3 = (300u64..400).map(|x| crate::Value::new(x.to_be_bytes(), "qwe", true, 2));

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
