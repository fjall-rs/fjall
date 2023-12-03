use crate::{
    memtable::MemTable,
    merge::{BoxedIterator, MergeIterator},
    segment::Segment,
    value::{ParsedInternalKey, SeqNo},
    Value,
};
use std::{
    collections::BTreeMap,
    ops::Bound,
    sync::{Arc, RwLockReadGuard},
};

pub struct MemTableGuard<'a> {
    pub(crate) active: RwLockReadGuard<'a, MemTable>,
    pub(crate) immutable: RwLockReadGuard<'a, BTreeMap<String, Arc<MemTable>>>,
}

pub struct Range<'a> {
    guard: MemTableGuard<'a>,
    bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    segments: Vec<Arc<Segment>>,
}

impl<'a> Range<'a> {
    pub fn new(
        guard: MemTableGuard<'a>,
        bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        segments: Vec<Arc<Segment>>,
    ) -> Self {
        Self {
            guard,
            bounds,
            segments,
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct RangeIterator<'a> {
    iter: BoxedIterator<'a>,
}

impl<'a> RangeIterator<'a> {
    fn new(lock: &'a Range<'a>) -> Self {
        let lo = match &lock.bounds.0 {
            // NOTE: See memtable.rs for range explanation
            Bound::Included(key) => {
                Bound::Included(ParsedInternalKey::new(key.clone(), SeqNo::MAX, true))
            }
            Bound::Excluded(key) => {
                Bound::Excluded(ParsedInternalKey::new(key.clone(), SeqNo::MAX, true))
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let hi = match &lock.bounds.0 {
            // NOTE: See memtable.rs for range explanation, this is the reverse case
            // where we need to go all the way to the last seqno of an item
            //
            // Example: We search for (Unbounded..Excluded(abdef))
            //
            // key -> seqno
            //
            // a   -> 7 <<< This is the lowest key that matches the range
            // abc -> 5
            // abc -> 4
            // abc -> 3 <<< This is the highest key that matches the range
            // abcdef -> 6
            // abcdef -> 5
            //
            Bound::Included(key) => Bound::Included(ParsedInternalKey::new(key.clone(), 0, false)),
            Bound::Excluded(key) => Bound::Excluded(ParsedInternalKey::new(key.clone(), 0, false)),
            Bound::Unbounded => Bound::Unbounded,
        };

        let range = (lo, hi);

        let mut segment_iters: Vec<BoxedIterator<'a>> = vec![];

        for segment in &lock.segments {
            let reader = segment
                .range(lock.bounds.clone())
                .expect("failed to init range iter");

            segment_iters.push(Box::new(reader));
        }

        let mut iters: Vec<BoxedIterator<'a>> = vec![Box::new(MergeIterator::new(segment_iters))];

        for (_, memtable) in lock.guard.immutable.iter() {
            iters.push(Box::new(memtable.items.range(range.clone()).map(|entry| {
                Ok(Value::from((entry.key().clone(), entry.value().clone())))
            })));
        }

        let memtable_iter = {
            lock.guard
                .active
                .items
                .range(range)
                .map(|entry| Ok(Value::from((entry.key().clone(), entry.value().clone()))))
        };

        iters.push(Box::new(memtable_iter));

        let iter = Box::new(MergeIterator::new(iters).evict_old_versions(true).filter(
            |x| match x {
                Ok(value) => !value.is_tombstone,
                Err(_) => true,
            },
        ));

        Self { iter }
    }
}

impl<'a> Iterator for RangeIterator<'a> {
    type Item = crate::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.iter.next()?.map(|x| (x.key, x.value)))
    }
}

impl<'a> DoubleEndedIterator for RangeIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        Some(self.iter.next_back()?.map(|x| (x.key, x.value)))
    }
}

impl<'a> IntoIterator for &'a Range<'a> {
    type IntoIter = RangeIterator<'a>;
    type Item = <Self::IntoIter as Iterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        RangeIterator::new(self)
    }
}
