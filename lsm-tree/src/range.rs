use crate::{
    memtable::MemTable,
    merge::{BoxedIterator, MergeIterator},
    segment::Segment,
    value::{ParsedInternalKey, SeqNo, UserKey, UserValue, ValueType},
    Value,
};
use guardian::ArcRwLockReadGuardian;
use std::{collections::BTreeMap, ops::Bound, sync::Arc};

pub struct MemTableGuard {
    pub(crate) active: ArcRwLockReadGuardian<MemTable>,
    pub(crate) sealed: ArcRwLockReadGuardian<BTreeMap<Arc<str>, Arc<MemTable>>>,
}

pub struct Range {
    guard: MemTableGuard,
    bounds: (Bound<UserKey>, Bound<UserKey>),
    segments: Vec<Arc<Segment>>,
    seqno: Option<SeqNo>,
}

impl Range {
    #[must_use]
    pub fn new(
        guard: MemTableGuard,
        bounds: (Bound<UserKey>, Bound<UserKey>),
        segments: Vec<Arc<Segment>>,
        seqno: Option<SeqNo>,
    ) -> Self {
        Self {
            guard,
            bounds,
            segments,
            seqno,
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct RangeIterator<'a> {
    iter: BoxedIterator<'a>,
}

impl<'a> RangeIterator<'a> {
    fn new(lock: &'a Range, seqno: Option<SeqNo>) -> Self {
        let lo = match &lock.bounds.0 {
            // NOTE: See memtable.rs for range explanation
            Bound::Included(key) => Bound::Included(ParsedInternalKey::new(
                key.clone(),
                SeqNo::MAX,
                crate::value::ValueType::Tombstone,
            )),
            Bound::Excluded(key) => Bound::Excluded(ParsedInternalKey::new(
                key.clone(),
                0,
                crate::value::ValueType::Tombstone,
            )),
            Bound::Unbounded => Bound::Unbounded,
        };

        let hi = match &lock.bounds.1 {
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
            Bound::Included(key) => Bound::Included(ParsedInternalKey::new(
                key.clone(),
                0,
                crate::value::ValueType::Value,
            )),
            Bound::Excluded(key) => Bound::Excluded(ParsedInternalKey::new(
                key.clone(),
                0,
                crate::value::ValueType::Value,
            )),
            Bound::Unbounded => Bound::Unbounded,
        };

        let range = (lo, hi);

        let mut segment_iters: Vec<BoxedIterator<'a>> = vec![];

        for segment in &lock.segments {
            let reader = segment.range(lock.bounds.clone());
            segment_iters.push(Box::new(reader));
        }

        let mut iters: Vec<BoxedIterator<'a>> = vec![Box::new(MergeIterator::new(segment_iters))];

        for (_, memtable) in lock.guard.sealed.iter() {
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

        let mut iter = MergeIterator::new(iters).evict_old_versions(true);

        if let Some(seqno) = seqno {
            iter = iter.snapshot_seqno(seqno);
        }

        let iter = Box::new(iter.filter(|x| match x {
            Ok(value) => value.value_type != ValueType::Tombstone,
            Err(_) => true,
        }));

        Self { iter }
    }
}

impl<'a> Iterator for RangeIterator<'a> {
    type Item = crate::Result<(UserKey, UserValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.iter.next()?.map(|x| (x.key, x.value)))
    }
}

impl<'a> DoubleEndedIterator for RangeIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        Some(self.iter.next_back()?.map(|x| (x.key, x.value)))
    }
}

impl<'a> IntoIterator for &'a Range {
    type IntoIter = RangeIterator<'a>;
    type Item = <Self::IntoIter as Iterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        RangeIterator::new(self, self.seqno)
    }
}
