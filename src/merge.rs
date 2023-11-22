use crate::{segment::Segment, Value};

/// This iterator can iterate through N segments simultaneously in order
/// This is achieved by advancing the iterators that yield the lowest/highest item
/// and merging using a simple k-way merge algorithm
///
/// If multiple iterators yield the same key value, the freshest one (by timestamp) will be picked
pub struct MergeIterator<'a> {
    iterators: Vec<Box<dyn DoubleEndedIterator<Item = crate::Result<Value>> + 'a>>,
    lo_state: Vec<Option<Value>>,
    hi_state: Vec<Option<Value>>,
    lo_initialized: bool,
    hi_initialized: bool,
}

impl<'a> MergeIterator<'a> {
    /// Initializes a new interleaved iterator
    pub fn new(
        iterators: Vec<Box<dyn DoubleEndedIterator<Item = crate::Result<Value>> + 'a>>,
    ) -> Self {
        let initial_state: Vec<Option<_>> = iterators.iter().map(|_| None).collect();
        let hi_state = initial_state.clone();

        Self {
            iterators,
            lo_state: initial_state,
            hi_state,
            lo_initialized: false,
            hi_initialized: false,
        }
    }

    pub fn from_segments(segments: &Vec<&Segment>) -> crate::Result<Box<MergeIterator<'a>>> {
        let mut iter_vec: Vec<Box<dyn DoubleEndedIterator<Item = crate::Result<Value>>>> =
            Vec::new();

        for segment in segments {
            let iter = Box::new(segment.iter()?);
            iter_vec.push(iter);
        }

        Ok(Box::new(MergeIterator::new(iter_vec)))
    }

    /// Finds the lowest key entry from all iterators
    fn get_min_entry(&self) -> Option<Value> {
        let min_entry = self
            .lo_state
            .iter()
            .filter_map(std::option::Option::as_ref)
            // TODO: use min_by
            .reduce(|min_entry, x| if min_entry.key <= x.key { min_entry } else { x });

        min_entry.cloned()
    }

    /// Finds the highest key entry from all iterators
    fn get_max_entry(&self) -> Option<Value> {
        let max_entry = self
            .hi_state
            .iter()
            .filter_map(std::option::Option::as_ref)
            // TODO: use min_by
            .reduce(|max_entry, x| if max_entry.key >= x.key { max_entry } else { x });

        max_entry.cloned()
    }

    /// Returns the indices of all iterators that should be advanced forward
    fn get_indices_to_advance_forwards(&self, min_entry: &Value) -> Vec<usize> {
        self.lo_state
            .iter()
            .enumerate()
            .filter(|(_, x)| {
                if let Some(x) = x {
                    x.key == min_entry.key
                } else {
                    false
                }
            })
            .map(|(i, _)| i)
            .collect()
    }

    /// Returns the indices of all iterators that should be advanced backward
    fn get_indices_to_advance_backwards(&self, max_entry: &Value) -> Vec<usize> {
        self.hi_state
            .iter()
            .enumerate()
            .filter(|(_, x)| x.is_some())
            .filter(|(_, x)| x.as_ref().unwrap().key == max_entry.key)
            .map(|(i, _)| i)
            .collect()
    }

    /// Advances selected iterators forwards
    fn advance_iterators_forwards(&mut self, indices: Vec<usize>) -> crate::Result<()> {
        for index in indices {
            let iterator = &mut self.iterators[index];
            self.lo_state[index] = iterator.next().transpose()?;
        }
        Ok(())
    }

    /// Advances selected iterators backwards
    fn advance_iterators_backwards(&mut self, indices: Vec<usize>) -> crate::Result<()> {
        for index in indices {
            let iterator = &mut self.iterators[index];
            self.hi_state[index] = iterator.next_back().transpose()?;
        }
        Ok(())
    }

    // TODO: refactor with hi_entry
    fn get_newest_lo_entry(&self, indices: &[usize]) -> Value {
        indices
            .iter()
            .filter_map(|index| self.lo_state.get(*index))
            .flatten()
            // TODO: use max_by
            .reduce(|newer, x| if newer.seqno >= x.seqno { newer } else { x })
            .unwrap()
            .clone()
    }

    fn get_newest_hi_entry(&self, indices: &[usize]) -> Value {
        indices
            .iter()
            .filter_map(|index| self.hi_state.get(*index))
            .flatten()
            // TODO: use max_by
            .reduce(|newer, x| if newer.seqno >= x.seqno { newer } else { x })
            .unwrap()
            .clone()
    }
}

impl<'a> Iterator for MergeIterator<'a> {
    type Item = crate::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.lo_initialized {
            let next_lo_state: Result<Vec<_>, _> = self
                .iterators
                .iter_mut()
                .map(Iterator::next)
                .map(Option::transpose)
                .collect();
            match next_lo_state {
                Ok(next_state) => self.lo_state = next_state,
                Err(error) => return Some(Err(error)),
            }
            self.lo_initialized = true;
        }

        let min_entry = self.get_min_entry();

        match min_entry {
            Some(min_entry) => {
                let to_advance = self.get_indices_to_advance_forwards(&min_entry);
                let newest_entry: Value = self.get_newest_lo_entry(&to_advance);

                let advance_result = self.advance_iterators_forwards(to_advance);
                match advance_result {
                    Ok(_) => {}
                    Err(error) => return Some(Err(error)),
                };

                Some(Ok(newest_entry))
            }
            None => None,
        }
    }
}

impl<'a> DoubleEndedIterator for MergeIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if !self.hi_initialized {
            let next_hi_state: Result<Vec<_>, _> = self
                .iterators
                .iter_mut()
                .map(DoubleEndedIterator::next_back)
                .map(Option::transpose)
                .collect();
            match next_hi_state {
                Ok(next_state) => self.hi_state = next_state,
                Err(error) => return Some(Err(error)),
            }
            self.hi_initialized = true;
        }

        let max_entry = self.get_max_entry();

        match max_entry {
            Some(max_entry) => {
                let to_advance = self.get_indices_to_advance_backwards(&max_entry);
                let newest_entry: Value = self.get_newest_hi_entry(&to_advance);

                let advance_result = self.advance_iterators_backwards(to_advance);
                match advance_result {
                    Ok(_) => {}
                    Err(error) => return Some(Err(error)),
                };

                Some(Ok(newest_entry))
            }
            None => None,
        }
    }
}
