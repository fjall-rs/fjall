use super::HiddenSet;
use crate::{segment::Segment, value::UserKey};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ops::DerefMut, sync::Arc};

#[derive(Serialize, Deserialize)]
pub struct Level(Vec<Arc<str>>);

impl std::ops::Deref for Level {
    type Target = Vec<Arc<str>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Level {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Default for Level {
    fn default() -> Self {
        Self(Vec::with_capacity(20))
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct ResolvedLevel(pub(crate) Vec<Arc<Segment>>);

impl std::ops::Deref for ResolvedLevel {
    type Target = Vec<Arc<Segment>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ResolvedLevel {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl ResolvedLevel {
    pub fn new(
        level: &Level,
        hidden_set: &HiddenSet,
        segments: &HashMap<Arc<str>, Arc<Segment>>,
    ) -> Self {
        let mut new_level = Vec::new();

        for segment_id in level.iter() {
            if !hidden_set.contains(segment_id) {
                new_level.push(
                    segments
                        .get(segment_id)
                        .cloned()
                        .expect("where's the segment at?"),
                );
            }
        }

        Self(new_level)
    }

    /// Gets the level (compressed) size in bytes
    pub fn size(&self) -> u64 {
        self.iter().map(|x| x.metadata.file_size).sum()
    }

    pub fn get_overlapping_segments(&self, start: UserKey, end: UserKey) -> Vec<Arc<str>> {
        use std::ops::Bound::Included;

        let bounds = (Included(start), Included(end));

        self.0
            .iter()
            .filter(|x| Segment::check_key_range_overlap(x, &bounds))
            .map(|x| &x.metadata.id)
            .cloned()
            .collect()
    }
}
