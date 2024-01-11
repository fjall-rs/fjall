use std::collections::VecDeque;

pub struct LruList<T: Clone + Eq + PartialEq> {
    items: VecDeque<T>,
}

impl<T: Clone + Eq + PartialEq> Default for LruList<T> {
    fn default() -> Self {
        Self {
            items: VecDeque::default(),
        }
    }
}

impl<T: Clone + Eq + PartialEq> LruList<T> {
    #[must_use]
    pub fn with_capacity(n: usize) -> Self {
        Self {
            items: VecDeque::with_capacity(n),
        }
    }

    pub fn remove_by(&mut self, f: impl FnMut(&T) -> bool) {
        self.items.retain(f);
    }

    pub fn remove(&mut self, item: &T) {
        self.remove_by(|x| x != item);
    }

    pub fn refresh(&mut self, item: T) {
        self.remove(&item);
        self.items.push_back(item);
    }

    pub fn get_least_recently_used(&mut self) -> Option<T> {
        let front = self.items.pop_front()?;
        self.items.push_back(front.clone());
        Some(front)
    }
}
