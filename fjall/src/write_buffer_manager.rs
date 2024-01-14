use std::sync::{atomic::AtomicU64, Arc};

#[derive(Clone, Default, Debug)]
pub struct WriteBufferManager(Arc<AtomicU64>);

impl std::ops::Deref for WriteBufferManager {
    type Target = AtomicU64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl WriteBufferManager {
    pub fn get(&self) -> u64 {
        self.load(std::sync::atomic::Ordering::Acquire)
    }

    // Adds some bytes to the write buffer counter
    //
    // Returns the counter *after* incrementing
    pub fn allocate(&self, n: u64) -> u64 {
        let before = self.fetch_add(n, std::sync::atomic::Ordering::AcqRel);
        before + n
    }

    // Frees some bytes from the write buffer counter
    //
    // Returns the counter *after* decrementing
    pub fn free(&self, n: u64) -> u64 {
        use std::sync::atomic::Ordering::{Acquire, SeqCst};

        loop {
            let now = self.load(Acquire);
            let subbed = now.saturating_sub(n);

            if self.compare_exchange(now, subbed, SeqCst, SeqCst).is_ok() {
                return subbed;
            }
        }
    }
}
