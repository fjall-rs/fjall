use bloom_filters::BloomFilter as _;
use bloom_filters::{ClassicBloomFilter, DefaultBuildHashKernels, DefaultBuildHasher};
use std::hash::Hash;
use std::io::Write;
use std::path::Path;

const FP_RATE: f64 = 0.01;

#[allow(clippy::module_name_repetitions)]
pub struct BloomFilter(ClassicBloomFilter<DefaultBuildHashKernels<DefaultBuildHasher>>);

impl BloomFilter {
    pub fn new(len: usize) -> Self {
        let bloom_filter = ClassicBloomFilter::new(
            len,
            FP_RATE,
            DefaultBuildHashKernels::new(0, DefaultBuildHasher),
        );

        Self(bloom_filter)
    }

    pub fn insert<K: AsRef<[u8]> + Hash>(&mut self, item: &K) {
        self.0.insert(item);
    }

    pub fn contains<K: AsRef<[u8]> + Hash>(&self, item: &K) -> bool {
        self.0.contains(item)
    }

    // TODO: test that after reading from a file, all items are still readable
    pub fn from_file<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let bytes = std::fs::read(path)?;

        let bloom_filter = ClassicBloomFilter::with_raw_data(
            &bytes,
            0,
            DefaultBuildHashKernels::new(0, DefaultBuildHasher),
        );

        Ok(Self(bloom_filter))
    }

    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()> {
        let bytes = self.0.buckets().raw_data();

        let mut file = std::fs::OpenOptions::new()
            .truncate(true)
            .create(true)
            .write(true)
            .open(path)?;

        file.write_all(&bytes)
    }
}
