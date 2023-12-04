use bloom_filters::BloomFilter as _;
use bloom_filters::{ClassicBloomFilter, DefaultBuildHashKernels, DefaultBuildHasher};
use std::hash::Hash;
use std::io::Write;
use std::path::Path;

const BASE_FP_RATE: f64 = 0.0125;

#[allow(clippy::module_name_repetitions)]
pub struct BloomFilter(ClassicBloomFilter<DefaultBuildHashKernels<DefaultBuildHasher>>);

impl BloomFilter {
    pub fn new(len: usize, fp_rate: f64) -> Self {
        let bloom_filter = ClassicBloomFilter::new(
            len,
            fp_rate,
            DefaultBuildHashKernels::new(0, DefaultBuildHasher),
        );

        Self(bloom_filter)
    }

    // http://daslab.seas.harvard.edu/monkey/
    pub fn monkey(level_count: u8, target_level: u8) -> f64 {
        BASE_FP_RATE * 0.1_f64.powf(f64::from(level_count - target_level - 1))
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

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn test_monkey() {
        dbg!(BloomFilter::monkey(7, 6));
        assert!(f64::abs(BASE_FP_RATE - BloomFilter::monkey(7, 6)) < f64::EPSILON);
        assert!(f64::abs(BASE_FP_RATE / 10.0 - BloomFilter::monkey(7, 5)) < f64::EPSILON);
        assert!(f64::abs(BASE_FP_RATE / 100.0 - BloomFilter::monkey(7, 4)) < f64::EPSILON);
        assert!(f64::abs(BASE_FP_RATE / 1_000.0 - BloomFilter::monkey(7, 3)) < f64::EPSILON);
        assert!(f64::abs(BASE_FP_RATE / 10_000.0 - BloomFilter::monkey(7, 2)) < f64::EPSILON);
        assert!(f64::abs(BASE_FP_RATE / 100_000.0 - BloomFilter::monkey(7, 1)) < f64::EPSILON);
    }
}
