use crate::serde::{Deserializable, Serializable};
use crate::{DeserializeError, SerializeError};
use bit_vec::BitVec;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use seahash::SeaHasher;
use std::fs::File;
use std::hash::Hasher;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

/// A basic bloom filter
#[derive(Debug)]
pub struct BloomFilter {
    /// Raw bytes exposed as bit field
    inner: BitVec,

    /// Bit count
    m: usize,

    /// Number of hash functions
    k: usize,
}

impl Serializable for BloomFilter {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        writer.write_u64::<BigEndian>(self.m as u64)?;
        writer.write_u64::<BigEndian>(self.k as u64)?;
        writer.write_all(&self.inner.to_bytes())?;
        Ok(())
    }
}

impl Deserializable for BloomFilter {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let m = reader.read_u64::<BigEndian>()? as usize;
        let k = reader.read_u64::<BigEndian>()? as usize;

        let mut bytes = vec![0; m / 8];
        reader.read_exact(&mut bytes)?;

        Ok(Self::from_raw(m, k, &bytes))
    }
}

impl BloomFilter {
    /// Stores a bloom filter to a file
    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), SerializeError> {
        let mut writer = BufWriter::with_capacity(128_000, File::create(path)?);
        self.serialize(&mut writer)?;
        writer.flush()?;
        writer.get_mut().sync_all()?;
        Ok(())
    }

    /// Loads a bloom filter from a file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, DeserializeError> {
        let mut reader = BufReader::with_capacity(128_000, File::open(path)?);
        Self::deserialize(&mut reader)
    }

    fn from_raw(m: usize, k: usize, bytes: &[u8]) -> Self {
        Self {
            inner: BitVec::from_bytes(bytes),
            m,
            k,
        }
    }

    pub(crate) fn calculate_m(n: usize, fp_rate: f32) -> usize {
        use std::f32::consts::LN_2;

        let n = n as f32;
        let ln2_squared = LN_2.powi(2);

        let m = -(n * fp_rate.ln() / ln2_squared);
        ((m / 8.0).ceil() * 8.0) as usize
    }

    /// Heuristically get the somewhat-optimal k value for a given desired FPR
    fn get_k_heuristic(fp_rate: f32) -> usize {
        match fp_rate {
            _ if fp_rate > 0.4 => 1,
            _ if fp_rate > 0.2 => 2,
            _ if fp_rate > 0.1 => 3,
            _ if fp_rate > 0.05 => 4,
            _ if fp_rate > 0.03 => 5,
            _ if fp_rate > 0.02 => 5,
            _ if fp_rate > 0.01 => 7,
            _ if fp_rate > 0.001 => 10,
            _ if fp_rate > 0.0001 => 13,
            _ if fp_rate > 0.00001 => 17,
            _ => 20,
        }
    }

    /// Constructs a bloom filter that can hold `item_count` items
    /// while maintaining a certain false positive rate.
    #[must_use]
    pub fn with_fp_rate(item_count: usize, fp_rate: f32) -> Self {
        // NOTE: Some sensible minimum
        let fp_rate = fp_rate.max(0.000_001);

        let k = Self::get_k_heuristic(fp_rate);
        let m = Self::calculate_m(item_count, fp_rate);

        Self {
            inner: BitVec::from_elem(m, false),
            m,
            k,
        }
    }

    /// Returns `true` if the item may be contained.
    ///
    /// Will never have a false negative.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        let (mut h1, mut h2) = Self::get_hash(key);

        for i in 0..(self.k as u64) {
            let idx = h1 % (self.m as u64);

            if !self.inner.get(idx as usize).expect("should be in bounds") {
                return false;
            }

            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_add(i);
        }

        true
    }

    /// Adds the key to the filter
    pub fn set_with_hash(&mut self, (mut h1, mut h2): (u64, u64)) {
        for i in 0..(self.k as u64) {
            let idx = h1 % (self.m as u64);

            self.enable_bit(idx as usize);

            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_add(i);
        }
    }

    /// Sets the bit at the given index to `true`
    fn enable_bit(&mut self, idx: usize) {
        self.inner.set(idx, true);
    }

    /// Gets the hash of a key
    #[must_use]
    pub fn get_hash(key: &[u8]) -> (u64, u64) {
        let mut hasher = SeaHasher::default();
        hasher.write(key);
        let h1 = hasher.finish();

        hasher.write(key);
        let h2 = hasher.finish();

        (h1, h2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn bloom_calculate_m() {
        assert_eq!(9_592, BloomFilter::calculate_m(1_000, 0.01));
        assert_eq!(4_800, BloomFilter::calculate_m(1_000, 0.1));
        assert_eq!(4_792_536, BloomFilter::calculate_m(1_000_000, 0.1));
    }

    #[test]
    fn bloom_basic() {
        let mut filter = BloomFilter::with_fp_rate(10, 0.0001);

        for key in [
            b"item0", b"item1", b"item2", b"item3", b"item4", b"item5", b"item6", b"item7",
            b"item8", b"item9",
        ] {
            assert!(!filter.contains(key));
            filter.set_with_hash(BloomFilter::get_hash(key));
            assert!(filter.contains(key));

            assert!(!filter.contains(b"asdasdasdasdasdasdasd"));
        }
    }

    #[test]
    fn bloom_fpr() {
        let item_count = 1_000_000;
        let fpr = 0.01;

        let mut filter = BloomFilter::with_fp_rate(item_count, fpr);

        for key in (0..item_count).map(|_| nanoid::nanoid!()) {
            let key = key.as_bytes();

            filter.set_with_hash(BloomFilter::get_hash(key));
            assert!(filter.contains(key));
        }

        let mut false_positives = 0;

        for key in (0..item_count).map(|_| nanoid::nanoid!()) {
            let key = key.as_bytes();

            if filter.contains(key) {
                false_positives += 1;
            }
        }

        assert!((10_000 - false_positives) < 200);
    }
}
