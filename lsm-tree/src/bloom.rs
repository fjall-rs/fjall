use crate::serde::{Deserializable, Serializable};
use crate::{DeserializeError, SerializeError};
use bit_vec::BitVec;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use seahash::SeaHasher;
use std::f32::consts::LN_2;
use std::fs::File;
use std::hash::Hasher;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::u128;

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

fn calculate_m(item_count: usize, false_positive_rate: f32) -> usize {
    let ln2_squared = LN_2.powi(2);
    let result = -((item_count as f32) * false_positive_rate.ln()) / ln2_squared;
    ((result / 8.0).ceil() * 8.0) as usize
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

    /// Constructs a bloom filter that can hold `item_count` items
    /// while maintaining a certain false positive rate.
    pub fn with_fp_rate(item_count: usize, fp_rate: f32) -> Self {
        // NOTE: Some sensible minimum
        let fp_rate = fp_rate.max(0.000_001);

        let k = 7;
        let m = calculate_m(item_count, fp_rate);

        Self {
            inner: BitVec::from_elem(m, false),
            m,
            k,
        }
    }

    /// Returns `true` if the item may be contained.
    ///
    /// Will never have a false negative.
    pub fn contains(&self, key: &[u8]) -> bool {
        let hash = Self::get_hash(key);

        let h1 = (hash & 0xFFFF_FFFF_FFFF_FFFF) as usize;
        let h2 = ((hash >> 64) & 0xFFFF_FFFF_FFFF_FFFF) as usize;

        let mut current_hash = h1;
        for _ in 0..self.k {
            current_hash = current_hash.wrapping_add(self.k * h2);
            let idx = current_hash % self.m;

            if !self.inner.get(idx).expect("should be in bounds") {
                return false;
            }
        }

        true
    }

    /// Adds the key to the filter
    pub fn set_with_hash(&mut self, hash: u128) {
        let h1 = (hash & 0xFFFF_FFFF_FFFF_FFFF) as usize;
        let h2 = ((hash >> 64) & 0xFFFF_FFFF_FFFF_FFFF) as usize;

        let mut current_hash = h1;
        for _ in 0..self.k {
            current_hash = current_hash.wrapping_add(self.k * h2);
            let idx = current_hash % self.m;

            self.set_pos(idx);
        }
    }

    fn set_pos(&mut self, idx: usize) {
        self.inner.set(idx, true);
    }

    /// Gets the hash of a key
    pub fn get_hash(key: &[u8]) -> u128 {
        let mut hasher = SeaHasher::default();
        hasher.write(key);
        let h1 = hasher.finish();

        hasher.write(key);
        let h2 = hasher.finish();

        u128::from(h1) << 64 | u128::from(h2)
    }
}
