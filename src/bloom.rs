//! Compact bloom filter for per-block field value sketching.
//!
//! Each block stores a small bloom filter (64 bytes = 512 bits) that records
//! which (field_id, value_hash) pairs appear in the block. On a grep query,
//! we check the bloom filter first — if it says "definitely not present",
//! we skip decompressing the entire block.
//!
//! False positive rate with 512 bits and ~300 entries (typical 30s block):
//! ~5% with 3 hash functions. Good enough to skip 50%+ of blocks.

use anyhow::Result;
use std::io::{Read, Write};

pub const BLOOM_SIZE_BYTES: usize = 64; // 512 bits
const BLOOM_BITS: usize = BLOOM_SIZE_BYTES * 8;
const NUM_HASHES: usize = 3;

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: [u8; BLOOM_SIZE_BYTES],
}

impl BloomFilter {
    pub fn new() -> Self {
        BloomFilter {
            bits: [0u8; BLOOM_SIZE_BYTES],
        }
    }

    /// Insert a (field_id, value) pair into the bloom filter.
    pub fn insert(&mut self, field_id: u16, value: &str) {
        let base_hash = hash_pair(field_id, value);
        for i in 0..NUM_HASHES {
            let bit_pos = hash_nth(base_hash, i) % BLOOM_BITS;
            self.bits[bit_pos / 8] |= 1 << (bit_pos % 8);
        }
    }

    /// Check if a (field_id, value) pair might be in the bloom filter.
    /// Returns false = definitely not present. true = maybe present.
    pub fn might_contain(&self, field_id: u16, value: &str) -> bool {
        let base_hash = hash_pair(field_id, value);
        for i in 0..NUM_HASHES {
            let bit_pos = hash_nth(base_hash, i) % BLOOM_BITS;
            if self.bits[bit_pos / 8] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize to exactly BLOOM_SIZE_BYTES.
    pub fn write_to<W: Write>(&self, w: &mut W) -> Result<()> {
        w.write_all(&self.bits)?;
        Ok(())
    }

    /// Deserialize from BLOOM_SIZE_BYTES.
    pub fn read_from<R: Read>(r: &mut R) -> Result<Self> {
        let mut bits = [0u8; BLOOM_SIZE_BYTES];
        r.read_exact(&mut bits)?;
        Ok(BloomFilter { bits })
    }

    /// Check if the filter is empty (all zeros = no inserts).
    pub fn is_empty(&self) -> bool {
        self.bits.iter().all(|&b| b == 0)
    }
}

/// FNV-1a hash of (field_id, value).
fn hash_pair(field_id: u16, value: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    // Hash field_id
    for b in field_id.to_le_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    // Hash value (case-insensitive)
    for b in value.as_bytes() {
        h ^= b.to_ascii_lowercase() as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Generate the i-th hash from a base hash (double hashing technique).
fn hash_nth(base: u64, i: usize) -> usize {
    let h1 = base as usize;
    let h2 = (base >> 32) as usize;
    h1.wrapping_add(i.wrapping_mul(h2)).wrapping_add(i * i)
}
