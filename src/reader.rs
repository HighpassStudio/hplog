//! HPLOG reader — opens .hplog files, reads index, supports time-range seeking.
//! Supports bloom filter skip-scan and parallel block decompression.

use crate::block;
use crate::bloom::BloomFilter;
use crate::dictionary::Dictionary;
use crate::format::*;
use crate::query::Filter;
use crate::writer::format_timestamp;
use anyhow::{bail, Context, Result};
use rayon::prelude::*;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

pub struct LxfReader {
    file: File,
    #[allow(dead_code)]
    pub header: FileHeader,
    pub dict: Dictionary,
    pub index: Vec<IndexEntry>,
    pub file_size: u64,
}

impl LxfReader {
    pub fn open(path: &str) -> Result<Self> {
        let mut file = File::open(path).with_context(|| format!("Cannot open {}", path))?;
        let file_size = file.metadata()?.len();

        // Read file header
        let header = FileHeader::read_from(&mut file)?;

        // Read footer (last 24 bytes)
        if file_size < FOOTER_SIZE as u64 {
            bail!("File too small for footer");
        }
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer_buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_buf)?;

        if &footer_buf[0..8] != FOOTER_MAGIC {
            bail!("Invalid footer magic");
        }
        let index_offset = u64::from_le_bytes(footer_buf[8..16].try_into()?);

        // Read index
        file.seek(SeekFrom::Start(index_offset))?;
        let mut magic_buf = [0u8; 8];
        file.read_exact(&mut magic_buf)?;
        if &magic_buf != INDEX_MAGIC {
            bail!("Invalid index magic");
        }

        let mut buf8 = [0u8; 8];
        file.read_exact(&mut buf8)?;
        let block_count = u64::from_le_bytes(buf8) as usize;

        let mut index = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            index.push(IndexEntry::read_from(&mut file)?);
        }

        // Read dictionary
        let dict_start = if let Some(last_block) = index.last() {
            last_block.byte_offset + BLOCK_HEADER_SIZE as u64 + last_block.compressed_size as u64
        } else {
            FILE_HEADER_SIZE as u64
        };

        file.seek(SeekFrom::Start(dict_start))?;
        let mut dict_len_buf = [0u8; 4];
        file.read_exact(&mut dict_len_buf)?;
        let dict_compressed_len = u32::from_le_bytes(dict_len_buf) as usize;

        let mut dict_compressed = vec![0u8; dict_compressed_len];
        file.read_exact(&mut dict_compressed)?;
        let dict_bytes = block::decompress_block(&dict_compressed)?;
        let dict = Dictionary::from_bytes(&dict_bytes)?;

        Ok(LxfReader {
            file,
            header,
            dict,
            index,
            file_size,
        })
    }

    /// Read block header + bloom filter without decompressing the payload.
    #[allow(dead_code)]
    fn read_block_header(&mut self, idx: usize) -> Result<(BlockHeader, BloomFilter)> {
        let ie = &self.index[idx];
        self.file.seek(SeekFrom::Start(ie.byte_offset))?;
        BlockHeader::read_from(&mut self.file)
    }

    /// Read and decompress a single block by index position.
    #[allow(dead_code)]
    pub fn read_block(&mut self, idx: usize) -> Result<(BlockHeader, Vec<LogEntry>)> {
        let ie = &self.index[idx];
        self.file.seek(SeekFrom::Start(ie.byte_offset))?;

        let (bh, _bloom) = BlockHeader::read_from(&mut self.file)?;

        let mut compressed = vec![0u8; bh.compressed_size as usize];
        self.file.read_exact(&mut compressed)?;

        // Verify checksum
        let checksum = crc32fast::hash(&compressed);
        if checksum != bh.checksum {
            bail!(
                "Block {} checksum mismatch: expected {:08x}, got {:08x}",
                bh.block_id,
                bh.checksum,
                checksum
            );
        }

        let raw = block::decompress_block(&compressed)?;
        let entries = block::deserialize_entries(&raw, bh.time_start)?;

        Ok((bh, entries))
    }

    /// Read raw block data (header + compressed payload) for parallel processing.
    fn read_block_raw(&mut self, idx: usize) -> Result<(BlockHeader, BloomFilter, Vec<u8>)> {
        let ie = &self.index[idx];
        self.file.seek(SeekFrom::Start(ie.byte_offset))?;

        let (bh, bloom) = BlockHeader::read_from(&mut self.file)?;

        let mut compressed = vec![0u8; bh.compressed_size as usize];
        self.file.read_exact(&mut compressed)?;

        Ok((bh, bloom, compressed))
    }

    /// Read all entries from all blocks (parallel decompression).
    pub fn read_all(&mut self) -> Result<Vec<LogEntry>> {
        // Read all raw blocks sequentially (I/O is sequential)
        let mut raw_blocks = Vec::with_capacity(self.index.len());
        for i in 0..self.index.len() {
            raw_blocks.push(self.read_block_raw(i)?);
        }

        // Decompress + deserialize in parallel
        let results: Vec<Result<Vec<LogEntry>>> = raw_blocks
            .into_par_iter()
            .map(|(bh, _bloom, compressed)| {
                let raw = block::decompress_block(&compressed)?;
                block::deserialize_entries(&raw, bh.time_start)
            })
            .collect();

        let mut all = Vec::new();
        for r in results {
            all.extend(r?);
        }
        Ok(all)
    }

    /// Read entries in a time range [from_ns, to_ns] with parallel decompression.
    pub fn read_range(&mut self, from_ns: u64, to_ns: u64) -> Result<Vec<LogEntry>> {
        let start_idx = self.index.partition_point(|ie| ie.time_end < from_ns);
        let end_idx = self.index.partition_point(|ie| ie.time_start <= to_ns);

        // Read raw blocks in range
        let mut raw_blocks = Vec::new();
        for i in start_idx..end_idx {
            raw_blocks.push(self.read_block_raw(i)?);
        }

        // Parallel decompress + filter
        let results: Vec<Result<Vec<LogEntry>>> = raw_blocks
            .into_par_iter()
            .map(|(bh, _bloom, compressed)| {
                let raw = block::decompress_block(&compressed)?;
                let entries = block::deserialize_entries(&raw, bh.time_start)?;
                Ok(entries
                    .into_iter()
                    .filter(|e| e.timestamp >= from_ns && e.timestamp <= to_ns)
                    .collect())
            })
            .collect();

        let mut all = Vec::new();
        for r in results {
            all.extend(r?);
        }
        Ok(all)
    }

    /// Grep with bloom filter skip-scan and parallel decompression.
    /// Only decompresses blocks where the bloom filter says the value might exist.
    pub fn grep_filtered(
        &mut self,
        filters: &[Filter],
        from_ns: Option<u64>,
        to_ns: Option<u64>,
    ) -> Result<(Vec<LogEntry>, GrepStats)> {
        let total_blocks = self.index.len();

        // Determine block range from time filter
        let start_idx = from_ns
            .map(|f| self.index.partition_point(|ie| ie.time_end < f))
            .unwrap_or(0);
        let end_idx = to_ns
            .map(|t| self.index.partition_point(|ie| ie.time_start <= t))
            .unwrap_or(total_blocks);

        let range_blocks = end_idx - start_idx;

        // Read block headers + bloom filters, skip blocks that can't match
        let mut candidate_blocks: Vec<(BlockHeader, Vec<u8>)> = Vec::new();
        let mut bloom_skipped = 0u64;

        for i in start_idx..end_idx {
            let (bh, bloom, compressed) = self.read_block_raw(i)?;

            // Check bloom filter for each filter condition
            let mut dominated = true;
            for f in filters {
                if !bloom.might_contain(f.field_id, &f.value) {
                    dominated = false;
                    break;
                }
            }

            if dominated {
                candidate_blocks.push((bh, compressed));
            } else {
                bloom_skipped += 1;
            }
        }

        let decompressed_blocks = candidate_blocks.len() as u64;

        // Parallel decompress + filter
        let filter_specs: Vec<(u16, String)> = filters
            .iter()
            .map(|f| (f.field_id, f.value.clone()))
            .collect();

        let results: Vec<Result<Vec<LogEntry>>> = candidate_blocks
            .into_par_iter()
            .map(|(bh, compressed)| {
                let raw = block::decompress_block(&compressed)?;
                let entries = block::deserialize_entries(&raw, bh.time_start)?;
                let matched: Vec<LogEntry> = entries
                    .into_iter()
                    .filter(|entry| {
                        // Time filter
                        if let Some(f) = from_ns {
                            if entry.timestamp < f {
                                return false;
                            }
                        }
                        if let Some(t) = to_ns {
                            if entry.timestamp > t {
                                return false;
                            }
                        }
                        // Field filters
                        for (fid, fval) in &filter_specs {
                            let mut found = false;
                            for (eid, eval) in &entry.fields {
                                if eid == fid && eval.display_string().eq_ignore_ascii_case(fval) {
                                    found = true;
                                    break;
                                }
                            }
                            if !found {
                                return false;
                            }
                        }
                        true
                    })
                    .collect();
                Ok(matched)
            })
            .collect();

        let mut all = Vec::new();
        for r in results {
            all.extend(r?);
        }

        Ok((
            all,
            GrepStats {
                total_blocks: total_blocks as u64,
                range_blocks: range_blocks as u64,
                bloom_skipped,
                decompressed_blocks,
            },
        ))
    }

    /// Format a log entry as human-readable text.
    pub fn format_entry(&self, entry: &LogEntry) -> String {
        let ts = format_timestamp(entry.timestamp);
        let mut parts = vec![ts];
        for (field_id, value) in &entry.fields {
            let name = self.dict.get_name(*field_id).unwrap_or("?");
            parts.push(format!("{}={}", name, value.display_string()));
        }
        parts.join(" ")
    }

    /// Format entry as JSON.
    pub fn format_entry_json(&self, entry: &LogEntry) -> String {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "timestamp".to_string(),
            serde_json::Value::String(format_timestamp(entry.timestamp)),
        );
        for (field_id, value) in &entry.fields {
            let name = self.dict.get_name(*field_id).unwrap_or("?").to_string();
            let json_val = match value {
                FieldValue::String(s) => serde_json::Value::String(s.clone()),
                FieldValue::I64(n) => serde_json::json!(*n),
                FieldValue::F64(n) => serde_json::json!(*n),
                FieldValue::Bool(b) => serde_json::Value::Bool(*b),
                FieldValue::Null => serde_json::Value::Null,
                FieldValue::Json(s) => {
                    serde_json::from_str(s).unwrap_or(serde_json::Value::String(s.clone()))
                }
            };
            obj.insert(name, json_val);
        }
        serde_json::to_string(&obj).unwrap_or_default()
    }

    /// Get file statistics without decompressing blocks.
    pub fn stats(&self) -> FileStats {
        let total_compressed: u64 = self.index.iter().map(|ie| ie.compressed_size as u64).sum();
        let time_range_secs = if !self.index.is_empty() {
            let first = self.index.first().unwrap().time_start;
            let last = self.index.last().unwrap().time_end;
            (last - first) / 1_000_000_000
        } else {
            0
        };

        FileStats {
            file_size: self.file_size,
            block_count: self.index.len() as u32,
            dict_fields: self.dict.len() as u32,
            total_compressed,
            time_range_secs,
            first_ts: self.index.first().map(|ie| ie.time_start).unwrap_or(0),
            last_ts: self.index.last().map(|ie| ie.time_end).unwrap_or(0),
        }
    }
}

pub struct FileStats {
    pub file_size: u64,
    pub block_count: u32,
    pub dict_fields: u32,
    pub total_compressed: u64,
    pub time_range_secs: u64,
    pub first_ts: u64,
    pub last_ts: u64,
}

pub struct GrepStats {
    pub total_blocks: u64,
    pub range_blocks: u64,
    pub bloom_skipped: u64,
    pub decompressed_blocks: u64,
}
