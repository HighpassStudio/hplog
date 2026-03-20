//! HPLOG writer — reads newline-delimited JSON, writes .hplog files.
//!
//! Groups entries into time-windowed blocks (default 30s),
//! compresses each block independently, writes tail index on finish.

use crate::block;
use crate::bloom::BloomFilter;
use crate::dictionary::Dictionary;
use crate::format::*;
use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufWriter, Write};

/// Known timestamp field names, checked in order.
const TS_FIELDS: &[&str] = &["timestamp", "ts", "@timestamp", "time", "datetime", "date"];

pub struct LxfWriter {
    out: BufWriter<File>,
    dict: Dictionary,
    block_window_ns: u64,
    current_block_start: u64,
    current_entries: Vec<LogEntry>,
    block_id: u32,
    index_entries: Vec<IndexEntry>,
    bytes_written: u64,
    first_ts: u64,
    last_ts: u64,
    total_entries: u64,
}

impl LxfWriter {
    pub fn new(path: &str, block_window_secs: u64) -> Result<Self> {
        let file = File::create(path).with_context(|| format!("Cannot create {}", path))?;
        let mut out = BufWriter::new(file);

        // Write placeholder header (will rewrite on finish)
        let header = FileHeader {
            version: VERSION,
            flags: 0,
            dict_offset: 0,
            block_count: 0,
            first_ts: 0,
            last_ts: 0,
        };
        header.write_to(&mut out)?;
        let bytes_written = FILE_HEADER_SIZE as u64;

        Ok(LxfWriter {
            out,
            dict: Dictionary::new(),
            block_window_ns: block_window_secs * 1_000_000_000,
            current_block_start: 0,
            current_entries: Vec::new(),
            block_id: 0,
            index_entries: Vec::new(),
            bytes_written,
            first_ts: u64::MAX,
            last_ts: 0,
            total_entries: 0,
        })
    }

    /// Parse a JSON line and add the entry.
    pub fn write_json_line(&mut self, line: &str) -> Result<()> {
        let val: serde_json::Value = serde_json::from_str(line)?;
        let obj = match val.as_object() {
            Some(o) => o,
            None => return Ok(()), // skip non-object lines
        };

        // Extract timestamp
        let timestamp = self.extract_timestamp(obj);

        // Track global time range
        if timestamp < self.first_ts {
            self.first_ts = timestamp;
        }
        if timestamp > self.last_ts {
            self.last_ts = timestamp;
        }

        // Check if we need to start a new block
        if self.current_entries.is_empty() {
            self.current_block_start = timestamp;
        } else if timestamp >= self.current_block_start + self.block_window_ns {
            self.flush_block()?;
            self.current_block_start = timestamp;
        }

        // Convert JSON fields to LogEntry
        let mut fields = Vec::new();
        for (key, value) in obj {
            let field_id = self.dict.get_or_insert(key);
            let field_value = json_to_field_value(value);
            fields.push((field_id, field_value));
        }

        self.current_entries.push(LogEntry { timestamp, fields });
        self.total_entries += 1;
        Ok(())
    }

    /// Flush current block to disk.
    fn flush_block(&mut self) -> Result<()> {
        if self.current_entries.is_empty() {
            return Ok(());
        }

        let block_start = self.current_block_start;
        let block_end = self
            .current_entries
            .last()
            .map(|e| e.timestamp)
            .unwrap_or(block_start);
        let entry_count = self.current_entries.len() as u32;

        // Serialize entries
        let raw = block::serialize_entries(&self.current_entries, block_start);
        let uncompressed_size = raw.len() as u32;

        // Compress
        let compressed = block::compress_block(&raw)?;
        let compressed_size = compressed.len() as u32;

        // CRC32 of compressed data
        let checksum = crc32fast::hash(&compressed);

        // Build bloom filter for this block
        let mut bloom = BloomFilter::new();
        for entry in &self.current_entries {
            for (field_id, value) in &entry.fields {
                bloom.insert(*field_id, &value.display_string());
            }
        }

        // Write block header + bloom filter
        let block_offset = self.bytes_written;
        let header = BlockHeader {
            block_id: self.block_id,
            time_start: block_start,
            time_end: block_end,
            entry_count,
            compressed_size,
            uncompressed_size,
            checksum,
        };
        header.write_to(&mut self.out, &bloom)?;
        self.bytes_written += BLOCK_HEADER_SIZE as u64;

        // Write compressed payload
        self.out.write_all(&compressed)?;
        self.bytes_written += compressed_size as u64;

        // Record index entry
        self.index_entries.push(IndexEntry {
            time_start: block_start,
            time_end: block_end,
            byte_offset: block_offset,
            compressed_size,
        });

        self.block_id += 1;
        self.current_entries.clear();
        Ok(())
    }

    /// Finish writing: flush last block, write dictionary, index, footer.
    pub fn finish(mut self) -> Result<WriterStats> {
        // Flush remaining entries
        self.flush_block()?;

        // Write dictionary (compressed)
        let _dict_offset = self.bytes_written;
        let dict_bytes = self.dict.to_bytes();
        let dict_compressed = block::compress_block(&dict_bytes)?;
        let dict_len = dict_compressed.len() as u32;
        self.out.write_all(&dict_len.to_le_bytes())?;
        self.out.write_all(&dict_compressed)?;
        self.bytes_written += 4 + dict_compressed.len() as u64;

        // Write index
        let index_offset = self.bytes_written;
        self.out.write_all(INDEX_MAGIC)?;
        let block_count = self.index_entries.len() as u64;
        self.out.write_all(&block_count.to_le_bytes())?;
        self.bytes_written += 16;

        for entry in &self.index_entries {
            entry.write_to(&mut self.out)?;
            self.bytes_written += IndexEntry::SIZE as u64;
        }

        // Write footer
        self.out.write_all(FOOTER_MAGIC)?;
        self.out.write_all(&index_offset.to_le_bytes())?;
        let file_checksum: u32 = 0; // placeholder
        self.out.write_all(&file_checksum.to_le_bytes())?;
        self.out.write_all(&[0u8; 4])?; // reserved
        self.bytes_written += FOOTER_SIZE as u64;

        self.out.flush()?;

        // Rewrite file header with final values
        let file = self.out.into_inner()?;
        drop(file);

        // TODO: rewrite header with correct dict_offset and block_count
        // For now we wrote them at offset 0 as placeholder

        Ok(WriterStats {
            total_entries: self.total_entries,
            block_count: block_count as u32,
            dict_fields: self.dict.len() as u32,
            file_size: self.bytes_written,
            first_ts: self.first_ts,
            last_ts: self.last_ts,
        })
    }

    /// Extract timestamp from JSON object as epoch nanoseconds.
    fn extract_timestamp(&self, obj: &serde_json::Map<String, serde_json::Value>) -> u64 {
        for &field in TS_FIELDS {
            if let Some(val) = obj.get(field) {
                if let Some(s) = val.as_str() {
                    return parse_timestamp_str(s);
                }
                if let Some(n) = val.as_f64() {
                    // Could be epoch seconds, millis, or nanos
                    if n < 1e12 {
                        return (n * 1e9) as u64; // seconds
                    } else if n < 1e15 {
                        return (n * 1e6) as u64; // milliseconds
                    } else {
                        return n as u64; // already nanos
                    }
                }
                if let Some(n) = val.as_i64() {
                    if n < 1_000_000_000_000 {
                        return (n as u64) * 1_000_000_000;
                    } else if n < 1_000_000_000_000_000 {
                        return (n as u64) * 1_000_000;
                    } else {
                        return n as u64;
                    }
                }
            }
        }
        // No timestamp found — use 0
        0
    }
}

/// Parse an ISO 8601 timestamp string to epoch nanoseconds.
/// Public wrapper for main.rs time parsing.
pub fn parse_timestamp_str_pub(s: &str) -> u64 {
    parse_timestamp_str(s)
}

fn parse_timestamp_str(s: &str) -> u64 {
    // Try common formats
    // "2026-03-19T14:32:05.123Z"
    // "2026-03-19T14:32:05.123456789Z"
    // "2026-03-19 14:32:05"
    // Manual parse for common ISO format
    let s = s.trim().trim_end_matches('Z');
    let parts: Vec<&str> = s.splitn(2, 'T').collect();
    if parts.len() < 2 {
        let parts2: Vec<&str> = s.splitn(2, ' ').collect();
        if parts2.len() < 2 {
            return 0;
        }
        return parse_date_time(parts2[0], parts2[1]);
    }
    parse_date_time(parts[0], parts[1])
}

fn parse_date_time(date: &str, time: &str) -> u64 {
    let date_parts: Vec<u32> = date.split('-').filter_map(|s| s.parse().ok()).collect();
    if date_parts.len() != 3 {
        return 0;
    }
    let time_and_frac: Vec<&str> = time.splitn(2, '.').collect();
    let time_parts: Vec<u32> = time_and_frac[0]
        .split(':')
        .filter_map(|s| s.parse().ok())
        .collect();
    if time_parts.len() != 3 {
        return 0;
    }

    let frac_nanos: u64 = if time_and_frac.len() > 1 {
        let frac = time_and_frac[1].trim_end_matches('Z');
        let padded = format!("{:0<9}", frac);
        padded[..9].parse().unwrap_or(0)
    } else {
        0
    };

    // Simple days-since-epoch calculation (good enough for log timestamps)
    let y = date_parts[0] as i64;
    let m = date_parts[1] as i64;
    let d = date_parts[2] as i64;

    // Days from epoch (simplified, handles 2000-2100 fine)
    let mut days: i64 = 0;
    for year in 1970..y {
        days += if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
            366
        } else {
            365
        };
    }
    let month_days = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let is_leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    for mo in 1..m {
        days += month_days[mo as usize] as i64;
        if mo == 2 && is_leap {
            days += 1;
        }
    }
    days += d - 1;

    let secs = days * 86400
        + time_parts[0] as i64 * 3600
        + time_parts[1] as i64 * 60
        + time_parts[2] as i64;
    (secs as u64) * 1_000_000_000 + frac_nanos
}

fn json_to_field_value(val: &serde_json::Value) -> FieldValue {
    match val {
        serde_json::Value::String(s) => FieldValue::String(s.clone()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                FieldValue::I64(i)
            } else if let Some(f) = n.as_f64() {
                FieldValue::F64(f)
            } else {
                FieldValue::String(n.to_string())
            }
        }
        serde_json::Value::Bool(b) => FieldValue::Bool(*b),
        serde_json::Value::Null => FieldValue::Null,
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            FieldValue::Json(val.to_string())
        }
    }
}

#[allow(dead_code)]
pub struct WriterStats {
    pub total_entries: u64,
    pub block_count: u32,
    pub dict_fields: u32,
    pub file_size: u64,
    pub first_ts: u64,
    pub last_ts: u64,
}

/// Format nanosecond timestamp as ISO 8601.
pub fn format_timestamp(nanos: u64) -> String {
    let secs = nanos / 1_000_000_000;
    let frac = nanos % 1_000_000_000;

    // Convert epoch seconds to date/time
    let mut remaining = secs as i64;
    let mut year: i64 = 1970;
    loop {
        let days_in_year: i64 = if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
            366
        } else {
            365
        };
        if remaining < days_in_year * 86400 {
            break;
        }
        remaining -= days_in_year * 86400;
        year += 1;
    }

    let month_days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let is_leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let mut month: i64 = 1;
    let mut day_secs = remaining;
    for (i, &md) in month_days.iter().enumerate() {
        let md = if i == 1 && is_leap { md + 1 } else { md };
        if day_secs < md * 86400 {
            break;
        }
        day_secs -= md * 86400;
        month += 1;
    }
    let day = day_secs / 86400 + 1;
    let rem = day_secs % 86400;
    let hour = rem / 3600;
    let min = (rem % 3600) / 60;
    let sec = rem % 60;

    if frac > 0 {
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
            year,
            month,
            day,
            hour,
            min,
            sec,
            frac / 1_000_000
        )
    } else {
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
            year, month, day, hour, min, sec
        )
    }
}
