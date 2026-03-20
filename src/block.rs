//! Block read/write — accumulates log entries into time-windowed
//! blocks, serializes with delta-encoded timestamps, compresses with zstd.

use crate::format::*;
use anyhow::Result;

/// Serialize a list of log entries into a compressed block.
/// Timestamps are delta-encoded relative to block_start_ts.
pub fn serialize_entries(entries: &[LogEntry], block_start_ts: u64) -> Vec<u8> {
    let mut buf = Vec::new();

    for entry in entries {
        // Delta timestamp (relative to block start)
        let delta = entry.timestamp.saturating_sub(block_start_ts);
        buf.extend_from_slice(&encode_varint(delta));

        // Field count
        buf.push(entry.fields.len() as u8);

        // Fields
        for (field_id, value) in &entry.fields {
            buf.extend_from_slice(&field_id.to_le_bytes());
            buf.push(value.value_type());
            let value_bytes = value.to_bytes();
            buf.extend_from_slice(&encode_varint(value_bytes.len() as u64));
            buf.extend_from_slice(&value_bytes);
        }
    }

    buf
}

/// Deserialize entries from an uncompressed block payload.
pub fn deserialize_entries(data: &[u8], block_start_ts: u64) -> Result<Vec<LogEntry>> {
    let mut entries = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        // Delta timestamp
        let (delta, new_pos) = decode_varint(data, pos)?;
        pos = new_pos;
        let timestamp = block_start_ts + delta;

        // Field count
        if pos >= data.len() {
            break;
        }
        let field_count = data[pos] as usize;
        pos += 1;

        // Fields
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            if pos + 3 > data.len() {
                break;
            }
            let field_id = u16::from_le_bytes(data[pos..pos + 2].try_into()?);
            let value_type = data[pos + 2];
            pos += 3;

            let (value_len, new_pos) = decode_varint(data, pos)?;
            pos = new_pos;
            let value_len = value_len as usize;

            if pos + value_len > data.len() {
                break;
            }
            let value = FieldValue::from_bytes(value_type, &data[pos..pos + value_len])?;
            pos += value_len;

            fields.push((field_id, value));
        }

        entries.push(LogEntry { timestamp, fields });
    }

    Ok(entries)
}

/// Compress raw block data with zstd.
pub fn compress_block(data: &[u8]) -> Result<Vec<u8>> {
    Ok(zstd::encode_all(data, ZSTD_LEVEL)?)
}

/// Decompress a zstd-compressed block.
pub fn decompress_block(data: &[u8]) -> Result<Vec<u8>> {
    Ok(zstd::decode_all(data)?)
}
