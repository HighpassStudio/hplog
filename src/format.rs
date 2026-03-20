//! HPLOG binary format constants and core structs.

use crate::bloom::BloomFilter;
use anyhow::{bail, Result};
use std::io::{Read, Write};

// Magic bytes
pub const FILE_MAGIC: &[u8; 4] = b"HPLG";
pub const INDEX_MAGIC: &[u8; 8] = b"HPLG_IDX";
pub const FOOTER_MAGIC: &[u8; 8] = b"HPLG_END";

pub const FILE_HEADER_SIZE: usize = 64;
pub const BLOCK_HEADER_SIZE: usize = 100; // 36 header + 64 bloom filter
pub const FOOTER_SIZE: usize = 24; // magic(8) + index_offset(8) + checksum(4) + reserved(4)

pub const VERSION: u16 = 1;
#[allow(dead_code)]
pub const DEFAULT_BLOCK_WINDOW_SECS: u64 = 30;
pub const ZSTD_LEVEL: i32 = 3;

// Value types for log entry fields
pub const VAL_STRING: u8 = 0;
pub const VAL_I64: u8 = 1;
pub const VAL_F64: u8 = 2;
pub const VAL_BOOL: u8 = 3;
pub const VAL_NULL: u8 = 4;
pub const VAL_JSON: u8 = 5;

/// File header — first 64 bytes of every .hplog file.
#[derive(Debug, Clone)]
pub struct FileHeader {
    pub version: u16,
    pub flags: u32,
    pub dict_offset: u64,
    pub block_count: u64,
    pub first_ts: u64,
    pub last_ts: u64,
}

impl FileHeader {
    pub fn write_to<W: Write>(&self, w: &mut W) -> Result<()> {
        w.write_all(FILE_MAGIC)?;
        w.write_all(&self.version.to_le_bytes())?;
        w.write_all(&self.flags.to_le_bytes())?;
        w.write_all(&self.dict_offset.to_le_bytes())?;
        w.write_all(&self.block_count.to_le_bytes())?;
        w.write_all(&self.first_ts.to_le_bytes())?;
        w.write_all(&self.last_ts.to_le_bytes())?;
        // Reserved: pad to 64 bytes (64 - 4 - 2 - 4 - 8 - 8 - 8 - 8 = 22)
        w.write_all(&[0u8; 22])?;
        Ok(())
    }

    pub fn read_from<R: Read>(r: &mut R) -> Result<Self> {
        let mut magic = [0u8; 4];
        r.read_exact(&mut magic)?;
        if &magic != FILE_MAGIC {
            bail!("Invalid HPLOG file magic: {:?}", magic);
        }
        let mut buf2 = [0u8; 2];
        let mut buf4 = [0u8; 4];
        let mut buf8 = [0u8; 8];

        r.read_exact(&mut buf2)?;
        let version = u16::from_le_bytes(buf2);

        r.read_exact(&mut buf4)?;
        let flags = u32::from_le_bytes(buf4);

        r.read_exact(&mut buf8)?;
        let dict_offset = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf8)?;
        let block_count = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf8)?;
        let first_ts = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf8)?;
        let last_ts = u64::from_le_bytes(buf8);

        // Skip reserved
        let mut reserved = [0u8; 22];
        r.read_exact(&mut reserved)?;

        Ok(FileHeader {
            version,
            flags,
            dict_offset,
            block_count,
            first_ts,
            last_ts,
        })
    }
}

/// Block header — prefix for each compressed block.
#[derive(Debug, Clone)]
pub struct BlockHeader {
    pub block_id: u32,
    pub time_start: u64,
    pub time_end: u64,
    pub entry_count: u32,
    pub compressed_size: u32,
    pub uncompressed_size: u32,
    pub checksum: u32,
}

impl BlockHeader {
    pub fn write_to<W: Write>(&self, w: &mut W, bloom: &BloomFilter) -> Result<()> {
        w.write_all(&self.block_id.to_le_bytes())?;
        w.write_all(&self.time_start.to_le_bytes())?;
        w.write_all(&self.time_end.to_le_bytes())?;
        w.write_all(&self.entry_count.to_le_bytes())?;
        w.write_all(&self.compressed_size.to_le_bytes())?;
        w.write_all(&self.uncompressed_size.to_le_bytes())?;
        w.write_all(&self.checksum.to_le_bytes())?;
        bloom.write_to(w)?;
        Ok(())
    }

    pub fn read_from<R: Read>(r: &mut R) -> Result<(Self, BloomFilter)> {
        let mut buf4 = [0u8; 4];
        let mut buf8 = [0u8; 8];

        r.read_exact(&mut buf4)?;
        let block_id = u32::from_le_bytes(buf4);

        r.read_exact(&mut buf8)?;
        let time_start = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf8)?;
        let time_end = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf4)?;
        let entry_count = u32::from_le_bytes(buf4);

        r.read_exact(&mut buf4)?;
        let compressed_size = u32::from_le_bytes(buf4);

        r.read_exact(&mut buf4)?;
        let uncompressed_size = u32::from_le_bytes(buf4);

        r.read_exact(&mut buf4)?;
        let checksum = u32::from_le_bytes(buf4);

        let bloom = BloomFilter::read_from(r)?;

        Ok((
            BlockHeader {
                block_id,
                time_start,
                time_end,
                entry_count,
                compressed_size,
                uncompressed_size,
                checksum,
            },
            bloom,
        ))
    }
}

/// Index entry — maps a time range to a block's byte offset.
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub time_start: u64,
    pub time_end: u64,
    pub byte_offset: u64,
    pub compressed_size: u32,
}

impl IndexEntry {
    pub const SIZE: usize = 28; // 8 + 8 + 8 + 4

    pub fn write_to<W: Write>(&self, w: &mut W) -> Result<()> {
        w.write_all(&self.time_start.to_le_bytes())?;
        w.write_all(&self.time_end.to_le_bytes())?;
        w.write_all(&self.byte_offset.to_le_bytes())?;
        w.write_all(&self.compressed_size.to_le_bytes())?;
        Ok(())
    }

    pub fn read_from<R: Read>(r: &mut R) -> Result<Self> {
        let mut buf4 = [0u8; 4];
        let mut buf8 = [0u8; 8];

        r.read_exact(&mut buf8)?;
        let time_start = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf8)?;
        let time_end = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf8)?;
        let byte_offset = u64::from_le_bytes(buf8);

        r.read_exact(&mut buf4)?;
        let compressed_size = u32::from_le_bytes(buf4);

        Ok(IndexEntry {
            time_start,
            time_end,
            byte_offset,
            compressed_size,
        })
    }
}

/// A single field value in a log entry.
#[derive(Debug, Clone)]
pub enum FieldValue {
    String(String),
    I64(i64),
    F64(f64),
    Bool(bool),
    Null,
    Json(String), // serialized JSON for nested objects/arrays
}

impl FieldValue {
    pub fn value_type(&self) -> u8 {
        match self {
            FieldValue::String(_) => VAL_STRING,
            FieldValue::I64(_) => VAL_I64,
            FieldValue::F64(_) => VAL_F64,
            FieldValue::Bool(_) => VAL_BOOL,
            FieldValue::Null => VAL_NULL,
            FieldValue::Json(_) => VAL_JSON,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            FieldValue::String(s) => s.as_bytes().to_vec(),
            FieldValue::I64(n) => n.to_le_bytes().to_vec(),
            FieldValue::F64(n) => n.to_le_bytes().to_vec(),
            FieldValue::Bool(b) => vec![if *b { 1 } else { 0 }],
            FieldValue::Null => vec![],
            FieldValue::Json(s) => s.as_bytes().to_vec(),
        }
    }

    pub fn from_bytes(vtype: u8, data: &[u8]) -> Result<Self> {
        match vtype {
            VAL_STRING => Ok(FieldValue::String(String::from_utf8_lossy(data).into())),
            VAL_I64 => {
                let arr: [u8; 8] = data.try_into().map_err(|_| anyhow::anyhow!("bad i64"))?;
                Ok(FieldValue::I64(i64::from_le_bytes(arr)))
            }
            VAL_F64 => {
                let arr: [u8; 8] = data.try_into().map_err(|_| anyhow::anyhow!("bad f64"))?;
                Ok(FieldValue::F64(f64::from_le_bytes(arr)))
            }
            VAL_BOOL => Ok(FieldValue::Bool(data.first().copied().unwrap_or(0) != 0)),
            VAL_NULL => Ok(FieldValue::Null),
            VAL_JSON => Ok(FieldValue::Json(String::from_utf8_lossy(data).into())),
            _ => bail!("Unknown value type: {}", vtype),
        }
    }

    pub fn display_string(&self) -> String {
        match self {
            FieldValue::String(s) => s.clone(),
            FieldValue::I64(n) => n.to_string(),
            FieldValue::F64(n) => format!("{:.6}", n),
            FieldValue::Bool(b) => b.to_string(),
            FieldValue::Null => "null".to_string(),
            FieldValue::Json(s) => s.clone(),
        }
    }
}

/// A single log entry with timestamp and fields.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp: u64,                 // epoch nanoseconds
    pub fields: Vec<(u16, FieldValue)>, // (field_id, value)
}

// Varint encoding (7-bit continuation, same as ARCX)
pub fn encode_varint(mut val: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    loop {
        let byte = (val & 0x7F) as u8;
        val >>= 7;
        if val == 0 {
            buf.push(byte);
            break;
        } else {
            buf.push(byte | 0x80);
        }
    }
    buf
}

pub fn decode_varint(data: &[u8], offset: usize) -> Result<(u64, usize)> {
    let mut val: u64 = 0;
    let mut shift = 0;
    let mut pos = offset;
    loop {
        if pos >= data.len() {
            bail!("Varint extends beyond data");
        }
        let byte = data[pos];
        val |= ((byte & 0x7F) as u64) << shift;
        pos += 1;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            bail!("Varint too long");
        }
    }
    Ok((val, pos))
}
