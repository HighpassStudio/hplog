# HPLOG Binary Format Specification

**Version:** 1
**Extension:** `.hplog`
**Magic:** `HPLG` (4 bytes)

## Overview

HPLOG is a block-indexed, dictionary-compressed binary log file format. It is designed for:

- **Instant time-range seeking** — O(log n) block lookup via tail index
- **Field name deduplication** — dictionary encodes field names as 2-byte IDs
- **Independent block decompression** — each time-windowed block is separately zstd-compressed
- **Append-only writing** — pure sequential append during logging; index written on close
- **Crash recovery** — index can be reconstructed by scanning block headers

## File Layout

```
+──────────────+────────────+─────────+─────────+─────+─────────+────────────+────────+
│  FileHeader  │ Dictionary │ Block 1 │ Block 2 │ ... │ Block N │   Index    │ Footer │
│   (64 B)     │  (var)     │  (var)  │  (var)  │     │  (var)  │   (var)    │ (24 B) │
+──────────────+────────────+─────────+─────────+─────+─────────+────────────+────────+
```

Reading proceeds from the end: read Footer → seek to Index → binary search for blocks → decompress only needed blocks.

---

## File Header (64 bytes)

Written first. Updated on close with final values.

| Offset | Size | Type   | Field          | Description                           |
|--------|------|--------|----------------|---------------------------------------|
| 0      | 4    | bytes  | magic          | `HPLG` (0x48 0x50 0x4C 0x47)        |
| 4      | 2    | u16 LE | version        | Format version (currently 1)          |
| 6      | 4    | u32 LE | flags          | Bit flags (reserved, all 0 for v1)    |
| 10     | 8    | u64 LE | dict_offset    | Byte offset of dictionary block       |
| 18     | 8    | u64 LE | block_count    | Total number of log blocks            |
| 26     | 8    | u64 LE | first_ts       | Earliest timestamp (epoch nanoseconds)|
| 34     | 8    | u64 LE | last_ts        | Latest timestamp (epoch nanoseconds)  |
| 42     | 22   | bytes  | reserved       | Zero-filled, reserved for future use  |

**Total: 64 bytes**

---

## Dictionary Block

Immediately follows the last log block (before the index). Contains all field names seen during writing. Compressed with zstd.

### On-Disk Format

| Offset | Size | Type   | Field          | Description                          |
|--------|------|--------|----------------|--------------------------------------|
| 0      | 4    | u32 LE | compressed_len | Length of compressed dictionary data  |
| 4      | var  | bytes  | compressed     | zstd-compressed dictionary payload    |

### Decompressed Dictionary Payload

| Offset | Size | Type   | Field      | Description                        |
|--------|------|--------|------------|------------------------------------|
| 0      | 4    | u32 LE | count      | Number of field name entries        |
| 4+     | var  | repeat | entries    | Repeated for each field:            |

Each dictionary entry:

| Size | Type   | Field    | Description              |
|------|--------|----------|--------------------------|
| 2    | u16 LE | field_id | Sequential ID (0, 1, 2…) |
| 2    | u16 LE | name_len | Length of field name      |
| var  | UTF-8  | name     | Field name bytes          |

Field IDs are assigned sequentially starting from 0. Maximum 65,535 unique field names per file.

---

## Log Block

Each block contains log entries from a single time window (default: 30 seconds). Blocks are independently compressed with zstd.

### Block Header (36 bytes, uncompressed)

| Offset | Size | Type   | Field             | Description                          |
|--------|------|--------|-------------------|--------------------------------------|
| 0      | 4    | u32 LE | block_id          | Sequential block number (0, 1, 2…)   |
| 4      | 8    | u64 LE | time_start        | First entry timestamp (epoch nanos)   |
| 12     | 8    | u64 LE | time_end          | Last entry timestamp (epoch nanos)    |
| 20     | 4    | u32 LE | entry_count       | Number of log entries in this block   |
| 24     | 4    | u32 LE | compressed_size   | Size of compressed payload in bytes   |
| 28     | 4    | u32 LE | uncompressed_size | Size of raw payload before compression|
| 32     | 4    | u32 LE | checksum          | CRC32 of compressed payload           |

**Total: 36 bytes**

### Compressed Payload

Immediately follows the block header. Contains `compressed_size` bytes of zstd-compressed data. On decompression, yields the raw entry stream.

### Entry Encoding (within decompressed payload)

Entries are packed sequentially with no separators. Each entry:

| Size   | Type    | Field        | Description                                    |
|--------|---------|--------------|------------------------------------------------|
| 1–10   | varint  | delta_ts     | Timestamp delta from block's `time_start` (ns) |
| 1      | u8      | field_count  | Number of fields in this entry                 |
| var    | repeat  | fields       | Repeated for each field (see below)            |

Each field:

| Size   | Type    | Field      | Description                          |
|--------|---------|------------|--------------------------------------|
| 2      | u16 LE  | field_id   | Dictionary reference                 |
| 1      | u8      | value_type | Type tag (see Value Types)           |
| 1–10   | varint  | value_len  | Length of value data in bytes         |
| var    | bytes   | value_data | Encoded value                        |

### Value Types

| Type Tag | Name   | Encoding                                |
|----------|--------|-----------------------------------------|
| 0        | String | Raw UTF-8 bytes                         |
| 1        | I64    | 8 bytes, little-endian signed integer   |
| 2        | F64    | 8 bytes, little-endian IEEE 754 double  |
| 3        | Bool   | 1 byte (0 = false, 1 = true)           |
| 4        | Null   | 0 bytes (value_len = 0)                |
| 5        | Json   | Raw UTF-8 JSON string (objects/arrays)  |

### Varint Encoding

7-bit continuation encoding (same as Protocol Buffers):

- Each byte uses the low 7 bits for data and the high bit as a continuation flag.
- If bit 7 is set, more bytes follow.
- If bit 7 is clear, this is the last byte.

Example: value 300 → `0xAC 0x02` (300 = 0b100101100 → 0b0101100 | 0x80, 0b0000010)

---

## Index

Written after the dictionary, before the footer. Maps time ranges to block byte offsets for O(log n) seeking.

### Index Format

| Offset | Size | Type   | Field       | Description                    |
|--------|------|--------|-------------|--------------------------------|
| 0      | 8    | bytes  | magic       | `HPLG_IDX` (8 bytes)          |
| 8      | 8    | u64 LE | block_count | Number of index entries         |
| 16+    | var  | repeat | entries     | One per block (see below)      |

Each index entry (28 bytes):

| Size | Type   | Field           | Description                             |
|------|--------|-----------------|-----------------------------------------|
| 8    | u64 LE | time_start      | Block's first timestamp (epoch nanos)   |
| 8    | u64 LE | time_end        | Block's last timestamp (epoch nanos)    |
| 8    | u64 LE | byte_offset     | Absolute byte offset of block header    |
| 4    | u32 LE | compressed_size | Compressed payload size (excluding hdr) |

Index entries are ordered by `time_start`. Time-range queries binary search this array.

---

## Footer (24 bytes)

Last 24 bytes of the file. Entry point for all reading operations.

| Offset | Size | Type   | Field        | Description                        |
|--------|------|--------|--------------|------------------------------------|
| 0      | 8    | bytes  | magic        | `HPLG_END` (8 bytes)              |
| 8      | 8    | u64 LE | index_offset | Absolute byte offset of the index  |
| 16     | 4    | u32 LE | checksum     | File-level checksum (reserved, 0)  |
| 20     | 4    | bytes  | reserved     | Zero-filled                        |

**Total: 24 bytes**

---

## Reading Algorithm

### Full Scan (`hplog cat`)
1. Read FileHeader (64 B)
2. Read Footer (last 24 B) → get `index_offset`
3. Read Index → get all block offsets
4. Read Dictionary → decode field names
5. For each block: seek → read header → decompress → decode entries → output

### Time-Range Query (`hplog read --from T1 --to T2`)
1. Read Footer → Index
2. Binary search index for blocks where `time_end >= T1` and `time_start <= T2`
3. Read Dictionary
4. Decompress only matching blocks
5. Filter entries within [T1, T2]

### Field-Aware Grep (`hplog grep "level=ERROR"`)
1. Read Footer → Index → Dictionary
2. Look up field name → field_id in dictionary
3. Optionally narrow by time range
4. Decompress qualifying blocks
5. For each entry: check if field_id has matching value

---

## Crash Recovery

If the writer crashes before writing the index and footer, the file contains a valid header and zero or more complete blocks. Recovery:

1. Read FileHeader
2. Scan forward from byte 64, reading BlockHeaders
3. For each valid block (magic-free, checksum passes): record in rebuilt index
4. Write recovered index and footer

---

## Compression

- **Algorithm:** zstd (level 3 by default)
- **Scope:** Per-block (each block compressed independently)
- **Dictionary block:** Also zstd-compressed
- **Typical ratio:** 80–90% reduction on JSON log data (dictionary + zstd combined)

---

## Timestamps

All timestamps are **epoch nanoseconds** (nanoseconds since 1970-01-01T00:00:00Z).

Within a block, timestamps are **delta-encoded** relative to the block's `time_start` value, stored as varints. This typically reduces each timestamp to 1–4 bytes instead of 8.

---

## Limits

| Limit                    | Value          |
|--------------------------|----------------|
| Max unique field names   | 65,535         |
| Max block compressed size| 4 GB (u32)     |
| Max file size            | 16 EB (u64)    |
| Max entries per block    | 4 billion (u32)|
| Timestamp resolution     | 1 nanosecond   |

---

## MIME Type

`application/x-hplog`

## File Extension

`.hplog`
