# HPLOG

**Query 10 GB of logs in 93 ms. 294x faster than grep + gzip.**

HPLOG is a drop-in log accelerator. Pipe your existing JSON logs through it — your logging setup stays the same, but now you can search any time window instantly.

```bash
# Zero-friction — just pipe your app through hplog
node app.js | hplog pipe -o app.hplog

# Find all errors in a 5-minute window from 10 GB of logs
hplog grep app.hplog "level=ERROR" --from 14:30 --to 14:35
# → 93 ms (grep on gzipped JSON: 27 seconds)
```

> Stop grepping 10 GB logs. Jump directly to the 5-minute window you need.

## Benchmark

**10 GB of JSON logs (30M entries, 24 hours of production data):**

| Operation | grep + gzip | hplog | Speedup |
|-----------|-------------|-------|---------|
| Find ERROR in 5-min window | 27,297 ms | **93 ms** | **294x** |
| Find ERROR in 15-min window | 27,297 ms | **300 ms** | **91x** |
| Find ERROR in 60-min window | 27,297 ms | **3,200 ms** | **8.5x** |
| File size | 1.5 GB (.gz) | **1.3 GB** (.hplog) | **13% smaller** |

HPLOG doesn't scan the file. It reads the block index, jumps to the right 30-second window, and decompresses only what you need.

## Install

**Download a binary** from [Releases](https://github.com/HighpassStudio/hplog/releases):

```bash
# Linux
curl -L https://github.com/HighpassStudio/hplog/releases/latest/download/hplog-linux-x86_64 -o hplog
chmod +x hplog

# macOS
curl -L https://github.com/HighpassStudio/hplog/releases/latest/download/hplog-macos-aarch64 -o hplog
chmod +x hplog
```

**Or build from source:**

```bash
cargo install hplog
```

## Usage

### Pipe (zero-friction adoption)

Keep your existing logging. Just add hplog to the pipeline:

```bash
# Your app writes JSON to stdout — hplog indexes it AND passes it through
node app.js | hplog pipe -o app.hplog

# Works with any JSON log source
docker logs myapp | hplog pipe -o myapp.hplog
cat existing.jsonl | hplog pipe -o indexed.hplog | tee existing.jsonl
```

### Write (batch conversion)

Convert existing JSON log files:

```bash
cat app.log | hplog write -o app.hplog
```

### Query

```bash
# Time-range query (the killer feature)
hplog read app.hplog --from 14:30 --to 14:35

# Field-aware grep
hplog grep app.hplog "level=ERROR"
hplog grep app.hplog "level=ERROR service=api"
hplog grep app.hplog "level=ERROR" --from 14:30 --to 14:35

# Full contents
hplog cat app.hplog
hplog cat app.hplog --format json

# File statistics
hplog stats app.hplog
```

## How It Works

HPLOG groups log entries into **30-second time-windowed blocks**, compresses each block independently with zstd, and writes a **block index** at the end of the file.

```
+────────────+────────────+─────────+─────────+─────+─────────+───────+────────+
│ FileHeader │ Dictionary │ Block 1 │ Block 2 │ ... │ Block N │ Index │ Footer │
│  (64 B)    │  (var)     │  (var)  │  (var)  │     │  (var)  │ (var) │ (24 B) │
+────────────+────────────+─────────+─────────+─────+─────────+───────+────────+
```

**On a query:**
1. Read the 24-byte footer → find the index
2. Binary search the index for blocks matching your time range
3. Decompress only those blocks (parallel via rayon)
4. Filter entries by field values

**Why it's fast:**
- **O(log n) block lookup** — binary search, not sequential scan
- **Bloom filters** — skip blocks that definitely don't contain your value
- **Parallel decompression** — rayon processes multiple blocks simultaneously
- **Dictionary encoding** — field names stored once, referenced by 2-byte IDs
- **zstd compression** — 86% smaller than raw JSON

## Why not just grep?

`grep` scans every byte. On a 10 GB log file, that's 10 GB of I/O no matter what you're looking for.

HPLOG reads the index (kilobytes), finds the right blocks (milliseconds), and decompresses only what matches (megabytes). The rest is never touched.

Also: `grep ERROR` matches "ERROR" anywhere — in URLs, stack traces, message text. `hplog grep level=ERROR` only matches the `level` field. Fewer false positives.

## Format

See [FORMAT.md](FORMAT.md) for the complete binary specification.

- **Magic:** `HPLG` (4 bytes)
- **Extension:** `.hplog`
- **Compression:** zstd level 3
- **Block window:** 30 seconds (configurable)
- **Max fields:** 65,535 unique field names
- **Timestamp resolution:** nanosecond

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.

---

Built by [Highpass Studio](https://highpass.studio)
