//! HPLOG — Block-indexed, dictionary-compressed log file format.
//!
//! Commands:
//!   hplog write -o output.hplog       # pipe JSON stdin → .hplog
//!   hplog cat input.hplog              # dump as human-readable text
//!   hplog read input.hplog --from T --to T  # time range query
//!   hplog grep input.hplog "level=ERROR"    # field-aware grep
//!   hplog stats input.hplog            # file statistics

mod block;
mod bloom;
mod dictionary;
mod format;
mod query;
mod reader;
mod writer;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::io::{self, BufRead};
use std::time::Instant;

#[derive(Parser)]
#[command(
    name = "hplog",
    about = "Block-indexed log file format with instant time-range seeking"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Write JSON logs from stdin to an .hplog file
    Write {
        /// Output file path
        #[arg(short, long)]
        output: String,

        /// Block time window in seconds (default: 30)
        #[arg(long, default_value = "30")]
        block_window: u64,
    },

    /// Dump all entries as human-readable text
    Cat {
        /// Input .hplog file
        input: String,

        /// Output format: logfmt, json
        #[arg(long, default_value = "logfmt")]
        format: String,
    },

    /// Read entries in a time range
    Read {
        /// Input .hplog file
        input: String,

        /// Start time (ISO 8601 or HH:MM:SS)
        #[arg(long)]
        from: String,

        /// End time (ISO 8601 or HH:MM:SS)
        #[arg(long)]
        to: String,

        /// Output format: logfmt, json
        #[arg(long, default_value = "logfmt")]
        format: String,
    },

    /// Field-aware grep (e.g., "level=ERROR service=api")
    Grep {
        /// Input .hplog file
        input: String,

        /// Query string (field=value, space-separated for AND)
        query: String,

        /// Optional time range start
        #[arg(long)]
        from: Option<String>,

        /// Optional time range end
        #[arg(long)]
        to: Option<String>,

        /// Output format: logfmt, json
        #[arg(long, default_value = "logfmt")]
        format: String,
    },

    /// Show file statistics
    Stats {
        /// Input .hplog file
        input: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Write {
            output,
            block_window,
        } => cmd_write(&output, block_window),
        Commands::Cat { input, format } => cmd_cat(&input, &format),
        Commands::Read {
            input,
            from,
            to,
            format,
        } => cmd_read(&input, &from, &to, &format),
        Commands::Grep {
            input,
            query,
            from,
            to,
            format,
        } => cmd_grep(&input, &query, from.as_deref(), to.as_deref(), &format),
        Commands::Stats { input } => cmd_stats(&input),
    }
}

fn cmd_write(output: &str, block_window: u64) -> Result<()> {
    let t0 = Instant::now();
    let mut w = writer::LxfWriter::new(output, block_window)?;

    let stdin = io::stdin();
    let mut lines = 0u64;
    let mut errors = 0u64;

    for line in stdin.lock().lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match w.write_json_line(line) {
            Ok(()) => lines += 1,
            Err(_) => errors += 1,
        }
        if lines % 100_000 == 0 && lines > 0 {
            eprint!("\r[hplog] {} lines written...", lines);
        }
    }

    let stats = w.finish()?;
    let elapsed = t0.elapsed();

    eprintln!(
        "\r[hplog] Written {} entries to {}",
        stats.total_entries, output
    );
    eprintln!(
        "[hplog] {} blocks, {} fields, {} in {:.2}s",
        stats.block_count,
        stats.dict_fields,
        format_size(stats.file_size),
        elapsed.as_secs_f64()
    );
    if errors > 0 {
        eprintln!("[hplog] {} lines skipped (parse errors)", errors);
    }

    Ok(())
}

fn cmd_cat(input: &str, fmt: &str) -> Result<()> {
    let mut r = reader::LxfReader::open(input)?;
    let entries = r.read_all()?;

    for entry in &entries {
        match fmt {
            "json" => println!("{}", r.format_entry_json(entry)),
            _ => println!("{}", r.format_entry(entry)),
        }
    }

    eprintln!("[hplog] {} entries", entries.len());
    Ok(())
}

fn cmd_read(input: &str, from: &str, to: &str, fmt: &str) -> Result<()> {
    let t0 = Instant::now();
    let mut r = reader::LxfReader::open(input)?;

    let from_ns = parse_time_arg(from, &r)?;
    let to_ns = parse_time_arg(to, &r)?;

    let entries = r.read_range(from_ns, to_ns)?;
    let elapsed = t0.elapsed();

    for entry in &entries {
        match fmt {
            "json" => println!("{}", r.format_entry_json(entry)),
            _ => println!("{}", r.format_entry(entry)),
        }
    }

    eprintln!(
        "[hplog] {} entries in {:.1}ms ({} blocks scanned)",
        entries.len(),
        elapsed.as_secs_f64() * 1000.0,
        r.index.len() // TODO: count only scanned blocks
    );

    Ok(())
}

fn cmd_grep(
    input: &str,
    query_str: &str,
    from: Option<&str>,
    to: Option<&str>,
    fmt: &str,
) -> Result<()> {
    let t0 = Instant::now();
    let mut r = reader::LxfReader::open(input)?;

    let filters = query::parse_filters(query_str, &r.dict)?;

    let from_ns = from.map(|f| parse_time_arg(f, &r)).transpose()?;
    let to_ns = to.map(|t| parse_time_arg(t, &r)).transpose()?;

    let (entries, grep_stats) = r.grep_filtered(&filters, from_ns, to_ns)?;

    for entry in &entries {
        match fmt {
            "json" => println!("{}", r.format_entry_json(entry)),
            _ => println!("{}", r.format_entry(entry)),
        }
    }

    let elapsed = t0.elapsed();
    eprintln!(
        "[hplog] {} matches in {:.1}ms ({} blocks: {} in range, {} bloom-skipped, {} decompressed)",
        entries.len(),
        elapsed.as_secs_f64() * 1000.0,
        grep_stats.total_blocks,
        grep_stats.range_blocks,
        grep_stats.bloom_skipped,
        grep_stats.decompressed_blocks,
    );

    Ok(())
}

fn cmd_stats(input: &str) -> Result<()> {
    let r = reader::LxfReader::open(input)?;
    let stats = r.stats();

    println!("File:       {}", input);
    println!("Size:       {}", format_size(stats.file_size));
    println!("Blocks:     {}", stats.block_count);
    println!("Dict:       {} fields", stats.dict_fields);
    println!(
        "Time range: {} — {}",
        writer::format_timestamp(stats.first_ts),
        writer::format_timestamp(stats.last_ts)
    );
    println!("Duration:   {}s", stats.time_range_secs);
    println!(
        "Block data: {} (compressed)",
        format_size(stats.total_compressed)
    );

    Ok(())
}

/// Parse a time argument — supports ISO 8601 or HH:MM:SS (assumes first/last day in file).
fn parse_time_arg(s: &str, reader: &reader::LxfReader) -> Result<u64> {
    // If it looks like just HH:MM:SS, combine with the file's date
    if s.len() <= 8 && s.contains(':') && !s.contains('-') {
        let parts: Vec<u64> = s.split(':').filter_map(|p| p.parse().ok()).collect();
        if parts.len() >= 2 {
            let h = parts[0];
            let m = parts[1];
            let sec = if parts.len() > 2 { parts[2] } else { 0 };
            // Use the file's first timestamp to get the date
            let first_ts = reader.index.first().map(|ie| ie.time_start).unwrap_or(0);
            let day_start = (first_ts / 86_400_000_000_000) * 86_400_000_000_000;
            return Ok(day_start
                + h * 3_600_000_000_000
                + m * 60_000_000_000
                + sec * 1_000_000_000);
        }
    }
    // Try full ISO 8601
    // Simple parse: reuse writer's parse
    let ts = crate::writer::parse_timestamp_str_pub(s);
    if ts > 0 {
        return Ok(ts);
    }
    anyhow::bail!("Cannot parse time: {}", s);
}

fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / 1024.0 / 1024.0)
    } else {
        format!("{:.2} GB", bytes as f64 / 1024.0 / 1024.0 / 1024.0)
    }
}
