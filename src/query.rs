//! Field-aware grep — search log entries by field=value filters.

use crate::format::LogEntry;
use crate::dictionary::Dictionary;
use anyhow::{bail, Result};

/// A parsed query filter: field_id + expected value string.
pub struct Filter {
    pub field_id: u16,
    pub field_name: String,
    pub value: String,
}

/// Parse a query string like "level=ERROR" or "service=api" into filters.
pub fn parse_filters(query: &str, dict: &Dictionary) -> Result<Vec<Filter>> {
    let mut filters = Vec::new();

    for part in query.split_whitespace() {
        let eq_pos = part
            .find('=')
            .ok_or_else(|| anyhow::anyhow!("Invalid filter '{}' — expected field=value", part))?;
        let field_name = &part[..eq_pos];
        let value = &part[eq_pos + 1..];

        let field_id = dict
            .get_id(field_name)
            .ok_or_else(|| anyhow::anyhow!("Field '{}' not found in dictionary", field_name))?;

        filters.push(Filter {
            field_id,
            field_name: field_name.to_string(),
            value: value.to_string(),
        });
    }

    if filters.is_empty() {
        bail!("No valid filters parsed from query");
    }

    Ok(filters)
}

/// Check if a log entry matches all filters.
pub fn entry_matches(entry: &LogEntry, filters: &[Filter]) -> bool {
    for filter in filters {
        let mut found = false;
        for (fid, fval) in &entry.fields {
            if *fid == filter.field_id {
                let val_str = fval.display_string();
                // Case-insensitive comparison for strings
                if val_str.eq_ignore_ascii_case(&filter.value) {
                    found = true;
                }
                break;
            }
        }
        if !found {
            return false;
        }
    }
    true
}
