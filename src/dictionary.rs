//! Field name dictionary — maps field names to 2-byte IDs.
//!
//! Written once at the start of the file (after header). New fields
//! get the next available ID. Compressed with zstd.

use anyhow::Result;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Dictionary {
    name_to_id: HashMap<String, u16>,
    id_to_name: Vec<String>,
}

impl Dictionary {
    pub fn new() -> Self {
        Dictionary {
            name_to_id: HashMap::new(),
            id_to_name: Vec::new(),
        }
    }

    /// Get or insert a field name, returning its 2-byte ID.
    pub fn get_or_insert(&mut self, name: &str) -> u16 {
        if let Some(&id) = self.name_to_id.get(name) {
            return id;
        }
        let id = self.id_to_name.len() as u16;
        self.name_to_id.insert(name.to_string(), id);
        self.id_to_name.push(name.to_string());
        id
    }

    /// Look up a field name by ID.
    pub fn get_name(&self, id: u16) -> Option<&str> {
        self.id_to_name.get(id as usize).map(|s| s.as_str())
    }

    /// Look up a field ID by name.
    pub fn get_id(&self, name: &str) -> Option<u16> {
        self.name_to_id.get(name).copied()
    }

    pub fn len(&self) -> usize {
        self.id_to_name.len()
    }

    /// Serialize to bytes: [u32 count, then (u16 id, u16 name_len, utf8 bytes)...]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let count = self.id_to_name.len() as u32;
        buf.extend_from_slice(&count.to_le_bytes());
        for (i, name) in self.id_to_name.iter().enumerate() {
            let id = i as u16;
            let name_bytes = name.as_bytes();
            let name_len = name_bytes.len() as u16;
            buf.extend_from_slice(&id.to_le_bytes());
            buf.extend_from_slice(&name_len.to_le_bytes());
            buf.extend_from_slice(name_bytes);
        }
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut dict = Dictionary::new();
        if data.len() < 4 {
            return Ok(dict);
        }
        let count = u32::from_le_bytes(data[0..4].try_into()?) as usize;
        let mut pos = 4;
        for _ in 0..count {
            if pos + 4 > data.len() {
                break;
            }
            let _id = u16::from_le_bytes(data[pos..pos + 2].try_into()?);
            let name_len = u16::from_le_bytes(data[pos + 2..pos + 4].try_into()?) as usize;
            pos += 4;
            if pos + name_len > data.len() {
                break;
            }
            let name = String::from_utf8_lossy(&data[pos..pos + name_len]).into_owned();
            dict.get_or_insert(&name);
            pos += name_len;
        }
        Ok(dict)
    }
}
