//! BADC-CSV header parsing and metadata extraction
//!
//! This module handles the extraction of essential metadata from BADC-CSV file headers,
//! focusing on the minimal set of information needed for robust data processing.

use crate::Result;
use crate::constants::MIDAS_MISSING_VALUE;

/// Basic header metadata extracted from BADC-CSV files
#[derive(Debug, Clone)]
pub struct SimpleHeader {
    /// Missing value marker (usually "NA")
    pub missing_value: String,

    /// File title (optional)
    pub title: Option<String>,

    /// Data source (optional)
    pub source: Option<String>,
}

impl SimpleHeader {
    /// Parse header section to extract essential metadata
    pub fn parse(header_lines: &[String]) -> Result<Self> {
        let mut missing_value = MIDAS_MISSING_VALUE.to_string();
        let mut title = None;
        let mut source = None;

        for line in header_lines {
            let parts: Vec<&str> = line.split(',').collect();

            // Look for global attributes (3 parts: key,G,value)
            if parts.len() >= 3 && parts[1].trim() == "G" {
                match parts[0].trim() {
                    "missing_value" => missing_value = parts[2].trim().to_string(),
                    "title" => title = Some(parts[2].trim().to_string()),
                    "source" => source = Some(parts[2].trim().to_string()),
                    _ => {} // Ignore other global attributes
                }
            }
        }

        Ok(SimpleHeader {
            missing_value,
            title,
            source,
        })
    }

    /// Check if a field value represents missing data
    pub fn is_missing_value(&self, value: &str) -> bool {
        let trimmed = value.trim();
        trimmed == self.missing_value || trimmed.is_empty()
    }
}
