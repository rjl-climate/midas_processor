//! Column mapping and categorization for dynamic BADC-CSV structure
//!
//! This module analyzes CSV headers to identify different types of columns:
//! measurements, quality flags, and metadata fields.

use crate::Result;
use csv::StringRecord;
use std::collections::HashMap;

/// Column mapping for dynamic data structure
#[derive(Debug, Clone)]
pub struct ColumnMapping {
    /// Column name to index mapping
    pub name_to_index: HashMap<String, usize>,

    /// Identified measurement columns (not temporal/metadata/quality)
    pub measurement_columns: Vec<String>,

    /// Quality flag columns (ending with "_q")
    pub quality_columns: Vec<String>,
}

impl ColumnMapping {
    /// Analyze column headers to identify measurement and quality columns
    pub fn analyze(headers: &StringRecord) -> Result<Self> {
        let mut name_to_index = HashMap::new();
        let mut measurement_columns = Vec::new();
        let mut quality_columns = Vec::new();

        // Required temporal and metadata columns that are NOT measurements
        let required_columns = [
            "ob_end_time",
            "ob_hour_count",
            "id",
            "src_id",
            "id_type",
            "met_domain_name",
            "rec_st_ind",
            "version_num",
            "meto_stmp_time",
            "midas_stmp_etime",
        ];

        for (index, header) in headers.iter().enumerate() {
            let column_name = header.trim().to_string();
            name_to_index.insert(column_name.clone(), index);

            // Categorize columns
            if column_name.ends_with("_q") {
                // Quality flag column
                quality_columns.push(column_name);
            } else if !required_columns.contains(&column_name.as_str()) {
                // This is likely a measurement column
                // Skip descriptor columns (_j suffix) as they're not needed
                if !column_name.ends_with("_j") {
                    measurement_columns.push(column_name);
                }
            }
        }

        Ok(ColumnMapping {
            name_to_index,
            measurement_columns,
            quality_columns,
        })
    }

    /// Get the index for a given column name
    pub fn get_index(&self, column_name: &str) -> Option<usize> {
        self.name_to_index.get(column_name).copied()
    }

    /// Check if a column exists in the mapping
    pub fn has_column(&self, column_name: &str) -> bool {
        self.name_to_index.contains_key(column_name)
    }

    /// Get statistics about the column mapping
    pub fn stats(&self) -> (usize, usize, usize) {
        (
            self.name_to_index.len(),
            self.measurement_columns.len(),
            self.quality_columns.len(),
        )
    }
}
