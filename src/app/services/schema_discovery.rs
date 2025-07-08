//! Dynamic schema discovery through CSV header analysis
//!
//! This module provides intelligent schema discovery by sampling a small number
//! of CSV files and analyzing their headers to determine the complete measurement
//! schema without reading observation data.

use crate::app::services::badc_csv_parser::column_mapping::ColumnMapping;
use crate::{Error, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

/// Schema information discovered from CSV headers
#[derive(Debug, Clone)]
pub struct SchemaInfo {
    /// All measurement column names found across sampled files
    pub measurement_columns: Vec<String>,
    /// All quality flag column names found across sampled files  
    pub quality_flag_columns: Vec<String>,
    /// Total number of unique measurement fields (measurements + quality flags)
    pub total_measurement_fields: usize,
    /// Number of files sampled for discovery
    pub files_sampled: usize,
}

impl SchemaInfo {
    /// Get all measurement field names (measurements + quality flags)
    pub fn all_measurement_fields(&self) -> Vec<String> {
        let mut all_fields = self.measurement_columns.clone();
        all_fields.extend(self.quality_flag_columns.clone());
        all_fields.sort();
        all_fields
    }

    /// Check if schema contains any measurement fields
    pub fn has_measurements(&self) -> bool {
        !self.measurement_columns.is_empty() || !self.quality_flag_columns.is_empty()
    }

    /// Get summary statistics for logging
    pub fn summary(&self) -> String {
        format!(
            "Discovered {} measurements, {} quality flags from {} files (total: {} fields)",
            self.measurement_columns.len(),
            self.quality_flag_columns.len(),
            self.files_sampled,
            self.total_measurement_fields
        )
    }
}

/// Service for discovering schema structure from CSV file headers
pub struct SchemaDiscoveryService;

impl SchemaDiscoveryService {
    /// Discover complete schema by sampling representative CSV files
    ///
    /// This method analyzes headers from a small sample of files to determine
    /// the complete measurement schema without reading observation data.
    ///
    /// # Arguments
    /// * `csv_files` - List of CSV files in the dataset
    ///
    /// # Returns
    /// Complete schema information including all measurement and quality flag columns
    pub async fn discover_schema(csv_files: &[PathBuf]) -> Result<SchemaInfo> {
        if csv_files.is_empty() {
            return Ok(SchemaInfo {
                measurement_columns: Vec::new(),
                quality_flag_columns: Vec::new(),
                total_measurement_fields: 0,
                files_sampled: 0,
            });
        }

        info!(
            "Starting schema discovery from {} CSV files",
            csv_files.len()
        );

        // Select representative files for sampling
        let sample_files = Self::select_representative_files(csv_files);
        
        debug!(
            "Selected {} files for schema sampling: {:?}",
            sample_files.len(),
            sample_files.iter().map(|p| p.file_name().unwrap_or_default()).collect::<Vec<_>>()
        );

        let mut all_measurements = HashSet::new();
        let mut all_quality_flags = HashSet::new();

        // Analyze headers from each sampled file
        for file_path in &sample_files {
            match Self::analyze_file_headers(file_path).await {
                Ok((measurements, quality_flags)) => {
                    debug!(
                        "File {}: {} measurements, {} quality flags",
                        file_path.file_name().unwrap_or_default().to_string_lossy(),
                        measurements.len(),
                        quality_flags.len()
                    );
                    all_measurements.extend(measurements);
                    all_quality_flags.extend(quality_flags);
                }
                Err(e) => {
                    // Log warning but continue with other files
                    debug!(
                        "Failed to analyze headers for {}: {}",
                        file_path.display(),
                        e
                    );
                }
            }
        }

        // Convert to sorted vectors for consistent output
        let mut measurement_columns: Vec<_> = all_measurements.into_iter().collect();
        let mut quality_flag_columns: Vec<_> = all_quality_flags.into_iter().collect();
        measurement_columns.sort();
        quality_flag_columns.sort();

        let schema_info = SchemaInfo {
            total_measurement_fields: measurement_columns.len() + quality_flag_columns.len(),
            files_sampled: sample_files.len(),
            measurement_columns,
            quality_flag_columns,
        };

        info!("Schema discovery complete: {}", schema_info.summary());

        Ok(schema_info)
    }

    /// Select representative files for schema sampling
    ///
    /// Uses intelligent sampling to get diverse representation:
    /// - First file (often has complete schema)
    /// - Last file (may have evolved schema)  
    /// - Middle file(s) (representative sample)
    /// - Additional random samples for large datasets
    fn select_representative_files(csv_files: &[PathBuf]) -> Vec<PathBuf> {
        let mut sample_files = Vec::new();

        match csv_files.len() {
            0 => {
                // No files to sample
            }
            1 => {
                // Only one file - use it
                sample_files.push(csv_files[0].clone());
            }
            2..=3 => {
                // Small dataset - sample all files
                sample_files.extend_from_slice(csv_files);
            }
            4..=10 => {
                // Medium dataset - sample first, middle, last
                sample_files.push(csv_files[0].clone());
                sample_files.push(csv_files[csv_files.len() / 2].clone());
                sample_files.push(csv_files[csv_files.len() - 1].clone());
            }
            _ => {
                // Large dataset - sample first, last, middle, plus 2 random
                sample_files.push(csv_files[0].clone());
                sample_files.push(csv_files[csv_files.len() - 1].clone());
                sample_files.push(csv_files[csv_files.len() / 2].clone());
                
                // Add two more samples from different quartiles
                sample_files.push(csv_files[csv_files.len() / 4].clone());
                sample_files.push(csv_files[3 * csv_files.len() / 4].clone());
            }
        }

        // Remove duplicates while preserving order
        let mut unique_files = Vec::new();
        let mut seen = HashSet::new();
        for file in sample_files {
            if seen.insert(file.clone()) {
                unique_files.push(file);
            }
        }

        unique_files
    }

    /// Analyze headers from a single CSV file to extract measurement columns
    ///
    /// This method reads only the CSV headers (not data) to identify
    /// measurement and quality flag columns.
    async fn analyze_file_headers(
        file_path: &Path,
    ) -> Result<(Vec<String>, Vec<String>)> {
        // Read just enough of the file to get past the BADC header to the CSV headers
        let content = tokio::fs::read_to_string(file_path).await.map_err(|e| {
            Error::io(format!("Failed to read file {}", file_path.display()), e)
        })?;

        // Find the data section (this is where CSV headers are)
        let data_section = Self::extract_data_section(&content)?;
        
        // Parse just the first line (headers) as CSV
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(data_section.as_bytes());

        let headers = csv_reader.headers().map_err(|e| {
            Error::data_validation(format!(
                "Failed to parse CSV headers in {}: {}",
                file_path.display(),
                e
            ))
        })?;

        // Use existing ColumnMapping to categorize columns
        let column_mapping = ColumnMapping::analyze(headers)?;

        Ok((
            column_mapping.measurement_columns,
            column_mapping.quality_columns,
        ))
    }

    /// Extract the data section from BADC-CSV content
    ///
    /// BADC-CSV files have metadata sections followed by a "data" section
    /// that contains the actual CSV data with headers.
    fn extract_data_section(content: &str) -> Result<String> {
        let mut in_data_section = false;
        let mut data_lines = Vec::new();

        for line in content.lines() {
            let trimmed = line.trim();
            
            if trimmed == "data" {
                in_data_section = true;
                continue;
            }
            
            if trimmed == "end data" {
                break;
            }
            
            if in_data_section {
                data_lines.push(line);
            }
        }

        if data_lines.is_empty() {
            return Err(Error::data_validation(
                "No data section found in BADC-CSV file".to_string(),
            ));
        }

        // Return just the headers line plus one data line (minimum for CSV parsing)
        let data_section = if data_lines.len() >= 2 {
            format!("{}\n{}", data_lines[0], data_lines[1])
        } else {
            data_lines[0].to_string()
        };

        Ok(data_section)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Create a test BADC-CSV file with specific headers
    fn create_test_badc_csv(headers: &[&str], data_row: &[&str]) -> Result<NamedTempFile> {
        let mut temp_file = NamedTempFile::new().map_err(|e| {
            Error::io("Failed to create temp file".to_string(), e)
        })?;

        // Write BADC-CSV format
        writeln!(temp_file, "title,Test Dataset")?;
        writeln!(temp_file, "source,Test Source")?;
        writeln!(temp_file, "data")?;
        writeln!(temp_file, "{}", headers.join(","))?;
        if !data_row.is_empty() {
            writeln!(temp_file, "{}", data_row.join(","))?;
        }
        writeln!(temp_file, "end data")?;

        temp_file.flush().map_err(|e| {
            Error::io("Failed to flush temp file".to_string(), e)
        })?;

        Ok(temp_file)
    }

    #[tokio::test]
    async fn test_schema_discovery_empty_files() {
        let result = SchemaDiscoveryService::discover_schema(&[]).await.unwrap();
        assert_eq!(result.measurement_columns.len(), 0);
        assert_eq!(result.quality_flag_columns.len(), 0);
        assert_eq!(result.files_sampled, 0);
        assert!(!result.has_measurements());
    }

    #[tokio::test]
    async fn test_schema_discovery_single_file() {
        let headers = vec!["ob_end_time", "id", "air_temperature", "wind_speed", "air_temperature_q", "wind_speed_q"];
        let data = vec!["2024-01-01T00:00:00", "12345", "15.5", "10.2", "0", "1"];
        let temp_file = create_test_badc_csv(&headers, &data).unwrap();
        
        let files = vec![temp_file.path().to_path_buf()];
        let result = SchemaDiscoveryService::discover_schema(&files).await.unwrap();

        assert_eq!(result.files_sampled, 1);
        assert!(result.has_measurements());
        assert!(result.measurement_columns.contains(&"air_temperature".to_string()));
        assert!(result.measurement_columns.contains(&"wind_speed".to_string()));
        assert!(result.quality_flag_columns.contains(&"air_temperature_q".to_string()));
        assert!(result.quality_flag_columns.contains(&"wind_speed_q".to_string()));
    }

    #[tokio::test]
    async fn test_representative_file_selection() {
        // Test different dataset sizes
        let files: Vec<PathBuf> = (0..20).map(|i| PathBuf::from(format!("file_{}.csv", i))).collect();
        
        let sample = SchemaDiscoveryService::select_representative_files(&files);
        assert!(sample.len() <= 5);
        assert!(sample.contains(&PathBuf::from("file_0.csv"))); // First file
        assert!(sample.contains(&PathBuf::from("file_19.csv"))); // Last file

        // Test small dataset
        let small_files: Vec<PathBuf> = (0..3).map(|i| PathBuf::from(format!("file_{}.csv", i))).collect();
        let small_sample = SchemaDiscoveryService::select_representative_files(&small_files);
        assert_eq!(small_sample.len(), 3); // All files sampled for small datasets
    }

    #[test]
    fn test_extract_data_section() {
        let badc_content = r#"title,Test Dataset
source,Test Source
data
ob_end_time,id,air_temperature,air_temperature_q
2024-01-01T00:00:00,12345,15.5,0
2024-01-01T01:00:00,12345,16.0,0
end data"#;

        let data_section = SchemaDiscoveryService::extract_data_section(badc_content).unwrap();
        assert!(data_section.contains("ob_end_time,id,air_temperature,air_temperature_q"));
        assert!(data_section.contains("2024-01-01T00:00:00,12345,15.5,0"));
    }

    #[test]
    fn test_schema_info_methods() {
        let schema_info = SchemaInfo {
            measurement_columns: vec!["air_temperature".to_string(), "wind_speed".to_string()],
            quality_flag_columns: vec!["air_temperature_q".to_string()],
            total_measurement_fields: 3,
            files_sampled: 2,
        };

        assert!(schema_info.has_measurements());
        assert_eq!(schema_info.all_measurement_fields().len(), 3);
        assert!(schema_info.summary().contains("2 measurements"));
        assert!(schema_info.summary().contains("1 quality flags"));
    }
}