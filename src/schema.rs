//! Schema management and dataset type detection.
//!
//! Manages Polars schemas for different MIDAS dataset types,
//! handles dataset type detection from file paths, and provides
//! schema validation and column analysis capabilities.

use crate::error::{MidasError, Result};
use crate::models::{DatasetConfig, DatasetType};
use polars::prelude::*;
use std::path::Path;
use tracing::{debug, warn};

/// Schema manager for different MIDAS dataset types
#[derive(Clone, Debug)]
pub struct SchemaManager {
    configs: std::collections::HashMap<DatasetType, DatasetConfig>,
}

impl Default for SchemaManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaManager {
    pub fn new() -> Self {
        Self {
            configs: std::collections::HashMap::new(),
        }
    }

    /// Initialize schema for a dataset type by analyzing sample files
    pub async fn initialize_schema(
        &mut self,
        dataset_type: DatasetType,
        sample_files: &[std::path::PathBuf],
    ) -> Result<()> {
        debug!(
            "Analyzing {} sample files for dataset type: {:?}",
            sample_files.len(),
            dataset_type
        );

        if sample_files.is_empty() {
            return Err(MidasError::Configuration {
                message: format!(
                    "No sample files provided for dataset type: {:?}",
                    dataset_type
                ),
            });
        }

        // Take first file to get the schema structure
        let sample_file = &sample_files[0];
        let schema = self.infer_schema_from_file(sample_file).await?;

        // Analyze multiple files to detect empty columns
        let empty_columns = self.analyze_empty_columns(sample_files, &schema).await?;

        let common_patterns = crate::models::CommonPatterns {
            always_na_columns: empty_columns.clone(),
            descriptor_columns: schema
                .iter_names()
                .filter(|name| name.ends_with("_j"))
                .map(|s| s.to_string())
                .collect(),
            timestamp_columns: schema
                .iter_names()
                .filter(|name| name.contains("time"))
                .map(|s| s.to_string())
                .collect(),
        };

        let config = crate::models::DatasetConfig {
            dataset_type: dataset_type.clone(),
            schema,
            empty_columns,
            common_patterns,
        };

        self.configs.insert(dataset_type, config);
        Ok(())
    }

    /// Infer schema from a single BADC-CSV file
    async fn infer_schema_from_file(&self, file_path: &std::path::Path) -> Result<Schema> {
        use crate::header::parse_badc_header;

        // Parse header to get data boundaries
        let (_, boundaries) = parse_badc_header(file_path)?;

        // Read the file to get column headers
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let file = File::open(file_path)?;
        let reader = BufReader::new(file);

        let mut column_line = None;
        for (line_num, line_result) in reader.lines().enumerate() {
            let line = line_result?;

            // The line right after "data" marker contains column headers
            if line_num == boundaries.skip_rows {
                column_line = Some(line);
                break;
            }
        }

        let column_line = column_line.ok_or_else(|| MidasError::Configuration {
            message: format!(
                "Could not find column headers in file: {}",
                file_path.display()
            ),
        })?;

        // Parse column names
        let column_names: Vec<&str> = column_line.split(',').collect();

        // Create schema with inferred types
        let mut fields = Vec::new();
        for name in column_names {
            let name = name.trim();
            let data_type = self.infer_column_type(name);
            fields.push(Field::new(name.into(), data_type));
        }

        Ok(Schema::from_iter(fields))
    }

    /// Infer data type for a column based on its name and common patterns
    fn infer_column_type(&self, column_name: &str) -> DataType {
        match column_name {
            // Primary datetime columns - parse as datetime with timezone
            "ob_end_time" | "ob_date" | "ob_end_ctime" => DataType::Datetime(TimeUnit::Nanoseconds, None),

            // Timestamp columns that may contain actual timestamps or be empty
            name if name.contains("stmp_time") || name.contains("ctime") => DataType::String,

            // String identifier columns
            "id" | "id_type" | "met_domain_name" => DataType::String,

            // Quality flag columns (usually integers)
            name if name.ends_with("_q") => DataType::Int32,

            // Descriptor columns (usually strings)
            name if name.ends_with("_j") => DataType::String,

            // Version and count columns
            "version_num" | "ob_hour_count" | "ob_day_cnt" | "src_id" | "rec_st_ind" => {
                DataType::Int32
            }

            // Measurement columns (temperatures, precipitation, radiation, etc.)
            name if name.contains("temp")
                || name.contains("prcp")
                || name.contains("irad")
                || name.contains("wind")
                || name.contains("gust")
                || name.contains("ilmn")
                || name.contains("bal_amt") =>
            {
                DataType::Float64
            }

            // Default to string for unknown patterns
            _ => DataType::String,
        }
    }

    /// Analyze multiple files to detect columns that are consistently empty
    async fn analyze_empty_columns(
        &self,
        sample_files: &[std::path::PathBuf],
        schema: &Schema,
    ) -> Result<Vec<String>> {
        let mut empty_count: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        let total_files = sample_files.len().min(10); // Limit to 10 files for analysis

        for file_path in sample_files.iter().take(total_files) {
            if let Ok(empty_cols) = self.detect_empty_columns_in_file(file_path, schema).await {
                for col in empty_cols {
                    *empty_count.entry(col).or_insert(0) += 1;
                }
            }
        }

        // Consider columns empty only if they're empty in ALL sample files
        let empty_columns: Vec<String> = empty_count
            .into_iter()
            .filter(|(_, count)| *count == total_files)
            .map(|(col, _)| col)
            .collect();

        debug!(
            "Detected {} consistently empty columns",
            empty_columns.len()
        );
        Ok(empty_columns)
    }

    /// Detect empty columns in a single file
    async fn detect_empty_columns_in_file(
        &self,
        file_path: &std::path::Path,
        schema: &Schema,
    ) -> Result<Vec<String>> {
        use crate::header::parse_badc_header;
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let (_, boundaries) = parse_badc_header(file_path)?;
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);

        let mut empty_columns = Vec::new();
        let column_names: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();
        let mut all_na_flags: Vec<bool> = vec![true; column_names.len()];

        // Sample up to 100 data rows
        let mut rows_checked = 0;
        const MAX_ROWS_TO_CHECK: usize = 100;

        for (line_num, line_result) in reader.lines().enumerate() {
            // Skip header and column names (boundaries.skip_rows + 1)
            if line_num <= boundaries.skip_rows {
                continue; // Skip header and column line
            }

            if rows_checked >= MAX_ROWS_TO_CHECK {
                break;
            }

            let line = line_result?;
            if line.trim() == "end data" {
                break;
            }

            let values: Vec<&str> = line.split(',').collect();
            for (i, value) in values.iter().enumerate() {
                if i < all_na_flags.len() {
                    let trimmed = value.trim();
                    if !trimmed.is_empty() && trimmed != "NA" {
                        all_na_flags[i] = false;
                    }
                }
            }

            rows_checked += 1;
        }

        // Collect column names that are all NA
        for (i, is_empty) in all_na_flags.iter().enumerate() {
            if *is_empty && i < column_names.len() {
                empty_columns.push(column_names[i].clone());
            }
        }

        Ok(empty_columns)
    }

    /// Check if schema is initialized for a dataset type
    pub fn has_schema(&self, dataset_type: &DatasetType) -> bool {
        self.configs.contains_key(dataset_type)
    }

    /// Get configuration for a dataset type
    pub fn get_config(&self, dataset_type: &DatasetType) -> Result<&DatasetConfig> {
        self.configs
            .get(dataset_type)
            .ok_or_else(|| MidasError::Configuration {
                message: format!(
                    "No configuration found for dataset type: {:?}",
                    dataset_type
                ),
            })
    }

    /// Detect dataset type from file path
    pub fn detect_dataset_type(&self, file_path: &Path) -> Result<DatasetType> {
        // Try to detect from the path structure
        if let Some(dataset_type) = DatasetType::from_path(file_path) {
            return Ok(dataset_type);
        }

        // Fallback: check parent directories
        let _path_str = file_path.to_string_lossy().to_lowercase();
        for ancestor in file_path.ancestors() {
            let ancestor_str = ancestor.to_string_lossy().to_lowercase();
            if ancestor_str.contains("rain") {
                return Ok(DatasetType::Rain);
            } else if ancestor_str.contains("temperature") {
                return Ok(DatasetType::Temperature);
            } else if ancestor_str.contains("wind") {
                return Ok(DatasetType::Wind);
            } else if ancestor_str.contains("radiation") {
                return Ok(DatasetType::Radiation);
            }
        }

        Err(MidasError::Configuration {
            message: format!(
                "Could not detect dataset type from path: {}",
                file_path.display()
            ),
        })
    }

    /// Create enhanced schema with metadata columns
    pub fn create_enhanced_schema(&self, dataset_type: &DatasetType) -> Result<Schema> {
        let config = self.get_config(dataset_type)?;
        let mut enhanced_schema = config.schema.clone();

        // Add metadata columns
        enhanced_schema
            .insert_at_index(0, "latitude".into(), DataType::Float64)
            .unwrap();
        enhanced_schema
            .insert_at_index(1, "longitude".into(), DataType::Float64)
            .unwrap();
        enhanced_schema
            .insert_at_index(2, "station_name".into(), DataType::String)
            .unwrap();
        enhanced_schema
            .insert_at_index(3, "station_id".into(), DataType::String)
            .unwrap();
        enhanced_schema
            .insert_at_index(4, "county".into(), DataType::String)
            .unwrap();
        enhanced_schema
            .insert_at_index(5, "height".into(), DataType::Float64)
            .unwrap();
        enhanced_schema
            .insert_at_index(6, "height_units".into(), DataType::String)
            .unwrap();

        Ok(enhanced_schema)
    }

    /// Get columns to exclude for streaming efficiency
    pub fn get_excluded_columns(&self, dataset_type: &DatasetType) -> Result<Vec<String>> {
        let config = self.get_config(dataset_type)?;
        Ok(config.empty_columns.clone())
    }

    /// Validate that a file matches expected schema
    pub fn validate_file_schema(
        &self,
        file_path: &Path,
        dataset_type: &DatasetType,
        actual_columns: &[String],
    ) -> Result<()> {
        let config = self.get_config(dataset_type)?;
        let expected_count = config.schema.len();
        let actual_count = actual_columns.len();

        if actual_count != expected_count {
            warn!(
                "Schema mismatch in {}: expected {} columns, found {}",
                file_path.display(),
                expected_count,
                actual_count
            );

            // Log column differences for debugging
            let expected_cols: Vec<String> =
                config.schema.iter_names().map(|s| s.to_string()).collect();
            debug!("Expected columns: {:?}", expected_cols);
            debug!("Actual columns: {:?}", actual_columns);
        }

        Ok(()) // Don't fail on schema mismatches, just warn
    }

    /// Report discovery results for a dataset type
    pub fn report_discovery(&self, dataset_type: &DatasetType) -> Result<()> {
        let config = self.get_config(dataset_type)?;

        println!("\n=== Discovery Results for {:?} Dataset ===", dataset_type);
        println!("Total columns detected: {}", config.schema.len());

        println!("\nColumn Schema:");
        for (name, data_type) in config.schema.iter() {
            println!("  {} -> {:?}", name, data_type);
        }

        if !config.empty_columns.is_empty() {
            println!("\nEmpty columns (100% NA across all sample files):");
            for col in &config.empty_columns {
                println!("  - {}", col);
            }
        } else {
            println!("\nNo consistently empty columns detected.");
        }

        println!("\nColumn patterns identified:");
        if !config.common_patterns.timestamp_columns.is_empty() {
            println!(
                "  Timestamp columns: {:?}",
                config.common_patterns.timestamp_columns
            );
        }
        if !config.common_patterns.descriptor_columns.is_empty() {
            println!(
                "  Descriptor columns: {:?}",
                config.common_patterns.descriptor_columns
            );
        }
        if !config.common_patterns.always_na_columns.is_empty() {
            println!(
                "  Always NA columns: {:?}",
                config.common_patterns.always_na_columns
            );
        }

        Ok(())
    }
}

/// Complete schema definitions for each dataset type
impl DatasetConfig {
    /// Enhanced rain dataset schema with all columns
    pub fn rain_config() -> (Schema, Vec<String>, crate::models::CommonPatterns) {
        let schema = Schema::from_iter([
            Field::new("ob_date".into(), DataType::String),
            Field::new("id".into(), DataType::String),
            Field::new("id_type".into(), DataType::String),
            Field::new("version_num".into(), DataType::Int32),
            Field::new("met_domain_name".into(), DataType::String),
            Field::new("ob_end_ctime".into(), DataType::String),
            Field::new("ob_day_cnt".into(), DataType::Int32),
            Field::new("src_id".into(), DataType::Int32),
            Field::new("rec_st_ind".into(), DataType::Int32),
            Field::new("prcp_amt".into(), DataType::Float64),
            Field::new("ob_day_cnt_q".into(), DataType::Int32),
            Field::new("prcp_amt_q".into(), DataType::Int32),
            Field::new("prcp_amt_j".into(), DataType::String),
            Field::new("meto_stmp_time".into(), DataType::String),
            Field::new("midas_stmp_etime".into(), DataType::String),
        ]);

        // Columns frequently empty in historical data
        let empty_columns = vec![
            "prcp_amt_j".to_string(),
            "meto_stmp_time".to_string(),
            "midas_stmp_etime".to_string(),
        ];

        let common_patterns = crate::models::CommonPatterns {
            always_na_columns: vec!["prcp_amt_j".to_string()],
            descriptor_columns: vec!["prcp_amt_j".to_string()],
            timestamp_columns: vec!["meto_stmp_time".to_string(), "midas_stmp_etime".to_string()],
        };

        (schema, empty_columns, common_patterns)
    }

    /// Enhanced temperature dataset schema
    pub fn temperature_config() -> (Schema, Vec<String>, crate::models::CommonPatterns) {
        let schema = Schema::from_iter([
            Field::new("ob_end_time".into(), DataType::String),
            Field::new("id_type".into(), DataType::String),
            Field::new("id".into(), DataType::String),
            Field::new("ob_hour_count".into(), DataType::Int32),
            Field::new("version_num".into(), DataType::Int32),
            Field::new("met_domain_name".into(), DataType::String),
            Field::new("src_id".into(), DataType::Int32),
            Field::new("rec_st_ind".into(), DataType::Int32),
            Field::new("max_air_temp".into(), DataType::Float64),
            Field::new("min_air_temp".into(), DataType::Float64),
            Field::new("min_grss_temp".into(), DataType::Float64),
            Field::new("min_conc_temp".into(), DataType::Float64),
            Field::new("max_air_temp_q".into(), DataType::Int32),
            Field::new("min_air_temp_q".into(), DataType::Int32),
            Field::new("min_grss_temp_q".into(), DataType::Int32),
            Field::new("min_conc_temp_q".into(), DataType::Int32),
            Field::new("max_air_temp_j".into(), DataType::String),
            Field::new("min_air_temp_j".into(), DataType::String),
            Field::new("min_grss_temp_j".into(), DataType::String),
            Field::new("min_conc_temp_j".into(), DataType::String),
            Field::new("meto_stmp_time".into(), DataType::String),
            Field::new("midas_stmp_etime".into(), DataType::String),
        ]);

        // Commonly empty columns in temperature data
        let empty_columns = vec![
            "min_grss_temp".to_string(),
            "min_conc_temp".to_string(),
            "min_grss_temp_q".to_string(),
            "min_conc_temp_q".to_string(),
            "min_grss_temp_j".to_string(),
            "min_conc_temp_j".to_string(),
        ];

        let common_patterns = crate::models::CommonPatterns {
            always_na_columns: vec!["min_grss_temp".to_string(), "min_conc_temp".to_string()],
            descriptor_columns: vec![
                "max_air_temp_j".to_string(),
                "min_air_temp_j".to_string(),
                "min_grss_temp_j".to_string(),
                "min_conc_temp_j".to_string(),
            ],
            timestamp_columns: vec!["meto_stmp_time".to_string(), "midas_stmp_etime".to_string()],
        };

        (schema, empty_columns, common_patterns)
    }

    /// Wind dataset schema (complete)
    pub fn wind_config() -> (Schema, Vec<String>, crate::models::CommonPatterns) {
        let schema = Schema::from_iter([
            Field::new("ob_end_time".into(), DataType::String),
            Field::new("id_type".into(), DataType::String),
            Field::new("id".into(), DataType::String),
            Field::new("ob_hour_count".into(), DataType::Int32),
            Field::new("met_domain_name".into(), DataType::String),
            Field::new("version_num".into(), DataType::Int32),
            Field::new("src_id".into(), DataType::Int32),
            Field::new("rec_st_ind".into(), DataType::Int32),
            Field::new("mean_wind_dir".into(), DataType::Int32),
            Field::new("mean_wind_speed".into(), DataType::Float64),
            Field::new("max_gust_dir".into(), DataType::Int32),
            Field::new("max_gust_speed".into(), DataType::Float64),
            Field::new("max_gust_ctime".into(), DataType::String),
            Field::new("mean_wind_dir_q".into(), DataType::Int32),
            Field::new("mean_wind_speed_q".into(), DataType::Int32),
            Field::new("max_gust_dir_q".into(), DataType::Int32),
            Field::new("max_gust_speed_q".into(), DataType::Int32),
            Field::new("max_gust_ctime_q".into(), DataType::Int32),
            Field::new("mean_wind_dir_j".into(), DataType::String),
            Field::new("mean_wind_speed_j".into(), DataType::String),
            Field::new("max_gust_dir_j".into(), DataType::String),
            Field::new("max_gust_speed_j".into(), DataType::String),
            Field::new("meto_stmp_time".into(), DataType::String),
            Field::new("midas_stmp_etime".into(), DataType::String),
        ]);

        let empty_columns = vec![
            "mean_wind_dir".to_string(),
            "max_gust_dir".to_string(),
            "max_gust_ctime".to_string(),
        ];

        let common_patterns = crate::models::CommonPatterns {
            always_na_columns: vec!["mean_wind_dir".to_string(), "max_gust_dir".to_string()],
            descriptor_columns: vec![
                "mean_wind_dir_j".to_string(),
                "mean_wind_speed_j".to_string(),
                "max_gust_dir_j".to_string(),
                "max_gust_speed_j".to_string(),
            ],
            timestamp_columns: vec!["meto_stmp_time".to_string(), "midas_stmp_etime".to_string()],
        };

        (schema, empty_columns, common_patterns)
    }

    /// Radiation dataset schema (needs refinement based on actual data)
    pub fn radiation_config() -> (Schema, Vec<String>, crate::models::CommonPatterns) {
        let schema = Schema::from_iter([
            Field::new("ob_end_time".into(), DataType::String),
            Field::new("id".into(), DataType::String),
            Field::new("id_type".into(), DataType::String),
            Field::new("ob_hour_count".into(), DataType::Int32),
            Field::new("version_num".into(), DataType::Int32),
            Field::new("met_domain_name".into(), DataType::String),
            Field::new("src_id".into(), DataType::Int32),
            Field::new("rec_st_ind".into(), DataType::Int32),
            Field::new("glbl_irad_amt".into(), DataType::Int32),
            Field::new("meto_stmp_time".into(), DataType::String),
            Field::new("midas_stmp_etime".into(), DataType::String),
        ]);

        let empty_columns = vec![];
        let common_patterns = crate::models::CommonPatterns {
            always_na_columns: vec![],
            descriptor_columns: vec![],
            timestamp_columns: vec!["meto_stmp_time".to_string(), "midas_stmp_etime".to_string()],
        };

        (schema, empty_columns, common_patterns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_dataset_type_detection() {
        let manager = SchemaManager::new();

        let rain_path = PathBuf::from("/path/to/uk-daily-rain-obs-202407/data.csv");
        assert_eq!(
            manager.detect_dataset_type(&rain_path).unwrap(),
            DatasetType::Rain
        );

        let temp_path = PathBuf::from("/path/to/uk-daily-temperature-obs-202507/data.csv");
        assert_eq!(
            manager.detect_dataset_type(&temp_path).unwrap(),
            DatasetType::Temperature
        );
    }

    #[test]
    fn test_schema_manager_initialization() {
        let manager = SchemaManager::new();

        // Schema manager starts empty
        assert!(!manager.has_schema(&DatasetType::Rain));
        assert!(!manager.has_schema(&DatasetType::Temperature));
        assert!(!manager.has_schema(&DatasetType::Wind));
        assert!(!manager.has_schema(&DatasetType::Radiation));

        // Should return error for uninitialized schemas
        assert!(manager.get_config(&DatasetType::Rain).is_err());
    }
}
