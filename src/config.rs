//! Configuration management for MIDAS processor
//!
//! This module provides a layered configuration system that loads settings from:
//! 1. Default values
//! 2. Configuration file (TOML format)
//! 3. Environment variables
//! 4. Command-line arguments
//!
//! The configuration follows the specifications defined in PLANNING.md.

use crate::constants::{
    DEFAULT_DATASETS, DEFAULT_MEMORY_LIMIT_GB, DEFAULT_PARALLEL_WORKERS, PARQUET_COMPRESSION,
    PARQUET_PAGE_SIZE_MB, PARQUET_ROW_GROUP_SIZE,
};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// =============================================================================
// Configuration Structure
// =============================================================================

/// Complete configuration for the MIDAS processor
///
/// This structure contains all configuration options organized by functional area.
/// It supports layered loading from files, environment variables, and command-line arguments.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Processing-related configuration
    pub processing: ProcessingConfig,

    /// Quality control configuration
    #[serde(default)]
    pub quality_control: QualityControlConfig,

    /// Parquet output configuration
    #[serde(default)]
    pub parquet: ParquetConfig,

    /// Performance tuning configuration
    #[serde(default)]
    pub performance: PerformanceConfig,

    /// Logging configuration
    #[serde(default)]
    pub logging: LoggingConfig,
}

/// Processing-related configuration options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingConfig {
    /// Input path to MIDAS fetcher cache directory
    pub input_path: PathBuf,

    /// Output path for generated Parquet files
    pub output_path: PathBuf,

    /// Cache path for storing downloaded and processed data
    #[serde(default = "default_cache_path")]
    pub cache_path: PathBuf,

    /// Custom output path for Parquet files specifically
    /// If not specified, defaults to 'parquet' folder within input_path
    #[serde(default)]
    pub parquet_output_path: Option<PathBuf>,

    /// List of datasets to process
    #[serde(default = "default_datasets")]
    pub datasets: Vec<String>,

    /// Whether to perform a dry run (no actual processing)
    #[serde(default)]
    pub dry_run: bool,

    /// Force overwrite of existing output files
    #[serde(default)]
    pub force_overwrite: bool,
}

/// Processing quality control configuration options
///
/// Note: This configuration only controls processing-level quality checks.
/// ALL MIDAS data-level quality indicators are preserved without interpretation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityControlConfig {
    /// Require valid station metadata for all observations
    #[serde(default = "default_require_station_metadata")]
    pub require_station_metadata: bool,

    /// Exclude observations with no measurements (processing failure indicator)
    #[serde(default = "default_exclude_empty_measurements")]
    pub exclude_empty_measurements: bool,
}

/// Parquet output configuration options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetConfig {
    /// Compression algorithm for Parquet files
    #[serde(default = "default_compression")]
    pub compression: String,

    /// Row group size for Parquet files
    #[serde(default = "default_row_group_size")]
    pub row_group_size: usize,

    /// Page size in MB for Parquet files
    #[serde(default = "default_page_size_mb")]
    pub page_size_mb: usize,
}

/// Performance tuning configuration options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Number of parallel workers for processing
    #[serde(default = "default_parallel_workers")]
    pub parallel_workers: usize,

    /// Memory limit in GB for processing
    #[serde(default = "default_memory_limit_gb")]
    pub memory_limit_gb: usize,
}

/// Logging configuration options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level (error, warn, info, debug, trace)
    #[serde(default = "default_log_level")]
    pub level: String,

    /// Log format (json, pretty, compact)
    #[serde(default = "default_log_format")]
    pub format: String,

    /// Enable structured logging
    #[serde(default)]
    pub structured: bool,
}

// =============================================================================
// Default Value Functions
// =============================================================================

fn default_datasets() -> Vec<String> {
    DEFAULT_DATASETS.iter().map(|s| s.to_string()).collect()
}

fn default_cache_path() -> PathBuf {
    // Default cache location: ~/Library/Application Support/midas-fetcher/cache
    use directories::UserDirs;

    if let Some(user_dirs) = UserDirs::new() {
        user_dirs
            .home_dir()
            .join("Library")
            .join("Application Support")
            .join("midas-fetcher")
            .join("cache")
    } else {
        // Fallback if home directory can't be determined
        PathBuf::from("/tmp/midas-fetcher-cache")
    }
}

fn default_require_station_metadata() -> bool {
    true // Require valid station metadata by default
}

fn default_exclude_empty_measurements() -> bool {
    true // Exclude empty measurements by default (indicates processing failure)
}

fn default_compression() -> String {
    PARQUET_COMPRESSION.to_string()
}

fn default_row_group_size() -> usize {
    PARQUET_ROW_GROUP_SIZE
}

fn default_page_size_mb() -> usize {
    PARQUET_PAGE_SIZE_MB
}

fn default_parallel_workers() -> usize {
    DEFAULT_PARALLEL_WORKERS
}

fn default_memory_limit_gb() -> usize {
    DEFAULT_MEMORY_LIMIT_GB
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "pretty".to_string()
}

// =============================================================================
// Default Implementations
// =============================================================================

impl Default for QualityControlConfig {
    fn default() -> Self {
        Self {
            require_station_metadata: default_require_station_metadata(),
            exclude_empty_measurements: default_exclude_empty_measurements(),
        }
    }
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            compression: default_compression(),
            row_group_size: default_row_group_size(),
            page_size_mb: default_page_size_mb(),
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            parallel_workers: default_parallel_workers(),
            memory_limit_gb: default_memory_limit_gb(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
            structured: false,
        }
    }
}

// =============================================================================
// Configuration Loading and Validation
// =============================================================================

impl Config {
    /// Create a new configuration with default values
    pub fn new(input_path: PathBuf, output_path: PathBuf) -> Self {
        Self {
            processing: ProcessingConfig {
                input_path,
                output_path,
                cache_path: default_cache_path(),
                parquet_output_path: None,
                datasets: default_datasets(),
                dry_run: false,
                force_overwrite: false,
            },
            quality_control: QualityControlConfig::default(),
            parquet: ParquetConfig::default(),
            performance: PerformanceConfig::default(),
            logging: LoggingConfig::default(),
        }
    }

    /// Load configuration from file
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            Error::configuration(format!(
                "Failed to read config file '{}': {}",
                path.display(),
                e
            ))
        })?;

        toml::from_str(&content).map_err(|e| {
            Error::configuration(format!(
                "Failed to parse config file '{}': {}",
                path.display(),
                e
            ))
        })
    }

    /// Load configuration with layered precedence
    pub fn load_layered(
        input_path: Option<PathBuf>,
        output_path: Option<PathBuf>,
        config_file: Option<&std::path::Path>,
    ) -> Result<Self> {
        // Start with smart default configuration
        let mut config = Self {
            processing: ProcessingConfig {
                input_path: input_path.unwrap_or_else(default_cache_path),
                output_path: output_path.unwrap_or_else(|| PathBuf::from("./output")),
                cache_path: default_cache_path(),
                parquet_output_path: None,
                datasets: default_datasets(),
                dry_run: false,
                force_overwrite: false,
            },
            quality_control: QualityControlConfig::default(),
            parquet: ParquetConfig::default(),
            performance: PerformanceConfig::default(),
            logging: LoggingConfig::default(),
        };

        // Layer 1: Config file
        if let Some(config_path) = config_file {
            if config_path.exists() {
                let file_config = Self::from_file(config_path)?;
                config.merge(file_config)?;
            }
        }

        // Layer 2: Environment variables
        config.merge_env()?;

        // Layer 3: Command-line arguments are handled by the caller

        // Validate final configuration
        config.validate()?;

        Ok(config)
    }

    /// Merge another configuration into this one
    pub fn merge(&mut self, other: Self) -> Result<()> {
        // Only merge non-default values from other config
        if !other.processing.input_path.as_os_str().is_empty() {
            self.processing.input_path = other.processing.input_path;
        }
        if !other.processing.output_path.as_os_str().is_empty() {
            self.processing.output_path = other.processing.output_path;
        }
        if !other.processing.cache_path.as_os_str().is_empty() {
            self.processing.cache_path = other.processing.cache_path;
        }
        if other.processing.parquet_output_path.is_some() {
            self.processing.parquet_output_path = other.processing.parquet_output_path;
        }
        if !other.processing.datasets.is_empty() {
            self.processing.datasets = other.processing.datasets;
        }

        // Merge other fields
        self.processing.dry_run = other.processing.dry_run;
        self.processing.force_overwrite = other.processing.force_overwrite;
        self.quality_control = other.quality_control;
        self.parquet = other.parquet;
        self.performance = other.performance;
        self.logging = other.logging;

        Ok(())
    }

    /// Merge environment variables into configuration
    pub fn merge_env(&mut self) -> Result<()> {
        // Processing environment variables
        if let Ok(input_path) = std::env::var("MIDAS_INPUT_PATH") {
            self.processing.input_path = PathBuf::from(input_path);
        }
        if let Ok(output_path) = std::env::var("MIDAS_OUTPUT_PATH") {
            self.processing.output_path = PathBuf::from(output_path);
        }
        if let Ok(cache_path) = std::env::var("MIDAS_CACHE_PATH") {
            self.processing.cache_path = PathBuf::from(cache_path);
        }
        if let Ok(parquet_output_path) = std::env::var("MIDAS_PARQUET_OUTPUT_PATH") {
            self.processing.parquet_output_path = Some(PathBuf::from(parquet_output_path));
        }
        if let Ok(datasets) = std::env::var("MIDAS_DATASETS") {
            self.processing.datasets = datasets.split(',').map(|s| s.trim().to_string()).collect();
        }
        if let Ok(dry_run) = std::env::var("MIDAS_DRY_RUN") {
            self.processing.dry_run = dry_run.parse().unwrap_or(false);
        }

        // Quality control environment variables
        if let Ok(require_station_metadata) = std::env::var("MIDAS_REQUIRE_STATION_METADATA") {
            self.quality_control.require_station_metadata =
                require_station_metadata.parse().unwrap_or(true);
        }
        if let Ok(exclude_empty_measurements) = std::env::var("MIDAS_EXCLUDE_EMPTY_MEASUREMENTS") {
            self.quality_control.exclude_empty_measurements =
                exclude_empty_measurements.parse().unwrap_or(true);
        }

        // Performance environment variables
        if let Ok(workers) = std::env::var("MIDAS_PARALLEL_WORKERS") {
            self.performance.parallel_workers = workers.parse().unwrap_or(DEFAULT_PARALLEL_WORKERS);
        }
        if let Ok(memory) = std::env::var("MIDAS_MEMORY_LIMIT_GB") {
            self.performance.memory_limit_gb = memory.parse().unwrap_or(DEFAULT_MEMORY_LIMIT_GB);
        }

        // Logging environment variables
        if let Ok(level) = std::env::var("MIDAS_LOG_LEVEL") {
            self.logging.level = level;
        }

        Ok(())
    }

    /// Validate configuration for consistency and valid ranges
    pub fn validate(&self) -> Result<()> {
        // Validate required paths
        if self.processing.input_path.as_os_str().is_empty() {
            return Err(Error::configuration("Input path is required".to_string()));
        }
        if self.processing.output_path.as_os_str().is_empty() {
            return Err(Error::configuration("Output path is required".to_string()));
        }
        if self.processing.cache_path.as_os_str().is_empty() {
            return Err(Error::configuration("Cache path is required".to_string()));
        }

        // Validate input path exists
        if !self.processing.input_path.exists() {
            return Err(Error::configuration(format!(
                "Input path does not exist: {}",
                self.processing.input_path.display()
            )));
        }

        // Validate datasets are not empty
        if self.processing.datasets.is_empty() {
            return Err(Error::configuration(
                "At least one dataset must be specified".to_string(),
            ));
        }

        // Note: Processing quality control validation
        // MIDAS data quality indicators are not validated here as they are preserved

        // Validate parquet settings
        if self.parquet.row_group_size == 0 {
            return Err(Error::configuration(
                "Row group size must be greater than 0".to_string(),
            ));
        }
        if self.parquet.page_size_mb == 0 {
            return Err(Error::configuration(
                "Page size must be greater than 0".to_string(),
            ));
        }

        // Validate performance settings
        if self.performance.parallel_workers == 0 {
            return Err(Error::configuration(
                "Parallel workers must be greater than 0".to_string(),
            ));
        }
        if self.performance.parallel_workers > 100 {
            return Err(Error::configuration(
                "Parallel workers must be 100 or less".to_string(),
            ));
        }
        if self.performance.memory_limit_gb == 0 {
            return Err(Error::configuration(
                "Memory limit must be greater than 0".to_string(),
            ));
        }

        // Validate logging settings
        let valid_log_levels = ["error", "warn", "info", "debug", "trace"];
        if !valid_log_levels.contains(&self.logging.level.as_str()) {
            return Err(Error::configuration(format!(
                "Invalid log level '{}': must be one of {}",
                self.logging.level,
                valid_log_levels.join(", ")
            )));
        }

        let valid_log_formats = ["json", "pretty", "compact"];
        if !valid_log_formats.contains(&self.logging.format.as_str()) {
            return Err(Error::configuration(format!(
                "Invalid log format '{}': must be one of {}",
                self.logging.format,
                valid_log_formats.join(", ")
            )));
        }

        Ok(())
    }

    /// Get memory limit in bytes
    pub fn memory_limit_bytes(&self) -> usize {
        self.performance.memory_limit_gb * 1024 * 1024 * 1024
    }

    /// Get the resolved parquet output path
    /// Returns the custom parquet output path if specified, otherwise defaults to 'parquet' subfolder in input directory
    pub fn get_parquet_output_path(&self) -> PathBuf {
        match &self.processing.parquet_output_path {
            Some(path) => path.clone(),
            None => self.processing.input_path.join("parquet"),
        }
    }

    /// Get default configuration file path
    pub fn default_config_path() -> Result<PathBuf> {
        let home_dir = std::env::var("HOME")
            .map_err(|_| Error::configuration("HOME environment variable not set".to_string()))?;

        let config_dir = PathBuf::from(home_dir)
            .join(".config")
            .join("midas-processor");
        Ok(config_dir.join("config.toml"))
    }

    /// Create output directory if it doesn't exist
    pub fn ensure_output_directory(&self) -> Result<()> {
        if !self.processing.output_path.exists() {
            std::fs::create_dir_all(&self.processing.output_path).map_err(|e| {
                Error::configuration(format!(
                    "Failed to create output directory '{}': {}",
                    self.processing.output_path.display(),
                    e
                ))
            })?;
        }
        Ok(())
    }

    /// Create cache directory if it doesn't exist
    pub fn ensure_cache_directory(&self) -> Result<()> {
        if !self.processing.cache_path.exists() {
            std::fs::create_dir_all(&self.processing.cache_path).map_err(|e| {
                Error::configuration(format!(
                    "Failed to create cache directory '{}': {}",
                    self.processing.cache_path.display(),
                    e
                ))
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use tempfile::TempDir;

    #[test]
    fn test_config_creation() {
        let input_path = PathBuf::from("/tmp/input");
        let output_path = PathBuf::from("/tmp/output");
        let config = Config::new(input_path.clone(), output_path.clone());

        assert_eq!(config.processing.input_path, input_path);
        assert_eq!(config.processing.output_path, output_path);
        assert_eq!(config.processing.datasets, default_datasets());
        assert!(!config.processing.dry_run);
        assert!(!config.processing.force_overwrite);
    }

    #[test]
    fn test_config_defaults() {
        let qc_config = QualityControlConfig::default();
        assert!(qc_config.require_station_metadata);
        assert!(qc_config.exclude_empty_measurements);

        let parquet_config = ParquetConfig::default();
        assert_eq!(parquet_config.compression, PARQUET_COMPRESSION);
        assert_eq!(parquet_config.row_group_size, PARQUET_ROW_GROUP_SIZE);
        assert_eq!(parquet_config.page_size_mb, PARQUET_PAGE_SIZE_MB);

        let perf_config = PerformanceConfig::default();
        assert_eq!(perf_config.parallel_workers, DEFAULT_PARALLEL_WORKERS);
        assert_eq!(perf_config.memory_limit_gb, DEFAULT_MEMORY_LIMIT_GB);
    }

    #[test]
    fn test_config_validation() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().to_path_buf();
        let output_path = temp_dir.path().join("output");

        let config = Config::new(input_path, output_path);
        assert!(config.validate().is_ok());

        // Test invalid configuration
        let mut invalid_config = config.clone();
        invalid_config.processing.input_path = PathBuf::from("/nonexistent/path");
        assert!(invalid_config.validate().is_err());

        invalid_config.processing.input_path = config.processing.input_path.clone();
        invalid_config.processing.datasets.clear();
        assert!(invalid_config.validate().is_err());

        invalid_config.processing.datasets = config.processing.datasets.clone();
        invalid_config.performance.parallel_workers = 0;
        assert!(invalid_config.validate().is_err());

        invalid_config.performance.parallel_workers = config.performance.parallel_workers;
        invalid_config.logging.level = "invalid".to_string();
        assert!(invalid_config.validate().is_err());
    }

    #[test]
    fn test_config_merge() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().to_path_buf();
        let output_path = temp_dir.path().join("output");

        let mut config1 = Config::new(input_path.clone(), output_path.clone());
        let mut config2 = Config::new(input_path.clone(), output_path.clone());

        config2.processing.dry_run = true;
        config2.quality_control.require_station_metadata = false;
        config2.performance.parallel_workers = 16;

        config1.merge(config2).unwrap();

        assert!(config1.processing.dry_run);
        assert!(!config1.quality_control.require_station_metadata);
        assert_eq!(config1.performance.parallel_workers, 16);
    }

    #[test]
    fn test_config_env_vars() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().to_path_buf();
        let output_path = temp_dir.path().join("output");

        let mut config = Config::new(input_path, output_path);

        // Set environment variables
        unsafe {
            env::set_var("MIDAS_DRY_RUN", "true");
            env::set_var("MIDAS_REQUIRE_STATION_METADATA", "false");
            env::set_var("MIDAS_PARALLEL_WORKERS", "12");
            env::set_var("MIDAS_LOG_LEVEL", "debug");
        }

        config.merge_env().unwrap();

        assert!(config.processing.dry_run);
        assert!(!config.quality_control.require_station_metadata);
        assert_eq!(config.performance.parallel_workers, 12);
        assert_eq!(config.logging.level, "debug");

        // Clean up environment variables
        unsafe {
            env::remove_var("MIDAS_DRY_RUN");
            env::remove_var("MIDAS_REQUIRE_STATION_METADATA");
            env::remove_var("MIDAS_PARALLEL_WORKERS");
            env::remove_var("MIDAS_LOG_LEVEL");
        }
    }

    #[test]
    fn test_memory_limit_bytes() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().to_path_buf();
        let output_path = temp_dir.path().join("output");

        let mut config = Config::new(input_path, output_path);
        config.performance.memory_limit_gb = 8;

        assert_eq!(config.memory_limit_bytes(), 8 * 1024 * 1024 * 1024);
    }

    #[test]
    fn test_config_file_parsing() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");

        let config_content = r#"
[processing]
input_path = "/tmp/test_input"
output_path = "/tmp/test_output"
datasets = ["uk-daily-temperature-obs"]
dry_run = true

[quality_control]
require_station_metadata = false
exclude_empty_measurements = true

[performance]
parallel_workers = 6
memory_limit_gb = 8
"#;

        std::fs::write(&config_path, config_content).unwrap();

        let config = Config::from_file(&config_path).unwrap();
        assert_eq!(
            config.processing.input_path,
            PathBuf::from("/tmp/test_input")
        );
        assert_eq!(
            config.processing.output_path,
            PathBuf::from("/tmp/test_output")
        );
        assert_eq!(config.processing.datasets, vec!["uk-daily-temperature-obs"]);
        assert!(config.processing.dry_run);
        assert!(!config.quality_control.require_station_metadata);
        assert!(config.quality_control.exclude_empty_measurements);
        assert_eq!(config.performance.parallel_workers, 6);
        assert_eq!(config.performance.memory_limit_gb, 8);
    }
}
