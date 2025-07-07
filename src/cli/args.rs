//! Command-line argument definitions for MIDAS processor
//!
//! This module defines the complete CLI interface using clap derive API,
//! following the specifications in PLANNING.md.

use crate::constants::{
    DATASET_NAMES, DEFAULT_DATASETS, DEFAULT_MEMORY_LIMIT_GB, DEFAULT_PARALLEL_WORKERS,
};
use crate::{Error, Result};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
use std::str::FromStr;

/// CLI arguments for the MIDAS weather data processor
///
/// Converts UK Met Office MIDAS weather observation data from CSV format
/// into optimized Apache Parquet files for fast Python data analysis.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "midas-processor",
    version,
    about = "Convert UK Met Office MIDAS weather data from CSV to optimized Parquet format",
    long_about = "A production-ready tool that processes UK Met Office MIDAS weather observation data \
                  from CSV format into optimized Apache Parquet files. Handles scientific data quality \
                  control, station metadata enrichment, and produces files optimized for Python data \
                  analysis workflows with 10x+ faster loading than CSV."
)]
pub struct Args {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

/// Available subcommands for the MIDAS processor
#[derive(Debug, Clone, Subcommand)]
pub enum Commands {
    /// Process MIDAS data from CSV to Parquet format (default command)
    Process(ProcessArgs),
    /// Generate station registry reports and visualizations
    Stations(StationsArgs),
}

/// Arguments for the process command (main data processing)
#[derive(Debug, Clone, Parser)]
pub struct ProcessArgs {
    /// Input path to MIDAS fetcher cache directory
    ///
    /// Should contain directories like uk-daily-temperature-obs/, uk-daily-rain-obs/, etc.
    /// with their respective qcv-1/ and capability/ subdirectories.
    /// If not specified, defaults to ~/Library/Application Support/midas-fetcher/cache
    #[arg(
        short = 'i',
        long = "input",
        value_name = "PATH",
        help = "Input path to MIDAS fetcher cache directory"
    )]
    pub input_path: Option<PathBuf>,

    /// Output path for generated Parquet files
    ///
    /// Will be created if it doesn't exist. Generated files will be named
    /// like uk-daily-temperature-obs.parquet, stations.parquet, etc.
    /// If not specified, defaults to ./output
    #[arg(
        short = 'o',
        long = "output",
        value_name = "PATH",
        help = "Output path for generated Parquet files"
    )]
    pub output_path: Option<PathBuf>,

    /// Cache path for storing downloaded and processed data
    ///
    /// Defaults to ~/.cache/midas-processor if not specified. Will be created
    /// if it doesn't exist. Used for caching downloaded data and intermediate files.
    #[arg(
        long = "cache-path",
        value_name = "PATH",
        help = "Cache path for storing downloaded and processed data"
    )]
    pub cache_path: Option<PathBuf>,

    /// Specific datasets to process (comma-separated list)
    ///
    /// If not specified, processes default datasets: uk-daily-temperature-obs, uk-daily-rain-obs.
    /// Available datasets: uk-daily-temperature-obs, uk-daily-weather-obs, uk-daily-rain-obs,
    /// uk-hourly-weather-obs, uk-hourly-rain-obs, uk-mean-wind-obs, uk-radiation-obs, uk-soil-temperature-obs
    #[arg(
        short = 'd',
        long = "datasets",
        value_name = "LIST",
        help = "Comma-separated list of datasets to process",
        long_help = "Specific datasets to process as a comma-separated list.\n\
                     Available datasets:\n  \
                     uk-daily-temperature-obs, uk-daily-weather-obs, uk-daily-rain-obs,\n  \
                     uk-hourly-weather-obs, uk-hourly-rain-obs, uk-mean-wind-obs,\n  \
                     uk-radiation-obs, uk-soil-temperature-obs\n\n\
                     If not specified, processes default datasets: uk-daily-temperature-obs, uk-daily-rain-obs"
    )]
    pub datasets: Option<DatasetList>,

    /// Include suspect quality data in output
    ///
    /// By default, only data that passed all quality control checks is included.
    /// This flag includes data that failed at least one QC check but may still be usable.
    #[arg(
        long = "include-suspect",
        help = "Include suspect quality data in output"
    )]
    pub include_suspect: bool,

    /// Include unchecked quality data in output
    ///
    /// By default, only data that has been through quality control is included.
    /// This flag includes data that has not been quality controlled.
    #[arg(
        long = "include-unchecked",
        help = "Include unchecked quality data in output"
    )]
    pub include_unchecked: bool,

    /// Minimum quality control version to accept
    ///
    /// MIDAS data goes through multiple QC versions. This sets the minimum
    /// version number to accept (higher is better quality).
    #[arg(
        long = "qc-version",
        value_name = "VERSION",
        default_value = "1",
        help = "Minimum quality control version to accept"
    )]
    pub qc_version: i32,

    /// Perform a dry run without actual processing
    ///
    /// Shows what would be processed without creating any output files.
    /// Useful for previewing operations and validating configuration.
    #[arg(
        long = "dry-run",
        help = "Show what would be processed without creating output files"
    )]
    pub dry_run: bool,

    /// Force overwrite of existing output files
    ///
    /// By default, the processor will not overwrite existing Parquet files.
    /// This flag forces overwriting for reprocessing data.
    #[arg(long = "force", help = "Force overwrite of existing output files")]
    pub force_overwrite: bool,

    /// Path to configuration file
    ///
    /// TOML configuration file for advanced settings. If not specified,
    /// looks for ~/.config/midas-processor/config.toml
    #[arg(
        short = 'c',
        long = "config",
        value_name = "FILE",
        help = "Path to configuration file (TOML format)"
    )]
    pub config_file: Option<PathBuf>,

    /// Number of parallel workers
    ///
    /// Controls how many files are processed concurrently. More workers
    /// can speed up processing but use more memory and CPU.
    #[arg(
        short = 'j',
        long = "workers",
        value_name = "COUNT",
        default_value_t = DEFAULT_PARALLEL_WORKERS,
        help = "Number of parallel workers for processing"
    )]
    pub workers: usize,

    /// Memory limit in GB
    ///
    /// Maximum memory usage for processing. When approaching this limit,
    /// the processor will flush buffers and reduce concurrency.
    #[arg(
        short = 'm',
        long = "memory-limit",
        value_name = "GB", 
        default_value_t = DEFAULT_MEMORY_LIMIT_GB,
        help = "Memory limit in GB for processing"
    )]
    pub memory_limit_gb: usize,

    /// Logging verbosity level
    #[arg(
        short = 'v',
        long = "verbose",
        action = clap::ArgAction::Count,
        help = "Increase logging verbosity (-v: info, -vv: debug, -vvv: trace)"
    )]
    pub verbose: u8,

    /// Suppress output (quiet mode)
    ///
    /// Only show errors and critical messages. Overrides verbose settings.
    #[arg(
        short = 'q',
        long = "quiet",
        help = "Suppress output except errors",
        conflicts_with = "verbose"
    )]
    pub quiet: bool,

    /// Output format for machine-readable results
    #[arg(
        long = "output-format",
        value_enum,
        default_value = "human",
        help = "Output format for results"
    )]
    pub output_format: OutputFormat,
}

/// Arguments for the stations command (registry reports and visualizations)
#[derive(Debug, Clone, Parser)]
pub struct StationsArgs {
    /// Cache path for station registry loading
    ///
    /// Path to the MIDAS cache directory containing station metadata. If not specified,
    /// uses the configured default cache location.
    #[arg(
        long = "cache-path",
        value_name = "PATH",
        help = "Cache path for station registry loading"
    )]
    pub cache_path: Option<PathBuf>,

    /// Specific datasets to include in station registry (comma-separated list)
    ///
    /// If not specified, loads all datasets found in the cache.
    /// Available datasets: uk-daily-temperature-obs, uk-daily-weather-obs, uk-daily-rain-obs,
    /// uk-hourly-weather-obs, uk-hourly-rain-obs, uk-mean-wind-obs, uk-radiation-obs, uk-soil-temperature-obs
    #[arg(
        short = 'd',
        long = "datasets",
        value_name = "LIST",
        help = "Comma-separated list of datasets to include"
    )]
    pub datasets: Option<DatasetList>,

    /// Output format for station report
    #[arg(
        long = "format",
        value_enum,
        default_value = "human",
        help = "Output format for station report"
    )]
    pub output_format: OutputFormat,

    /// Output file for station report
    ///
    /// If not specified, outputs to stdout
    #[arg(
        short = 'o',
        long = "output-file",
        value_name = "FILE",
        help = "Output file for station report"
    )]
    pub output_file: Option<PathBuf>,

    /// Include detailed station information
    ///
    /// By default, shows summary statistics. This flag includes full station listings.
    #[arg(
        long = "detailed",
        help = "Include detailed station information in report"
    )]
    pub detailed: bool,

    /// Filter stations by geographic region
    ///
    /// Specify a bounding box as min_lat,max_lat,min_lon,max_lon
    #[arg(
        long = "region",
        value_name = "BBOX",
        help = "Filter stations by bounding box (min_lat,max_lat,min_lon,max_lon)"
    )]
    pub region: Option<String>,

    /// Filter stations active during date range
    ///
    /// Specify date range as start_date,end_date (YYYY-MM-DD format)
    #[arg(
        long = "active-period",
        value_name = "DATES",
        help = "Filter stations active during date range (start_date,end_date)"
    )]
    pub active_period: Option<String>,

    /// Enable verbose logging output
    #[arg(
        short = 'v',
        long = "verbose",
        action = clap::ArgAction::Count,
        help = "Enable verbose logging (-v: info, -vv: debug, -vvv: trace)"
    )]
    pub verbose: u8,
}

/// Output format options for machine-readable results
#[derive(Debug, Clone, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable output
    Human,
    /// JSON format for scripting
    Json,
    /// CSV format for data analysis
    Csv,
}

/// Wrapper for parsing comma-separated dataset lists
#[derive(Debug, Clone)]
pub struct DatasetList {
    pub datasets: Vec<String>,
}

impl FromStr for DatasetList {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let datasets: Vec<String> = s
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if datasets.is_empty() {
            return Err(Error::data_validation(
                "Dataset list cannot be empty".to_string(),
            ));
        }

        // Validate each dataset name
        for dataset in &datasets {
            if !DATASET_NAMES.contains(&dataset.as_str()) {
                return Err(Error::data_validation(format!(
                    "Unknown dataset '{}'. Available datasets: {}",
                    dataset,
                    DATASET_NAMES.join(", ")
                )));
            }
        }

        Ok(DatasetList { datasets })
    }
}

impl Args {
    /// Get the command if one was specified
    pub fn get_command(&self) -> Commands {
        self.command
            .clone()
            .expect("Command should be present when get_command() is called")
    }
}

impl ProcessArgs {
    /// Validate the process command arguments for consistency
    pub fn validate(&self) -> Result<()> {
        // Validate input path exists (only if explicitly provided)
        if let Some(input_path) = &self.input_path {
            if !input_path.exists() {
                return Err(Error::configuration(format!(
                    "Input path does not exist: {}",
                    input_path.display()
                )));
            }

            if !input_path.is_dir() {
                return Err(Error::configuration(format!(
                    "Input path is not a directory: {}",
                    input_path.display()
                )));
            }
        }

        // Validate workers count
        if self.workers == 0 {
            return Err(Error::configuration(
                "Number of workers must be greater than 0".to_string(),
            ));
        }

        if self.workers > 100 {
            return Err(Error::configuration(
                "Number of workers cannot exceed 100".to_string(),
            ));
        }

        // Validate memory limit
        if self.memory_limit_gb == 0 {
            return Err(Error::configuration(
                "Memory limit must be greater than 0 GB".to_string(),
            ));
        }

        // Validate QC version
        if self.qc_version < 0 {
            return Err(Error::configuration(
                "QC version must be non-negative".to_string(),
            ));
        }

        // Validate config file exists if specified
        if let Some(config_file) = &self.config_file {
            if !config_file.exists() {
                return Err(Error::configuration(format!(
                    "Config file does not exist: {}",
                    config_file.display()
                )));
            }
        }

        Ok(())
    }

    /// Get the list of datasets to process
    pub fn get_datasets(&self) -> Vec<String> {
        match &self.datasets {
            Some(dataset_list) => dataset_list.datasets.clone(),
            None => DEFAULT_DATASETS.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// Determine the appropriate log level based on verbosity flags
    pub fn get_log_level(&self) -> &'static str {
        if self.quiet {
            "error"
        } else {
            match self.verbose {
                0 => "warn",
                1 => "info",
                2 => "debug",
                _ => "trace",
            }
        }
    }

    /// Check if we should show progress bars (not in quiet mode)
    pub fn show_progress(&self) -> bool {
        !self.quiet
    }
}

impl StationsArgs {
    /// Validate the stations command arguments for consistency
    pub fn validate(&self) -> Result<()> {
        // Validate cache path exists if specified
        if let Some(cache_path) = &self.cache_path {
            if !cache_path.exists() {
                return Err(Error::configuration(format!(
                    "Cache path does not exist: {}",
                    cache_path.display()
                )));
            }

            if !cache_path.is_dir() {
                return Err(Error::configuration(format!(
                    "Cache path is not a directory: {}",
                    cache_path.display()
                )));
            }
        }

        // Validate output file directory exists if specified
        if let Some(output_file) = &self.output_file {
            if let Some(parent) = output_file.parent() {
                if !parent.exists() {
                    return Err(Error::configuration(format!(
                        "Output file directory does not exist: {}",
                        parent.display()
                    )));
                }
            }
        }

        // Validate region format if specified
        if let Some(region) = &self.region {
            self.parse_region(region)?;
        }

        // Validate active period format if specified
        if let Some(active_period) = &self.active_period {
            self.parse_active_period(active_period)?;
        }

        Ok(())
    }

    /// Get the list of datasets to include in registry
    pub fn get_datasets(&self) -> Option<Vec<String>> {
        self.datasets.as_ref().map(|list| list.datasets.clone())
    }

    /// Determine the appropriate log level based on verbosity flags
    pub fn get_log_level(&self) -> &'static str {
        match self.verbose {
            0 => "warn", // Default level for stations command
            1 => "info",
            2 => "debug",
            _ => "trace",
        }
    }

    /// Parse region bounding box string
    pub fn parse_region(&self, region: &str) -> Result<(f64, f64, f64, f64)> {
        let parts: Vec<&str> = region.split(',').collect();
        if parts.len() != 4 {
            return Err(Error::configuration(
                "Region must be in format: min_lat,max_lat,min_lon,max_lon".to_string(),
            ));
        }

        let min_lat: f64 = parts[0]
            .trim()
            .parse()
            .map_err(|_| Error::configuration(format!("Invalid min_lat: {}", parts[0])))?;
        let max_lat: f64 = parts[1]
            .trim()
            .parse()
            .map_err(|_| Error::configuration(format!("Invalid max_lat: {}", parts[1])))?;
        let min_lon: f64 = parts[2]
            .trim()
            .parse()
            .map_err(|_| Error::configuration(format!("Invalid min_lon: {}", parts[2])))?;
        let max_lon: f64 = parts[3]
            .trim()
            .parse()
            .map_err(|_| Error::configuration(format!("Invalid max_lon: {}", parts[3])))?;

        if min_lat >= max_lat {
            return Err(Error::configuration(
                "min_lat must be less than max_lat".to_string(),
            ));
        }
        if min_lon >= max_lon {
            return Err(Error::configuration(
                "min_lon must be less than max_lon".to_string(),
            ));
        }

        Ok((min_lat, max_lat, min_lon, max_lon))
    }

    /// Parse active period date range string
    pub fn parse_active_period(
        &self,
        active_period: &str,
    ) -> Result<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)> {
        let parts: Vec<&str> = active_period.split(',').collect();
        if parts.len() != 2 {
            return Err(Error::configuration(
                "Active period must be in format: start_date,end_date (YYYY-MM-DD)".to_string(),
            ));
        }

        let start_date = chrono::NaiveDate::parse_from_str(parts[0].trim(), "%Y-%m-%d")
            .map_err(|_| Error::configuration(format!("Invalid start date: {}", parts[0])))?
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| Error::configuration("Invalid start date time".to_string()))?
            .and_utc();

        let end_date = chrono::NaiveDate::parse_from_str(parts[1].trim(), "%Y-%m-%d")
            .map_err(|_| Error::configuration(format!("Invalid end date: {}", parts[1])))?
            .and_hms_opt(23, 59, 59)
            .ok_or_else(|| Error::configuration("Invalid end date time".to_string()))?
            .and_utc();

        if start_date >= end_date {
            return Err(Error::configuration(
                "Start date must be before end date".to_string(),
            ));
        }

        Ok((start_date, end_date))
    }
}

impl Default for ProcessArgs {
    fn default() -> Self {
        Self {
            input_path: None,
            output_path: None,
            cache_path: None,
            datasets: None,
            include_suspect: false,
            include_unchecked: false,
            qc_version: 1,
            dry_run: false,
            force_overwrite: false,
            config_file: None,
            workers: DEFAULT_PARALLEL_WORKERS,
            memory_limit_gb: DEFAULT_MEMORY_LIMIT_GB,
            verbose: 0,
            quiet: false,
            output_format: OutputFormat::Human,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_dataset_list_parsing() {
        // Valid single dataset
        let result = DatasetList::from_str("uk-daily-temperature-obs").unwrap();
        assert_eq!(result.datasets, vec!["uk-daily-temperature-obs"]);

        // Valid multiple datasets
        let result = DatasetList::from_str("uk-daily-temperature-obs,uk-daily-rain-obs").unwrap();
        assert_eq!(
            result.datasets,
            vec!["uk-daily-temperature-obs", "uk-daily-rain-obs"]
        );

        // Valid with spaces
        let result =
            DatasetList::from_str(" uk-daily-temperature-obs , uk-daily-rain-obs ").unwrap();
        assert_eq!(
            result.datasets,
            vec!["uk-daily-temperature-obs", "uk-daily-rain-obs"]
        );

        // Invalid dataset name
        let result = DatasetList::from_str("invalid-dataset");
        assert!(result.is_err());

        // Empty string
        let result = DatasetList::from_str("");
        assert!(result.is_err());

        // Only commas
        let result = DatasetList::from_str(",,,");
        assert!(result.is_err());
    }

    #[test]
    fn test_process_args_validation() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_path_buf();

        let args = ProcessArgs {
            input_path: Some(temp_path.clone()),
            output_path: Some(temp_path.join("output")),
            cache_path: None,
            datasets: None,
            include_suspect: false,
            include_unchecked: false,
            qc_version: 1,
            dry_run: false,
            force_overwrite: false,
            config_file: None,
            workers: 4,
            memory_limit_gb: 8,
            verbose: 0,
            quiet: false,
            output_format: OutputFormat::Human,
        };

        assert!(args.validate().is_ok());

        // Test invalid workers
        let mut invalid_args = args.clone();
        invalid_args.workers = 0;
        assert!(invalid_args.validate().is_err());

        invalid_args.workers = 101;
        assert!(invalid_args.validate().is_err());

        // Test invalid memory limit
        let mut invalid_args = args.clone();
        invalid_args.memory_limit_gb = 0;
        assert!(invalid_args.validate().is_err());

        // Test invalid QC version
        let mut invalid_args = args.clone();
        invalid_args.qc_version = -1;
        assert!(invalid_args.validate().is_err());

        // Test nonexistent input path
        let mut invalid_args = args.clone();
        invalid_args.input_path = Some(PathBuf::from("/nonexistent/path"));
        assert!(invalid_args.validate().is_err());
    }

    #[test]
    fn test_process_args_get_datasets() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_path_buf();

        // Default datasets
        let args = ProcessArgs {
            input_path: Some(temp_path.clone()),
            output_path: Some(temp_path.join("output")),
            cache_path: None,
            datasets: None,
            include_suspect: false,
            include_unchecked: false,
            qc_version: 1,
            dry_run: false,
            force_overwrite: false,
            config_file: None,
            workers: 4,
            memory_limit_gb: 8,
            verbose: 0,
            quiet: false,
            output_format: OutputFormat::Human,
        };

        let datasets = args.get_datasets();
        assert_eq!(datasets, DEFAULT_DATASETS);

        // Custom datasets
        let mut args = args;
        args.datasets = Some(DatasetList {
            datasets: vec!["uk-daily-temperature-obs".to_string()],
        });

        let datasets = args.get_datasets();
        assert_eq!(datasets, vec!["uk-daily-temperature-obs"]);
    }

    #[test]
    fn test_log_level() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_path_buf();

        let mut args = ProcessArgs {
            input_path: Some(temp_path.clone()),
            output_path: Some(temp_path.join("output")),
            cache_path: None,
            datasets: None,
            include_suspect: false,
            include_unchecked: false,
            qc_version: 1,
            dry_run: false,
            force_overwrite: false,
            config_file: None,
            workers: 4,
            memory_limit_gb: 8,
            verbose: 0,
            quiet: false,
            output_format: OutputFormat::Human,
        };

        // Default level
        assert_eq!(args.get_log_level(), "warn");

        // Verbose levels
        args.verbose = 1;
        assert_eq!(args.get_log_level(), "info");

        args.verbose = 2;
        assert_eq!(args.get_log_level(), "debug");

        args.verbose = 3;
        assert_eq!(args.get_log_level(), "trace");

        // Quiet mode
        args.quiet = true;
        assert_eq!(args.get_log_level(), "error");
    }

    #[test]
    fn test_show_progress() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_path_buf();

        let mut args = ProcessArgs {
            input_path: Some(temp_path.clone()),
            output_path: Some(temp_path.join("output")),
            cache_path: None,
            datasets: None,
            include_suspect: false,
            include_unchecked: false,
            qc_version: 1,
            dry_run: false,
            force_overwrite: false,
            config_file: None,
            workers: 4,
            memory_limit_gb: 8,
            verbose: 0,
            quiet: false,
            output_format: OutputFormat::Human,
        };

        assert!(args.show_progress());

        args.quiet = true;
        assert!(!args.show_progress());
    }
}
