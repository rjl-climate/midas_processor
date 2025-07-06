//! Command-line argument definitions for MIDAS processor
//!
//! This module defines the complete CLI interface using clap derive API,
//! following the specifications in PLANNING.md.

use crate::constants::{
    DATASET_NAMES, DEFAULT_DATASETS, DEFAULT_MEMORY_LIMIT_GB, DEFAULT_PARALLEL_WORKERS,
};
use crate::{Error, Result};
use clap::{Parser, ValueEnum};
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
    /// Input path to MIDAS fetcher cache directory
    ///
    /// Should contain directories like uk-daily-temperature-obs/, uk-daily-rain-obs/, etc.
    /// with their respective qcv-1/ and capability/ subdirectories.
    #[arg(
        short = 'i',
        long = "input",
        value_name = "PATH",
        help = "Input path to MIDAS fetcher cache directory"
    )]
    pub input_path: PathBuf,

    /// Output path for generated Parquet files
    ///
    /// Will be created if it doesn't exist. Generated files will be named
    /// like uk-daily-temperature-obs.parquet, stations.parquet, etc.
    #[arg(
        short = 'o',
        long = "output",
        value_name = "PATH",
        help = "Output path for generated Parquet files"
    )]
    pub output_path: PathBuf,

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
    /// Validate the command line arguments for consistency
    pub fn validate(&self) -> Result<()> {
        // Validate input path exists
        if !self.input_path.exists() {
            return Err(Error::configuration(format!(
                "Input path does not exist: {}",
                self.input_path.display()
            )));
        }

        if !self.input_path.is_dir() {
            return Err(Error::configuration(format!(
                "Input path is not a directory: {}",
                self.input_path.display()
            )));
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
    fn test_args_validation() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_path_buf();

        let args = Args {
            input_path: temp_path.clone(),
            output_path: temp_path.join("output"),
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
        invalid_args.input_path = PathBuf::from("/nonexistent/path");
        assert!(invalid_args.validate().is_err());
    }

    #[test]
    fn test_get_datasets() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_path_buf();

        // Default datasets
        let args = Args {
            input_path: temp_path.clone(),
            output_path: temp_path.join("output"),
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

        let mut args = Args {
            input_path: temp_path.clone(),
            output_path: temp_path.join("output"),
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

        let mut args = Args {
            input_path: temp_path.clone(),
            output_path: temp_path.join("output"),
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
