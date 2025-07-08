//! Shared components for CLI commands
//!
//! This module contains common types, utilities, and functions used across
//! multiple CLI command implementations.

use crate::cli::args::{ProcessArgs, StationsArgs, ValidateArgs};
use crate::config::Config;
use crate::{Error, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use tracing::{debug, info, warn};

/// Processing statistics for reporting across all commands
#[derive(Debug, Clone, Default)]
pub struct ProcessingStats {
    /// Number of datasets processed
    pub datasets_processed: usize,
    /// Number of files processed
    pub files_processed: usize,
    /// Number of stations loaded
    pub stations_loaded: usize,
    /// Number of observations processed
    pub observations_processed: usize,
    /// Number of errors encountered
    pub errors_encountered: usize,
    /// Total processing time
    pub processing_time: std::time::Duration,
    /// Output file sizes in bytes
    pub output_sizes: Vec<(String, u64)>,
}

impl ProcessingStats {
    /// Calculate total output size in bytes
    pub fn total_output_size(&self) -> u64 {
        self.output_sizes.iter().map(|(_, size)| size).sum()
    }

    /// Format output size in human-readable format
    pub fn format_size(bytes: u64) -> String {
        const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
        let mut size = bytes as f64;
        let mut unit_index = 0;

        while size >= 1024.0 && unit_index < UNITS.len() - 1 {
            size /= 1024.0;
            unit_index += 1;
        }

        if unit_index == 0 {
            format!("{} {}", bytes, UNITS[unit_index])
        } else {
            format!("{:.2} {}", size, UNITS[unit_index])
        }
    }
}

/// Set up structured logging for process command
pub fn setup_logging(args: &ProcessArgs) -> Result<()> {
    use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

    let log_level = args.get_log_level();

    // Create filter
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("midas_processor={}", log_level)));

    // Set up subscriber based on output format preference
    if args.quiet {
        // Minimal logging for quiet mode
        tracing_subscriber::registry()
            .with(filter)
            .with(
                fmt::layer()
                    .with_target(false)
                    .with_level(true)
                    .with_writer(std::io::stderr)
                    .compact(),
            )
            .init();
    } else {
        // Standard logging with timestamps
        tracing_subscriber::registry()
            .with(filter)
            .with(
                fmt::layer()
                    .with_target(false)
                    .with_level(true)
                    .with_timer(fmt::time::uptime())
                    .with_writer(std::io::stderr),
            )
            .init();
    }

    debug!("Logging initialized at level: {}", log_level);
    Ok(())
}

/// Set up structured logging for stations command
pub fn setup_stations_logging(args: &StationsArgs) -> Result<()> {
    use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

    let log_level = args.get_log_level();

    // Create filter
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("midas_processor={}", log_level)));

    // Standard logging with timestamps
    tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .with_target(false)
                .with_level(true)
                .with_timer(fmt::time::uptime())
                .with_writer(std::io::stderr),
        )
        .init();

    debug!("Logging initialized at level: {}", log_level);
    Ok(())
}

/// Set up structured logging for validate command
pub fn setup_validate_logging(args: &ValidateArgs) -> Result<()> {
    use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

    let log_level = args.get_log_level();

    // Create filter
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("midas_processor={}", log_level)));

    // Standard logging with timestamps
    tracing_subscriber::registry()
        .with(filter)
        .with(
            fmt::layer()
                .with_target(false)
                .with_level(true)
                .with_timer(fmt::time::uptime())
                .with_writer(std::io::stderr),
        )
        .init();

    debug!("Logging initialized at level: {}", log_level);
    Ok(())
}

/// Load configuration using layered approach (file -> env -> args)
pub async fn load_configuration(args: &ProcessArgs) -> Result<Config> {
    info!("Loading configuration");

    // Determine config file path
    let default_config_path = if args.config_file.is_none() {
        Config::default_config_path().ok()
    } else {
        None
    };

    let config_file = match &args.config_file {
        Some(path) => Some(path.as_path()),
        None => {
            // Try default config file location
            default_config_path
                .as_ref()
                .filter(|path| path.exists())
                .map(|path| path.as_path())
        }
    };

    if let Some(config_path) = config_file {
        info!("Using config file: {}", config_path.display());
    } else {
        info!("No config file found, using defaults and environment variables");
    }

    // Load with layered configuration
    let mut config = Config::load_layered(
        args.input_path.clone(),
        args.output_path.clone(),
        config_file,
    )?;

    // Apply CLI argument overrides
    apply_cli_overrides(&mut config, args)?;

    // Final validation
    config.validate()?;

    Ok(config)
}

/// Apply CLI argument overrides to configuration
pub fn apply_cli_overrides(config: &mut Config, args: &ProcessArgs) -> Result<()> {
    // Override path settings if explicitly provided
    if let Some(input_path) = &args.input_path {
        config.processing.input_path = input_path.clone();
    }
    if let Some(output_path) = &args.output_path {
        config.processing.output_path = output_path.clone();
    }
    if let Some(cache_path) = &args.cache_path {
        config.processing.cache_path = cache_path.clone();
    }
    if let Some(parquet_output_path) = &args.parquet_output_path {
        config.processing.parquet_output_path = Some(parquet_output_path.clone());
    }

    // Override processing settings
    config.processing.datasets = args.get_datasets();
    config.processing.dry_run = args.dry_run;
    config.processing.force_overwrite = args.force_overwrite;

    // Override processing quality control settings (MIDAS data quality preserved)
    config.quality_control.require_station_metadata = !args.allow_missing_stations;
    config.quality_control.exclude_empty_measurements = !args.include_empty_measurements;

    // Override performance settings
    config.performance.parallel_workers = args.workers;
    config.performance.memory_limit_gb = args.memory_limit_gb;

    // Override logging settings
    config.logging.level = args.get_log_level().to_string();
    config.logging.structured = !args.quiet;

    Ok(())
}

/// Validate and prepare output directories
pub async fn prepare_directories(config: &Config) -> Result<()> {
    info!("Preparing output directories");

    // Create output directory if it doesn't exist
    config.ensure_output_directory()?;

    // Create metadata subdirectory
    let metadata_dir = config.processing.output_path.join("metadata");
    if !metadata_dir.exists() {
        std::fs::create_dir_all(&metadata_dir).map_err(|e| {
            Error::configuration(format!(
                "Failed to create metadata directory '{}': {}",
                metadata_dir.display(),
                e
            ))
        })?;
    }

    info!(
        "Output directory prepared: {}",
        config.processing.output_path.display()
    );
    Ok(())
}

/// Discover CSV files in a dataset directory
pub fn discover_csv_files(dataset_dir: &std::path::Path) -> Result<Vec<std::path::PathBuf>> {
    use walkdir::WalkDir;

    let mut csv_files = Vec::new();

    // Look for CSV files in qcv-1 subdirectory (latest quality control version)
    let qcv_dir = dataset_dir.join("qcv-1");
    if qcv_dir.exists() {
        for entry in WalkDir::new(&qcv_dir)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("csv") {
                // Skip capability files - we only want observation data files
                if !path.to_string_lossy().contains("capability") {
                    csv_files.push(path.to_path_buf());
                }
            }
        }
    }

    // If no qcv-1 directory, try qcv-0 (original quality control version)
    if csv_files.is_empty() {
        let qcv0_dir = dataset_dir.join("qcv-0");
        if qcv0_dir.exists() {
            for entry in WalkDir::new(&qcv0_dir)
                .follow_links(false)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("csv") {
                    // Skip capability files
                    if !path.to_string_lossy().contains("capability") {
                        csv_files.push(path.to_path_buf());
                    }
                }
            }
        }
    }

    // Sort files for consistent processing order
    csv_files.sort();

    debug!(
        "Discovered {} CSV files in {}",
        csv_files.len(),
        dataset_dir.display()
    );
    for file in &csv_files {
        debug!("  Found: {}", file.display());
    }

    Ok(csv_files)
}

/// Discover files in a dataset directory (legacy function for compatibility)
pub async fn discover_dataset_files(input_dir: &std::path::Path) -> Result<usize> {
    let files = discover_csv_files(input_dir)?;
    Ok(files.len())
}

/// Check if an error is critical enough to stop processing
pub fn is_critical_error(error: &Error) -> bool {
    matches!(
        error,
        Error::Configuration { .. }
            | Error::MemoryLimitExceeded { .. }
            | Error::ProcessingInterrupted { .. }
    )
}

/// Discover available datasets in the cache directory
pub fn discover_datasets(cache_path: &PathBuf) -> Result<Vec<String>> {
    use std::fs;

    let mut datasets = Vec::new();

    if !cache_path.exists() {
        return Err(Error::configuration(format!(
            "Cache path does not exist: {}",
            cache_path.display()
        )));
    }

    for entry in fs::read_dir(cache_path).map_err(|e| {
        Error::configuration(format!(
            "Failed to read cache directory {}: {}",
            cache_path.display(),
            e
        ))
    })? {
        let entry = entry
            .map_err(|e| Error::configuration(format!("Failed to read directory entry: {}", e)))?;

        let path = entry.path();
        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                // Check if it looks like a MIDAS dataset name
                if name.starts_with("uk-") && (name.contains("-obs") || name.contains("-weather")) {
                    datasets.push(name.to_string());
                }
            }
        }
    }

    datasets.sort();

    if datasets.is_empty() {
        warn!(
            "No MIDAS datasets found in cache directory: {}",
            cache_path.display()
        );
    } else {
        debug!("Discovered {} datasets: {:?}", datasets.len(), datasets);
    }

    Ok(datasets)
}

/// Create a progress bar with appropriate styling
pub fn create_progress_bar(total: u64, message: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg} [{per_sec}] ETA: {eta}")
            .unwrap()
            .progress_chars("#>-"),
    );
    pb.set_message(message.to_string());
    pb
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_processing_stats_default() {
        let stats = ProcessingStats::default();
        assert_eq!(stats.datasets_processed, 0);
        assert_eq!(stats.files_processed, 0);
        assert_eq!(stats.total_output_size(), 0);
    }

    #[test]
    fn test_processing_stats_total_output_size() {
        let stats = ProcessingStats {
            output_sizes: vec![
                ("file1.parquet".to_string(), 1000),
                ("file2.parquet".to_string(), 2000),
            ],
            ..Default::default()
        };
        assert_eq!(stats.total_output_size(), 3000);
    }

    #[test]
    fn test_format_size() {
        assert_eq!(ProcessingStats::format_size(500), "500 B");
        assert_eq!(ProcessingStats::format_size(1536), "1.50 KB");
        assert_eq!(ProcessingStats::format_size(1048576), "1.00 MB");
        assert_eq!(ProcessingStats::format_size(1073741824), "1.00 GB");
    }

    #[test]
    fn test_is_critical_error() {
        let config_error = Error::configuration("Test config error".to_string());
        let memory_error = Error::memory_limit_exceeded(512, 256);
        let io_error = Error::io(
            "Test IO error".to_string(),
            std::io::Error::new(std::io::ErrorKind::NotFound, "test"),
        );

        assert!(is_critical_error(&config_error));
        assert!(is_critical_error(&memory_error));
        assert!(!is_critical_error(&io_error));
    }

    #[test]
    fn test_discover_csv_files_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let result = discover_csv_files(temp_dir.path());
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_discover_dataset_files() {
        let temp_dir = TempDir::new().unwrap();
        let result = discover_dataset_files(temp_dir.path()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }
}
