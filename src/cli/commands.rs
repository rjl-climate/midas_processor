//! Command implementations for MIDAS processor CLI
//!
//! This module contains the main command execution logic, progress reporting,
//! and error handling for the CLI interface.

use crate::cli::args::{Args, OutputFormat};
use crate::config::Config;
use crate::{Error, Result};
use indicatif::{HumanDuration, ProgressBar, ProgressStyle};
use std::time::Instant;
use tracing::{debug, error, info, warn};

/// Processing statistics for reporting
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

/// Main command runner for MIDAS processor
///
/// This function orchestrates the entire processing workflow:
/// 1. Set up logging and configuration
/// 2. Validate inputs and create output directories
/// 3. Process datasets with progress reporting
/// 4. Generate summary statistics
pub async fn run(args: Args) -> Result<ProcessingStats> {
    let start_time = Instant::now();

    // Set up logging
    setup_logging(&args)?;

    info!("Starting MIDAS processor");
    debug!("Command line arguments: {:?}", args);

    // Validate arguments
    args.validate()?;

    // Load configuration with layered approach
    let config = load_configuration(&args).await?;
    debug!("Loaded configuration: {:?}", config);

    // Validate and prepare directories
    prepare_directories(&config).await?;

    // Get datasets to process
    let datasets = args.get_datasets();
    info!("Processing {} datasets: {:?}", datasets.len(), datasets);

    if args.dry_run {
        return run_dry_run(&config, &datasets).await;
    }

    // Set up progress reporting
    let progress_bar = if args.show_progress() {
        let pb = ProgressBar::new(datasets.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("#>-"),
        );
        pb.set_message("Initializing...");
        Some(pb)
    } else {
        None
    };

    // Process each dataset
    let mut stats = ProcessingStats {
        datasets_processed: datasets.len(),
        ..Default::default()
    };

    for (i, dataset) in datasets.iter().enumerate() {
        if let Some(pb) = &progress_bar {
            pb.set_position(i as u64);
            pb.set_message(format!("Processing {}", dataset));
        }

        info!("Processing dataset: {}", dataset);

        match process_dataset(&config, dataset).await {
            Ok(dataset_stats) => {
                stats.files_processed += dataset_stats.files_processed;
                stats.stations_loaded += dataset_stats.stations_loaded;
                stats.observations_processed += dataset_stats.observations_processed;
                stats.output_sizes.extend(dataset_stats.output_sizes);

                info!(
                    "Completed {}: {} files, {} observations",
                    dataset, dataset_stats.files_processed, dataset_stats.observations_processed
                );
            }
            Err(e) => {
                error!("Failed to process dataset {}: {}", dataset, e);
                stats.errors_encountered += 1;

                // Continue with other datasets unless it's a critical error
                if is_critical_error(&e) {
                    return Err(e);
                }
            }
        }
    }

    if let Some(pb) = &progress_bar {
        pb.finish_with_message("Processing complete");
    }

    stats.processing_time = start_time.elapsed();

    // Generate final report
    generate_final_report(&args, &stats)?;

    Ok(stats)
}

/// Set up structured logging based on CLI arguments
fn setup_logging(args: &Args) -> Result<()> {
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

/// Load configuration using layered approach (file -> env -> args)
async fn load_configuration(args: &Args) -> Result<Config> {
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
        Some(args.input_path.clone()),
        Some(args.output_path.clone()),
        config_file,
    )?;

    // Apply CLI argument overrides
    apply_cli_overrides(&mut config, args)?;

    // Final validation
    config.validate()?;

    Ok(config)
}

/// Apply CLI argument overrides to configuration
fn apply_cli_overrides(config: &mut Config, args: &Args) -> Result<()> {
    // Override processing settings
    config.processing.datasets = args.get_datasets();
    config.processing.dry_run = args.dry_run;
    config.processing.force_overwrite = args.force_overwrite;

    // Override quality control settings
    config.quality_control.include_suspect = args.include_suspect;
    config.quality_control.include_unchecked = args.include_unchecked;
    config.quality_control.min_quality_version = args.qc_version;

    // Override performance settings
    config.performance.parallel_workers = args.workers;
    config.performance.memory_limit_gb = args.memory_limit_gb;

    // Override logging settings
    config.logging.level = args.get_log_level().to_string();
    config.logging.structured = !args.quiet;

    Ok(())
}

/// Validate and prepare output directories
async fn prepare_directories(config: &Config) -> Result<()> {
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

/// Perform a dry run showing what would be processed
async fn run_dry_run(config: &Config, datasets: &[String]) -> Result<ProcessingStats> {
    info!("Performing dry run - no files will be created");

    let mut stats = ProcessingStats {
        datasets_processed: datasets.len(),
        ..Default::default()
    };

    for dataset in datasets {
        info!("Would process dataset: {}", dataset);

        // Simulate discovery of input files
        let input_dir = config.processing.input_path.join(dataset);
        if !input_dir.exists() {
            warn!("Dataset directory does not exist: {}", input_dir.display());
            continue;
        }

        // Count files that would be processed
        let file_count = discover_dataset_files(&input_dir).await?;
        stats.files_processed += file_count;

        // Estimate output file path
        let output_file = config
            .processing
            .output_path
            .join(format!("{}.parquet", dataset));
        info!("Would create: {}", output_file.display());
    }

    info!(
        "Dry run complete: {} datasets, {} files would be processed",
        stats.datasets_processed, stats.files_processed
    );

    Ok(stats)
}

/// Process a single dataset (placeholder implementation)
async fn process_dataset(_config: &Config, dataset: &str) -> Result<ProcessingStats> {
    // This is a placeholder implementation for Task 4
    // The actual processing logic will be implemented in future tasks
    // when the station registry, CSV parser, and Parquet writer are available

    warn!(
        "Dataset processing not yet implemented - placeholder for {}",
        dataset
    );

    // Return mock statistics
    Ok(ProcessingStats {
        files_processed: 10,                                             // Mock value
        stations_loaded: 100,                                            // Mock value
        observations_processed: 10000,                                   // Mock value
        output_sizes: vec![(format!("{}.parquet", dataset), 1_000_000)], // Mock 1MB file
        ..Default::default()
    })
}

/// Discover files in a dataset directory (placeholder implementation)
async fn discover_dataset_files(_input_dir: &std::path::Path) -> Result<usize> {
    // Placeholder implementation
    // In future tasks, this will use walkdir to recursively find CSV files
    Ok(10) // Mock file count
}

/// Check if an error is critical enough to stop processing
fn is_critical_error(error: &Error) -> bool {
    matches!(
        error,
        Error::Configuration { .. }
            | Error::MemoryLimitExceeded { .. }
            | Error::ProcessingInterrupted { .. }
    )
}

/// Generate final processing report
fn generate_final_report(args: &Args, stats: &ProcessingStats) -> Result<()> {
    info!("Generating final report");

    match args.output_format {
        OutputFormat::Human => generate_human_report(stats),
        OutputFormat::Json => generate_json_report(stats),
        OutputFormat::Csv => generate_csv_report(stats),
    }
}

/// Generate human-readable report
fn generate_human_report(stats: &ProcessingStats) -> Result<()> {
    let duration = HumanDuration(stats.processing_time);
    let total_size = ProcessingStats::format_size(stats.total_output_size());

    println!("\n🎉 MIDAS Processing Complete!");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📊 Processing Summary:");
    println!("   • Datasets processed: {}", stats.datasets_processed);
    println!("   • Files processed: {}", stats.files_processed);
    println!("   • Stations loaded: {}", stats.stations_loaded);
    println!(
        "   • Observations processed: {}",
        stats.observations_processed
    );
    println!("   • Total output size: {}", total_size);
    println!("   • Processing time: {}", duration);

    if stats.errors_encountered > 0 {
        println!("⚠️  Errors encountered: {}", stats.errors_encountered);
    }

    if !stats.output_sizes.is_empty() {
        println!("\n📁 Output Files:");
        for (filename, size) in &stats.output_sizes {
            println!("   • {}: {}", filename, ProcessingStats::format_size(*size));
        }
    }

    println!();
    Ok(())
}

/// Generate JSON report for machine consumption
fn generate_json_report(stats: &ProcessingStats) -> Result<()> {
    let json_stats = serde_json::json!({
        "datasets_processed": stats.datasets_processed,
        "files_processed": stats.files_processed,
        "stations_loaded": stats.stations_loaded,
        "observations_processed": stats.observations_processed,
        "errors_encountered": stats.errors_encountered,
        "processing_time_seconds": stats.processing_time.as_secs_f64(),
        "total_output_size_bytes": stats.total_output_size(),
        "output_files": stats.output_sizes.iter().map(|(name, size)| {
            serde_json::json!({
                "filename": name,
                "size_bytes": size
            })
        }).collect::<Vec<_>>()
    });

    println!("{}", serde_json::to_string_pretty(&json_stats).unwrap());
    Ok(())
}

/// Generate CSV report for data analysis
fn generate_csv_report(stats: &ProcessingStats) -> Result<()> {
    println!("metric,value");
    println!("datasets_processed,{}", stats.datasets_processed);
    println!("files_processed,{}", stats.files_processed);
    println!("stations_loaded,{}", stats.stations_loaded);
    println!("observations_processed,{}", stats.observations_processed);
    println!("errors_encountered,{}", stats.errors_encountered);
    println!(
        "processing_time_seconds,{}",
        stats.processing_time.as_secs_f64()
    );
    println!("total_output_size_bytes,{}", stats.total_output_size());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_processing_stats() {
        let mut stats = ProcessingStats::default();

        // Test empty stats
        assert_eq!(stats.total_output_size(), 0);

        // Add some output files
        stats.output_sizes.push(("test1.parquet".to_string(), 1000));
        stats.output_sizes.push(("test2.parquet".to_string(), 2000));

        assert_eq!(stats.total_output_size(), 3000);
    }

    #[test]
    fn test_format_size() {
        assert_eq!(ProcessingStats::format_size(0), "0 B");
        assert_eq!(ProcessingStats::format_size(512), "512 B");
        assert_eq!(ProcessingStats::format_size(1024), "1.00 KB");
        assert_eq!(ProcessingStats::format_size(1536), "1.50 KB");
        assert_eq!(ProcessingStats::format_size(1024 * 1024), "1.00 MB");
        assert_eq!(ProcessingStats::format_size(1024 * 1024 * 1024), "1.00 GB");
    }

    #[test]
    fn test_is_critical_error() {
        assert!(is_critical_error(&Error::configuration("test")));
        assert!(is_critical_error(&Error::memory_limit_exceeded(100, 50)));
        assert!(is_critical_error(&Error::processing_interrupted("test")));

        assert!(!is_critical_error(&Error::data_validation("test")));
        assert!(!is_critical_error(&Error::file_not_found("test")));
    }

    #[tokio::test]
    async fn test_prepare_directories() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("output");

        let config = Config::new(temp_dir.path().to_path_buf(), output_path.clone());

        // Should create directories
        assert!(prepare_directories(&config).await.is_ok());
        assert!(output_path.exists());
        assert!(output_path.join("metadata").exists());
    }

    #[tokio::test]
    async fn test_dry_run() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().to_path_buf();
        let output_path = temp_dir.path().join("output");

        // Create mock dataset directory
        let dataset_dir = input_path.join("uk-daily-temperature-obs");
        std::fs::create_dir_all(&dataset_dir).unwrap();

        let config = Config::new(input_path, output_path);
        let datasets = vec!["uk-daily-temperature-obs".to_string()];

        let stats = run_dry_run(&config, &datasets).await.unwrap();
        assert_eq!(stats.datasets_processed, 1);
        assert!(stats.files_processed > 0);
    }

    #[test]
    fn test_apply_cli_overrides() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_path_buf();

        let mut config = Config::new(temp_path.clone(), temp_path.join("output"));

        let args = Args {
            input_path: temp_path.clone(),
            output_path: temp_path.join("output"),
            datasets: None,
            include_suspect: true,
            include_unchecked: true,
            qc_version: 2,
            dry_run: true,
            force_overwrite: true,
            config_file: None,
            workers: 16,
            memory_limit_gb: 32,
            verbose: 2,
            quiet: false,
            output_format: OutputFormat::Json,
        };

        apply_cli_overrides(&mut config, &args).unwrap();

        assert!(config.processing.dry_run);
        assert!(config.processing.force_overwrite);
        assert!(config.quality_control.include_suspect);
        assert!(config.quality_control.include_unchecked);
        assert_eq!(config.quality_control.min_quality_version, 2);
        assert_eq!(config.performance.parallel_workers, 16);
        assert_eq!(config.performance.memory_limit_gb, 32);
        assert_eq!(config.logging.level, "debug");
    }
}
