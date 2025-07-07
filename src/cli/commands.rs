//! Command implementations for MIDAS processor CLI
//!
//! This module contains the main command execution logic, progress reporting,
//! and error handling for the CLI interface.

use crate::app::services::station_registry::StationRegistry;
use crate::cli::args::{Args, Commands, OutputFormat, ProcessArgs, StationsArgs};
use crate::config::Config;
use crate::{Error, Result};
use chrono::TimeZone;
use indicatif::{HumanDuration, ProgressBar, ProgressStyle};
use std::path::PathBuf;
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
/// This function dispatches to the appropriate subcommand handler based on CLI args.
pub async fn run(args: Args) -> Result<ProcessingStats> {
    match args.get_command() {
        Commands::Process(process_args) => run_process(process_args).await,
        Commands::Stations(stations_args) => run_stations(stations_args).await,
    }
}

/// Process command runner for MIDAS processor
///
/// This function orchestrates the entire processing workflow:
/// 1. Set up logging and configuration
/// 2. Validate inputs and create output directories
/// 3. Process datasets with progress reporting
/// 4. Generate summary statistics
pub async fn run_process(args: ProcessArgs) -> Result<ProcessingStats> {
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

/// Stations command runner for MIDAS processor
///
/// This function generates station registry reports and visualizations.
pub async fn run_stations(args: StationsArgs) -> Result<ProcessingStats> {
    let start_time = Instant::now();

    // Set up logging
    setup_stations_logging(&args)?;

    info!("Starting MIDAS station registry report");
    debug!("Stations arguments: {:?}", args);

    // Validate arguments
    args.validate()?;

    // Determine cache path
    let cache_path = match &args.cache_path {
        Some(path) => path.clone(),
        None => {
            // Use default cache location
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
    };

    info!(
        "Loading station registry from cache: {}",
        cache_path.display()
    );

    // Determine datasets to load
    let datasets = match args.get_datasets() {
        Some(datasets) => datasets,
        None => {
            // Auto-discover datasets in cache
            discover_datasets(&cache_path)?
        }
    };

    info!(
        "Loading station registry for {} datasets: {:?}",
        datasets.len(),
        datasets
    );

    // Load station registry with progress bar
    let (registry, load_stats) = StationRegistry::load_from_cache(
        &cache_path,
        &datasets,
        true, // Show progress
    )
    .await?;

    info!(
        "Station registry loaded: {} stations from {} files in {:.2}s",
        load_stats.stations_loaded,
        load_stats.files_processed,
        load_stats.load_duration.as_secs_f64()
    );

    // Generate report
    generate_station_report(&args, &registry, &load_stats)?;

    // Convert to processing stats for consistency
    let stats = ProcessingStats {
        datasets_processed: datasets.len(),
        files_processed: load_stats.files_processed,
        stations_loaded: load_stats.stations_loaded,
        observations_processed: 0, // Not applicable for stations command
        errors_encountered: load_stats.errors.len(),
        processing_time: start_time.elapsed(),
        output_sizes: if let Some(output_file) = &args.output_file {
            // Try to get file size if we wrote to a file
            if let Ok(metadata) = std::fs::metadata(output_file) {
                vec![(output_file.display().to_string(), metadata.len())]
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        },
    };

    info!(
        "Station report completed in {:.2}s",
        stats.processing_time.as_secs_f64()
    );

    Ok(stats)
}

/// Set up structured logging for stations command
fn setup_stations_logging(args: &StationsArgs) -> Result<()> {
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

/// Set up structured logging based on CLI arguments
fn setup_logging(args: &ProcessArgs) -> Result<()> {
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
async fn load_configuration(args: &ProcessArgs) -> Result<Config> {
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
fn apply_cli_overrides(config: &mut Config, args: &ProcessArgs) -> Result<()> {
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
fn generate_final_report(args: &ProcessArgs, stats: &ProcessingStats) -> Result<()> {
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

/// Discover available datasets in the cache directory
fn discover_datasets(cache_path: &PathBuf) -> Result<Vec<String>> {
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

/// Generate station registry report based on output format
fn generate_station_report(
    args: &StationsArgs,
    registry: &StationRegistry,
    load_stats: &crate::app::services::station_registry::LoadStats,
) -> Result<()> {
    match args.output_format {
        OutputFormat::Human => generate_human_station_report(args, registry, load_stats),
        OutputFormat::Json => generate_json_station_report(args, registry, load_stats),
        OutputFormat::Csv => generate_csv_station_report(args, registry, load_stats),
    }
}

/// Generate human-readable station report
fn generate_human_station_report(
    args: &StationsArgs,
    registry: &StationRegistry,
    load_stats: &crate::app::services::station_registry::LoadStats,
) -> Result<()> {
    use std::collections::HashMap;

    let stations = registry.stations();
    let metadata = registry.metadata();

    // Apply filters
    let filtered_stations = apply_station_filters(args, &stations)?;

    let mut output = format!(
        "📊 MIDAS Station Registry Report\n\
         ================================\n\
         📁 Cache Path: {}\n\
         📦 Datasets: {}\n\
         🏭 Total Stations: {} (showing {} after filters)\n\
         📄 Files Processed: {}\n\
         ⏱️  Load Time: {:.2}s\n\
         \n",
        metadata.cache_path.display(),
        metadata.loaded_datasets.join(", "),
        metadata.station_count,
        filtered_stations.len(),
        load_stats.files_processed,
        load_stats.load_duration.as_secs_f64()
    );

    if !load_stats.errors.is_empty() {
        output.push_str(&format!(
            "⚠️  Load Errors: {} (see log for details)\n\n",
            load_stats.errors.len()
        ));
    }

    if !filtered_stations.is_empty() {
        // Geographic distribution analysis
        let mut county_counts: HashMap<String, usize> = HashMap::new();
        let mut elevation_ranges = [0; 4]; // [0-50m, 50-200m, 200-500m, 500m+]
        let mut temporal_active_2020 = 0;
        let mut oldest_station: Option<&crate::app::models::Station> = None;
        let mut newest_station: Option<&crate::app::models::Station> = None;

        let reference_2020 = chrono::Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();

        for station in &filtered_stations {
            // County distribution
            *county_counts
                .entry(station.historic_county.clone())
                .or_insert(0) += 1;

            // Elevation distribution
            let elevation = station.height_meters;
            if elevation < 50.0 {
                elevation_ranges[0] += 1;
            } else if elevation < 200.0 {
                elevation_ranges[1] += 1;
            } else if elevation < 500.0 {
                elevation_ranges[2] += 1;
            } else {
                elevation_ranges[3] += 1;
            }

            // Temporal analysis
            if station.src_bgn_date <= reference_2020 && station.src_end_date >= reference_2020 {
                temporal_active_2020 += 1;
            }

            // Track oldest/newest
            if oldest_station.is_none()
                || station.src_bgn_date < oldest_station.unwrap().src_bgn_date
            {
                oldest_station = Some(station);
            }
            if newest_station.is_none()
                || station.src_end_date > newest_station.unwrap().src_end_date
            {
                newest_station = Some(station);
            }
        }

        // Geographic summary
        output.push_str("🗺️  Geographic Distribution:\n");
        let mut sorted_counties: Vec<_> = county_counts.iter().collect();
        sorted_counties.sort_by(|a, b| b.1.cmp(a.1));
        for (county, count) in sorted_counties.iter().take(10) {
            let percentage = (**count as f64 / filtered_stations.len() as f64) * 100.0;
            output.push_str(&format!(
                "   • {}: {} stations ({:.1}%)\n",
                county, count, percentage
            ));
        }
        if sorted_counties.len() > 10 {
            output.push_str(&format!(
                "   • ... and {} more counties\n",
                sorted_counties.len() - 10
            ));
        }
        output.push('\n');

        // Elevation analysis
        output.push_str("🏔️  Elevation Distribution:\n");
        output.push_str(&format!(
            "   • Sea level (0-50m): {} stations\n",
            elevation_ranges[0]
        ));
        output.push_str(&format!(
            "   • Low elevation (50-200m): {} stations\n",
            elevation_ranges[1]
        ));
        output.push_str(&format!(
            "   • Mid elevation (200-500m): {} stations\n",
            elevation_ranges[2]
        ));
        output.push_str(&format!(
            "   • High elevation (500m+): {} stations\n",
            elevation_ranges[3]
        ));
        output.push('\n');

        // Temporal analysis
        output.push_str("⏰ Temporal Coverage:\n");
        if let Some(oldest) = oldest_station {
            output.push_str(&format!(
                "   • Oldest record: {} ({})\n",
                oldest.src_bgn_date.format("%Y-%m-%d"),
                oldest.src_name
            ));
        }
        if let Some(newest) = newest_station {
            output.push_str(&format!(
                "   • Latest record: {} ({})\n",
                newest.src_end_date.format("%Y-%m-%d"),
                newest.src_name
            ));
        }
        output.push_str(&format!(
            "   • Active in 2020: {} stations\n",
            temporal_active_2020
        ));
        output.push('\n');

        // Detailed listings if requested
        if args.detailed {
            output.push_str("📋 Detailed Station Listing:\n");
            output.push_str("ID     | Name                    | County              | Lat     | Lon      | Elev(m) | Active Period\n");
            output.push_str("-------|-------------------------|---------------------|---------|----------|---------|----------------------------\n");

            let mut sorted_stations = filtered_stations.clone();
            sorted_stations.sort_by_key(|s| s.src_id);

            for station in sorted_stations.iter().take(50) {
                // Limit to first 50 for readability
                output.push_str(&format!(
                    "{:6} | {:23} | {:19} | {:7.3} | {:8.3} | {:7.1} | {} to {}\n",
                    station.src_id,
                    if station.src_name.len() > 23 {
                        station.src_name[..20].to_owned() + "..."
                    } else {
                        station.src_name.clone()
                    },
                    if station.historic_county.len() > 19 {
                        station.historic_county[..16].to_owned() + "..."
                    } else {
                        station.historic_county.clone()
                    },
                    station.high_prcn_lat,
                    station.high_prcn_lon,
                    station.height_meters,
                    station.src_bgn_date.format("%Y-%m-%d"),
                    station.src_end_date.format("%Y-%m-%d")
                ));
            }

            if sorted_stations.len() > 50 {
                output.push_str(&format!("\n... and {} more stations (use --output-file with CSV format for complete listing)\n", sorted_stations.len() - 50));
            }
        } else {
            output.push_str("💡 Use --detailed flag for complete station listings\n");
        }
    } else {
        output.push_str("No stations found matching the specified filters.\n");
    }

    // Output the report
    match &args.output_file {
        Some(path) => {
            std::fs::write(path, &output).map_err(|e| {
                Error::configuration(format!(
                    "Failed to write report to {}: {}",
                    path.display(),
                    e
                ))
            })?;
            info!("Station report written to: {}", path.display());
        }
        None => {
            println!("{}", output);
        }
    }

    Ok(())
}

/// Generate JSON station report
fn generate_json_station_report(
    args: &StationsArgs,
    registry: &StationRegistry,
    load_stats: &crate::app::services::station_registry::LoadStats,
) -> Result<()> {
    use serde_json::json;

    let stations = registry.stations();
    let metadata = registry.metadata();
    let filtered_stations = apply_station_filters(args, &stations)?;

    let json_stations: Vec<_> = filtered_stations
        .iter()
        .map(|station| {
            json!({
                "src_id": station.src_id,
                "name": station.src_name,
                "coordinates": {
                    "latitude": station.high_prcn_lat,
                    "longitude": station.high_prcn_lon
                },
                "elevation_m": station.height_meters,
                "location": {
                    "county": station.historic_county,
                    "grid_reference": {
                        "east": station.east_grid_ref,
                        "north": station.north_grid_ref,
                        "type": station.grid_ref_type
                    }
                },
                "active_period": {
                    "start": station.src_bgn_date.format("%Y-%m-%d").to_string(),
                    "end": station.src_end_date.format("%Y-%m-%d").to_string()
                },
                "authority": station.authority
            })
        })
        .collect();

    let json_report = json!({
        "metadata": {
            "cache_path": metadata.cache_path,
            "datasets": metadata.loaded_datasets,
            "total_stations_in_registry": metadata.station_count,
            "stations_in_report": filtered_stations.len(),
            "files_processed": load_stats.files_processed,
            "load_duration_seconds": load_stats.load_duration.as_secs_f64(),
            "load_errors": load_stats.errors.len(),
            "generated_at": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
        },
        "filters_applied": {
            "datasets": args.get_datasets(),
            "region": args.region,
            "active_period": args.active_period
        },
        "stations": json_stations
    });

    let json_string = serde_json::to_string_pretty(&json_report)
        .map_err(|e| Error::configuration(format!("Failed to serialize station report: {}", e)))?;

    match &args.output_file {
        Some(path) => {
            std::fs::write(path, &json_string).map_err(|e| {
                Error::configuration(format!(
                    "Failed to write JSON report to {}: {}",
                    path.display(),
                    e
                ))
            })?;
            info!("JSON station report written to: {}", path.display());
        }
        None => {
            println!("{}", json_string);
        }
    }

    Ok(())
}

/// Generate CSV station report
fn generate_csv_station_report(
    args: &StationsArgs,
    registry: &StationRegistry,
    _load_stats: &crate::app::services::station_registry::LoadStats,
) -> Result<()> {
    let stations = registry.stations();
    let filtered_stations = apply_station_filters(args, &stations)?;

    let mut csv = String::new();
    csv.push_str("src_id,name,latitude,longitude,elevation_m,county,authority,start_date,end_date,east_grid_ref,north_grid_ref,grid_ref_type\n");

    let mut sorted_stations = filtered_stations;
    sorted_stations.sort_by_key(|s| s.src_id);

    for station in sorted_stations {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{}\n",
            station.src_id,
            csv_escape(&station.src_name),
            station.high_prcn_lat,
            station.high_prcn_lon,
            station.height_meters,
            csv_escape(&station.historic_county),
            csv_escape(&station.authority),
            station.src_bgn_date.format("%Y-%m-%d"),
            station.src_end_date.format("%Y-%m-%d"),
            station
                .east_grid_ref
                .map_or_else(|| "".to_string(), |v| v.to_string()),
            station
                .north_grid_ref
                .map_or_else(|| "".to_string(), |v| v.to_string()),
            station.grid_ref_type.as_deref().unwrap_or("")
        ));
    }

    match &args.output_file {
        Some(path) => {
            std::fs::write(path, &csv).map_err(|e| {
                Error::configuration(format!(
                    "Failed to write CSV report to {}: {}",
                    path.display(),
                    e
                ))
            })?;
            info!("CSV station report written to: {}", path.display());
        }
        None => {
            println!("{}", csv);
        }
    }

    Ok(())
}

/// Apply filters to station list based on command arguments
fn apply_station_filters<'a>(
    args: &StationsArgs,
    stations: &'a [&'a crate::app::models::Station],
) -> Result<Vec<&'a crate::app::models::Station>> {
    let mut filtered = stations.to_vec();

    // Apply geographic region filter
    if let Some(region) = &args.region {
        let (min_lat, max_lat, min_lon, max_lon) = args.parse_region(region)?;
        filtered.retain(|station| {
            station.high_prcn_lat >= min_lat
                && station.high_prcn_lat <= max_lat
                && station.high_prcn_lon >= min_lon
                && station.high_prcn_lon <= max_lon
        });
    }

    // Apply active period filter
    if let Some(active_period) = &args.active_period {
        let (start_date, end_date) = args.parse_active_period(active_period)?;
        filtered.retain(|station| {
            // Station is active if its operational period overlaps with the query period
            station.src_bgn_date <= end_date && station.src_end_date >= start_date
        });
    }

    Ok(filtered)
}

/// Escape CSV field values
fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
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

        let args = ProcessArgs {
            input_path: Some(temp_path.clone()),
            output_path: Some(temp_path.join("output")),
            cache_path: None,
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
