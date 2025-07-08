//! Process command implementation for MIDAS processor CLI
//!
//! This module contains the complete data processing workflow including
//! configuration loading, dataset processing, and report generation.

use super::parallel_processor::ParallelProcessor;
use super::shared::{
    ProcessingStats, create_progress_bar, discover_csv_files, discover_datasets, is_critical_error,
    load_configuration, prepare_directories, setup_logging,
};
use crate::app::services::badc_csv_parser::BadcCsvParser;
use crate::app::services::parquet_writer::{WriterConfig, write_dataset_to_parquet};
use crate::app::services::record_processor::RecordProcessor;
use crate::app::services::station_registry::StationRegistry;
use crate::cli::args::{OutputFormat, ProcessArgs};
use crate::config::Config;
use crate::{Error, Result};
use indicatif::HumanDuration;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

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

    // Get datasets to process - use interactive selection if none specified
    let datasets = if args.datasets.is_none() && !args.quiet {
        // Interactive mode - discover available datasets and prompt user
        info!("No datasets specified, entering interactive mode");

        let input_path = match &args.input_path {
            Some(path) => path.clone(),
            None => config.processing.input_path.clone(),
        };

        let available_datasets = discover_datasets(&input_path)
            .map_err(|e| Error::configuration(format!("Failed to discover datasets: {}", e)))?;

        if available_datasets.is_empty() {
            return Err(Error::configuration(format!(
                "No MIDAS datasets found in input directory: {}",
                input_path.display()
            )));
        }

        {
            info!(
                "Prompting user for dataset selection from {} available datasets",
                available_datasets.len()
            );
            let selected = crate::cli::input::prompt_dataset_selection(&available_datasets)?;
            info!("User selected {} datasets: {:?}", selected.len(), selected);
            selected
        }
    } else {
        args.get_datasets()
    };

    info!("Processing {} datasets: {:?}", datasets.len(), datasets);

    if args.dry_run {
        return run_dry_run(&config, &datasets).await;
    }

    // Process each dataset
    let mut stats = ProcessingStats {
        datasets_processed: datasets.len(),
        ..Default::default()
    };

    for (i, dataset) in datasets.iter().enumerate() {
        info!(
            "Processing dataset {} of {}: {}",
            i + 1,
            datasets.len(),
            dataset
        );

        match process_dataset(&config, dataset, args.show_progress()).await {
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

    stats.processing_time = start_time.elapsed();

    // Generate final report
    generate_final_report(&args, &stats)?;

    Ok(stats)
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
        let csv_files = discover_csv_files(&input_dir)?;
        stats.files_processed += csv_files.len();

        // Estimate output file path
        let parquet_output_path = config.get_parquet_output_path();
        let output_file = crate::app::services::parquet_writer::utils::create_dataset_output_path(
            dataset,
            &parquet_output_path,
        );
        info!("Would create: {}", output_file.display());
    }

    info!(
        "Dry run complete: {} datasets, {} files would be processed",
        stats.datasets_processed, stats.files_processed
    );

    Ok(stats)
}

/// Process a single dataset with full pipeline
async fn process_dataset(
    config: &Config,
    dataset: &str,
    show_progress: bool,
) -> Result<ProcessingStats> {
    info!("Processing dataset: {}", dataset);
    let start_time = Instant::now();

    // Build dataset input path
    let dataset_path = config.processing.input_path.join(dataset);
    if !dataset_path.exists() {
        return Err(Error::file_not_found(format!(
            "Dataset directory not found: {}",
            dataset_path.display()
        )));
    }

    // Load station registry for this dataset
    info!("Loading station registry for dataset: {}", dataset);
    let (station_registry, load_stats) =
        StationRegistry::load_for_dataset(&config.processing.input_path, dataset, false).await?;

    info!(
        "Station registry loaded: {} stations from {} files in {:.2}s",
        load_stats.stations_loaded,
        load_stats.files_processed,
        load_stats.load_duration.as_secs_f64()
    );

    // Discover CSV files to process
    let csv_files = discover_csv_files(&dataset_path)?;
    info!("Discovered {} CSV files to process", csv_files.len());

    if csv_files.is_empty() {
        warn!("No CSV files found in dataset: {}", dataset);
        return Ok(ProcessingStats {
            datasets_processed: 1,
            stations_loaded: load_stats.stations_loaded,
            ..Default::default()
        });
    }

    // Decide whether to use parallel or sequential processing
    let use_parallel = config.performance.parallel_workers > 1 && csv_files.len() > 1;

    if use_parallel {
        info!(
            "Using parallel processing with {} workers for {} files",
            config.performance.parallel_workers,
            csv_files.len()
        );

        // Use parallel processing
        let processor = ParallelProcessor::new(
            Arc::new(config.clone()),
            dataset.to_string(),
            Arc::new(station_registry),
        );

        let parallel_result = processor
            .process_files_parallel(&csv_files, show_progress)
            .await?;

        // Calculate output file size
        let parquet_output_path = config.get_parquet_output_path();
        let output_file = crate::app::services::parquet_writer::utils::create_dataset_output_path(
            dataset,
            &parquet_output_path,
        );

        let mut output_file_size = 0;
        if output_file.exists() {
            if let Ok(metadata) = std::fs::metadata(&output_file) {
                output_file_size = metadata.len();
            }
        }

        let processing_time = start_time.elapsed();

        info!(
            "Parallel processing complete: {} observations written in {:.2}s",
            parallel_result.writing_stats.observations_written,
            processing_time.as_secs_f64()
        );

        // Return comprehensive statistics
        Ok(ProcessingStats {
            datasets_processed: 1,
            files_processed: parallel_result.processing_stats.successful_files,
            stations_loaded: load_stats.stations_loaded,
            observations_processed: parallel_result.writing_stats.observations_written,
            errors_encountered: parallel_result.processing_stats.total_errors,
            processing_time,
            output_sizes: if output_file_size > 0 {
                let filename =
                    crate::app::services::parquet_writer::utils::create_versioned_filename(dataset);
                vec![(filename, output_file_size)]
            } else {
                vec![]
            },
        })
    } else {
        info!(
            "Using sequential processing (parallel_workers={}, files={})",
            config.performance.parallel_workers,
            csv_files.len()
        );

        // Fall back to sequential processing for single worker or single file
        process_dataset_sequential(
            config,
            dataset,
            csv_files,
            load_stats,
            show_progress,
            start_time,
        )
        .await
    }
}

/// Sequential processing fallback for single worker or small datasets
async fn process_dataset_sequential(
    config: &Config,
    dataset: &str,
    csv_files: Vec<std::path::PathBuf>,
    load_stats: crate::app::services::station_registry::LoadStats,
    show_progress: bool,
    start_time: Instant,
) -> Result<ProcessingStats> {
    // Load station registry for this dataset
    let (station_registry, _) =
        StationRegistry::load_for_dataset(&config.processing.input_path, dataset, false).await?;

    // Create processing components
    let registry_arc = Arc::new(station_registry);
    let parser = BadcCsvParser::new(registry_arc.clone());
    let processor = RecordProcessor::new(registry_arc, config.quality_control.clone());

    // Set up progress bar for this dataset's file processing
    let progress_bar = if show_progress {
        Some(create_progress_bar(
            csv_files.len() as u64,
            &format!("Parsing {} files (sequential)...", dataset),
        ))
    } else {
        None
    };

    // Process all CSV files and collect observations
    let mut all_observations = Vec::new();
    let mut files_processed = 0;
    let mut total_errors = 0;

    info!("Parsing {} CSV files sequentially...", csv_files.len());
    for (file_index, csv_file) in csv_files.iter().enumerate() {
        // Update progress bar
        if let Some(pb) = &progress_bar {
            pb.set_position(file_index as u64);
            let file_name = csv_file
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("unknown");
            pb.set_message(format!("Parsing {}", file_name));
        }

        match parser.parse_file(csv_file).await {
            Ok(result) => {
                files_processed += 1;
                all_observations.extend(result.observations);
                total_errors += result.stats.errors.len();

                debug!(
                    "Parsed {}: {} observations, {} errors",
                    csv_file.display(),
                    result.stats.observations_parsed,
                    result.stats.errors.len()
                );
            }
            Err(e) => {
                error!("Failed to parse {}: {}", csv_file.display(), e);
                total_errors += 1;
            }
        }

        // Increment progress after each file
        if let Some(pb) = &progress_bar {
            pb.inc(1);
        }
    }

    // Finish file parsing progress bar
    if let Some(pb) = &progress_bar {
        pb.finish_with_message(format!("Parsed {} files", csv_files.len()));
    }

    info!(
        "Parsing complete: {} observations from {} files",
        all_observations.len(),
        files_processed
    );

    // Process observations (enrichment, deduplication, quality filtering)
    if !all_observations.is_empty() {
        info!("Processing {} observations...", all_observations.len());

        let processing_result = processor
            .process_observations(all_observations, show_progress)
            .await?;
        all_observations = processing_result.observations;

        info!(
            "Processing complete: {} observations after processing",
            all_observations.len()
        );
    }

    // Write to Parquet
    let mut output_file_size = 0;
    let observations_count = all_observations.len();

    if !all_observations.is_empty() {
        info!(
            "Writing {} observations to Parquet format...",
            observations_count
        );

        // Show writing progress
        if show_progress {
            println!(
                "Writing {} observations to Parquet format...",
                observations_count
            );
        }

        // Configure Parquet writer
        let writer_config = WriterConfig {
            row_group_size: config.parquet.row_group_size,
            write_batch_size: 1024,
            memory_limit_bytes: config.memory_limit_bytes(),
            enable_dictionary_encoding: true,
            enable_statistics: true,
            data_page_size_bytes: config.parquet.page_size_mb * 1024 * 1024,
            compression: parquet::basic::Compression::SNAPPY,
        };

        // Write dataset to Parquet
        let parquet_output_path = config.get_parquet_output_path();
        let writing_stats = write_dataset_to_parquet(
            dataset,
            all_observations,
            &parquet_output_path,
            writer_config,
        )
        .await?;

        // Calculate output file size
        let output_file = crate::app::services::parquet_writer::utils::create_dataset_output_path(
            dataset,
            &parquet_output_path,
        );

        if output_file.exists() {
            if let Ok(metadata) = std::fs::metadata(&output_file) {
                output_file_size = metadata.len();
            }
        }

        info!(
            "Parquet writing complete: {} observations, {} bytes",
            writing_stats.observations_written,
            crate::app::services::parquet_writer::WritingStats::format_bytes(
                output_file_size as usize
            )
        );
    } else {
        warn!("No observations to write for dataset: {}", dataset);
    }

    let processing_time = start_time.elapsed();

    // Return comprehensive statistics
    Ok(ProcessingStats {
        datasets_processed: 1,
        files_processed,
        stations_loaded: load_stats.stations_loaded,
        observations_processed: observations_count,
        errors_encountered: total_errors,
        processing_time,
        output_sizes: if output_file_size > 0 {
            let filename =
                crate::app::services::parquet_writer::utils::create_versioned_filename(dataset);
            vec![(filename, output_file_size)]
        } else {
            vec![]
        },
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_dry_run() {
        let temp_dir = TempDir::new().unwrap();
        let input_path = temp_dir.path().to_path_buf();
        let output_path = temp_dir.path().join("output");

        // Create mock dataset directory with proper structure
        let dataset_dir = input_path.join("uk-daily-temperature-obs").join("qcv-1");
        std::fs::create_dir_all(&dataset_dir).unwrap();

        // Create a mock CSV file
        let csv_file = dataset_dir.join("test_data.csv");
        std::fs::write(&csv_file, "mock,csv,data\n1,2,3\n").unwrap();

        let config = Config::new(input_path, output_path);
        let datasets = vec!["uk-daily-temperature-obs".to_string()];

        let stats = run_dry_run(&config, &datasets).await.unwrap();
        assert_eq!(stats.datasets_processed, 1);
        assert!(stats.files_processed > 0);
    }

    #[test]
    fn test_generate_human_report() {
        let stats = ProcessingStats {
            datasets_processed: 2,
            files_processed: 10,
            stations_loaded: 100,
            observations_processed: 5000,
            errors_encountered: 1,
            processing_time: std::time::Duration::from_secs(120),
            output_sizes: vec![("test.parquet".to_string(), 1024)],
        };

        // Should not panic
        let result = generate_human_report(&stats);
        assert!(result.is_ok());
    }

    #[test]
    fn test_generate_json_report() {
        let stats = ProcessingStats {
            datasets_processed: 1,
            files_processed: 5,
            stations_loaded: 50,
            observations_processed: 1000,
            errors_encountered: 0,
            processing_time: std::time::Duration::from_secs(60),
            output_sizes: vec![("test.parquet".to_string(), 2048)],
        };

        // Should not panic
        let result = generate_json_report(&stats);
        assert!(result.is_ok());
    }

    #[test]
    fn test_generate_csv_report() {
        let stats = ProcessingStats {
            datasets_processed: 1,
            files_processed: 3,
            stations_loaded: 25,
            observations_processed: 500,
            errors_encountered: 2,
            processing_time: std::time::Duration::from_secs(30),
            output_sizes: vec![],
        };

        // Should not panic
        let result = generate_csv_report(&stats);
        assert!(result.is_ok());
    }
}
