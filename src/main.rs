use anyhow::Result;
use clap::Parser;
use colored::*;
use midas_processor::cli::{Args, dataset_discovery};
use midas_processor::config::{CompressionAlgorithm, MidasConfig, ParquetOptimizationConfig};
use midas_processor::processor::DatasetProcessor;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // Set optimized Polars streaming chunk size for better sink_parquet performance
    // SAFETY: This is called at the start of main before any threads are spawned
    unsafe {
        std::env::set_var("POLARS_STREAMING_CHUNK_SIZE", "100000");
    }

    let args = Args::parse();

    // Handle test merge functionality
    if let Some(station_dir) = &args.test_merge {
        return test_merge_functionality(station_dir).await;
    }

    // Determine dataset path - either from args or discovery
    let dataset_path = match args.dataset_path.clone() {
        Some(path) => path,
        None => {
            // Find the cache directory and discover datasets
            let cache_dir = dataset_discovery::find_cache_directory()?;
            if args.verbose {
                println!(
                    "{} {}",
                    "MIDAS cache directory:".bright_cyan(),
                    cache_dir.display()
                );
            }

            let datasets = dataset_discovery::discover_datasets(&cache_dir)?;
            if datasets.is_empty() {
                eprintln!("{}", "No MIDAS datasets found in cache.".bright_red());
                eprintln!("Please run midas-fetcher first to download datasets.");
                std::process::exit(1);
            }

            let selected = dataset_discovery::select_dataset(&datasets)?;
            println!(
                "{} {}",
                "Selected:".bright_green(),
                selected.name.bright_white().bold()
            );
            selected.path.clone()
        }
    };

    process_dataset(&args, dataset_path).await
}

async fn process_dataset(args: &Args, dataset_path: PathBuf) -> Result<()> {
    // Create configuration with defaults
    let mut config = MidasConfig::default();

    if args.discovery_only {
        config = config.with_discovery_only();
    }

    // Configure parquet compression
    let compression_algorithm = match args.compression.to_lowercase().as_str() {
        "snappy" => CompressionAlgorithm::Snappy,
        "zstd" => CompressionAlgorithm::Zstd,
        "lz4" => CompressionAlgorithm::Lz4,
        "none" | "uncompressed" => CompressionAlgorithm::Uncompressed,
        _ => {
            eprintln!(
                "Warning: Unknown compression '{}', using snappy",
                args.compression
            );
            CompressionAlgorithm::Snappy
        }
    };

    let parquet_config = ParquetOptimizationConfig {
        compression_algorithm,
        ..Default::default()
    };

    let config = config.with_parquet_optimization(parquet_config);

    let output_path = args.get_output_path(&dataset_path);
    
    // Show initialization progress
    println!("{}", "Initializing MIDAS dataset processor...".bright_yellow());
    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path))?.with_config(config);

    match processor.process().await {
        Ok(_stats) => {
            // Success messages are printed by the processor
        }
        Err(e) => {
            eprintln!("{} {:#}", "Error:".bright_red().bold(), e);
            std::process::exit(1);
        }
    }

    Ok(())
}

async fn test_merge_functionality(station_dir: &PathBuf) -> Result<()> {
    use midas_processor::config::MidasConfig;
    use midas_processor::processor::writer::ParquetWriter;
    use midas_processor::DatasetType;
    
    println!("{}", "Testing merge functionality...".bright_yellow());
    println!("Station directory: {}", station_dir.display());
    
    // Check if directory exists and has parquet files
    if !station_dir.exists() {
        eprintln!("{}", "Error: Station directory does not exist".bright_red());
        std::process::exit(1);
    }
    
    // Count parquet files
    let parquet_files: Vec<_> = std::fs::read_dir(station_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.path().extension()
                .is_some_and(|ext| ext == "parquet")
        })
        .collect();
    
    if parquet_files.is_empty() {
        eprintln!("{}", "Error: No parquet files found in directory".bright_red());
        std::process::exit(1);
    }
    
    println!("Found {} parquet files", parquet_files.len());
    
    // Create output path (same directory with .parquet extension)
    let output_path = station_dir.with_extension("parquet");
    println!("Output file: {}", output_path.display());
    
    // Create ParquetWriter
    let config = MidasConfig::default();
    let writer = ParquetWriter::new(output_path, config);
    
    // Determine dataset type from directory name
    let dataset_type = if station_dir.to_string_lossy().contains("rain") {
        DatasetType::Rain
    } else if station_dir.to_string_lossy().contains("temperature") {
        DatasetType::Temperature
    } else if station_dir.to_string_lossy().contains("wind") {
        DatasetType::Wind
    } else {
        DatasetType::Radiation
    };
    
    println!("Detected dataset type: {:?}", dataset_type);
    
    // Perform the merge
    let start_time = std::time::Instant::now();
    match writer.merge_station_parquet_files(station_dir, &dataset_type).await {
        Ok(_) => {
            let duration = start_time.elapsed();
            println!("{}", "✓ Merge completed successfully!".bright_green());
            println!("Time taken: {:?}", duration);
        }
        Err(e) => {
            eprintln!("{} {:#}", "Merge failed:".bright_red(), e);
            std::process::exit(1);
        }
    }
    
    Ok(())
}
