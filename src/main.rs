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

    let args = Args::parse();

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
