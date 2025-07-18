//! Command-line interface components.

use clap::Parser;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(name = "midas")]
#[command(about = "Convert MIDAS BADC-CSV weather data to optimized Parquet format")]
#[command(version = env!("CARGO_PKG_VERSION"))]
pub struct Args {
    /// Path to the MIDAS dataset directory (optional - will discover from cache if not provided)
    #[arg(value_name = "DATASET_PATH")]
    pub dataset_path: Option<PathBuf>,

    /// Output directory for Parquet files
    #[arg(short, long)]
    pub output_path: Option<PathBuf>,

    /// Discovery mode: analyze schema and empty columns then exit (no data processing)
    #[arg(long)]
    pub discovery_only: bool,

    /// Parquet compression algorithm (snappy, zstd, lz4, none)
    #[arg(long, default_value = "snappy")]
    pub compression: String,

    /// Enable verbose logging
    #[arg(short, long)]
    pub verbose: bool,
}

impl Args {
    /// Get the output path, defaulting to dataset_path/../parquet if not specified
    pub fn get_output_path(&self, dataset_path: &Path) -> PathBuf {
        match &self.output_path {
            Some(path) => path.clone(),
            None => {
                let dataset_name = dataset_path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy();
                dataset_path
                    .parent()
                    .unwrap_or(dataset_path)
                    .parent()
                    .unwrap_or(dataset_path)
                    .join("parquet")
                    .join(format!("{}.parquet", dataset_name))
            }
        }
    }
}

/// Dataset discovery and selection functionality
pub mod dataset_discovery {
    use super::*;
    use anyhow::{Context, Result};
    use colored::*;
    use std::io::{self, Write};

    #[derive(Debug, Clone)]
    pub struct DiscoveredDataset {
        pub name: String,
        pub path: PathBuf,
        pub size_estimate: String,
    }

    /// Find the midas-fetcher cache directory using standard directory conventions
    pub fn find_cache_directory() -> Result<PathBuf> {
        let data_dir = dirs::data_dir().context("Could not determine user data directory")?;

        let cache_path = data_dir.join("midas-fetcher").join("cache");

        if !cache_path.exists() {
            anyhow::bail!(
                "MIDAS cache directory not found at {}. Please run midas-fetcher first to download datasets.",
                cache_path.display()
            );
        }

        Ok(cache_path)
    }

    /// Discover available datasets in the cache directory
    pub fn discover_datasets(cache_dir: &std::path::Path) -> Result<Vec<DiscoveredDataset>> {
        let mut datasets = Vec::new();

        for entry in std::fs::read_dir(cache_dir).context("Failed to read cache directory")? {
            let entry = entry.context("Failed to read directory entry")?;
            let path = entry.path();

            if path.is_dir() {
                let name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();

                // Skip the parquet output directory
                if name == "parquet" {
                    continue;
                }

                // Skip MD5 checksum files
                if name.ends_with(".txt") {
                    continue;
                }

                // Check if this looks like a MIDAS dataset (has qcv-1 directory)
                if path.join("qcv-1").exists() {
                    let size_estimate = estimate_dataset_size(&path)?;

                    datasets.push(DiscoveredDataset {
                        name,
                        path,
                        size_estimate,
                    });
                }
            }
        }

        // Sort by name for consistent ordering
        datasets.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(datasets)
    }

    /// Estimate the size of a dataset by checking the qcv-1 directory
    fn estimate_dataset_size(dataset_path: &std::path::Path) -> Result<String> {
        let qcv_path = dataset_path.join("qcv-1");

        if !qcv_path.exists() {
            return Ok("Unknown".to_string());
        }

        let mut total_files = 0;
        let mut total_size = 0u64;

        // Walk through the directory tree
        for entry in walkdir::WalkDir::new(&qcv_path) {
            let entry = entry.context("Failed to walk directory")?;
            if entry.file_type().is_file()
                && entry.path().extension().is_some_and(|ext| ext == "csv")
            {
                total_files += 1;
                if let Ok(metadata) = entry.metadata() {
                    total_size += metadata.len();
                }
            }
        }

        let size_str = if total_size > 1_000_000_000 {
            format!("{:.1} GB", total_size as f64 / 1_000_000_000.0)
        } else if total_size > 1_000_000 {
            format!("{:.1} MB", total_size as f64 / 1_000_000.0)
        } else if total_size > 1_000 {
            format!("{:.1} KB", total_size as f64 / 1_000.0)
        } else {
            format!("{} bytes", total_size)
        };

        Ok(format!("{} files, ~{}", total_files, size_str))
    }

    /// Present datasets to user and get their selection
    pub fn select_dataset(datasets: &[DiscoveredDataset]) -> Result<&DiscoveredDataset> {
        if datasets.is_empty() {
            anyhow::bail!(
                "No MIDAS datasets found in cache. Please run midas-fetcher first to download datasets."
            );
        }

        println!("{}", "Available MIDAS datasets:".bright_green().bold());
        println!();

        for (i, dataset) in datasets.iter().enumerate() {
            println!(
                "  {}. {} {}",
                (i + 1).to_string().bright_yellow().bold(),
                dataset.name.bright_cyan(),
                format!("({})", dataset.size_estimate).bright_black()
            );
        }

        println!();
        print!("{}", "Select dataset to convert (number): ".bright_white());
        io::stdout().flush().context("Failed to flush stdout")?;

        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .context("Failed to read user input")?;

        let selection: usize = input
            .trim()
            .parse()
            .context("Please enter a valid number")?;

        if selection == 0 || selection > datasets.len() {
            anyhow::bail!(
                "Invalid selection. Please choose a number between 1 and {}",
                datasets.len()
            );
        }

        Ok(&datasets[selection - 1])
    }
}
