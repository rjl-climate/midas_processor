//! Parquet writing module for MIDAS datasets
//!
//! Handles writing processed data to optimized Parquet files with various
//! strategies including streaming, batching, and optimal row group sizing.

use crate::config::{MidasConfig, SystemProfile};
use crate::error::Result;
use crate::models::DatasetType;

use chrono::Utc;
use indicatif::{ProgressBar, ProgressStyle};
use polars::io::parquet::write::KeyValueMetadata;
use polars::prelude::StatisticsOptions;
use polars::prelude::{
    LazyFrame, ParquetWriteOptions, ParquetCompression, SortMultipleOptions, UnionArgs, col, concat,
};
use std::path::{Path, PathBuf};
use tracing::debug;

/// Parquet writer with optimization strategies
#[derive(Debug)]
pub struct ParquetWriter {
    output_path: PathBuf,
    config: MidasConfig,
    system_profile: SystemProfile,
}

impl ParquetWriter {
    /// Create a new Parquet writer
    pub fn new(output_path: PathBuf, config: MidasConfig) -> Self {
        let system_profile = SystemProfile::detect();
        Self {
            output_path,
            config,
            system_profile,
        }
    }

    /// Write parquet files per station for large datasets
    pub async fn write_per_station_parquet(
        &self,
        station_frames: Vec<(String, Vec<LazyFrame>)>, // (station_id, frames for that station)
        dataset_type: &DatasetType,
    ) -> Result<usize> {
        if station_frames.is_empty() {
            return Ok(0);
        }

        // Create output directory for station files
        let station_dir = self.output_path.with_extension("");
        std::fs::create_dir_all(&station_dir)?;
        
        println!("  Writing {} station parquet files to: {}", 
            station_frames.len(), 
            station_dir.display()
        );

        // Create progress bar for station writing
        let pb = ProgressBar::new(station_frames.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
                .unwrap()
                .progress_chars("#>-")
        );
        pb.set_message("Writing station files");

        let total_rows = 0;
        let mut successful_stations = 0;

        // Process each station
        for (station_id, frames) in station_frames {
            if frames.is_empty() {
                continue;
            }

            pb.set_message(format!("Writing station {}", station_id));
            
            // Create output path for this station
            let station_file = station_dir.join(format!("{}.parquet", station_id));
            
            debug!("Writing station {} with {} frames", station_id, frames.len());

            // Concatenate frames for this station
            let combined_frame = if frames.len() == 1 {
                frames.into_iter().next().unwrap()
            } else {
                concat(
                    frames,
                    UnionArgs {
                        parallel: true,
                        rechunk: true,
                        to_supertypes: true,
                        maintain_order: true,
                        ..Default::default()
                    },
                )?
            };

            // Sort by timestamp for this station
            let sorted_frame = combined_frame.sort_by_exprs(
                [col(dataset_type.primary_time_column())],
                SortMultipleOptions::default()
                    .with_multithreaded(true)
                    .with_nulls_last(true),
            );

            // Write this station's data
            let write_options = self.create_parquet_write_options();
            
            // Use streaming write for individual station
            tokio::task::spawn_blocking(move || -> crate::error::Result<()> {
                use polars::prelude::{SinkTarget, SinkOptions};
                
                sorted_frame
                    .with_new_streaming(true)
                    .sink_parquet(
                        SinkTarget::Path(station_file.into()),
                        write_options,
                        None,
                        SinkOptions::default(),
                    )?
                    .collect()?;
                
                Ok(())
            })
            .await
            .map_err(|e| crate::error::MidasError::ProcessingFailed {
                path: self.output_path.clone(),
                reason: format!("Failed to spawn write task: {}", e),
            })??;

            successful_stations += 1;
            pb.inc(1);
        }

        pb.finish_with_message(format!("Successfully wrote {} station files", successful_stations));
        Ok(total_rows)
    }

    /// Create parquet write options
    fn create_parquet_write_options(&self) -> ParquetWriteOptions {
        let parquet_config = &self.config.parquet_optimization;
        
        ParquetWriteOptions {
            compression: parquet_config.compression_algorithm.to_polars_compression(),
            statistics: if parquet_config.enable_statistics {
                StatisticsOptions::full()
            } else {
                StatisticsOptions::empty()
            },
            row_group_size: Some(500_000), // Smaller for per-station files
            data_page_size: Some(parquet_config.data_page_size),
            key_value_metadata: Some(self.create_file_metadata()),
            ..Default::default()
        }
    }

    /// Merge station parquet files into a single parquet file
    pub async fn merge_station_parquet_files(
        &self,
        station_dir: &Path,
        dataset_type: &DatasetType,
    ) -> Result<usize> {
        println!("  Merging station files into single parquet file...");
        
        // Pattern to match all parquet files in the directory
        let pattern = format!("{}/*.parquet", station_dir.display());
        
        debug!("Scanning parquet files with pattern: {}", pattern);
        
        // Create progress spinner
        let spinner = ProgressBar::new_spinner();
        spinner.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} [{elapsed_precise}] {msg}")
                .unwrap()
                .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ "),
        );
        spinner.set_message("Reading station files...");
        
        // Start spinner animation
        let spinner_clone = spinner.clone();
        let animation_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(80));
            loop {
                interval.tick().await;
                spinner_clone.tick();
            }
        });
        
        // Perform the merge operation
        let output_path = self.output_path.clone();
        let primary_time_col = dataset_type.primary_time_column().to_string();
        
        let result = tokio::task::spawn_blocking(move || -> crate::error::Result<usize> {
            use polars::prelude::{ScanArgsParquet, LazyFrame, SinkTarget, SinkOptions};
            
            // Scan all parquet files lazily
            let lazy_frame = LazyFrame::scan_parquet(
                pattern,
                ScanArgsParquet::default(),
            )?;
            
            // Sort by station_id and then by time (files are already sorted by time within each station)
            let sorted_frame = lazy_frame.sort_by_exprs(
                [col("station_id"), col(&primary_time_col)],
                SortMultipleOptions::default()
                    .with_multithreaded(true)
                    .with_nulls_last(true),
            );
            
            // Create write options
            let write_options = ParquetWriteOptions {
                compression: ParquetCompression::Snappy,
                statistics: StatisticsOptions::full(),
                row_group_size: Some(1_000_000), // 1M rows per group
                data_page_size: Some(1024 * 1024), // 1MB pages
                key_value_metadata: None, // Will be added separately
                ..Default::default()
            };
            
            // Write the merged file
            sorted_frame
                .with_new_streaming(true)
                .sink_parquet(
                    SinkTarget::Path(output_path.into()),
                    write_options,
                    None,
                    SinkOptions::default(),
                )?
                .collect()?;
            
            Ok(0) // Row count not easily available with sink_parquet
        })
        .await
        .map_err(|e| crate::error::MidasError::ProcessingFailed {
            path: self.output_path.clone(),
            reason: format!("Failed to spawn merge task: {}", e),
        })??;
        
        // Stop animation
        animation_task.abort();
        spinner.finish_with_message("Station files merged successfully");
        
        Ok(result)
    }

    /// Create metadata for the Parquet file
    fn create_file_metadata(&self) -> KeyValueMetadata {
        let mut metadata = Vec::new();

        // Application name and version
        metadata.push((
            "created_by".to_string(),
            format!("{} v{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")),
        ));

        // Creation timestamp
        metadata.push(("created_at".to_string(), Utc::now().to_rfc3339()));

        // GitHub repository
        metadata.push((
            "repository".to_string(),
            env!("CARGO_PKG_REPOSITORY").to_string(),
        ));

        KeyValueMetadata::from_static(metadata)
    }

    /// Write final parquet file with optimized structure for timeseries queries
    pub async fn write_final_parquet(
        &self,
        frames: Vec<LazyFrame>,
        station_count: usize,
        dataset_type: &DatasetType,
    ) -> Result<usize> {
        if frames.is_empty() {
            return Ok(0);
        }

        debug!(
            "Starting streaming parquet write with {} frames",
            frames.len()
        );

        println!("  Consolidating {} data frames into parquet format...", frames.len());
        println!("  Estimated stations: {}", station_count);

        // Always use streaming approach
        self.write_with_optimal_row_groups(frames, station_count, dataset_type)
            .await
    }

    /// Write batches using true Polars streaming
    async fn write_with_optimal_row_groups(
        &self,
        batches: Vec<LazyFrame>,
        station_count: usize,
        dataset_type: &DatasetType,
    ) -> Result<usize> {
        debug!(
            "Starting true streaming parquet write for {} batches",
            batches.len()
        );

        // Calculate optimal row group size for streaming write
        let parquet_config = &self.config.parquet_optimization;
        let estimated_total_rows = batches.len() * 500_000; // Rough estimate
        let optimal_row_group_size = parquet_config.calculate_optimal_row_group_size(
            estimated_total_rows,
            station_count,
            &self.system_profile,
        );

        debug!(
            "Streaming write: calculated optimal row group size: {} rows for ~{} total rows from {} stations",
            optimal_row_group_size, estimated_total_rows, station_count
        );

        // Create parquet write options with optimal row group size
        let mut write_options = self.create_parquet_write_options();
        write_options.row_group_size = Some(optimal_row_group_size);

        // Hierarchical concatenation to avoid large union operations that cause issues
        let combined_frame = if batches.len() <= 10 {
            println!("  Small batch count ({}), concatenating directly", batches.len());
            // Small number of batches - concatenate directly
            concat(
                batches,
                UnionArgs {
                    parallel: true,      // Enable parallel concatenation for speed
                    rechunk: true,       // Enable rechunking for memory efficiency
                    to_supertypes: true, // Enable type coercion for schema union
                    maintain_order: true,
                    ..Default::default()
                },
            )?
        } else {
            println!("  Large batch count ({}), using hierarchical concatenation", batches.len());
            // Large number of batches - concatenate in chunks to avoid complex unions
            let mut chunk_frames = Vec::new();
            let total_chunks = (batches.len() + 9) / 10; // Round up division
            for (i, chunk) in batches.chunks(10).enumerate() {
                if i % 100 == 0 {
                    println!("    Processing chunk {}/{} ({} frames)", i + 1, total_chunks, chunk.len());
                }
                let chunk_frame = concat(
                    chunk,
                    UnionArgs {
                        parallel: true,      // Enable parallel concatenation for speed
                        rechunk: true,       // Enable rechunking for memory efficiency
                        to_supertypes: true, // Enable type coercion for schema union
                        maintain_order: true,
                        ..Default::default()
                    },
                )?;
                chunk_frames.push(chunk_frame);
            }

            // Now concatenate the chunks
            println!("    Concatenating {} chunk frames into final dataset", chunk_frames.len());
            concat(
                chunk_frames,
                UnionArgs {
                    parallel: true,      // Enable parallel concatenation for speed
                    rechunk: true,       // Enable rechunking for memory efficiency
                    to_supertypes: true, // Enable type coercion for schema union
                    maintain_order: true,
                    ..Default::default()
                },
            )?
        };

        // Conditionally sort based on station count to avoid memory issues
        let sort_threshold = 3000; // Conservative threshold based on successful cases
        let final_frame = if station_count > sort_threshold {
            println!("  Skipping sort for {} stations (exceeds threshold of {})", station_count, sort_threshold);
            println!("  Large datasets will be written unsorted to avoid memory issues");
            combined_frame // No sorting for large datasets
        } else {
            println!("  Sorting final dataset by station_id and {} ({} stations)", 
                dataset_type.primary_time_column(), station_count);
            combined_frame.sort_by_exprs(
                [col("station_id"), col(dataset_type.primary_time_column())],
                SortMultipleOptions::default()
                    .with_multithreaded(true)    // Enable multithreaded sorting
                    .with_nulls_last(true),      // Optimize null handling
            )
        };

        // Ensure output directory exists
        if let Some(parent) = self.output_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        println!("  Writing parquet file to: {}", self.output_path.display());

        // Create spinner to show parquet creation progress
        let spinner = ProgressBar::new_spinner();
        spinner.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} [{elapsed_precise}] {msg}")
                .unwrap()
                .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ "),
        );
        spinner.set_message("Writing parquet file...");

        // Clone spinner for the animation task
        let spinner_clone = spinner.clone();

        // Start spinner animation task
        let animation_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(80));
            loop {
                interval.tick().await;
                spinner_clone.tick();
            }
        });

        // Use streaming parquet write to avoid loading entire dataset into memory
        let output_path = self.output_path.clone();
        debug!("Writing parquet to: {:?}", output_path);

        // Use sink_parquet for streaming write
        let result = tokio::task::spawn_blocking(move || -> crate::error::Result<usize> {
            use polars::prelude::{SinkTarget, SinkOptions};
            
            println!("  Starting sink_parquet operation...");
            let start = std::time::Instant::now();
            
            // Execute streaming parquet write - sink_parquet + collect streams to disk
            final_frame
                .with_new_streaming(true)
                .sink_parquet(
                    SinkTarget::Path(output_path.clone().into()),
                    write_options,
                    None, // No cloud options
                    SinkOptions::default(),
                )?
                .collect()?;

            println!("  Sink operation completed in {:?}", start.elapsed());
            
            // Return estimated row count (will be calculated differently in streaming mode)
            Ok(0) // Placeholder - actual count not available with sink_parquet
        })
        .await
        .map_err(|e| crate::error::MidasError::ProcessingFailed {
            path: self.output_path.clone(),
            reason: format!("Failed to spawn write task: {}", e),
        })??;

        // Stop the animation task
        animation_task.abort();

        spinner.finish_with_message("Parquet file created successfully");
        debug!(
            "Parquet write completed: {} rows to {:?}",
            result, self.output_path
        );

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MidasConfig;
    use tempfile::TempDir;

    fn create_test_writer(temp_dir: &TempDir) -> ParquetWriter {
        let output_path = temp_dir.path().join("test.parquet");
        let config = MidasConfig::default();
        ParquetWriter::new(output_path, config)
    }

    #[tokio::test]
    async fn test_parquet_writer_creation() {
        let temp_dir = TempDir::new().unwrap();
        let writer = create_test_writer(&temp_dir);

        // Verify writer was created successfully
        assert!(writer.output_path.ends_with("test.parquet"));
    }

    #[tokio::test]
    async fn test_empty_frames_handling() {
        let temp_dir = TempDir::new().unwrap();
        let writer = create_test_writer(&temp_dir);

        let empty_frames: Vec<LazyFrame> = vec![];
        let dataset_type = DatasetType::Temperature;
        let result = writer
            .write_final_parquet(empty_frames, 0, &dataset_type)
            .await;

        // Should handle empty frames gracefully
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_metadata_creation() {
        let temp_dir = TempDir::new().unwrap();
        let writer = create_test_writer(&temp_dir);

        let metadata = writer.create_file_metadata();

        // Verify metadata is the correct type
        match &metadata {
            KeyValueMetadata::Static(kv_pairs) => {
                assert_eq!(kv_pairs.len(), 3);

                // Check that all required fields are present
                let keys: Vec<&str> = kv_pairs.iter().map(|kv| kv.key.as_str()).collect();
                assert!(keys.contains(&"created_by"));
                assert!(keys.contains(&"created_at"));
                assert!(keys.contains(&"repository"));

                // Find and verify created_by value
                let created_by = kv_pairs
                    .iter()
                    .find(|kv| kv.key == "created_by")
                    .unwrap()
                    .value
                    .as_ref()
                    .unwrap();
                assert!(created_by.contains("midas_processor"));
                assert!(created_by.contains("v"));

                // Find and verify repository URL
                let repository = kv_pairs
                    .iter()
                    .find(|kv| kv.key == "repository")
                    .unwrap()
                    .value
                    .as_ref()
                    .unwrap();
                assert_eq!(repository, "https://github.com/rjl-climate/midas_processor");
            }
            _ => panic!("Expected Static metadata variant"),
        }
    }

    #[test]
    fn test_write_options_with_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let writer = create_test_writer(&temp_dir);

        let parquet_config = &writer.config.parquet_optimization;

        // Test that write options can be created with metadata
        let write_options = ParquetWriteOptions {
            compression: parquet_config.compression_algorithm.to_polars_compression(),
            statistics: if parquet_config.enable_statistics {
                StatisticsOptions::full()
            } else {
                StatisticsOptions::empty()
            },
            row_group_size: Some(1_000_000),
            data_page_size: Some(parquet_config.data_page_size),
            key_value_metadata: Some(writer.create_file_metadata()),
            ..Default::default()
        };

        // Verify metadata is included
        assert!(write_options.key_value_metadata.is_some());
        let metadata = write_options.key_value_metadata.unwrap();
        match metadata {
            KeyValueMetadata::Static(kv_pairs) => {
                assert_eq!(kv_pairs.len(), 3);
            }
            _ => panic!("Expected Static metadata variant"),
        }
    }

    #[test]
    fn test_system_profile_detection() {
        let temp_dir = TempDir::new().unwrap();
        let writer = create_test_writer(&temp_dir);

        // System profile should be detected during writer creation
        // This test just ensures the detection doesn't panic
        let _profile = &writer.system_profile;
    }

    #[tokio::test]
    async fn test_row_group_size_calculation() {
        let temp_dir = TempDir::new().unwrap();
        let writer = create_test_writer(&temp_dir);

        let parquet_config = &writer.config.parquet_optimization;
        let station_count = 10;
        let total_rows = 1_000_000;

        let optimal_size = parquet_config.calculate_optimal_row_group_size(
            total_rows,
            station_count,
            &writer.system_profile,
        );

        // Should return a reasonable row group size
        assert!(optimal_size > 0);
        assert!(optimal_size <= total_rows);
    }
}
