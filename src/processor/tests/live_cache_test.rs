//! Live cache integration tests
//!
//! These tests use actual MIDAS data from the live cache to verify
//! the CSV scanning performance and data integrity.
//!
//! To run these tests:
//! ```bash
//! cargo test --lib test_live_cache_solar_scanning -- --ignored --nocapture
//! cargo test --lib test_live_cache_csv_reading_performance -- --ignored --nocapture
//! ```
//!
//! These tests require actual MIDAS data to be present in the cache directory.
//! Download data first using the MIDAS downloader before running these tests.

use crate::processor::DatasetProcessor;
use std::path::PathBuf;
use tempfile::TempDir;

#[tokio::test]
#[ignore] // This test requires live cache data and is ignored by default
async fn test_live_cache_radiation_scanning() {
    // Use the specific radiation dataset provided
    let dataset_path = PathBuf::from(
        "/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-radiation-obs-202507",
    );

    if !dataset_path.exists() {
        panic!(
            "Dataset not found at: {}. Please ensure the radiation dataset is downloaded.",
            dataset_path.display()
        );
    }

    println!("Testing with dataset: {}", dataset_path.display());

    // Create temporary output directory
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("solar_test_output.parquet");

    // Create processor
    let mut processor = DatasetProcessor::new(dataset_path.clone(), Some(output_path.clone()))
        .expect("Failed to create processor");

    // Configure for discovery only mode to test scanning performance
    let config = processor.config.clone().with_discovery_only();
    processor = processor.with_config(config);

    // Run the scanning phase
    let start_time = std::time::Instant::now();
    let stats = processor.process().await.expect("Processing failed");
    let scanning_time = start_time.elapsed();

    // Verify the scanning worked
    println!("=== Live Cache Solar Scanning Test Results ===");
    println!("Dataset path: {}", dataset_path.display());
    println!("Scanning time: {:?}", scanning_time);
    println!("Files processed: {}", stats.files_processed);
    println!("Files failed: {}", stats.files_failed);
    println!("Processing time: {}ms", stats.processing_time_ms);

    // Assertions for data integrity
    // Note: In discovery mode, files_processed is 0 because it only analyzes schema
    assert_eq!(stats.files_failed, 0, "Should have no failed files");
    assert!(
        scanning_time.as_secs() < 60,
        "Scanning should complete within 60 seconds"
    );

    // The key validation is that we discovered files and analyzed the schema
    // This is shown in the console output above

    // Verify we can actually read some data by doing a small processing run
    let config = crate::config::MidasConfig::default().with_sample_size(5); // Only process 5 sample files, discovery_only is false by default

    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path.clone()))
        .expect("Failed to create processor for data verification")
        .with_config(config);

    let start_time = std::time::Instant::now();
    let stats = processor.process().await.expect("Data verification failed");
    let processing_time = start_time.elapsed();

    println!("\n=== Data Verification Results ===");
    println!("Processing time: {:?}", processing_time);
    println!("Files processed: {}", stats.files_processed);
    println!("Total rows: {}", stats.total_rows);
    println!("Output file exists: {}", output_path.exists());

    // Final assertions
    assert!(stats.files_processed > 0, "Should have processed files");
    assert!(stats.total_rows > 0, "Should have processed some rows");
    assert!(output_path.exists(), "Output file should exist");

    // Check output file size is reasonable
    let file_size = std::fs::metadata(&output_path).unwrap().len();
    assert!(file_size > 1000, "Output file should be larger than 1KB");

    println!("Output file size: {} bytes", file_size);
    println!("✅ Live cache solar scanning test passed!");
}

#[tokio::test]
#[ignore] // This test requires live cache data and is ignored by default
async fn test_live_cache_csv_reading_performance() {
    // Test specifically the CSV reading performance with the radiation dataset
    let dataset_path = PathBuf::from(
        "/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-radiation-obs-202507",
    );

    if !dataset_path.exists() {
        panic!(
            "Dataset not found at: {}. Please ensure the radiation dataset is downloaded.",
            dataset_path.display()
        );
    }

    println!("Performance testing with: {}", dataset_path.display());

    // Create processor in discovery mode
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("performance_test.parquet");

    let config = crate::config::MidasConfig::default()
        .with_discovery_only()
        .with_sample_size(100); // Test with 100 files

    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path))
        .expect("Failed to create processor")
        .with_config(config);

    // Measure just the CSV scanning phase
    let start = std::time::Instant::now();
    let stats = processor.process().await.expect("Performance test failed");
    let total_time = start.elapsed();

    println!("\n=== CSV Reading Performance Test ===");
    println!("Total files discovered: {}", stats.files_processed);
    println!("Files failed: {}", stats.files_failed);
    println!("Total time: {:?}", total_time);
    println!(
        "Average time per file: {:?}",
        total_time
            .checked_div(stats.files_processed as u32)
            .unwrap_or_else(|| std::time::Duration::from_secs(0))
    );

    // Performance assertions
    assert!(stats.files_processed > 0, "Should discover files");
    assert!(
        total_time.as_secs() < 120,
        "Should complete within 2 minutes"
    );

    if stats.files_processed > 10 {
        let avg_ms_per_file = total_time.as_millis() / stats.files_processed as u128;
        assert!(
            avg_ms_per_file < 1000,
            "Should process each file in under 1 second on average"
        );
        println!("Average processing time per file: {}ms", avg_ms_per_file);
    }

    println!("✅ CSV reading performance test passed!");
}
