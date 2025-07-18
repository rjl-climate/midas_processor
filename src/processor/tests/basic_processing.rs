//! Basic processing integration tests

use crate::config::MidasConfig;
use crate::processor::DatasetProcessor;
use std::fs;
use tempfile::TempDir;

/// Helper to create a minimal test MIDAS dataset structure
fn create_minimal_dataset(temp_dir: &TempDir) -> (std::path::PathBuf, std::path::PathBuf) {
    let dataset_path = temp_dir.path().join("test-dataset");
    let qcv_path = dataset_path.join("qcv-1");
    let station_path = qcv_path.join("county1").join("station1");

    fs::create_dir_all(&station_path).unwrap();

    // Create a minimal CSV file with proper BADC header
    let csv_content = r#"observation_station,G,Test Station
midas_station_id,G,00001
historic_county_name,G,testshire
location,G,51.5,-0.1
height,G,10,m
data
ob_end_ctime,prcp_amt,prcp_amt_q
2023-01-01,5.0,0
2023-01-02,3.2,0
2023-01-03,0.0,0
end data"#;

    fs::write(station_path.join("rain_2023.csv"), csv_content).unwrap();

    // Create output directory
    let output_path = temp_dir.path().join("output").join("test-dataset.parquet");
    fs::create_dir_all(output_path.parent().unwrap()).unwrap();

    (dataset_path, output_path)
}

#[tokio::test]
async fn test_basic_processing_pipeline() {
    let temp_dir = TempDir::new().unwrap();
    let (dataset_path, output_path) = create_minimal_dataset(&temp_dir);

    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path.clone())).unwrap();

    // Run the processing pipeline
    let stats = processor.process().await.unwrap();

    // Verify basic statistics
    assert_eq!(stats.files_processed, 1);
    assert_eq!(stats.files_failed, 0);
    assert!(stats.total_rows > 0);
    assert_eq!(stats.output_path, output_path);

    // Verify output file was created
    assert!(output_path.exists());

    // Verify file has reasonable size
    let metadata = fs::metadata(&output_path).unwrap();
    assert!(metadata.len() > 0);
}

#[tokio::test]
async fn test_discovery_only_mode() {
    let temp_dir = TempDir::new().unwrap();
    let (dataset_path, output_path) = create_minimal_dataset(&temp_dir);

    let config = MidasConfig {
        discovery_only: true,
        ..Default::default()
    };

    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path.clone()))
        .unwrap()
        .with_config(config);

    // Run the processing pipeline in discovery mode
    let stats = processor.process().await.unwrap();

    // Verify no files were processed
    assert_eq!(stats.files_processed, 0);
    assert_eq!(stats.files_failed, 0);
    assert_eq!(stats.total_rows, 0);

    // Verify output file was NOT created
    assert!(!output_path.exists());
}

#[tokio::test]
async fn test_empty_dataset() {
    let temp_dir = TempDir::new().unwrap();
    let dataset_path = temp_dir.path().join("empty-dataset");
    let qcv_path = dataset_path.join("qcv-1");
    fs::create_dir_all(&qcv_path).unwrap();

    let output_path = temp_dir.path().join("output").join("empty-dataset.parquet");

    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path.clone())).unwrap();

    // Run the processing pipeline
    let stats = processor.process().await.unwrap();

    // Verify no files were processed
    assert_eq!(stats.files_processed, 0);
    assert_eq!(stats.files_failed, 0);
    assert_eq!(stats.total_rows, 0);

    // Verify output file was NOT created
    assert!(!output_path.exists());
}

#[tokio::test]
async fn test_custom_output_path() {
    let temp_dir = TempDir::new().unwrap();
    let (dataset_path, _) = create_minimal_dataset(&temp_dir);

    let custom_output = temp_dir.path().join("custom").join("my_output.parquet");
    fs::create_dir_all(custom_output.parent().unwrap()).unwrap();

    let mut processor = DatasetProcessor::new(dataset_path, Some(custom_output.clone())).unwrap();

    // Run the processing pipeline
    let stats = processor.process().await.unwrap();

    // Verify output was written to custom path
    assert_eq!(stats.output_path, custom_output);
    assert!(custom_output.exists());
}

#[tokio::test]
async fn test_processing_with_custom_config() {
    let temp_dir = TempDir::new().unwrap();
    let (dataset_path, output_path) = create_minimal_dataset(&temp_dir);

    let config = MidasConfig {
        max_concurrent_files: 1,
        sample_size: 1,
        ..Default::default()
    };

    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path.clone()))
        .unwrap()
        .with_config(config);

    // Run the processing pipeline
    let stats = processor.process().await.unwrap();

    // Verify processing completed successfully
    assert_eq!(stats.files_processed, 1);
    assert_eq!(stats.files_failed, 0);
    assert!(stats.total_rows > 0);
    assert!(output_path.exists());
}
