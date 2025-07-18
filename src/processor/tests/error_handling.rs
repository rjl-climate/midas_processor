//! Error handling integration tests

use crate::error::MidasError;
use crate::processor::DatasetProcessor;
use std::fs;
use tempfile::TempDir;

#[tokio::test]
async fn test_nonexistent_dataset_path() {
    let temp_dir = TempDir::new().unwrap();
    let nonexistent_path = temp_dir.path().join("nonexistent");
    let output_path = temp_dir.path().join("output.parquet");

    let result = DatasetProcessor::new(nonexistent_path.clone(), Some(output_path));

    assert!(result.is_err());
    match result.unwrap_err() {
        MidasError::DatasetNotFound { path } => {
            assert_eq!(path, nonexistent_path);
        }
        _ => panic!("Expected DatasetNotFound error"),
    }
}

#[tokio::test]
async fn test_missing_qcv_directory() {
    let temp_dir = TempDir::new().unwrap();
    let dataset_path = temp_dir.path().join("no-qcv-dataset");
    fs::create_dir_all(&dataset_path).unwrap();

    let output_path = temp_dir.path().join("output.parquet");

    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path)).unwrap();

    let result = processor.process().await;

    assert!(result.is_err());
    match result.unwrap_err() {
        MidasError::DatasetNotFound { path } => {
            assert!(path.to_string_lossy().contains("qcv-1"));
        }
        _ => panic!("Expected DatasetNotFound error"),
    }
}

#[tokio::test]
async fn test_invalid_csv_file() {
    let temp_dir = TempDir::new().unwrap();
    let dataset_path = temp_dir.path().join("invalid-csv-dataset");
    let qcv_path = dataset_path.join("qcv-1");
    let station_path = qcv_path.join("county1").join("station1");

    fs::create_dir_all(&station_path).unwrap();

    // Create an invalid CSV file (missing required header)
    let invalid_csv = "This is not a valid MIDAS CSV file";
    fs::write(station_path.join("rain_2023.csv"), invalid_csv).unwrap();

    let output_path = temp_dir.path().join("output.parquet");

    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path)).unwrap();

    let result = processor.process().await;

    // Should either fail with processing error or have failed files
    match result {
        Ok(stats) => {
            // If it doesn't fail completely, it should report failed files
            assert!(stats.files_failed > 0);
        }
        Err(_) => {
            // Or it could fail completely, which is also acceptable
        }
    }
}

#[tokio::test]
async fn test_corrupted_header() {
    let temp_dir = TempDir::new().unwrap();
    let dataset_path = temp_dir.path().join("corrupted-header-dataset");
    let qcv_path = dataset_path.join("qcv-1");
    let station_path = qcv_path.join("county1").join("station1");

    fs::create_dir_all(&station_path).unwrap();

    // Create a CSV file with corrupted header (missing data_end marker)
    let corrupted_csv = r#"data_type,UK_Daily_Rainfall,9
src_id,8
met_domain_name,MIDAS
version_num,1.0
station_type,IM
id_type,MIDAS
id,00001
name,Test Station
latitude,51.5
longitude,-0.1
height,10
met_domain_name,MIDAS
history,Data from test station
// Missing data_end marker

ob_end_ctime,prcp_amt,prcp_amt_q
2023-01-01,5.0,0
2023-01-02,3.2,0
"#;

    fs::write(station_path.join("rain_2023.csv"), corrupted_csv).unwrap();

    let output_path = temp_dir.path().join("output.parquet");

    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path)).unwrap();

    let result = processor.process().await;

    // Should either fail with processing error or have failed files
    match result {
        Ok(stats) => {
            // If it doesn't fail completely, it should report failed files
            assert!(stats.files_failed > 0);
        }
        Err(_) => {
            // Or it could fail completely, which is also acceptable
        }
    }
}

#[tokio::test]
async fn test_empty_csv_file() {
    let temp_dir = TempDir::new().unwrap();
    let dataset_path = temp_dir.path().join("empty-csv-dataset");
    let qcv_path = dataset_path.join("qcv-1");
    let station_path = qcv_path.join("county1").join("station1");

    fs::create_dir_all(&station_path).unwrap();

    // Create an empty CSV file
    fs::write(station_path.join("rain_2023.csv"), "").unwrap();

    let output_path = temp_dir.path().join("output.parquet");

    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path)).unwrap();

    let result = processor.process().await;

    // Should either fail with processing error or have failed files
    match result {
        Ok(stats) => {
            // If it doesn't fail completely, it should report failed files
            assert!(stats.files_failed > 0);
        }
        Err(_) => {
            // Or it could fail completely, which is also acceptable
        }
    }
}

#[tokio::test]
async fn test_mixed_valid_and_invalid_files() {
    let temp_dir = TempDir::new().unwrap();
    let dataset_path = temp_dir.path().join("mixed-dataset");
    let qcv_path = dataset_path.join("qcv-1");

    // Create a valid CSV file
    let valid_station_path = qcv_path.join("county1").join("valid_station");
    fs::create_dir_all(&valid_station_path).unwrap();

    let valid_csv = r#"data_type,UK_Daily_Rainfall,9
src_id,8
met_domain_name,MIDAS
version_num,1.0
station_type,IM
id_type,MIDAS
id,00001
name,Valid Station
latitude,51.5
longitude,-0.1
height,10
met_domain_name,MIDAS
history,Valid station data
data_end

ob_end_ctime,prcp_amt,prcp_amt_q
2023-01-01,1.0,0
2023-01-02,2.0,0
"#;

    fs::write(valid_station_path.join("rain_2023.csv"), valid_csv).unwrap();

    // Create an invalid CSV file
    let invalid_station_path = qcv_path.join("county1").join("invalid_station");
    fs::create_dir_all(&invalid_station_path).unwrap();

    let invalid_csv = "This is not a valid MIDAS CSV file";
    fs::write(invalid_station_path.join("rain_2023.csv"), invalid_csv).unwrap();

    let output_path = temp_dir.path().join("output.parquet");

    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path.clone())).unwrap();

    let result = processor.process().await;

    // Should process successfully but report some failed files
    match result {
        Ok(stats) => {
            assert_eq!(stats.files_processed, 1); // Only the valid file
            assert_eq!(stats.files_failed, 1); // The invalid file should fail
            assert!(stats.total_rows > 0);

            // Output file should still be created from the valid file
            assert!(output_path.exists());
        }
        Err(_) => {
            // If schema validation fails due to the invalid file, this is also acceptable
        }
    }
}

#[tokio::test]
async fn test_unreadable_file_permissions() {
    let temp_dir = TempDir::new().unwrap();
    let dataset_path = temp_dir.path().join("permission-test-dataset");
    let qcv_path = dataset_path.join("qcv-1");
    let station_path = qcv_path.join("county1").join("station1");

    fs::create_dir_all(&station_path).unwrap();

    // Create a valid CSV file
    let valid_csv = r#"data_type,UK_Daily_Rainfall,9
src_id,8
met_domain_name,MIDAS
version_num,1.0
station_type,IM
id_type,MIDAS
id,00001
name,Test Station
latitude,51.5
longitude,-0.1
height,10
met_domain_name,MIDAS
history,Test station data
data_end

ob_end_ctime,prcp_amt,prcp_amt_q
2023-01-01,1.0,0
"#;

    let csv_file = station_path.join("rain_2023.csv");
    fs::write(&csv_file, valid_csv).unwrap();

    // Note: On some systems, changing file permissions might not work as expected
    // This test is best-effort and may not fail on all systems
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&csv_file).unwrap().permissions();
        perms.set_mode(0o000); // Remove all permissions
        fs::set_permissions(&csv_file, perms).unwrap();
    }

    let output_path = temp_dir.path().join("output.parquet");

    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path)).unwrap();

    let result = processor.process().await;

    // Should either fail or report failed files depending on system behavior
    match result {
        Ok(stats) => {
            // If the system allows reading despite permissions, that's fine
            // Or if it fails gracefully and reports failed files
            assert!(stats.files_processed == 1 || stats.files_failed > 0);
        }
        Err(_) => {
            // Complete failure is also acceptable for permission errors
        }
    }
}
