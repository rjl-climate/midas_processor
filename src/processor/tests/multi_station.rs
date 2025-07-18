//! Multi-station processing integration tests

use crate::config::MidasConfig;
use crate::processor::DatasetProcessor;
use std::fs;
use tempfile::TempDir;

/// Helper to create a multi-station test dataset
fn create_multi_station_dataset(temp_dir: &TempDir) -> (std::path::PathBuf, std::path::PathBuf) {
    let dataset_path = temp_dir.path().join("multi-station-dataset");
    let qcv_path = dataset_path.join("qcv-1");

    // Create station 1 in county1
    let station1_path = qcv_path.join("county1").join("station1");
    fs::create_dir_all(&station1_path).unwrap();

    let csv_content_1 = r#"data_type,UK_Daily_Rainfall,9
src_id,8
met_domain_name,MIDAS
version_num,1.0
station_type,IM
id_type,MIDAS
id,00001
name,Test Station 1
latitude,51.5
longitude,-0.1
height,10
met_domain_name,MIDAS
history,Data from test station 1
data_end

ob_end_time,prcp_amt,prcp_amt_q
2023-01-01,5.0,0
2023-01-02,3.2,0
"#;

    fs::write(station1_path.join("rain_2023.csv"), csv_content_1).unwrap();

    // Create station 2 in county1
    let station2_path = qcv_path.join("county1").join("station2");
    fs::create_dir_all(&station2_path).unwrap();

    let csv_content_2 = r#"data_type,UK_Daily_Rainfall,9
src_id,8
met_domain_name,MIDAS
version_num,1.0
station_type,IM
id_type,MIDAS
id,00002
name,Test Station 2
latitude,52.0
longitude,-0.2
height,20
met_domain_name,MIDAS
history,Data from test station 2
data_end

ob_end_time,prcp_amt,prcp_amt_q
2023-01-01,2.5,0
2023-01-02,1.8,0
2023-01-03,4.1,0
"#;

    fs::write(station2_path.join("rain_2023.csv"), csv_content_2).unwrap();

    // Create station 3 in county2
    let station3_path = qcv_path.join("county2").join("station3");
    fs::create_dir_all(&station3_path).unwrap();

    let csv_content_3 = r#"data_type,UK_Daily_Rainfall,9
src_id,8
met_domain_name,MIDAS
version_num,1.0
station_type,IM
id_type,MIDAS
id,00003
name,Test Station 3
latitude,53.0
longitude,-0.3
height,30
met_domain_name,MIDAS
history,Data from test station 3
data_end

ob_end_time,prcp_amt,prcp_amt_q
2023-01-01,1.0,0
2023-01-02,0.5,0
2023-01-03,2.3,0
2023-01-04,0.0,0
"#;

    fs::write(station3_path.join("rain_2023.csv"), csv_content_3).unwrap();

    // Create output directory
    let output_path = temp_dir
        .path()
        .join("output")
        .join("multi-station-dataset.parquet");
    fs::create_dir_all(output_path.parent().unwrap()).unwrap();

    (dataset_path, output_path)
}

#[tokio::test]
async fn test_multi_station_processing() {
    let temp_dir = TempDir::new().unwrap();
    let (dataset_path, output_path) = create_multi_station_dataset(&temp_dir);

    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path.clone())).unwrap();

    // Run the processing pipeline
    let stats = processor.process().await.unwrap();

    // Verify all files were processed
    assert_eq!(stats.files_processed, 3);
    assert_eq!(stats.files_failed, 0);
    assert!(stats.total_rows > 0);

    // Should have processed data from all 3 stations
    assert!(stats.total_rows >= 9); // 2 + 3 + 4 rows minimum

    // Verify output file was created
    assert!(output_path.exists());

    // Verify file has reasonable size
    let metadata = fs::metadata(&output_path).unwrap();
    assert!(metadata.len() > 0);
}

#[tokio::test]
async fn test_multi_station_with_concurrent_processing() {
    let temp_dir = TempDir::new().unwrap();
    let (dataset_path, output_path) = create_multi_station_dataset(&temp_dir);

    let config = MidasConfig {
        max_concurrent_files: 2,
        sample_size: 3,
        ..Default::default()
    };

    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path.clone()))
        .unwrap()
        .with_config(config);

    // Run the processing pipeline
    let stats = processor.process().await.unwrap();

    // Verify all files were processed
    assert_eq!(stats.files_processed, 3);
    assert_eq!(stats.files_failed, 0);
    assert!(stats.total_rows > 0);

    // Verify output file was created
    assert!(output_path.exists());
}

#[tokio::test]
async fn test_large_dataset_simulation() {
    let temp_dir = TempDir::new().unwrap();
    let dataset_path = temp_dir.path().join("large-dataset");
    let qcv_path = dataset_path.join("qcv-1");

    // Create 10 stations with multiple files each
    for county in 1..=3 {
        for station in 1..=4 {
            let station_path = qcv_path
                .join(format!("county{}", county))
                .join(format!("station{}", station));
            fs::create_dir_all(&station_path).unwrap();

            // Create 2 files per station
            for year in 2022..=2023 {
                let csv_content = format!(
                    r#"data_type,UK_Daily_Rainfall,9
src_id,8
met_domain_name,MIDAS
version_num,1.0
station_type,IM
id_type,MIDAS
id,{:05}
name,Test Station {}-{}
latitude,{}.{}
longitude,-0.{}
height,{}0
met_domain_name,MIDAS
history,Data from test station {}-{}
data_end

ob_end_time,prcp_amt,prcp_amt_q
{}-01-01,{}.0,0
{}-01-02,{}.5,0
{}-01-03,{}.2,0
"#,
                    county * 100 + station,
                    county,
                    station,
                    50 + county,
                    station,
                    station,
                    county * 10 + station,
                    county,
                    station,
                    year,
                    station,
                    year,
                    station + 1,
                    year,
                    station + 2
                );

                fs::write(station_path.join(format!("rain_{}.csv", year)), csv_content).unwrap();
            }
        }
    }

    let output_path = temp_dir.path().join("output").join("large-dataset.parquet");
    fs::create_dir_all(output_path.parent().unwrap()).unwrap();

    let config = MidasConfig {
        max_concurrent_files: 4,
        sample_size: 5,
        ..Default::default()
    };

    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path.clone()))
        .unwrap()
        .with_config(config);

    // Run the processing pipeline
    let stats = processor.process().await.unwrap();

    // Verify all files were processed (3 counties × 4 stations × 2 years = 24 files)
    assert_eq!(stats.files_processed, 24);
    assert_eq!(stats.files_failed, 0);
    assert!(stats.total_rows > 0);

    // Should have processed data from all files (24 files × 3 rows = 72 rows)
    assert!(stats.total_rows >= 72);

    // Verify output file was created
    assert!(output_path.exists());
}

#[tokio::test]
async fn test_mixed_file_sizes() {
    let temp_dir = TempDir::new().unwrap();
    let dataset_path = temp_dir.path().join("mixed-size-dataset");
    let qcv_path = dataset_path.join("qcv-1");

    // Create small file
    let small_station_path = qcv_path.join("county1").join("small_station");
    fs::create_dir_all(&small_station_path).unwrap();

    let small_csv = r#"data_type,UK_Daily_Rainfall,9
src_id,8
met_domain_name,MIDAS
version_num,1.0
station_type,IM
id_type,MIDAS
id,00001
name,Small Station
latitude,51.5
longitude,-0.1
height,10
met_domain_name,MIDAS
history,Small station data
data_end

ob_end_time,prcp_amt,prcp_amt_q
2023-01-01,1.0,0
"#;

    fs::write(small_station_path.join("rain_2023.csv"), small_csv).unwrap();

    // Create large file
    let large_station_path = qcv_path.join("county1").join("large_station");
    fs::create_dir_all(&large_station_path).unwrap();

    let mut large_csv = r#"data_type,UK_Daily_Rainfall,9
src_id,8
met_domain_name,MIDAS
version_num,1.0
station_type,IM
id_type,MIDAS
id,00002
name,Large Station
latitude,51.6
longitude,-0.2
height,20
met_domain_name,MIDAS
history,Large station data
data_end

ob_end_time,prcp_amt,prcp_amt_q
"#
    .to_string();

    // Add many data rows
    for day in 1..=31 {
        large_csv.push_str(&format!("2023-01-{:02},{}.0,0\n", day, day % 10));
    }

    fs::write(large_station_path.join("rain_2023.csv"), large_csv).unwrap();

    let output_path = temp_dir
        .path()
        .join("output")
        .join("mixed-size-dataset.parquet");
    fs::create_dir_all(output_path.parent().unwrap()).unwrap();

    let mut processor = DatasetProcessor::new(dataset_path, Some(output_path.clone())).unwrap();

    // Run the processing pipeline
    let stats = processor.process().await.unwrap();

    // Verify both files were processed
    assert_eq!(stats.files_processed, 2);
    assert_eq!(stats.files_failed, 0);
    assert!(stats.total_rows > 0);

    // Should have processed 1 + 31 = 32 rows
    assert!(stats.total_rows >= 32);

    // Verify output file was created
    assert!(output_path.exists());
}
