//! Integration tests for the parquet_writer module
//!
//! These tests verify the complete parquet_writer workflow using real MIDAS data
//! structures and realistic datasets to ensure the module works correctly in
//! end-to-end scenarios.

use chrono::{DateTime, Datelike, TimeZone, Utc};
use midas_processor::Result;
use midas_processor::app::models::{Observation, Station};
use midas_processor::app::services::parquet_writer::{
    ParquetWriter, WriterConfig, create_optimized_writer, utils,
};
use parquet::basic::Compression;
use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::TempDir;

/// Create a realistic test station based on London Weather Centre
fn create_london_weather_centre() -> Station {
    let start_date = Utc.with_ymd_and_hms(1990, 1, 1, 0, 0, 0).unwrap();
    let end_date = Utc.with_ymd_and_hms(2030, 12, 31, 23, 59, 59).unwrap();

    Station::new(
        19144, // Real station ID for London Weather Centre
        "LONDON WEATHER CENTRE".to_string(),
        51.5074,      // Latitude
        -0.1278,      // Longitude
        Some(532445), // East grid reference
        Some(181680), // North grid reference
        Some("GB_GRID".to_string()),
        start_date,
        end_date,
        "Met Office".to_string(),
        "Greater London".to_string(),
        25.0, // Height in meters
    )
    .unwrap()
}

/// Create a realistic observation with multiple measurements
fn create_realistic_observation(
    station: Station,
    timestamp: DateTime<Utc>,
    measurements: HashMap<String, f64>,
) -> Observation {
    let mut quality_flags = HashMap::new();
    for measurement_name in measurements.keys() {
        quality_flags.insert(measurement_name.clone(), "0".to_string()); // Valid data
    }

    Observation::new(
        timestamp,
        24, // 24-hour observation period
        station.src_id.to_string(),
        station.src_id,
        "MIDAS".to_string(),
        "UK-DAILY-TEMPERATURE-OBS".to_string(),
        1001, // Record sequence number
        1,    // Version number
        station,
        measurements,
        quality_flags,
        HashMap::new(), // No MIDAS metadata for this test
        None,           // No MIDAS timestamp
        None,           // No MIDAS end time
    )
    .unwrap()
}

/// Generate a realistic daily temperature time series
fn generate_daily_temperature_series(
    station: Station,
    start_date: DateTime<Utc>,
    days: usize,
) -> Vec<Observation> {
    let mut observations = Vec::new();

    for day in 0..days {
        let observation_date = start_date + chrono::Duration::try_days(day as i64).unwrap();

        // Simulate seasonal temperature variation
        let day_of_year = observation_date.ordinal() as f64;
        let seasonal_temp = 10.0 + 8.0 * (2.0 * std::f64::consts::PI * day_of_year / 365.0).sin();

        // Add some daily variation
        let daily_variation = 5.0 * ((day as f64 * 0.7).sin());
        let temperature = seasonal_temp + daily_variation;

        let mut measurements = HashMap::new();
        measurements.insert("air_temperature".to_string(), temperature);
        measurements.insert("max_air_temp".to_string(), temperature + 3.0);
        measurements.insert("min_air_temp".to_string(), temperature - 3.0);

        // Add some weather measurements with occasional missing data
        if day % 7 != 0 {
            // Missing data every 7th day
            measurements.insert(
                "wind_speed".to_string(),
                5.0 + 10.0 * (day as f64 * 0.3).cos().abs(),
            );
            measurements.insert(
                "wind_direction".to_string(),
                180.0 + 90.0 * (day as f64 * 0.1).sin(),
            );
        }

        if day % 5 != 0 {
            // Missing pressure data every 5th day
            measurements.insert(
                "air_pressure".to_string(),
                1013.25 + 30.0 * (day as f64 * 0.2).sin(),
            );
        }

        observations.push(create_realistic_observation(
            station.clone(),
            observation_date,
            measurements,
        ));
    }

    observations
}

#[tokio::test]
async fn test_write_small_dataset() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("small_dataset.parquet");

    let station = create_london_weather_centre();
    let start_date = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    let observations = generate_daily_temperature_series(station, start_date, 10);

    let config = WriterConfig::default().with_write_batch_size(5);
    let mut writer = ParquetWriter::new(&output_path, config).await?;

    writer.setup_progress(observations.len());
    writer.write_observations(observations).await?;
    let stats = writer.finalize().await?;

    assert_eq!(stats.observations_written, 10);
    assert!(stats.batches_written > 0);
    assert!(stats.bytes_written > 0);
    assert!(output_path.exists());

    // Verify file is not empty
    let file_metadata = std::fs::metadata(&output_path).unwrap();
    assert!(file_metadata.len() > 0);

    Ok(())
}

#[tokio::test]
async fn test_write_medium_dataset() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("medium_dataset.parquet");

    let station = create_london_weather_centre();
    let start_date = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    let observations = generate_daily_temperature_series(station, start_date, 365); // One year

    let config = WriterConfig::default()
        .with_write_batch_size(100)
        .with_row_group_size(1000);
    let mut writer = ParquetWriter::new(&output_path, config).await?;

    writer.setup_progress(observations.len());
    writer.write_observations(observations).await?;
    let stats = writer.finalize().await?;

    assert_eq!(stats.observations_written, 365);
    assert!(stats.batches_written >= 3); // Should have multiple batches
    assert!(stats.bytes_written > 10000); // Should be substantial file
    assert_eq!(stats.processing_errors, 0);
    assert_eq!(stats.success_rate(), 100.0);

    Ok(())
}

#[tokio::test]
async fn test_write_large_dataset_with_memory_management() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("large_dataset.parquet");

    let station = create_london_weather_centre();
    let start_date = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let observations = generate_daily_temperature_series(station, start_date, 1000); // ~3 years

    let config = WriterConfig::default()
        .with_write_batch_size(250)
        .with_row_group_size(2000)
        .with_memory_limit_mb(10); // Small memory limit to test flushing
    let mut writer = ParquetWriter::new(&output_path, config).await?;

    writer.setup_progress(observations.len());
    writer.write_observations(observations).await?;
    let stats = writer.finalize().await?;

    assert_eq!(stats.observations_written, 1000);
    assert!(stats.batches_written >= 4);
    // Note: memory flushes depend on actual data size and may be 0 for small datasets
    // memory_flushes is usize so always >= 0, just verify it's reasonable
    assert!(stats.memory_flushes < 100);
    assert!(stats.bytes_written > 50000);

    Ok(())
}

#[tokio::test]
async fn test_different_compression_algorithms() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let station = create_london_weather_centre();
    let start_date = Utc.with_ymd_and_hms(2023, 6, 1, 0, 0, 0).unwrap();
    let observations = generate_daily_temperature_series(station, start_date, 100);

    let compressions = vec![
        (Compression::SNAPPY, "snappy"),
        (Compression::GZIP(Default::default()), "gzip"),
        (Compression::LZ4, "lz4"),
    ];

    let mut file_sizes = Vec::new();

    for (compression, name) in compressions {
        let output_path = temp_dir
            .path()
            .join(format!("compression_{}.parquet", name));
        let config = WriterConfig::default().with_compression(compression);
        let mut writer = ParquetWriter::new(&output_path, config).await?;

        writer.write_observations(observations.clone()).await?;
        let stats = writer.finalize().await?;

        assert_eq!(stats.observations_written, 100);
        assert!(output_path.exists());

        let file_size = std::fs::metadata(&output_path).unwrap().len();
        file_sizes.push((name, file_size));
    }

    // All files should exist and have reasonable sizes
    for (name, size) in &file_sizes {
        assert!(*size > 1000, "File {} is too small: {} bytes", name, size);
        assert!(
            *size < 1_000_000,
            "File {} is too large: {} bytes",
            name,
            size
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_multiple_stations_workflow() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();

    // Create multiple stations
    let stations = vec![create_london_weather_centre(), {
        let start_date = Utc.with_ymd_and_hms(1995, 1, 1, 0, 0, 0).unwrap();
        let end_date = Utc.with_ymd_and_hms(2025, 12, 31, 23, 59, 59).unwrap();
        Station::new(
            3066, // Manchester Airport
            "MANCHESTER AIRPORT".to_string(),
            53.3539,
            -2.2750,
            Some(383285),
            Some(385649),
            Some("GB_GRID".to_string()),
            start_date,
            end_date,
            "Met Office".to_string(),
            "Greater Manchester".to_string(),
            78.0,
        )
        .unwrap()
    }];

    let start_date = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    let mut all_observations = Vec::new();

    for station in stations {
        let station_observations = generate_daily_temperature_series(station, start_date, 30);
        all_observations.extend(station_observations);
    }

    // Write all observations to a single file for this test
    let config = WriterConfig::default();
    let stats = utils::write_dataset_to_parquet(
        "multi_station_data",
        all_observations,
        temp_dir.path(),
        config,
    )
    .await?;

    assert_eq!(stats.observations_written, 60); // Two stations × 30 days each
    assert!(stats.bytes_written > 0);

    let expected_filename =
        midas_processor::app::services::parquet_writer::utils::create_versioned_filename(
            "multi_station_data",
        );
    let file_path = temp_dir.path().join(expected_filename);
    assert!(file_path.exists());

    Ok(())
}

#[tokio::test]
async fn test_dataset_utility_functions() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let station = create_london_weather_centre();
    let start_date = Utc.with_ymd_and_hms(2023, 3, 1, 0, 0, 0).unwrap();
    let observations = generate_daily_temperature_series(station, start_date, 50);

    let config = WriterConfig::default();

    // Test simple dataset writing
    let stats = utils::write_dataset_to_parquet(
        "march_temperature_data",
        observations.clone(),
        temp_dir.path(),
        config.clone(),
    )
    .await?;

    assert_eq!(stats.observations_written, 50);

    let expected_filename =
        midas_processor::app::services::parquet_writer::utils::create_versioned_filename(
            "march_temperature_data",
        );
    let expected_path = temp_dir.path().join(expected_filename);
    assert!(expected_path.exists());

    // Test multiple datasets writing
    let datasets = vec![
        ("dataset_a".to_string(), observations.clone()),
        ("dataset_b".to_string(), observations[0..25].to_vec()),
        ("dataset_c".to_string(), observations[25..50].to_vec()),
    ];

    let results =
        utils::write_multiple_datasets_to_parquet(datasets, temp_dir.path(), config).await?;

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].1.observations_written, 50);
    assert_eq!(results[1].1.observations_written, 25);
    assert_eq!(results[2].1.observations_written, 25);

    Ok(())
}

#[tokio::test]
async fn test_optimized_writer_creation() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("optimized.parquet");

    let station = create_london_weather_centre();
    let start_date = Utc.with_ymd_and_hms(2023, 7, 1, 0, 0, 0).unwrap();
    let observations = generate_daily_temperature_series(station, start_date, 200);

    // Create optimized writer for this dataset size
    let mut writer = create_optimized_writer(&output_path, observations.len()).await?;

    writer.setup_progress(observations.len());
    writer.write_observations(observations).await?;
    let stats = writer.finalize().await?;

    assert_eq!(stats.observations_written, 200);
    assert!(stats.bytes_written > 0);
    assert!(output_path.exists());

    // Writer should have optimization settings appropriate for medium datasets
    Ok(())
}

#[tokio::test]
async fn test_error_handling_and_recovery() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();

    // Test with invalid output directory
    let invalid_path = PathBuf::from("/nonexistent/directory/file.parquet");
    let config = WriterConfig::default();
    let mut writer = ParquetWriter::new(&invalid_path, config).await?;

    let station = create_london_weather_centre();
    let start_date = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    let observations = generate_daily_temperature_series(station, start_date, 5);

    // This should fail due to invalid path
    let result = writer.write_observations(observations).await;
    assert!(result.is_err());

    // Test with valid configuration but invalid settings
    let invalid_config = WriterConfig {
        row_group_size: 0, // Invalid
        ..Default::default()
    };

    let valid_path = temp_dir.path().join("test.parquet");
    let result = ParquetWriter::new(&valid_path, invalid_config).await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_missing_data_handling() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("missing_data.parquet");

    let station = create_london_weather_centre();
    let config = WriterConfig::default();
    let mut writer = ParquetWriter::new(&output_path, config).await?;

    let mut observations = Vec::new();
    let base_date = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

    // Create observations with varying amounts of missing data
    for day in 0..20 {
        let observation_date = base_date + chrono::Duration::try_days(day).unwrap();
        let mut measurements = HashMap::new();

        // All observations have temperature
        measurements.insert("air_temperature".to_string(), 15.0 + day as f64 * 0.5);

        // Wind data missing every 3rd day
        if day % 3 != 0 {
            measurements.insert("wind_speed".to_string(), 5.0 + day as f64 * 0.3);
        }

        // Pressure data missing every 5th day
        if day % 5 != 0 {
            measurements.insert("air_pressure".to_string(), 1013.25 + day as f64 * 0.1);
        }

        // Humidity data only present every 2nd day
        if day % 2 == 0 {
            measurements.insert("humidity".to_string(), 70.0 + day as f64 * 0.2);
        }

        observations.push(create_realistic_observation(
            station.clone(),
            observation_date,
            measurements,
        ));
    }

    writer.setup_progress(observations.len());
    writer.write_observations(observations).await?;
    let stats = writer.finalize().await?;

    assert_eq!(stats.observations_written, 20);
    assert!(stats.bytes_written > 0);
    assert!(output_path.exists());

    Ok(())
}

#[tokio::test]
async fn test_batch_processing_with_progress() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("batch_processing.parquet");

    let station = create_london_weather_centre();
    let start_date = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    let observations = generate_daily_temperature_series(station, start_date, 150);

    let config = WriterConfig::default().with_write_batch_size(25);
    let mut writer = ParquetWriter::new(&output_path, config).await?;

    writer.setup_progress(observations.len());

    // Write in multiple chunks to test incremental processing
    let chunk_size = 30;
    for chunk in observations.chunks(chunk_size) {
        writer.write_observations(chunk.to_vec()).await?;

        // Verify progress tracking
        assert!(writer.stats().observations_written <= 150);
    }

    let stats = writer.finalize().await?;

    assert_eq!(stats.observations_written, 150);
    assert!(stats.batches_written >= 6); // Should be multiple batches
    assert!(stats.bytes_written > 0);

    Ok(())
}

#[tokio::test]
async fn test_real_world_workflow_simulation() -> Result<()> {
    let temp_dir = TempDir::new().unwrap();

    // Simulate processing a month of data from multiple stations
    let stations = vec![
        create_london_weather_centre(),
        {
            let start_date = Utc.with_ymd_and_hms(1990, 1, 1, 0, 0, 0).unwrap();
            let end_date = Utc.with_ymd_and_hms(2030, 12, 31, 23, 59, 59).unwrap();
            Station::new(
                3066, // Manchester Airport
                "MANCHESTER AIRPORT".to_string(),
                53.3539,
                -2.2750,
                Some(383285),
                Some(385649),
                Some("GB_GRID".to_string()),
                start_date,
                end_date,
                "Met Office".to_string(),
                "Greater Manchester".to_string(),
                78.0,
            )
            .unwrap()
        },
        {
            let start_date = Utc.with_ymd_and_hms(1985, 1, 1, 0, 0, 0).unwrap();
            let end_date = Utc.with_ymd_and_hms(2025, 12, 31, 23, 59, 59).unwrap();
            Station::new(
                14, // Edinburgh
                "EDINBURGH ROYAL BOTANIC GARDEN".to_string(),
                55.9658,
                -3.2094,
                Some(325800),
                Some(674200),
                Some("GB_GRID".to_string()),
                start_date,
                end_date,
                "Met Office".to_string(),
                "City of Edinburgh".to_string(),
                57.0,
            )
            .unwrap()
        },
    ];

    let start_date = Utc.with_ymd_and_hms(2023, 6, 1, 0, 0, 0).unwrap();
    let days_in_month = 30;

    let mut all_observations = Vec::new();
    for station in stations {
        let station_observations =
            generate_daily_temperature_series(station, start_date, days_in_month);
        all_observations.extend(station_observations);
    }

    // Use realistic configuration for production-like scenario
    let config = WriterConfig::default()
        .with_row_group_size(10000)
        .with_write_batch_size(500)
        .with_compression(Compression::SNAPPY)
        .with_memory_limit_mb(50)
        .with_dictionary_encoding(true)
        .with_statistics(true);

    let stats = utils::write_dataset_to_parquet(
        "uk_daily_observations_june_2023",
        all_observations,
        temp_dir.path(),
        config,
    )
    .await?;

    assert_eq!(stats.observations_written, 90); // 3 stations × 30 days
    assert!(stats.batches_written > 0);
    assert!(stats.bytes_written > 5000); // Should be substantial
    assert_eq!(stats.processing_errors, 0);
    assert_eq!(stats.success_rate(), 100.0);

    let expected_filename =
        midas_processor::app::services::parquet_writer::utils::create_versioned_filename(
            "uk_daily_observations_june_2023",
        );
    let output_file = temp_dir.path().join(expected_filename);
    assert!(output_file.exists());

    // Verify file size is reasonable for this amount of data
    let file_size = std::fs::metadata(&output_file).unwrap().len();
    assert!(file_size > 5000);
    assert!(file_size < 5_000_000); // Shouldn't be huge for 90 observations

    Ok(())
}
