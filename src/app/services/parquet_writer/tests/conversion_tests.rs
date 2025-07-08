//! Comprehensive unit tests for data conversion functionality

use super::{
    create_test_observation, create_test_observation_with_measurements, create_test_observations,
    create_test_observations_with_nan, assert_approx_eq,
};
use crate::app::services::parquet_writer::{
    conversion::*,
    schema::{create_weather_schema, extend_schema_with_measurements},
};
use arrow::array::*;

#[test]
fn test_observations_to_record_batch_empty() {
    let schema = create_weather_schema();
    let empty_observations: Vec<_> = vec![];
    
    let result = observations_to_record_batch(&empty_observations, schema);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("empty observations"));
}

#[test]
fn test_observations_to_record_batch_basic() {
    let base_schema = create_weather_schema();
    let observations = vec![create_test_observation()];
    
    // For this test, extend schema to include the measurements
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    let record_batch = observations_to_record_batch(&observations, extended_schema).unwrap();
    
    assert_eq!(record_batch.num_rows(), 1);
    assert!(record_batch.num_columns() > 20); // Should have many columns including measurements
}

#[test]
fn test_observations_to_record_batch_multiple() {
    let base_schema = create_weather_schema();
    let observations = create_test_observations(5);
    
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    let record_batch = observations_to_record_batch(&observations, extended_schema).unwrap();
    
    assert_eq!(record_batch.num_rows(), 5);
    assert!(record_batch.num_columns() > 20);
}

#[test]
fn test_observations_to_record_batch_timestamp_fields() {
    let base_schema = create_weather_schema();
    let observations = vec![create_test_observation()];
    
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    let record_batch = observations_to_record_batch(&observations, extended_schema).unwrap();
    
    // Find the ob_end_time column
    let schema = record_batch.schema();
    let ob_end_time_index = schema.fields()
        .iter()
        .position(|f| f.name() == "ob_end_time")
        .expect("ob_end_time field should exist");
    
    let column = record_batch.column(ob_end_time_index);
    let timestamp_array = column.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
    assert_eq!(timestamp_array.len(), 1);
    assert!(timestamp_array.value(0) > 0); // Should have a valid timestamp
}

#[test]
fn test_observations_to_record_batch_station_fields() {
    let base_schema = create_weather_schema();
    let observations = vec![create_test_observation()];
    
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    let record_batch = observations_to_record_batch(&observations, extended_schema).unwrap();
    
    let schema = record_batch.schema();
    
    // Test station_id field
    let station_id_index = schema.fields()
        .iter()
        .position(|f| f.name() == "station_id")
        .expect("station_id field should exist");
    let column = record_batch.column(station_id_index);
    let int32_array = column.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(int32_array.value(0), 12345);
    
    // Test station_latitude field
    let lat_index = schema.fields()
        .iter()
        .position(|f| f.name() == "station_latitude")
        .expect("station_latitude field should exist");
    let column = record_batch.column(lat_index);
    let float64_array = column.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_approx_eq(float64_array.value(0), 51.5074, 0.0001);
}

#[test]
fn test_observations_to_record_batch_measurement_fields() {
    let base_schema = create_weather_schema();
    let observations = vec![create_test_observation()];
    
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    let record_batch = observations_to_record_batch(&observations, extended_schema).unwrap();
    
    let schema = record_batch.schema();
    
    // Test air_temperature measurement
    let temp_index = schema.fields()
        .iter()
        .position(|f| f.name() == "air_temperature")
        .expect("air_temperature field should exist");
    let column = record_batch.column(temp_index);
    let float64_array = column.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_approx_eq(float64_array.value(0), 15.5, 0.0001);
    
    // Test quality flag
    let qual_index = schema.fields()
        .iter()
        .position(|f| f.name() == "q_air_temperature")
        .expect("q_air_temperature field should exist");
    let column = record_batch.column(qual_index);
    let string_array = column.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(string_array.value(qual_index), "0");
}

#[test]
fn test_observations_to_record_batch_optional_fields() {
    let base_schema = create_weather_schema();
    let observations = vec![create_test_observation()];
    
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    let record_batch = observations_to_record_batch(&observations, extended_schema).unwrap();
    
    let schema = record_batch.schema();
    
    // Test nullable grid reference field (should be None for test data)
    let grid_ref_index = schema.fields()
        .iter()
        .position(|f| f.name() == "east_grid_ref")
        .expect("east_grid_ref field should exist");
    let column = record_batch.column(grid_ref_index);
    let int32_array = column.as_any().downcast_ref::<Int32Array>().unwrap();
    assert!(int32_array.is_null(0));
}

#[test]
fn test_observations_to_record_batch_with_missing_measurements() {
    let base_schema = create_weather_schema();
    let observations = vec![
        create_test_observation_with_measurements(vec![("temperature", 20.0)]),
        create_test_observation_with_measurements(vec![("humidity", 80.0)]),
    ];
    
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    let record_batch = observations_to_record_batch(&observations, extended_schema).unwrap();
    
    assert_eq!(record_batch.num_rows(), 2);
    
    let schema = record_batch.schema();
    
    // Check temperature field - first observation should have value, second should be null
    let temp_index = schema.fields()
        .iter()
        .position(|f| f.name() == "temperature")
        .expect("temperature field should exist");
    let column = record_batch.column(temp_index);
    let float64_array = column.as_any().downcast_ref::<Float64Array>().unwrap();
    
    assert_approx_eq(float64_array.value(0), 20.0, 0.0001);
    assert!(float64_array.is_null(1));
    
    // Check humidity field - first observation should be null, second should have value
    let humidity_index = schema.fields()
        .iter()
        .position(|f| f.name() == "humidity")
        .expect("humidity field should exist");
    let column = record_batch.column(humidity_index);
    let float64_array = column.as_any().downcast_ref::<Float64Array>().unwrap();
    
    assert!(float64_array.is_null(0));
    assert_approx_eq(float64_array.value(1), 80.0, 0.0001);
}

#[test]
fn test_observations_to_record_batch_with_nan_values() {
    let base_schema = create_weather_schema();
    let observations = create_test_observations_with_nan(4);
    
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    let record_batch = observations_to_record_batch(&observations, extended_schema).unwrap();
    
    assert_eq!(record_batch.num_rows(), 4);
    
    let schema = record_batch.schema();
    let temp_index = schema.fields()
        .iter()
        .position(|f| f.name() == "air_temperature")
        .expect("air_temperature field should exist");
    let column = record_batch.column(temp_index);
    let float64_array = column.as_any().downcast_ref::<Float64Array>().unwrap();
    
    // First observation (index 0) should have NaN (since 0 % 4 == 0)
    assert!(float64_array.value(0).is_nan());
    
    // Second observation should have normal value
    assert!(!float64_array.value(1).is_nan());
}

#[test]
fn test_validate_observations_for_schema() {
    let base_schema = create_weather_schema();
    let observations = vec![create_test_observation()];
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    
    let result = validate_observations_for_schema(&observations, &extended_schema);
    assert!(result.is_ok());
}

#[test]
fn test_validate_observations_for_schema_empty() {
    let schema = create_weather_schema();
    let empty_observations: Vec<_> = vec![];
    
    let result = validate_observations_for_schema(&empty_observations, &schema);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("empty observations"));
}

#[test]
fn test_get_conversion_stats() {
    let observations = create_test_observations(3);
    let stats = get_conversion_stats(&observations);
    
    assert_eq!(stats.total_observations, 3);
    assert_eq!(stats.unique_measurement_types, 3); // air_temperature, wind_speed, humidity
    assert_eq!(stats.unique_quality_flag_types, 3);
    assert_eq!(stats.total_measurements, 9); // 3 observations * 3 measurements each
    assert_eq!(stats.total_quality_flags, 9);
    assert_eq!(stats.null_measurements, 0);
    assert_eq!(stats.null_quality_flags, 0);
}

#[test]
fn test_get_conversion_stats_empty() {
    let observations: Vec<_> = vec![];
    let stats = get_conversion_stats(&observations);
    
    assert_eq!(stats.total_observations, 0);
    assert_eq!(stats.unique_measurement_types, 0);
    assert_eq!(stats.unique_quality_flag_types, 0);
    assert_eq!(stats.total_measurements, 0);
    assert_eq!(stats.total_quality_flags, 0);
}

#[test]
fn test_get_conversion_stats_with_missing_data() {
    let observations = vec![
        create_test_observation_with_measurements(vec![("temp", 20.0)]),
        create_test_observation_with_measurements(vec![]), // No measurements
    ];
    
    let stats = get_conversion_stats(&observations);
    assert_eq!(stats.total_observations, 2);
    assert_eq!(stats.unique_measurement_types, 1);
    assert_eq!(stats.total_measurements, 1);
}

#[test]
fn test_get_conversion_stats_with_nan_values() {
    let observations = create_test_observations_with_nan(8);
    let stats = get_conversion_stats(&observations);
    
    assert_eq!(stats.total_observations, 8);
    // Should count NaN values as null measurements
    assert!(stats.null_measurements > 0);
}

#[test]
fn test_conversion_stats_calculations() {
    let stats = ConversionStats {
        total_observations: 100,
        unique_measurement_types: 5,
        unique_quality_flag_types: 5,
        total_measurements: 400,
        total_quality_flags: 300,
        null_measurements: 20,
        null_quality_flags: 15,
    };

    assert_approx_eq(stats.null_measurement_percentage(), 5.0, 0.01);
    assert_approx_eq(stats.null_quality_flag_percentage(), 5.0, 0.01);
    assert_approx_eq(stats.avg_measurements_per_observation(), 4.0, 0.01);
    assert_approx_eq(stats.avg_quality_flags_per_observation(), 3.0, 0.01);
}

#[test]
fn test_conversion_stats_edge_cases() {
    let empty_stats = ConversionStats {
        total_observations: 0,
        unique_measurement_types: 0,
        unique_quality_flag_types: 0,
        total_measurements: 0,
        total_quality_flags: 0,
        null_measurements: 0,
        null_quality_flags: 0,
    };

    assert_eq!(empty_stats.null_measurement_percentage(), 0.0);
    assert_eq!(empty_stats.null_quality_flag_percentage(), 0.0);
    assert_eq!(empty_stats.avg_measurements_per_observation(), 0.0);
    assert_eq!(empty_stats.avg_quality_flags_per_observation(), 0.0);
}

#[test]
fn test_conversion_stats_debug() {
    let stats = ConversionStats {
        total_observations: 10,
        unique_measurement_types: 5,
        unique_quality_flag_types: 5,
        total_measurements: 50,
        total_quality_flags: 40,
        null_measurements: 5,
        null_quality_flags: 4,
    };
    
    let debug_str = format!("{:?}", stats);
    assert!(debug_str.contains("ConversionStats"));
    assert!(debug_str.contains("total_observations: 10"));
}

#[test]
fn test_conversion_stats_clone() {
    let stats1 = ConversionStats {
        total_observations: 10,
        unique_measurement_types: 5,
        unique_quality_flag_types: 5,
        total_measurements: 50,
        total_quality_flags: 40,
        null_measurements: 5,
        null_quality_flags: 4,
    };
    
    let stats2 = stats1.clone();
    assert_eq!(stats1.total_observations, stats2.total_observations);
    assert_eq!(stats1.null_measurement_percentage(), stats2.null_measurement_percentage());
}

#[test]
fn test_conversion_stats_equality() {
    let stats1 = ConversionStats {
        total_observations: 10,
        unique_measurement_types: 5,
        unique_quality_flag_types: 5,
        total_measurements: 50,
        total_quality_flags: 40,
        null_measurements: 5,
        null_quality_flags: 4,
    };
    
    let stats2 = stats1.clone();
    let mut stats3 = stats1.clone();
    stats3.total_observations = 11;
    
    assert_eq!(stats1, stats2);
    assert_ne!(stats1, stats3);
}

#[test]
fn test_observations_to_record_batch_field_ordering() {
    let base_schema = create_weather_schema();
    let observations = vec![
        create_test_observation_with_measurements(vec![
            ("z_field", 1.0),
            ("a_field", 2.0),
            ("m_field", 3.0),
        ]),
    ];
    
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    let record_batch = observations_to_record_batch(&observations, extended_schema.clone()).unwrap();
    
    // Field order in record batch should match schema order
    let schema_fields: Vec<_> = extended_schema.fields().iter().map(|f| f.name()).collect();
    let batch_fields: Vec<_> = record_batch.schema().fields().iter().map(|f| f.name()).collect();
    assert_eq!(schema_fields, batch_fields);
}

#[test]
fn test_observations_to_record_batch_large_dataset() {
    let base_schema = create_weather_schema();
    let observations = create_test_observations(1000);
    
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    let record_batch = observations_to_record_batch(&observations, extended_schema).unwrap();
    
    assert_eq!(record_batch.num_rows(), 1000);
    
    // Verify that all rows have data
    let schema = record_batch.schema();
    let temp_index = schema.fields()
        .iter()
        .position(|f| f.name() == "air_temperature")
        .expect("air_temperature field should exist");
    let column = record_batch.column(temp_index);
    let float64_array = column.as_any().downcast_ref::<Float64Array>().unwrap();
    
    // Check first and last values
    assert!(!float64_array.value(0).is_nan());
    assert!(!float64_array.value(999).is_nan());
    assert_ne!(float64_array.value(0), float64_array.value(999)); // Should be different
}

#[test]
fn test_observations_to_record_batch_schema_mismatch() {
    let base_schema = create_weather_schema();
    let observations = vec![create_test_observation()];
    
    // Use base schema without extending it - should work but measurements won't be included
    let record_batch = observations_to_record_batch(&observations, base_schema).unwrap();
    
    assert_eq!(record_batch.num_rows(), 1);
    // Columns should only include base schema fields
    
    let schema = record_batch.schema();
    let field_names: Vec<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
    assert!(!field_names.contains(&"air_temperature".to_string()));
    assert!(!field_names.contains(&"q_air_temperature".to_string()));
}

#[test]
fn test_conversion_with_empty_quality_flags() {
    use std::collections::HashMap;
    
    let mut observations = create_test_observations(2);
    // Set some quality flags to empty strings
    observations[0].quality_flags.insert("air_temperature".to_string(), "".to_string());
    
    let stats = get_conversion_stats(&observations);
    assert!(stats.null_quality_flags > 0);
}

#[test]
fn test_conversion_stats_precision() {
    let stats = ConversionStats {
        total_observations: 1000,
        unique_measurement_types: 0,
        unique_quality_flag_types: 0,
        total_measurements: 3333,
        total_quality_flags: 0,
        null_measurements: 1111,
        null_quality_flags: 0,
    };
    
    // Should be 1111/3333 = 33.33...%
    let percentage = stats.null_measurement_percentage();
    assert!((percentage - 33.333333).abs() < 0.0001);
}