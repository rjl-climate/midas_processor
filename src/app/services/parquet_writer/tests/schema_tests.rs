//! Comprehensive unit tests for schema generation and management

use super::{create_test_observation, create_test_observation_with_measurements};
use crate::app::services::parquet_writer::schema::*;
use arrow::datatypes::{DataType, TimeUnit};
use std::collections::HashSet;

#[test]
fn test_create_weather_schema() {
    let schema = create_weather_schema();
    
    assert!(schema.fields().len() > 20); // Should have many base fields
    
    // Check that key fields are present
    let field_names: HashSet<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
    assert!(field_names.contains("ob_end_time"));
    assert!(field_names.contains("station_id"));
    assert!(field_names.contains("station_latitude"));
    assert!(field_names.contains("station_longitude"));
    assert!(field_names.contains("station_name"));
    assert!(field_names.contains("observation_id"));
}

#[test]
fn test_create_weather_schema_field_types() {
    let schema = create_weather_schema();
    
    // Check timestamp fields have correct type
    for field in schema.fields() {
        match field.name().as_str() {
            "ob_end_time" | "station_start_date" | "station_end_date" => {
                assert!(matches!(field.data_type(), DataType::Timestamp(TimeUnit::Nanosecond, None)));
                assert!(!field.is_nullable());
            }
            "meto_stmp_time" => {
                assert!(matches!(field.data_type(), DataType::Timestamp(TimeUnit::Nanosecond, None)));
                assert!(field.is_nullable());
            }
            "station_latitude" | "station_longitude" => {
                assert!(matches!(field.data_type(), DataType::Float64));
                assert!(!field.is_nullable());
            }
            "station_height_meters" => {
                assert!(matches!(field.data_type(), DataType::Float32));
                assert!(!field.is_nullable());
            }
            "station_id" | "ob_hour_count" | "rec_st_ind" | "version_num" => {
                assert!(matches!(field.data_type(), DataType::Int32));
                assert!(!field.is_nullable());
            }
            "east_grid_ref" | "north_grid_ref" | "midas_stmp_etime" => {
                assert!(matches!(field.data_type(), DataType::Int32));
                assert!(field.is_nullable());
            }
            "observation_id" | "station_name" | "station_county" => {
                assert!(matches!(field.data_type(), DataType::Utf8));
                assert!(!field.is_nullable());
            }
            "grid_ref_type" => {
                assert!(matches!(field.data_type(), DataType::Utf8));
                assert!(field.is_nullable());
            }
            _ => {} // Other fields
        }
    }
}

#[test]
fn test_extend_schema_with_measurements() {
    let base_schema = create_weather_schema();
    let base_field_count = base_schema.fields().len();
    
    let observations = vec![create_test_observation()];
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    
    // Should have more fields than base schema
    assert!(extended_schema.fields().len() > base_field_count);
    
    // Should contain measurement and quality flag fields
    let field_names: HashSet<_> = extended_schema.fields().iter().map(|f| f.name().clone()).collect();
    assert!(field_names.contains("air_temperature"));
    assert!(field_names.contains("wind_speed"));
    assert!(field_names.contains("q_air_temperature"));
    assert!(field_names.contains("q_wind_speed"));
}

#[test]
fn test_extend_schema_with_measurements_data_types() {
    let base_schema = create_weather_schema();
    let observations = vec![create_test_observation()];
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    
    // Check data types for dynamic fields
    for field in extended_schema.fields() {
        match field.name().as_str() {
            "air_temperature" | "wind_speed" => {
                assert!(matches!(field.data_type(), DataType::Float64));
                assert!(field.is_nullable());
            }
            "q_air_temperature" | "q_wind_speed" => {
                assert!(matches!(field.data_type(), DataType::Utf8));
                assert!(field.is_nullable());
            }
            _ => {} // Base fields already tested
        }
    }
}

#[test]
fn test_extend_schema_with_multiple_measurement_types() {
    let base_schema = create_weather_schema();
    let observations = vec![
        create_test_observation_with_measurements(vec![
            ("temperature", 15.0),
            ("humidity", 80.0),
        ]),
        create_test_observation_with_measurements(vec![
            ("pressure", 1013.25),
            ("wind_direction", 270.0),
        ]),
    ];
    
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    
    let field_names: HashSet<_> = extended_schema.fields().iter().map(|f| f.name().clone()).collect();
    assert!(field_names.contains("temperature"));
    assert!(field_names.contains("humidity"));
    assert!(field_names.contains("pressure"));
    assert!(field_names.contains("wind_direction"));
    assert!(field_names.contains("q_temperature"));
    assert!(field_names.contains("q_humidity"));
    assert!(field_names.contains("q_pressure"));
    assert!(field_names.contains("q_wind_direction"));
}

#[test]
fn test_extend_schema_consistent_ordering() {
    let base_schema = create_weather_schema();
    let observations1 = vec![
        create_test_observation_with_measurements(vec![
            ("z_measurement", 1.0),
            ("a_measurement", 2.0),
            ("m_measurement", 3.0),
        ]),
    ];
    let observations2 = vec![
        create_test_observation_with_measurements(vec![
            ("m_measurement", 3.0),
            ("a_measurement", 2.0),
            ("z_measurement", 1.0),
        ]),
    ];
    
    let schema1 = extend_schema_with_measurements(base_schema.clone(), &observations1).unwrap();
    let schema2 = extend_schema_with_measurements(base_schema, &observations2).unwrap();
    
    // Schemas should have fields in same order regardless of input order
    let fields1: Vec<_> = schema1.fields().iter().map(|f| f.name()).collect();
    let fields2: Vec<_> = schema2.fields().iter().map(|f| f.name()).collect();
    assert_eq!(fields1, fields2);
}

#[test]
fn test_get_base_field_names() {
    let base_fields = get_base_field_names();
    
    assert!(base_fields.contains(&"ob_end_time".to_string()));
    assert!(base_fields.contains(&"station_id".to_string()));
    assert!(base_fields.contains(&"station_latitude".to_string()));
    assert!(!base_fields.contains(&"air_temperature".to_string()));
    assert!(!base_fields.contains(&"q_air_temperature".to_string()));
    
    // Check that all fields from base schema are included
    let schema = create_weather_schema();
    let schema_fields: HashSet<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let base_fields_set: HashSet<_> = base_fields.into_iter().collect();
    assert_eq!(schema_fields, base_fields_set);
}

#[test]
fn test_is_measurement_field() {
    let base_fields = get_base_field_names();
    
    assert!(!is_measurement_field("ob_end_time", &base_fields));
    assert!(!is_measurement_field("station_id", &base_fields));
    assert!(!is_measurement_field("station_latitude", &base_fields));
    assert!(is_measurement_field("air_temperature", &base_fields));
    assert!(is_measurement_field("q_air_temperature", &base_fields));
    assert!(is_measurement_field("some_new_measurement", &base_fields));
}

#[test]
fn test_is_quality_flag_field() {
    assert!(!is_quality_flag_field("air_temperature"));
    assert!(!is_quality_flag_field("wind_speed"));
    assert!(!is_quality_flag_field("station_id"));
    assert!(!is_quality_flag_field("ob_end_time"));
    
    assert!(is_quality_flag_field("q_air_temperature"));
    assert!(is_quality_flag_field("q_wind_speed"));
    assert!(is_quality_flag_field("q_humidity"));
    assert!(is_quality_flag_field("q_"));
}

#[test]
fn test_get_measurement_from_quality_flag() {
    assert_eq!(
        get_measurement_from_quality_flag("q_air_temperature"),
        Some("air_temperature".to_string())
    );
    assert_eq!(
        get_measurement_from_quality_flag("q_wind_speed"),
        Some("wind_speed".to_string())
    );
    assert_eq!(
        get_measurement_from_quality_flag("q_"),
        Some("".to_string())
    );
    
    assert_eq!(get_measurement_from_quality_flag("air_temperature"), None);
    assert_eq!(get_measurement_from_quality_flag("station_id"), None);
    assert_eq!(get_measurement_from_quality_flag(""), None);
}

#[test]
fn test_validate_schema_success() {
    let base_schema = create_weather_schema();
    assert!(validate_schema(&base_schema).is_ok());
    
    let observations = vec![create_test_observation()];
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    assert!(validate_schema(&extended_schema).is_ok());
}

#[test]
fn test_validate_schema_missing_required_field() {
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;
    
    // Create schema missing a required field
    let fields = vec![
        Field::new("ob_end_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        // Missing other required fields like station_id, etc.
    ];
    let incomplete_schema = Schema::new(fields);
    
    let result = validate_schema(&incomplete_schema);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("missing required field"));
}

#[test]
fn test_validate_schema_wrong_data_types() {
    use arrow::datatypes::{Field, Schema};
    
    // Create a base schema with wrong data type for a timestamp field
    let mut fields = vec![];
    let base_schema = create_weather_schema();
    
    // Copy all fields but change ob_end_time to wrong type
    for field in base_schema.fields() {
        if field.name() == "ob_end_time" {
            fields.push(Field::new(field.name(), DataType::Utf8, field.is_nullable()));
        } else {
            fields.push(field.as_ref().clone());
        }
    }
    
    let invalid_schema = Schema::new(fields);
    let result = validate_schema(&invalid_schema);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("must be TimestampNanosecond"));
}

#[test]
fn test_get_schema_stats() {
    let base_schema = create_weather_schema();
    let base_stats = get_schema_stats(&base_schema);
    
    assert_eq!(base_stats.base_fields, base_schema.fields().len());
    assert_eq!(base_stats.measurement_fields, 0);
    assert_eq!(base_stats.quality_flag_fields, 0);
    assert!(!base_stats.has_measurements());
    assert_eq!(base_stats.dynamic_field_percentage(), 0.0);
    
    let observations = vec![create_test_observation()];
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    let extended_stats = get_schema_stats(&extended_schema);
    
    assert_eq!(extended_stats.measurement_fields, 2); // air_temperature, wind_speed
    assert_eq!(extended_stats.quality_flag_fields, 2); // q_air_temperature, q_wind_speed
    assert!(extended_stats.has_measurements());
    assert!(extended_stats.dynamic_field_percentage() > 0.0);
    assert!(extended_stats.total_fields > extended_stats.base_fields);
}

#[test]
fn test_schema_stats_calculations() {
    let stats = SchemaStats {
        total_fields: 100,
        base_fields: 80,
        measurement_fields: 15,
        quality_flag_fields: 5,
        nullable_fields: 30,
    };
    
    assert_eq!(stats.dynamic_field_percentage(), 20.0);
    assert!(stats.has_measurements());
    
    let stats_no_measurements = SchemaStats {
        total_fields: 50,
        base_fields: 50,
        measurement_fields: 0,
        quality_flag_fields: 0,
        nullable_fields: 10,
    };
    
    assert_eq!(stats_no_measurements.dynamic_field_percentage(), 0.0);
    assert!(!stats_no_measurements.has_measurements());
}

#[test]
fn test_schema_stats_edge_cases() {
    let empty_stats = SchemaStats {
        total_fields: 0,
        base_fields: 0,
        measurement_fields: 0,
        quality_flag_fields: 0,
        nullable_fields: 0,
    };
    
    assert_eq!(empty_stats.dynamic_field_percentage(), 0.0);
    assert!(!empty_stats.has_measurements());
}

#[test]
fn test_extend_schema_empty_observations() {
    let base_schema = create_weather_schema();
    let empty_observations: Vec<_> = vec![];
    
    let result = extend_schema_with_measurements(base_schema.clone(), &empty_observations).unwrap();
    
    // Should return schema identical to base schema
    assert_eq!(result.fields().len(), base_schema.fields().len());
    
    let base_field_names: HashSet<_> = base_schema.fields().iter().map(|f| f.name()).collect();
    let result_field_names: HashSet<_> = result.fields().iter().map(|f| f.name()).collect();
    assert_eq!(base_field_names, result_field_names);
}

#[test]
fn test_extend_schema_observations_with_no_measurements() {
    let base_schema = create_weather_schema();
    let observations = vec![create_test_observation_with_measurements(vec![])];
    
    let result = extend_schema_with_measurements(base_schema.clone(), &observations).unwrap();
    
    // Should return schema identical to base schema since no measurements
    assert_eq!(result.fields().len(), base_schema.fields().len());
}

#[test]
fn test_schema_field_name_sanitization() {
    // Test that field names are sorted consistently
    let base_schema = create_weather_schema();
    let observations = vec![
        create_test_observation_with_measurements(vec![
            ("field_z", 1.0),
            ("field_a", 2.0),
            ("field_m", 3.0),
        ]),
    ];
    
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    
    // Find the dynamic fields
    let field_names: Vec<_> = extended_schema.fields()
        .iter()
        .map(|f| f.name())
        .filter(|name| name.starts_with("field_") || name.starts_with("q_field_"))
        .collect();
    
    // Should be in alphabetical order
    let mut sorted_names = field_names.clone();
    sorted_names.sort();
    assert_eq!(field_names, sorted_names);
}

#[test]
fn test_validate_schema_dynamic_field_types() {
    let base_schema = create_weather_schema();
    let observations = vec![create_test_observation()];
    let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
    
    // Create a modified schema with wrong types for dynamic fields
    use arrow::datatypes::{Field, Schema};
    let mut fields = vec![];
    
    for field in extended_schema.fields() {
        if field.name() == "air_temperature" {
            // Change measurement field to wrong type
            fields.push(Field::new(field.name(), DataType::Utf8, field.is_nullable()));
        } else if field.name() == "q_air_temperature" {
            // Change quality flag field to wrong type
            fields.push(Field::new(field.name(), DataType::Int32, field.is_nullable()));
        } else {
            fields.push(field.as_ref().clone());
        }
    }
    
    let invalid_schema = Schema::new(fields);
    let result = validate_schema(&invalid_schema);
    assert!(result.is_err());
}

#[test]
fn test_schema_stats_debug() {
    let stats = SchemaStats {
        total_fields: 10,
        base_fields: 8,
        measurement_fields: 1,
        quality_flag_fields: 1,
        nullable_fields: 5,
    };
    
    let debug_str = format!("{:?}", stats);
    assert!(debug_str.contains("SchemaStats"));
    assert!(debug_str.contains("total_fields: 10"));
}

#[test]
fn test_schema_stats_equality() {
    let stats1 = SchemaStats {
        total_fields: 10,
        base_fields: 8,
        measurement_fields: 1,
        quality_flag_fields: 1,
        nullable_fields: 5,
    };
    
    let stats2 = SchemaStats {
        total_fields: 10,
        base_fields: 8,
        measurement_fields: 1,
        quality_flag_fields: 1,
        nullable_fields: 5,
    };
    
    let stats3 = SchemaStats {
        total_fields: 9,
        base_fields: 8,
        measurement_fields: 1,
        quality_flag_fields: 0,
        nullable_fields: 5,
    };
    
    assert_eq!(stats1, stats2);
    assert_ne!(stats1, stats3);
}

#[test]
fn test_schema_stats_clone() {
    let stats1 = SchemaStats {
        total_fields: 10,
        base_fields: 8,
        measurement_fields: 1,
        quality_flag_fields: 1,
        nullable_fields: 5,
    };
    
    let stats2 = stats1.clone();
    assert_eq!(stats1, stats2);
}