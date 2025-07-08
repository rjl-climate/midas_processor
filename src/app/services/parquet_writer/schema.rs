//! Arrow schema generation for MIDAS weather observations
//!
//! This module provides schema creation and extension functionality for converting
//! MIDAS weather observations to optimized Arrow/Parquet format with pandas compatibility.

use crate::app::models::Observation;
use crate::{Error, Result};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::debug;

/// Create optimized Arrow schema for MIDAS weather observations
///
/// This schema is designed for:
/// - Pandas compatibility (TimestampNanosecond)
/// - Query optimization (dictionary encoding for categorical data)
/// - Scientific data handling (proper null support)
/// - Time series analysis (timestamp indexing)
pub fn create_weather_schema() -> SchemaRef {
    let fields = vec![
        // Temporal fields - Primary sorting/indexing columns
        Field::new(
            "ob_end_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false, // Non-nullable - every observation has a timestamp
        ),
        Field::new("ob_hour_count", DataType::Int32, false),
        // Observation identifiers
        Field::new("observation_id", DataType::Utf8, false),
        Field::new("station_id", DataType::Int32, false),
        Field::new("id_type", DataType::Utf8, false),
        // Record metadata
        Field::new("met_domain_name", DataType::Utf8, false),
        Field::new("rec_st_ind", DataType::Int32, false),
        Field::new("version_num", DataType::Int32, false),
        // Station metadata (denormalized for fast access)
        Field::new("station_src_id", DataType::Int32, false),
        Field::new("station_name", DataType::Utf8, false),
        Field::new("station_latitude", DataType::Float64, false),
        Field::new("station_longitude", DataType::Float64, false),
        Field::new("station_height_meters", DataType::Float32, false),
        Field::new("station_county", DataType::Utf8, false),
        Field::new("station_authority", DataType::Utf8, false),
        // Grid reference (optional)
        Field::new("east_grid_ref", DataType::Int32, true), // Nullable
        Field::new("north_grid_ref", DataType::Int32, true), // Nullable
        Field::new("grid_ref_type", DataType::Utf8, true),  // Nullable
        // Station operational dates
        Field::new(
            "station_start_date",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "station_end_date",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        // Processing metadata (optional - may be missing in historical data)
        Field::new(
            "meto_stmp_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true, // Nullable - not present in historical data
        ),
        Field::new("midas_stmp_etime", DataType::Int32, true), // Nullable
    ];

    Arc::new(Schema::new(fields))
}

/// Extend schema with dynamic measurement and quality flag fields
///
/// This function analyzes the provided observations to identify all unique
/// measurement types and creates corresponding columns with proper data types.
pub fn extend_schema_with_measurements(
    base_schema: SchemaRef,
    observations: &[Observation],
) -> Result<SchemaRef> {
    let mut additional_fields: Vec<Arc<Field>> = Vec::new();
    let measurement_names = collect_measurement_names(observations);

    // Sort measurement names for consistent schema ordering
    let mut sorted_measurements: Vec<_> = measurement_names.into_iter().collect();
    sorted_measurements.sort();

    debug!(
        "Found {} dynamic measurement fields",
        sorted_measurements.len()
    );

    // Create fields for each measurement
    for measurement_name in sorted_measurements {
        if measurement_name.starts_with("q_") {
            // Quality flag fields as strings (preserve original MIDAS values)
            additional_fields.push(Arc::new(Field::new(
                &measurement_name,
                DataType::Utf8,
                true, // Nullable - quality flags may be missing
            )));
        } else {
            // Measurement fields as nullable floats
            additional_fields.push(Arc::new(Field::new(
                &measurement_name,
                DataType::Float64,
                true, // Nullable - measurements often have missing values
            )));
        }
    }

    debug!(
        "Created {} additional fields for measurements",
        additional_fields.len()
    );

    // Combine base schema with measurement fields
    let mut all_fields: Vec<_> = base_schema.fields().iter().cloned().collect();
    all_fields.extend(additional_fields);

    Ok(Arc::new(Schema::new(all_fields)))
}

/// Collect all unique measurement and quality flag names from observations
fn collect_measurement_names(observations: &[Observation]) -> HashSet<String> {
    let mut measurement_names = HashSet::new();

    // Collect all unique measurement names across all observations
    for observation in observations {
        for measurement_name in observation.measurements.keys() {
            measurement_names.insert(measurement_name.clone());
        }
        for quality_flag_name in observation.quality_flags.keys() {
            measurement_names.insert(format!("q_{}", quality_flag_name));
        }
    }

    measurement_names
}

/// Get the base field names that are always present in the schema
pub fn get_base_field_names() -> Vec<String> {
    let schema = create_weather_schema();
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

/// Check if a field name is a measurement field (not a base schema field)
pub fn is_measurement_field(field_name: &str, base_fields: &[String]) -> bool {
    !base_fields.contains(&field_name.to_string())
}

/// Check if a field name is a quality flag field
pub fn is_quality_flag_field(field_name: &str) -> bool {
    field_name.starts_with("q_")
}

/// Get measurement name from quality flag field name
pub fn get_measurement_from_quality_flag(quality_flag_field: &str) -> Option<String> {
    quality_flag_field
        .strip_prefix("q_")
        .map(|measurement_name| measurement_name.to_string())
}

/// Validate that a schema contains all required base fields
pub fn validate_schema(schema: &Schema) -> Result<()> {
    let base_schema = create_weather_schema();
    let required_fields: HashSet<_> = base_schema.fields().iter().map(|f| f.name()).collect();
    let schema_fields: HashSet<_> = schema.fields().iter().map(|f| f.name()).collect();

    // Check that all required fields are present
    for required_field in &required_fields {
        if !schema_fields.contains(required_field) {
            return Err(Error::data_validation(format!(
                "Schema missing required field: {}",
                required_field
            )));
        }
    }

    // Validate field data types for critical fields
    for field in schema.fields() {
        match field.name().as_str() {
            "ob_end_time" | "station_start_date" | "station_end_date" => {
                if !matches!(
                    field.data_type(),
                    DataType::Timestamp(TimeUnit::Nanosecond, None)
                ) {
                    return Err(Error::data_validation(format!(
                        "Field {} must be TimestampNanosecond, found {:?}",
                        field.name(),
                        field.data_type()
                    )));
                }
            }
            "meto_stmp_time" => {
                if !matches!(
                    field.data_type(),
                    DataType::Timestamp(TimeUnit::Nanosecond, None)
                ) {
                    return Err(Error::data_validation(format!(
                        "Field {} must be TimestampNanosecond, found {:?}",
                        field.name(),
                        field.data_type()
                    )));
                }
            }
            "station_latitude" | "station_longitude" => {
                if !matches!(field.data_type(), DataType::Float64) {
                    return Err(Error::data_validation(format!(
                        "Field {} must be Float64, found {:?}",
                        field.name(),
                        field.data_type()
                    )));
                }
            }
            "station_height_meters" => {
                if !matches!(field.data_type(), DataType::Float32) {
                    return Err(Error::data_validation(format!(
                        "Field {} must be Float32, found {:?}",
                        field.name(),
                        field.data_type()
                    )));
                }
            }
            _ => {
                // Dynamic measurement fields should be Float64 or Utf8
                if is_measurement_field(field.name(), &get_base_field_names()) {
                    if is_quality_flag_field(field.name()) {
                        if !matches!(field.data_type(), DataType::Utf8) {
                            return Err(Error::data_validation(format!(
                                "Quality flag field {} must be Utf8, found {:?}",
                                field.name(),
                                field.data_type()
                            )));
                        }
                    } else if !matches!(field.data_type(), DataType::Float64) {
                        return Err(Error::data_validation(format!(
                            "Measurement field {} must be Float64, found {:?}",
                            field.name(),
                            field.data_type()
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}

/// Get schema statistics for debugging and analysis
pub fn get_schema_stats(schema: &Schema) -> SchemaStats {
    let base_field_names = get_base_field_names();
    let mut measurement_fields = 0;
    let mut quality_flag_fields = 0;
    let mut nullable_fields = 0;

    for field in schema.fields() {
        if field.is_nullable() {
            nullable_fields += 1;
        }

        if is_measurement_field(field.name(), &base_field_names) {
            if is_quality_flag_field(field.name()) {
                quality_flag_fields += 1;
            } else {
                measurement_fields += 1;
            }
        }
    }

    SchemaStats {
        total_fields: schema.fields().len(),
        base_fields: base_field_names.len(),
        measurement_fields,
        quality_flag_fields,
        nullable_fields,
    }
}

/// Statistics about a schema structure
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaStats {
    pub total_fields: usize,
    pub base_fields: usize,
    pub measurement_fields: usize,
    pub quality_flag_fields: usize,
    pub nullable_fields: usize,
}

impl SchemaStats {
    /// Calculate the percentage of dynamic fields
    pub fn dynamic_field_percentage(&self) -> f64 {
        if self.total_fields == 0 {
            0.0
        } else {
            ((self.measurement_fields + self.quality_flag_fields) as f64 / self.total_fields as f64)
                * 100.0
        }
    }

    /// Check if the schema has any dynamic measurement fields
    pub fn has_measurements(&self) -> bool {
        self.measurement_fields > 0 || self.quality_flag_fields > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::models::{Observation, Station};
    use chrono::Utc;
    use std::collections::HashMap;

    fn create_test_station() -> Station {
        let start_date = Utc::now() - chrono::Duration::try_days(365).unwrap();
        let end_date = Utc::now() + chrono::Duration::try_days(365).unwrap();

        Station::new(
            12345,
            "TEST STATION".to_string(),
            51.5074,
            -0.1278,
            None,
            None,
            None,
            start_date,
            end_date,
            "Met Office".to_string(),
            "Greater London".to_string(),
            25.0,
        )
        .unwrap()
    }

    fn create_test_observation() -> Observation {
        let mut measurements = HashMap::new();
        measurements.insert("air_temperature".to_string(), 15.5);
        measurements.insert("wind_speed".to_string(), 10.2);

        let mut quality_flags = HashMap::new();
        quality_flags.insert("air_temperature".to_string(), "0".to_string());
        quality_flags.insert("wind_speed".to_string(), "1".to_string());

        Observation::new(
            Utc::now(),
            24,
            "12345".to_string(),
            12345,
            "SRCE".to_string(),
            "UK-DAILY-TEMPERATURE-OBS".to_string(),
            1001,
            1,
            create_test_station(),
            measurements,
            quality_flags,
            HashMap::new(),
            None,
            None,
        )
        .unwrap()
    }

    /// Test base schema contains all required weather observation fields
    /// Ensures pandas compatibility and proper timestamp handling for time series analysis
    #[test]
    fn test_create_weather_schema() {
        let schema = create_weather_schema();
        assert!(schema.fields().len() > 0);

        // Check that key fields are present
        let field_names: HashSet<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
        assert!(field_names.contains("ob_end_time"));
        assert!(field_names.contains("station_id"));
        assert!(field_names.contains("station_latitude"));
        assert!(field_names.contains("station_longitude"));

        // Check timestamp fields have correct type
        for field in schema.fields() {
            if field.name() == "ob_end_time" {
                assert!(matches!(
                    field.data_type(),
                    DataType::Timestamp(TimeUnit::Nanosecond, None)
                ));
                assert!(!field.is_nullable());
            }
        }
    }

    /// Test dynamic schema extension adds measurement fields from actual data
    /// Validates proper type mapping (Float64 measurements, Utf8 quality flags)
    #[test]
    fn test_extend_schema_with_measurements() {
        let base_schema = create_weather_schema();
        let observations = vec![create_test_observation()];

        let extended_schema =
            extend_schema_with_measurements(base_schema.clone(), &observations).unwrap();

        // Should have more fields than base schema
        assert!(extended_schema.fields().len() > base_schema.fields().len());

        // Should contain measurement and quality flag fields
        let field_names: HashSet<_> = extended_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert!(field_names.contains("air_temperature"));
        assert!(field_names.contains("wind_speed"));
        assert!(field_names.contains("q_air_temperature"));
        assert!(field_names.contains("q_wind_speed"));

        // Check data types
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

    /// Test measurement name collection discovers all unique fields from observations
    /// Ensures comprehensive coverage of both measurements and quality flags
    #[test]
    fn test_collect_measurement_names() {
        let observations = vec![create_test_observation()];
        let names = collect_measurement_names(&observations);

        assert!(names.contains("air_temperature"));
        assert!(names.contains("wind_speed"));
        assert!(names.contains("q_air_temperature"));
        assert!(names.contains("q_wind_speed"));
        assert_eq!(names.len(), 4);
    }

    /// Test base field identification separates static from dynamic schema components
    /// Enables efficient field categorization for schema extension logic
    #[test]
    fn test_get_base_field_names() {
        let base_fields = get_base_field_names();
        assert!(base_fields.contains(&"ob_end_time".to_string()));
        assert!(base_fields.contains(&"station_id".to_string()));
        assert!(base_fields.contains(&"station_latitude".to_string()));
        assert!(!base_fields.contains(&"air_temperature".to_string()));
    }

    /// Test measurement field detection distinguishes dynamic from base fields
    /// Critical for proper data conversion and column mapping during processing
    #[test]
    fn test_is_measurement_field() {
        let base_fields = get_base_field_names();
        assert!(!is_measurement_field("ob_end_time", &base_fields));
        assert!(!is_measurement_field("station_id", &base_fields));
        assert!(is_measurement_field("air_temperature", &base_fields));
        assert!(is_measurement_field("q_air_temperature", &base_fields));
    }

    /// Test quality flag identification using 'q_' prefix convention
    /// Ensures proper separation of measurements from their quality indicators
    #[test]
    fn test_is_quality_flag_field() {
        assert!(!is_quality_flag_field("air_temperature"));
        assert!(is_quality_flag_field("q_air_temperature"));
        assert!(is_quality_flag_field("q_wind_speed"));
        assert!(!is_quality_flag_field("station_id"));
    }

    /// Test quality flag to measurement name mapping for data validation
    /// Enables linking quality indicators to their corresponding measurements
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
        assert_eq!(get_measurement_from_quality_flag("air_temperature"), None);
        assert_eq!(get_measurement_from_quality_flag("station_id"), None);
    }

    /// Test schema validation ensures Arrow compatibility and required fields
    /// Prevents runtime errors by catching schema issues before data processing
    #[test]
    fn test_validate_schema() {
        let base_schema = create_weather_schema();
        assert!(validate_schema(&base_schema).is_ok());

        let observations = vec![create_test_observation()];
        let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
        assert!(validate_schema(&extended_schema).is_ok());
    }

    /// Test schema statistics provide insights into data structure complexity
    /// Enables monitoring of schema evolution and measurement diversity
    #[test]
    fn test_get_schema_stats() {
        let base_schema = create_weather_schema();
        let base_stats = get_schema_stats(&base_schema);

        assert_eq!(base_stats.base_fields, base_schema.fields().len());
        assert_eq!(base_stats.measurement_fields, 0);
        assert_eq!(base_stats.quality_flag_fields, 0);
        assert!(!base_stats.has_measurements());

        let observations = vec![create_test_observation()];
        let extended_schema = extend_schema_with_measurements(base_schema, &observations).unwrap();
        let extended_stats = get_schema_stats(&extended_schema);

        assert_eq!(extended_stats.measurement_fields, 2); // air_temperature, wind_speed
        assert_eq!(extended_stats.quality_flag_fields, 2); // q_air_temperature, q_wind_speed
        assert!(extended_stats.has_measurements());
        assert!(extended_stats.dynamic_field_percentage() > 0.0);
    }

    /// Test statistical calculations handle edge cases and percentage computations
    /// Ensures robust metrics for schema analysis and reporting
    #[test]
    fn test_schema_stats_calculations() {
        let mut stats = SchemaStats {
            total_fields: 100,
            base_fields: 80,
            measurement_fields: 15,
            quality_flag_fields: 5,
            nullable_fields: 30,
        };

        assert_eq!(stats.dynamic_field_percentage(), 20.0);
        assert!(stats.has_measurements());

        stats.measurement_fields = 0;
        stats.quality_flag_fields = 0;
        assert!(!stats.has_measurements());
        assert_eq!(stats.dynamic_field_percentage(), 0.0);
    }
}
