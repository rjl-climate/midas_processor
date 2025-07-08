//! Data conversion from MIDAS observations to Arrow RecordBatch format
//!
//! This module handles the core data transformation logic for converting MIDAS weather
//! observations into Arrow format with proper type handling and null value support.

use crate::app::models::Observation;
use crate::{Error, Result};

use arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int32Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tracing::debug;

/// Convert a slice of observations to an Arrow RecordBatch
///
/// This is the core data transformation function that converts MIDAS observations
/// into Arrow format with proper type handling and null value support.
pub fn observations_to_record_batch(
    observations: &[Observation],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    if observations.is_empty() {
        return Err(Error::data_validation(
            "Cannot create RecordBatch from empty observations".to_string(),
        ));
    }

    debug!(
        "Converting {} observations to RecordBatch with {} fields",
        observations.len(),
        schema.fields().len()
    );

    let mut arrays: Vec<ArrayRef> = Vec::new();

    // Process each field in the schema
    for field in schema.fields() {
        let array = create_array_for_field(field.name(), observations)?;
        arrays.push(array);
    }

    // Create RecordBatch from arrays
    RecordBatch::try_new(schema, arrays).map_err(|e| {
        Error::parquet_writing(format!("Failed to create RecordBatch: {}", e), Box::new(e))
    })
}

/// Create an Arrow array for a specific field from observations
fn create_array_for_field(field_name: &str, observations: &[Observation]) -> Result<ArrayRef> {
    let array = match field_name {
        // Temporal fields
        "ob_end_time" => {
            create_timestamp_array(observations, |obs| obs.ob_end_time.timestamp_nanos_opt())?
        }
        "ob_hour_count" => create_int32_array(observations, |obs| obs.ob_hour_count)?,

        // Observation identifiers
        "observation_id" => create_string_array(observations, |obs| obs.observation_id.as_str())?,
        "station_id" => create_int32_array(observations, |obs| obs.station_id)?,
        "id_type" => create_string_array(observations, |obs| obs.id_type.as_str())?,

        // Record metadata
        "met_domain_name" => create_string_array(observations, |obs| obs.met_domain_name.as_str())?,
        "rec_st_ind" => create_int32_array(observations, |obs| obs.rec_st_ind)?,
        "version_num" => create_int32_array(observations, |obs| obs.version_num)?,

        // Station metadata (denormalized)
        "station_src_id" => create_int32_array(observations, |obs| obs.station.src_id)?,
        "station_name" => create_string_array(observations, |obs| obs.station.src_name.as_str())?,
        "station_latitude" => create_float64_array(observations, |obs| obs.station.high_prcn_lat)?,
        "station_longitude" => create_float64_array(observations, |obs| obs.station.high_prcn_lon)?,
        "station_height_meters" => {
            create_float32_array(observations, |obs| obs.station.height_meters)?
        }
        "station_county" => {
            create_string_array(observations, |obs| obs.station.historic_county.as_str())?
        }
        "station_authority" => {
            create_string_array(observations, |obs| obs.station.authority.as_str())?
        }

        // Optional grid reference fields
        "east_grid_ref" => {
            create_optional_int32_array(observations, |obs| obs.station.east_grid_ref)?
        }
        "north_grid_ref" => {
            create_optional_int32_array(observations, |obs| obs.station.north_grid_ref)?
        }
        "grid_ref_type" => {
            create_optional_string_array(observations, |obs| obs.station.grid_ref_type.as_deref())?
        }

        // Station operational dates
        "station_start_date" => create_timestamp_array(observations, |obs| {
            obs.station.src_bgn_date.timestamp_nanos_opt()
        })?,
        "station_end_date" => create_timestamp_array(observations, |obs| {
            obs.station.src_end_date.timestamp_nanos_opt()
        })?,

        // Processing metadata (optional)
        "meto_stmp_time" => create_optional_timestamp_array(observations, |obs| {
            obs.meto_stmp_time.and_then(|dt| dt.timestamp_nanos_opt())
        })?,
        "midas_stmp_etime" => {
            create_optional_int32_array(observations, |obs| obs.midas_stmp_etime)?
        }

        // Dynamic measurement and quality flag fields
        field_name => {
            if let Some(measurement_name) = field_name.strip_prefix("q_") {
                // Quality flag field - extract the measurement name
                create_optional_string_array(observations, |obs| {
                    obs.quality_flags.get(measurement_name).map(|s| s.as_str())
                })?
            } else {
                // Measurement field
                create_optional_float64_array(observations, |obs| {
                    obs.measurements.get(field_name).copied()
                })?
            }
        }
    };

    Ok(array)
}

/// Create a non-nullable timestamp array
fn create_timestamp_array<F>(observations: &[Observation], extractor: F) -> Result<ArrayRef>
where
    F: Fn(&Observation) -> Option<i64>,
{
    let timestamps: Vec<i64> = observations
        .iter()
        .map(|obs| extractor(obs).unwrap_or(0))
        .collect();
    Ok(Arc::new(TimestampNanosecondArray::from(timestamps)))
}

/// Create a nullable timestamp array
fn create_optional_timestamp_array<F>(
    observations: &[Observation],
    extractor: F,
) -> Result<ArrayRef>
where
    F: Fn(&Observation) -> Option<i64>,
{
    let timestamps: Vec<Option<i64>> = observations.iter().map(extractor).collect();
    Ok(Arc::new(TimestampNanosecondArray::from(timestamps)))
}

/// Create a non-nullable int32 array
fn create_int32_array<F>(observations: &[Observation], extractor: F) -> Result<ArrayRef>
where
    F: Fn(&Observation) -> i32,
{
    let values: Vec<i32> = observations.iter().map(extractor).collect();
    Ok(Arc::new(Int32Array::from(values)))
}

/// Create a nullable int32 array
fn create_optional_int32_array<F>(observations: &[Observation], extractor: F) -> Result<ArrayRef>
where
    F: Fn(&Observation) -> Option<i32>,
{
    let values: Vec<Option<i32>> = observations.iter().map(extractor).collect();
    Ok(Arc::new(Int32Array::from(values)))
}

/// Create a non-nullable float64 array
fn create_float64_array<F>(observations: &[Observation], extractor: F) -> Result<ArrayRef>
where
    F: Fn(&Observation) -> f64,
{
    let values: Vec<f64> = observations.iter().map(extractor).collect();
    Ok(Arc::new(Float64Array::from(values)))
}

/// Create a nullable float64 array
fn create_optional_float64_array<F>(observations: &[Observation], extractor: F) -> Result<ArrayRef>
where
    F: Fn(&Observation) -> Option<f64>,
{
    let values: Vec<Option<f64>> = observations.iter().map(extractor).collect();
    Ok(Arc::new(Float64Array::from(values)))
}

/// Create a non-nullable float32 array
fn create_float32_array<F>(observations: &[Observation], extractor: F) -> Result<ArrayRef>
where
    F: Fn(&Observation) -> f32,
{
    let values: Vec<f32> = observations.iter().map(extractor).collect();
    Ok(Arc::new(Float32Array::from(values)))
}

/// Create a non-nullable string array
fn create_string_array<F>(observations: &[Observation], extractor: F) -> Result<ArrayRef>
where
    F: Fn(&Observation) -> &str,
{
    let values: Vec<&str> = observations.iter().map(extractor).collect();
    Ok(Arc::new(StringArray::from(values)))
}

/// Create a nullable string array
fn create_optional_string_array<F>(observations: &[Observation], extractor: F) -> Result<ArrayRef>
where
    F: Fn(&Observation) -> Option<&str>,
{
    let values: Vec<Option<&str>> = observations.iter().map(extractor).collect();
    Ok(Arc::new(StringArray::from(values)))
}

/// Validate that observations can be converted to the given schema
pub fn validate_observations_for_schema(
    observations: &[Observation],
    schema: &arrow::datatypes::Schema,
) -> Result<()> {
    if observations.is_empty() {
        return Err(Error::data_validation(
            "Cannot validate empty observations".to_string(),
        ));
    }

    // Check that all required measurements are present in at least one observation
    let mut found_measurements = std::collections::HashSet::new();
    let mut found_quality_flags = std::collections::HashSet::new();

    for observation in observations {
        for measurement_name in observation.measurements.keys() {
            found_measurements.insert(measurement_name.clone());
        }
        for quality_flag_name in observation.quality_flags.keys() {
            found_quality_flags.insert(format!("q_{}", quality_flag_name));
        }
    }

    // Warn about schema fields that have no data
    for field in schema.fields() {
        let field_name = field.name();
        if field_name.starts_with("q_") {
            if !found_quality_flags.contains(field_name) {
                debug!(
                    "Quality flag field '{}' not found in any observation",
                    field_name
                );
            }
        } else if !is_base_field(field_name) && !found_measurements.contains(field_name) {
            debug!(
                "Measurement field '{}' not found in any observation",
                field_name
            );
        }
    }

    Ok(())
}

/// Check if a field name is a base schema field (not a measurement)
fn is_base_field(field_name: &str) -> bool {
    matches!(
        field_name,
        "ob_end_time"
            | "ob_hour_count"
            | "observation_id"
            | "station_id"
            | "id_type"
            | "met_domain_name"
            | "rec_st_ind"
            | "version_num"
            | "station_src_id"
            | "station_name"
            | "station_latitude"
            | "station_longitude"
            | "station_height_meters"
            | "station_county"
            | "station_authority"
            | "east_grid_ref"
            | "north_grid_ref"
            | "grid_ref_type"
            | "station_start_date"
            | "station_end_date"
            | "meto_stmp_time"
            | "midas_stmp_etime"
    )
}

/// Get conversion statistics for a set of observations
pub fn get_conversion_stats(observations: &[Observation]) -> ConversionStats {
    let mut unique_measurements = std::collections::HashSet::new();
    let mut unique_quality_flags = std::collections::HashSet::new();
    let mut total_measurements = 0;
    let mut total_quality_flags = 0;
    let mut null_measurements = 0;
    let mut null_quality_flags = 0;

    for observation in observations {
        for (measurement_name, value) in &observation.measurements {
            unique_measurements.insert(measurement_name.clone());
            total_measurements += 1;
            if value.is_nan() {
                null_measurements += 1;
            }
        }

        for (quality_flag_name, value) in &observation.quality_flags {
            unique_quality_flags.insert(quality_flag_name.clone());
            total_quality_flags += 1;
            if value.is_empty() {
                null_quality_flags += 1;
            }
        }
    }

    ConversionStats {
        total_observations: observations.len(),
        unique_measurement_types: unique_measurements.len(),
        unique_quality_flag_types: unique_quality_flags.len(),
        total_measurements,
        total_quality_flags,
        null_measurements,
        null_quality_flags,
    }
}

/// Statistics about the conversion process
#[derive(Debug, Clone, PartialEq)]
pub struct ConversionStats {
    pub total_observations: usize,
    pub unique_measurement_types: usize,
    pub unique_quality_flag_types: usize,
    pub total_measurements: usize,
    pub total_quality_flags: usize,
    pub null_measurements: usize,
    pub null_quality_flags: usize,
}

impl ConversionStats {
    /// Calculate the percentage of null measurements
    pub fn null_measurement_percentage(&self) -> f64 {
        if self.total_measurements == 0 {
            0.0
        } else {
            (self.null_measurements as f64 / self.total_measurements as f64) * 100.0
        }
    }

    /// Calculate the percentage of null quality flags
    pub fn null_quality_flag_percentage(&self) -> f64 {
        if self.total_quality_flags == 0 {
            0.0
        } else {
            (self.null_quality_flags as f64 / self.total_quality_flags as f64) * 100.0
        }
    }

    /// Calculate average measurements per observation
    pub fn avg_measurements_per_observation(&self) -> f64 {
        if self.total_observations == 0 {
            0.0
        } else {
            self.total_measurements as f64 / self.total_observations as f64
        }
    }

    /// Calculate average quality flags per observation
    pub fn avg_quality_flags_per_observation(&self) -> f64 {
        if self.total_observations == 0 {
            0.0
        } else {
            self.total_quality_flags as f64 / self.total_observations as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::models::{Observation, Station};
    use crate::app::services::parquet_writer::schema::create_weather_schema;
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

    /// Test empty observation handling prevents Arrow RecordBatch creation errors
    /// Ensures graceful error handling when no data is available for processing
    #[test]
    fn test_observations_to_record_batch_empty() {
        let schema = create_weather_schema();
        let empty_observations: Vec<Observation> = vec![];

        let result = observations_to_record_batch(&empty_observations, schema);
        assert!(result.is_err());
    }

    /// Test basic observation to RecordBatch conversion creates valid Arrow structures
    /// Validates core data transformation pipeline for Parquet output
    #[test]
    fn test_observations_to_record_batch_basic() {
        let base_schema = create_weather_schema();
        let observations = vec![create_test_observation()];

        // For this test, use just the base schema without extensions
        let record_batch = observations_to_record_batch(&observations, base_schema).unwrap();

        assert_eq!(record_batch.num_rows(), 1);
        assert!(record_batch.num_columns() > 0);
    }

    /// Test timestamp array creation handles nanosecond precision for time series data
    /// Ensures pandas compatibility and proper temporal indexing in Parquet files
    #[test]
    fn test_create_timestamp_array() {
        let observations = vec![create_test_observation()];
        let array =
            create_timestamp_array(&observations, |obs| obs.ob_end_time.timestamp_nanos_opt())
                .unwrap();

        assert_eq!(array.len(), 1);
        // Verify it's a TimestampNanosecondArray
        assert!(
            array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .is_some()
        );
    }

    /// Test optional timestamp arrays handle missing values gracefully
    /// Critical for fields like meto_stmp_time that may be absent in historical data
    #[test]
    fn test_create_optional_timestamp_array() {
        let observations = vec![create_test_observation()];
        let array = create_optional_timestamp_array(&observations, |obs| {
            obs.meto_stmp_time.and_then(|dt| dt.timestamp_nanos_opt())
        })
        .unwrap();

        assert_eq!(array.len(), 1);
        // Should handle None values gracefully
        assert!(
            array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .is_some()
        );
    }

    /// Test integer array creation preserves exact values for station IDs and counts
    /// Validates numerical data integrity in Arrow conversion pipeline
    #[test]
    fn test_create_int32_array() {
        let observations = vec![create_test_observation()];
        let array = create_int32_array(&observations, |obs| obs.station_id).unwrap();

        assert_eq!(array.len(), 1);
        let int32_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int32_array.value(0), 12345);
    }

    /// Test optional integer arrays handle nullable fields like grid references
    /// Ensures proper null handling for incomplete station metadata
    #[test]
    fn test_create_optional_int32_array() {
        let observations = vec![create_test_observation()];
        let array =
            create_optional_int32_array(&observations, |obs| obs.station.east_grid_ref).unwrap();

        assert_eq!(array.len(), 1);
        assert!(array.as_any().downcast_ref::<Int32Array>().is_some());
    }

    /// Test high-precision float arrays maintain coordinate accuracy
    /// Critical for preserving latitude/longitude precision in scientific data
    #[test]
    fn test_create_float64_array() {
        let observations = vec![create_test_observation()];
        let array = create_float64_array(&observations, |obs| obs.station.high_prcn_lat).unwrap();

        assert_eq!(array.len(), 1);
        let float64_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((float64_array.value(0) - 51.5074).abs() < f64::EPSILON);
    }

    /// Test optional float arrays handle missing measurements without data loss
    /// Essential for sparse measurement data common in weather observations
    #[test]
    fn test_create_optional_float64_array() {
        let observations = vec![create_test_observation()];
        let array = create_optional_float64_array(&observations, |obs| {
            obs.measurements.get("air_temperature").copied()
        })
        .unwrap();

        assert_eq!(array.len(), 1);
        let float64_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((float64_array.value(0) - 15.5).abs() < f64::EPSILON);
    }

    /// Test single-precision floats optimize storage for fields like station height
    /// Balances precision needs with file size for non-critical measurements
    #[test]
    fn test_create_float32_array() {
        let observations = vec![create_test_observation()];
        let array = create_float32_array(&observations, |obs| obs.station.height_meters).unwrap();

        assert_eq!(array.len(), 1);
        let float32_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
        assert!((float32_array.value(0) - 25.0).abs() < f32::EPSILON);
    }

    /// Test string array creation preserves text data like station names
    /// Ensures proper UTF-8 encoding for metadata fields in Parquet output
    #[test]
    fn test_create_string_array() {
        let observations = vec![create_test_observation()];
        let array =
            create_string_array(&observations, |obs| obs.station.src_name.as_str()).unwrap();

        assert_eq!(array.len(), 1);
        let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_array.value(0), "TEST STATION");
    }

    /// Test optional string arrays handle missing quality flags and metadata
    /// Critical for preserving MIDAS quality indicators when present
    #[test]
    fn test_create_optional_string_array() {
        let observations = vec![create_test_observation()];
        let array = create_optional_string_array(&observations, |obs| {
            obs.quality_flags.get("air_temperature").map(|s| s.as_str())
        })
        .unwrap();

        assert_eq!(array.len(), 1);
        let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_array.value(0), "0");
    }

    /// Test observation validation prevents schema mismatches before conversion
    /// Ensures data compatibility and early error detection in processing pipeline
    #[test]
    fn test_validate_observations_for_schema() {
        let base_schema = create_weather_schema();
        let observations = vec![create_test_observation()];

        let result = validate_observations_for_schema(&observations, &base_schema);
        assert!(result.is_ok());

        // Test with empty observations
        let empty_observations: Vec<Observation> = vec![];
        let result = validate_observations_for_schema(&empty_observations, &base_schema);
        assert!(result.is_err());
    }

    /// Test base field identification separates static from dynamic schema elements
    /// Enables efficient field categorization during schema-driven conversion
    #[test]
    fn test_is_base_field() {
        assert!(is_base_field("ob_end_time"));
        assert!(is_base_field("station_id"));
        assert!(is_base_field("station_latitude"));
        assert!(!is_base_field("air_temperature"));
        assert!(!is_base_field("q_air_temperature"));
    }

    /// Test conversion statistics tracking provides data quality insights
    /// Enables monitoring of measurement diversity and completeness during processing
    #[test]
    fn test_get_conversion_stats() {
        let observations = vec![create_test_observation(); 3];
        let stats = get_conversion_stats(&observations);

        assert_eq!(stats.total_observations, 3);
        assert_eq!(stats.unique_measurement_types, 2); // air_temperature, wind_speed
        assert_eq!(stats.unique_quality_flag_types, 2); // air_temperature, wind_speed
        assert_eq!(stats.total_measurements, 6); // 3 observations * 2 measurements each
        assert_eq!(stats.total_quality_flags, 6); // 3 observations * 2 quality flags each
        assert_eq!(stats.null_measurements, 0);
        assert_eq!(stats.null_quality_flags, 0);
    }

    /// Test statistical calculations provide meaningful data quality metrics
    /// Validates null percentages and measurement density for quality reporting
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

        assert_eq!(stats.null_measurement_percentage(), 5.0);
        assert_eq!(stats.null_quality_flag_percentage(), 5.0);
        assert_eq!(stats.avg_measurements_per_observation(), 4.0);
        assert_eq!(stats.avg_quality_flags_per_observation(), 3.0);
    }

    /// Test edge cases prevent division-by-zero in statistical calculations
    /// Ensures robust behavior when processing empty or sparse datasets
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
}
