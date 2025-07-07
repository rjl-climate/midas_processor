//! Individual CSV record parsing for BADC-CSV files
//!
//! This module handles the parsing of individual observation records,
//! including measurement extraction and quality flag processing.

use chrono::Utc;
use csv::StringRecord;
use std::collections::HashMap;
use std::str::FromStr;
use tracing::debug;

use super::column_mapping::ColumnMapping;
use super::field_parsers::{
    parse_optional_datetime, parse_optional_i32, parse_required_datetime, parse_required_i32,
    parse_required_string,
};
use super::header::SimpleHeader;
use crate::app::models::{Observation, QualityFlag};
use crate::app::services::station_registry::StationRegistry;
use crate::{Error, Result};

/// Parse a single observation record from CSV data
pub async fn parse_observation_record(
    record: &StringRecord,
    mapping: &ColumnMapping,
    header: &SimpleHeader,
    station_registry: &StationRegistry,
) -> Result<Observation> {
    // Extract required fields
    let ob_end_time = parse_required_datetime(record, mapping, "ob_end_time")?;
    let ob_hour_count = parse_required_i32(record, mapping, "ob_hour_count")?;
    let observation_id = parse_required_string(record, mapping, "id")?;
    let station_id = parse_required_i32(record, mapping, "src_id")?;
    let id_type = parse_required_string(record, mapping, "id_type")?;
    let met_domain_name = parse_required_string(record, mapping, "met_domain_name")?;
    let rec_st_ind = parse_required_i32(record, mapping, "rec_st_ind")?;
    let version_num = parse_required_i32(record, mapping, "version_num")?;

    // Get station metadata from registry
    let station = station_registry
        .get_station(station_id)
        .ok_or_else(|| {
            Error::data_validation(format!("Station {} not found in registry", station_id))
        })?
        .clone();

    // Parse optional processing fields
    let meto_stmp_time =
        parse_optional_datetime(record, mapping, "meto_stmp_time").unwrap_or_else(Utc::now);
    let midas_stmp_etime = parse_optional_i32(record, mapping, "midas_stmp_etime").unwrap_or(0);

    // Parse measurements
    let measurements = parse_measurements(record, mapping, &header.missing_value);

    // Parse quality flags
    let quality_flags = parse_quality_flags(record, mapping, &header.missing_value);

    // Create and validate observation
    Observation::new(
        ob_end_time,
        ob_hour_count,
        observation_id,
        station_id,
        id_type,
        met_domain_name,
        rec_st_ind,
        version_num,
        station,
        measurements,
        quality_flags,
        meto_stmp_time,
        midas_stmp_etime,
    )
}

/// Parse measurements from dynamic columns
pub fn parse_measurements(
    record: &StringRecord,
    mapping: &ColumnMapping,
    missing_value: &str,
) -> HashMap<String, f64> {
    let mut measurements = HashMap::new();

    for column_name in &mapping.measurement_columns {
        if let Some(&index) = mapping.name_to_index.get(column_name) {
            if let Some(value_str) = record.get(index) {
                let trimmed = value_str.trim();

                // Skip missing values
                if trimmed == missing_value || trimmed.is_empty() {
                    continue;
                }

                // Try to parse as f64
                match trimmed.parse::<f64>() {
                    Ok(value) => {
                        measurements.insert(column_name.clone(), value);
                    }
                    Err(_) => {
                        // Log parse failure but continue processing
                        debug!(
                            "Failed to parse measurement '{}' = '{}' as float",
                            column_name, trimmed
                        );
                    }
                }
            }
        }
    }

    measurements
}

/// Parse quality flags from _q suffixed columns
pub fn parse_quality_flags(
    record: &StringRecord,
    mapping: &ColumnMapping,
    missing_value: &str,
) -> HashMap<String, QualityFlag> {
    let mut quality_flags = HashMap::new();

    for column_name in &mapping.quality_columns {
        if let Some(&index) = mapping.name_to_index.get(column_name) {
            if let Some(value_str) = record.get(index) {
                let trimmed = value_str.trim();

                // Skip missing values
                if trimmed == missing_value || trimmed.is_empty() {
                    continue;
                }

                // Extract measurement name by removing "_q" suffix
                let measurement_name = column_name
                    .strip_suffix("_q")
                    .unwrap_or(column_name)
                    .to_string();

                // Parse quality flag using existing robust logic
                match QualityFlag::from_str(trimmed) {
                    Ok(flag) => {
                        quality_flags.insert(measurement_name, flag);
                    }
                    Err(_) => {
                        debug!(
                            "Failed to parse quality flag '{}' = '{}'",
                            column_name, trimmed
                        );
                    }
                }
            }
        }
    }

    quality_flags
}
