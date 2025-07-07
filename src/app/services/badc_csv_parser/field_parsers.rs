//! Field parsing utilities for BADC-CSV records
//!
//! This module provides helper functions for parsing different data types
//! from CSV records with proper error handling and validation.

use super::column_mapping::ColumnMapping;
use crate::{Error, Result};
use chrono::{DateTime, Utc};
use csv::StringRecord;

/// Parse a required datetime field from a CSV record
pub fn parse_required_datetime(
    record: &StringRecord,
    mapping: &ColumnMapping,
    field_name: &str,
) -> Result<DateTime<Utc>> {
    let value_str = get_required_field(record, mapping, field_name)?;

    // Parse standard MIDAS datetime format (try with and without timezone)
    if let Ok(dt) = DateTime::parse_from_str(value_str, "%Y-%m-%d %H:%M:%S %z") {
        Ok(dt.with_timezone(&Utc))
    } else if let Ok(naive_dt) =
        chrono::NaiveDateTime::parse_from_str(value_str, "%Y-%m-%d %H:%M:%S")
    {
        Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc))
    } else {
        Err(Error::data_validation(format!(
            "Invalid datetime format for {}: '{}' (expected 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD HH:MM:SS +ZZZZ')",
            field_name, value_str
        )))
    }
}

/// Parse a required i32 field from a CSV record
pub fn parse_required_i32(
    record: &StringRecord,
    mapping: &ColumnMapping,
    field_name: &str,
) -> Result<i32> {
    let value_str = get_required_field(record, mapping, field_name)?;

    value_str.parse::<i32>().map_err(|e| {
        Error::data_validation(format!(
            "Invalid integer format for {}: '{}' ({})",
            field_name, value_str, e
        ))
    })
}

/// Parse a required string field from a CSV record
pub fn parse_required_string(
    record: &StringRecord,
    mapping: &ColumnMapping,
    field_name: &str,
) -> Result<String> {
    let value_str = get_required_field(record, mapping, field_name)?;
    Ok(value_str.to_string())
}

/// Parse an optional datetime field from a CSV record
pub fn parse_optional_datetime(
    record: &StringRecord,
    mapping: &ColumnMapping,
    field_name: &str,
) -> Option<DateTime<Utc>> {
    get_optional_field(record, mapping, field_name).and_then(|s| {
        if let Ok(dt) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S %z") {
            Some(dt.with_timezone(&Utc))
        } else if let Ok(naive_dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
            Some(DateTime::<Utc>::from_naive_utc_and_offset(naive_dt, Utc))
        } else {
            None
        }
    })
}

/// Parse an optional i32 field from a CSV record
pub fn parse_optional_i32(
    record: &StringRecord,
    mapping: &ColumnMapping,
    field_name: &str,
) -> Option<i32> {
    get_optional_field(record, mapping, field_name).and_then(|s| s.parse::<i32>().ok())
}

/// Get a required field value from a CSV record
pub fn get_required_field<'a>(
    record: &'a StringRecord,
    mapping: &ColumnMapping,
    field_name: &str,
) -> Result<&'a str> {
    let index = mapping.name_to_index.get(field_name).ok_or_else(|| {
        Error::data_validation(format!("Required column '{}' not found", field_name))
    })?;

    let value = record.get(*index).ok_or_else(|| {
        Error::data_validation(format!("No value for required column '{}'", field_name))
    })?;

    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Error::data_validation(format!(
            "Empty value for required column '{}'",
            field_name
        )));
    }

    Ok(trimmed)
}

/// Get an optional field value from a CSV record
pub fn get_optional_field<'a>(
    record: &'a StringRecord,
    mapping: &ColumnMapping,
    field_name: &str,
) -> Option<&'a str> {
    mapping
        .name_to_index
        .get(field_name)
        .and_then(|&index| record.get(index))
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
}
