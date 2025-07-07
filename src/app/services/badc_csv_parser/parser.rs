//! Core BADC-CSV parser implementation
//!
//! This module provides the main parser orchestration, handling file reading,
//! section splitting, and coordination between different parsing components.

use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::column_mapping::ColumnMapping;
use super::header::SimpleHeader;
use super::record_parser::parse_observation_record;
use super::stats::{ParseResult, ParseStats};
use crate::app::models::Observation;
use crate::app::services::station_registry::StationRegistry;
use crate::{Error, Result};

/// BADC-CSV parser for MIDAS observation files
///
/// This parser focuses on essential functionality:
/// - Robust numerical data extraction and type conversion
/// - Quality control field preservation (pass-through)
/// - Station metadata enrichment via registry
/// - Simple error handling with graceful degradation
#[derive(Debug)]
pub struct BadcCsvParser {
    station_registry: Arc<StationRegistry>,
}

impl BadcCsvParser {
    /// Create a new parser with station registry dependency
    pub fn new(station_registry: Arc<StationRegistry>) -> Self {
        Self { station_registry }
    }

    /// Parse a BADC-CSV file and return observations with statistics
    pub async fn parse_file(&self, file_path: &Path) -> Result<ParseResult> {
        info!("Parsing BADC-CSV file: {}", file_path.display());

        let mut stats = ParseStats::new();
        let mut observations = Vec::new();

        // Read file content
        let content = std::fs::read_to_string(file_path).map_err(|e| {
            Error::io_error(format!(
                "Failed to read file {}: {}",
                file_path.display(),
                e
            ))
        })?;

        // Split into header and data sections
        let (header_lines, data_section) = self.split_sections(&content)?;

        // Parse header for basic metadata
        let simple_header = SimpleHeader::parse(&header_lines)?;
        debug!(
            "Parsed header: missing_value={}",
            simple_header.missing_value
        );

        // Parse data section
        if let Some(data_content) = data_section {
            let _column_mapping = self
                .parse_data_section(&data_content, &simple_header, &mut observations, &mut stats)
                .await?;

            info!(
                "Parsed {} observations from {} records",
                stats.observations_parsed, stats.total_records
            );
        } else {
            warn!("No data section found in file");
        }

        Ok(ParseResult {
            observations,
            stats,
        })
    }

    /// Split file content into header and data sections
    fn split_sections(&self, content: &str) -> Result<(Vec<String>, Option<String>)> {
        let lines: Vec<&str> = content.lines().collect();

        // Find "data" marker
        let data_start = lines
            .iter()
            .position(|line| line.trim() == "data")
            .ok_or_else(|| Error::file_format("No 'data' section marker found in BADC-CSV file"))?;

        let header_lines = lines[..data_start].iter().map(|s| s.to_string()).collect();

        // Check if there's content after the data marker
        if data_start + 1 < lines.len() {
            let data_content = lines[data_start + 1..].join("\n");
            Ok((header_lines, Some(data_content)))
        } else {
            Ok((header_lines, None))
        }
    }

    /// Parse data section and extract observations
    async fn parse_data_section(
        &self,
        data_content: &str,
        header: &SimpleHeader,
        observations: &mut Vec<Observation>,
        stats: &mut ParseStats,
    ) -> Result<ColumnMapping> {
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(data_content.as_bytes());

        // Get column headers and create mapping
        let headers = csv_reader
            .headers()
            .map_err(|e| Error::file_format(format!("Failed to read CSV headers: {}", e)))?;

        let column_mapping = ColumnMapping::analyze(headers)?;
        let (total_cols, measurement_cols, quality_cols) = column_mapping.stats();
        debug!(
            "Column mapping: {} total, {} measurements, {} quality flags",
            total_cols, measurement_cols, quality_cols
        );

        // Parse data records
        for result in csv_reader.records() {
            stats.total_records += 1;

            match result {
                Ok(record) => {
                    // Skip "end data" line and other section markers
                    if let Some(first_field) = record.get(0) {
                        if first_field.trim().starts_with("end") {
                            stats.records_skipped += 1;
                            continue;
                        }
                    }

                    match parse_observation_record(
                        &record,
                        &column_mapping,
                        header,
                        &self.station_registry,
                    )
                    .await
                    {
                        Ok(observation) => {
                            observations.push(observation);
                            stats.observations_parsed += 1;
                        }
                        Err(e) => {
                            stats.records_skipped += 1;
                            stats
                                .errors
                                .push(format!("Record {}: {}", stats.total_records, e));
                            debug!("Skipped record {}: {}", stats.total_records, e);
                        }
                    }
                }
                Err(e) => {
                    stats.records_skipped += 1;
                    stats.errors.push(format!(
                        "CSV parse error at record {}: {}",
                        stats.total_records, e
                    ));
                }
            }
        }

        Ok(column_mapping)
    }
}
