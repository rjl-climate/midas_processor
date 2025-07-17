//! BADC-CSV header parsing and metadata extraction.
//!
//! Parses BADC-CSV formatted headers to extract station metadata
//! (location, elevation, station names) and calculate data boundaries
//! for proper CSV reading with skip_rows handling.

use crate::error::{MidasError, Result};
use crate::models::{DataBoundaries, StationMetadata};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use tracing::{debug, warn};

/// Extract metadata and data boundaries from BADC-CSV header
pub fn parse_badc_header(file_path: &Path) -> Result<(StationMetadata, DataBoundaries)> {
    let file = File::open(file_path).map_err(MidasError::Io)?;
    let reader = BufReader::new(file);

    let mut metadata = StationMetadataBuilder::new();
    let mut data_start_line = None;
    let mut total_lines = 0;

    for (line_num, line) in reader.lines().enumerate() {
        let line = line.map_err(MidasError::Io)?;
        total_lines = line_num + 1;

        // Check for data section marker
        if line.trim() == "data" {
            data_start_line = Some(line_num + 1); // Line after "data"
            continue;
        }

        // Check for end data marker
        if line.trim() == "end data" {
            break;
        }

        // Only parse metadata before data section
        if data_start_line.is_none() {
            metadata.parse_line(&line)?;
        }
    }

    let skip_rows = data_start_line.ok_or_else(|| MidasError::NoDataMarker {
        path: file_path.to_path_buf(),
    })?;

    // Calculate data rows (total - header - end marker)
    let data_rows = if total_lines > skip_rows {
        Some(total_lines - skip_rows - 1) // -1 for "end data" line
    } else {
        None
    };

    let boundaries = DataBoundaries {
        skip_rows,
        data_rows,
        total_lines,
    };

    let station_metadata = metadata.build(file_path)?;

    debug!(
        "Parsed header for {}: skip_rows={}, data_rows={:?}",
        file_path.display(),
        skip_rows,
        data_rows
    );

    Ok((station_metadata, boundaries))
}

/// Builder for station metadata extraction
struct StationMetadataBuilder {
    station_name: Option<String>,
    station_id: Option<String>,
    county: Option<String>,
    location: Option<(f64, f64)>,
    height: Option<(f64, String)>,
}

impl StationMetadataBuilder {
    fn new() -> Self {
        Self {
            station_name: None,
            station_id: None,
            county: None,
            location: None,
            height: None,
        }
    }

    fn parse_line(&mut self, line: &str) -> Result<()> {
        // Skip comments and empty lines
        if line.trim().is_empty() || line.starts_with('#') {
            return Ok(());
        }

        // Parse BADC-CSV metadata lines (key,G,value format)
        let parts: Vec<&str> = line.splitn(3, ',').collect();
        if parts.len() < 3 || parts[1] != "G" {
            return Ok(()); // Not a metadata line
        }

        let key = parts[0].trim();
        let value = parts[2].trim();

        match key {
            "observation_station" => {
                self.station_name = Some(value.to_string());
            }
            "midas_station_id" => {
                self.station_id = Some(value.to_string());
            }
            "historic_county_name" => {
                self.county = Some(value.to_string());
            }
            "location" => {
                self.location = parse_location(value)?;
            }
            "height" => {
                self.height = parse_height(value)?;
            }
            _ => {} // Ignore other metadata
        }

        Ok(())
    }

    fn build(self, file_path: &Path) -> Result<StationMetadata> {
        let station_name = self
            .station_name
            .ok_or_else(|| MidasError::HeaderParsingFailed {
                path: file_path.to_path_buf(),
                reason: "Missing observation_station".to_string(),
            })?;

        let station_id = self
            .station_id
            .ok_or_else(|| MidasError::HeaderParsingFailed {
                path: file_path.to_path_buf(),
                reason: "Missing midas_station_id".to_string(),
            })?;

        let county = self.county.ok_or_else(|| MidasError::HeaderParsingFailed {
            path: file_path.to_path_buf(),
            reason: "Missing historic_county_name".to_string(),
        })?;

        let (latitude, longitude) =
            self.location
                .ok_or_else(|| MidasError::HeaderParsingFailed {
                    path: file_path.to_path_buf(),
                    reason: "Missing or invalid location".to_string(),
                })?;

        let (height, height_units) =
            self.height.ok_or_else(|| MidasError::HeaderParsingFailed {
                path: file_path.to_path_buf(),
                reason: "Missing or invalid height".to_string(),
            })?;

        Ok(StationMetadata {
            station_name,
            station_id,
            county,
            latitude,
            longitude,
            height,
            height_units,
        })
    }
}

/// Parse location string "lat,lon" into (lat, lon) tuple
fn parse_location(value: &str) -> Result<Option<(f64, f64)>> {
    let coords: Vec<&str> = value.split(',').collect();
    if coords.len() != 2 {
        warn!("Invalid location format: {}", value);
        return Ok(None);
    }

    let lat = coords[0].trim().parse::<f64>().ok();
    let lon = coords[1].trim().parse::<f64>().ok();

    match (lat, lon) {
        (Some(lat), Some(lon)) => Ok(Some((lat, lon))),
        _ => {
            warn!("Could not parse coordinates: {}", value);
            Ok(None)
        }
    }
}

/// Parse height string "value,units" into (value, units) tuple
fn parse_height(value: &str) -> Result<Option<(f64, String)>> {
    let parts: Vec<&str> = value.split(',').collect();
    if parts.len() != 2 {
        warn!("Invalid height format: {}", value);
        return Ok(None);
    }

    let height_val = parts[0].trim().parse::<f64>().ok();
    let units = parts[1].trim().to_string();

    match height_val {
        Some(h) => Ok(Some((h, units))),
        None => {
            warn!("Could not parse height value: {}", value);
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_parse_location() {
        assert_eq!(
            parse_location("57.006,-3.398").unwrap(),
            Some((57.006, -3.398))
        );
        assert_eq!(parse_location("invalid").unwrap(), None);
    }

    #[test]
    fn test_parse_height() {
        assert_eq!(
            parse_height("339,m").unwrap(),
            Some((339.0, "m".to_string()))
        );
        assert_eq!(parse_height("invalid").unwrap(), None);
    }

    #[test]
    fn test_badc_header_parsing() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "observation_station,G,braemar").unwrap();
        writeln!(temp_file, "midas_station_id,G,00147").unwrap();
        writeln!(temp_file, "historic_county_name,G,aberdeenshire").unwrap();
        writeln!(temp_file, "location,G,57.006,-3.398").unwrap();
        writeln!(temp_file, "height,G,339,m").unwrap();
        writeln!(temp_file, "data").unwrap();
        writeln!(temp_file, "test,data,row").unwrap();
        writeln!(temp_file, "end data").unwrap();

        let (metadata, boundaries) = parse_badc_header(temp_file.path()).unwrap();

        assert_eq!(metadata.station_name, "braemar");
        assert_eq!(metadata.station_id, "00147");
        assert_eq!(metadata.county, "aberdeenshire");
        assert_eq!(metadata.latitude, 57.006);
        assert_eq!(metadata.longitude, -3.398);
        assert_eq!(metadata.height, 339.0);
        assert_eq!(metadata.height_units, "m");

        assert_eq!(boundaries.skip_rows, 6);
        assert_eq!(boundaries.data_rows, Some(1));
    }
}
