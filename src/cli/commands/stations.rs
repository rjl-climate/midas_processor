//! Stations command implementation for MIDAS processor CLI
//!
//! This module contains the station registry analysis and reporting functionality,
//! including geographic distribution, temporal analysis, and various output formats.

use super::shared::{ProcessingStats, discover_datasets, setup_stations_logging};
use crate::app::services::station_registry::StationRegistry;
use crate::cli::args::{OutputFormat, StationsArgs};
use crate::{Error, Result};
use chrono::TimeZone;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use tracing::{debug, info, warn};

/// Stations command runner for MIDAS processor
///
/// This function generates station registry reports and visualizations.
pub async fn run_stations(args: StationsArgs) -> Result<ProcessingStats> {
    let start_time = Instant::now();

    // Set up logging
    setup_stations_logging(&args)?;

    info!("Starting MIDAS station registry report");
    debug!("Stations arguments: {:?}", args);

    // Validate arguments
    args.validate()?;

    // Determine cache path
    let cache_path = match &args.cache_path {
        Some(path) => path.clone(),
        None => {
            // Use default cache location
            use directories::UserDirs;
            if let Some(user_dirs) = UserDirs::new() {
                user_dirs
                    .home_dir()
                    .join("Library")
                    .join("Application Support")
                    .join("midas-fetcher")
                    .join("cache")
            } else {
                // Fallback if home directory can't be determined
                PathBuf::from("/tmp/midas-fetcher-cache")
            }
        }
    };

    info!(
        "Loading station registry from cache: {}",
        cache_path.display()
    );

    // Determine datasets to load
    let datasets = match args.get_datasets() {
        Some(datasets) => datasets,
        None => {
            // Auto-discover datasets in cache
            discover_datasets(&cache_path)?
        }
    };

    info!(
        "Loading station registry for {} datasets: {:?}",
        datasets.len(),
        datasets
    );

    // Load station registries for each dataset separately and combine
    let mut combined_registry = StationRegistry::new(cache_path.clone());
    let mut combined_load_stats = crate::app::services::station_registry::LoadStats::new();

    for dataset in &datasets {
        info!("Loading stations for dataset: {}", dataset);

        let (dataset_registry, dataset_stats) = StationRegistry::load_for_dataset(
            &cache_path,
            dataset,
            true, // Show progress
        )
        .await?;

        // Merge stations from this dataset into combined registry
        for (&station_id, station) in dataset_registry.iter_stations() {
            if combined_registry.contains_station(station_id) {
                warn!(
                    "Station {} found in multiple datasets - keeping first occurrence",
                    station_id
                );
            } else {
                combined_registry.add_station(station.clone());
            }
        }

        // Accumulate statistics
        combined_load_stats.files_processed += dataset_stats.files_processed;
        combined_load_stats.stations_loaded += dataset_stats.stations_loaded;
        combined_load_stats.total_records_found += dataset_stats.total_records_found;
        combined_load_stats.datasets_processed += 1;
        combined_load_stats.errors.extend(dataset_stats.errors);
    }

    // Update combined registry metadata
    combined_registry.loaded_datasets = datasets.clone();
    combined_registry.files_processed = combined_load_stats.files_processed;
    combined_registry.total_records_found = combined_load_stats.total_records_found;
    combined_load_stats.load_duration = start_time.elapsed();

    info!(
        "Station registry loaded: {} stations from {} files across {} datasets in {:.2}s",
        combined_load_stats.stations_loaded,
        combined_load_stats.files_processed,
        datasets.len(),
        combined_load_stats.load_duration.as_secs_f64()
    );

    // Generate report
    generate_station_report(&args, &combined_registry, &combined_load_stats)?;

    // Convert to processing stats for consistency
    let stats = ProcessingStats {
        datasets_processed: datasets.len(),
        files_processed: combined_load_stats.files_processed,
        stations_loaded: combined_load_stats.stations_loaded,
        observations_processed: 0, // Not applicable for stations command
        errors_encountered: combined_load_stats.errors.len(),
        processing_time: start_time.elapsed(),
        output_sizes: if let Some(output_file) = &args.output_file {
            // Try to get file size if we wrote to a file
            if let Ok(metadata) = std::fs::metadata(output_file) {
                vec![(output_file.display().to_string(), metadata.len())]
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        },
    };

    info!(
        "Station report completed in {:.2}s",
        stats.processing_time.as_secs_f64()
    );

    Ok(stats)
}

/// Generate station registry report based on output format
fn generate_station_report(
    args: &StationsArgs,
    registry: &StationRegistry,
    load_stats: &crate::app::services::station_registry::LoadStats,
) -> Result<()> {
    match args.output_format {
        OutputFormat::Human => generate_human_station_report(args, registry, load_stats),
        OutputFormat::Json => generate_json_station_report(args, registry, load_stats),
        OutputFormat::Csv => generate_csv_station_report(args, registry, load_stats),
    }
}

/// Generate human-readable station report
fn generate_human_station_report(
    args: &StationsArgs,
    registry: &StationRegistry,
    load_stats: &crate::app::services::station_registry::LoadStats,
) -> Result<()> {
    let stations = registry.stations();
    let metadata = registry.metadata();

    // Apply filters
    let filtered_stations = apply_station_filters(args, &stations)?;

    let mut output = format!(
        "📊 MIDAS Station Registry Report\n\
         ================================\n\
         📁 Cache Path: {}\n\
         📦 Datasets: {}\n\
         🏭 Total Stations: {} (showing {} after filters)\n\
         📄 Files Processed: {}\n\
         ⏱️  Load Time: {:.2}s\n\
         \n",
        metadata.cache_path.display(),
        metadata.loaded_datasets.join(", "),
        metadata.station_count,
        filtered_stations.len(),
        load_stats.files_processed,
        load_stats.load_duration.as_secs_f64()
    );

    if !load_stats.errors.is_empty() {
        output.push_str(&format!(
            "⚠️  Load Errors: {} (see log for details)\n\n",
            load_stats.errors.len()
        ));
    }

    if !filtered_stations.is_empty() {
        // Geographic distribution analysis
        let mut county_counts: HashMap<String, usize> = HashMap::new();
        let mut elevation_ranges = [0; 4]; // [0-50m, 50-200m, 200-500m, 500m+]
        let mut temporal_active_2020 = 0;
        let mut oldest_station: Option<&crate::app::models::Station> = None;
        let mut newest_station: Option<&crate::app::models::Station> = None;

        let reference_2020 = chrono::Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();

        for station in &filtered_stations {
            // County distribution
            *county_counts
                .entry(station.historic_county.clone())
                .or_insert(0) += 1;

            // Elevation distribution
            let elevation = station.height_meters;
            if elevation < 50.0 {
                elevation_ranges[0] += 1;
            } else if elevation < 200.0 {
                elevation_ranges[1] += 1;
            } else if elevation < 500.0 {
                elevation_ranges[2] += 1;
            } else {
                elevation_ranges[3] += 1;
            }

            // Temporal analysis
            if station.src_bgn_date <= reference_2020 && station.src_end_date >= reference_2020 {
                temporal_active_2020 += 1;
            }

            // Track oldest/newest
            if oldest_station.is_none()
                || station.src_bgn_date < oldest_station.unwrap().src_bgn_date
            {
                oldest_station = Some(station);
            }
            if newest_station.is_none()
                || station.src_end_date > newest_station.unwrap().src_end_date
            {
                newest_station = Some(station);
            }
        }

        // Geographic summary
        output.push_str("🗺️  Geographic Distribution:\n");
        let mut sorted_counties: Vec<_> = county_counts.iter().collect();
        sorted_counties.sort_by(|a, b| b.1.cmp(a.1));
        for (county, count) in sorted_counties.iter().take(10) {
            let percentage = (**count as f64 / filtered_stations.len() as f64) * 100.0;
            output.push_str(&format!(
                "   • {}: {} stations ({:.1}%)\n",
                county, count, percentage
            ));
        }
        if sorted_counties.len() > 10 {
            output.push_str(&format!(
                "   • ... and {} more counties\n",
                sorted_counties.len() - 10
            ));
        }
        output.push('\n');

        // Elevation analysis
        output.push_str("🏔️  Elevation Distribution:\n");
        output.push_str(&format!(
            "   • Sea level (0-50m): {} stations\n",
            elevation_ranges[0]
        ));
        output.push_str(&format!(
            "   • Low elevation (50-200m): {} stations\n",
            elevation_ranges[1]
        ));
        output.push_str(&format!(
            "   • Mid elevation (200-500m): {} stations\n",
            elevation_ranges[2]
        ));
        output.push_str(&format!(
            "   • High elevation (500m+): {} stations\n",
            elevation_ranges[3]
        ));
        output.push('\n');

        // Temporal analysis
        output.push_str("⏰ Temporal Coverage:\n");
        if let Some(oldest) = oldest_station {
            output.push_str(&format!(
                "   • Oldest record: {} ({})\n",
                oldest.src_bgn_date.format("%Y-%m-%d"),
                oldest.src_name
            ));
        }
        if let Some(newest) = newest_station {
            output.push_str(&format!(
                "   • Latest record: {} ({})\n",
                newest.src_end_date.format("%Y-%m-%d"),
                newest.src_name
            ));
        }
        output.push_str(&format!(
            "   • Active in 2020: {} stations\n",
            temporal_active_2020
        ));
        output.push('\n');

        // Detailed listings if requested
        if args.detailed {
            output.push_str("📋 Detailed Station Listing:\n");
            output.push_str("ID     | Name                    | County              | Lat     | Lon      | Elev(m) | Active Period\n");
            output.push_str("-------|-------------------------|---------------------|---------|----------|---------|----------------------------\n");

            let mut sorted_stations = filtered_stations.clone();
            sorted_stations.sort_by_key(|s| s.src_id);

            for station in sorted_stations.iter().take(50) {
                // Limit to first 50 for readability
                output.push_str(&format!(
                    "{:6} | {:23} | {:19} | {:7.3} | {:8.3} | {:7.1} | {} to {}\n",
                    station.src_id,
                    if station.src_name.len() > 23 {
                        station.src_name[..20].to_owned() + "..."
                    } else {
                        station.src_name.clone()
                    },
                    if station.historic_county.len() > 19 {
                        station.historic_county[..16].to_owned() + "..."
                    } else {
                        station.historic_county.clone()
                    },
                    station.high_prcn_lat,
                    station.high_prcn_lon,
                    station.height_meters,
                    station.src_bgn_date.format("%Y-%m-%d"),
                    station.src_end_date.format("%Y-%m-%d")
                ));
            }

            if sorted_stations.len() > 50 {
                output.push_str(&format!("\n... and {} more stations (use --output-file with CSV format for complete listing)\n", sorted_stations.len() - 50));
            }
        } else {
            output.push_str("💡 Use --detailed flag for complete station listings\n");
        }
    } else {
        output.push_str("No stations found matching the specified filters.\n");
    }

    // Output the report
    match &args.output_file {
        Some(path) => {
            std::fs::write(path, &output).map_err(|e| {
                Error::configuration(format!(
                    "Failed to write report to {}: {}",
                    path.display(),
                    e
                ))
            })?;
            info!("Station report written to: {}", path.display());
        }
        None => {
            println!("{}", output);
        }
    }

    Ok(())
}

/// Generate JSON station report
fn generate_json_station_report(
    args: &StationsArgs,
    registry: &StationRegistry,
    load_stats: &crate::app::services::station_registry::LoadStats,
) -> Result<()> {
    use serde_json::json;

    let stations = registry.stations();
    let metadata = registry.metadata();
    let filtered_stations = apply_station_filters(args, &stations)?;

    let json_stations: Vec<_> = filtered_stations
        .iter()
        .map(|station| {
            json!({
                "src_id": station.src_id,
                "name": station.src_name,
                "coordinates": {
                    "latitude": station.high_prcn_lat,
                    "longitude": station.high_prcn_lon
                },
                "elevation_m": station.height_meters,
                "location": {
                    "county": station.historic_county,
                    "grid_reference": {
                        "east": station.east_grid_ref,
                        "north": station.north_grid_ref,
                        "type": station.grid_ref_type
                    }
                },
                "active_period": {
                    "start": station.src_bgn_date.format("%Y-%m-%d").to_string(),
                    "end": station.src_end_date.format("%Y-%m-%d").to_string()
                },
                "authority": station.authority
            })
        })
        .collect();

    let json_report = json!({
        "metadata": {
            "cache_path": metadata.cache_path,
            "datasets": metadata.loaded_datasets,
            "total_stations_in_registry": metadata.station_count,
            "stations_in_report": filtered_stations.len(),
            "files_processed": load_stats.files_processed,
            "load_duration_seconds": load_stats.load_duration.as_secs_f64(),
            "load_errors": load_stats.errors.len(),
            "generated_at": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
        },
        "filters_applied": {
            "datasets": args.get_datasets(),
            "region": args.region,
            "active_period": args.active_period
        },
        "stations": json_stations
    });

    let json_string = serde_json::to_string_pretty(&json_report)
        .map_err(|e| Error::configuration(format!("Failed to serialize station report: {}", e)))?;

    match &args.output_file {
        Some(path) => {
            std::fs::write(path, &json_string).map_err(|e| {
                Error::configuration(format!(
                    "Failed to write JSON report to {}: {}",
                    path.display(),
                    e
                ))
            })?;
            info!("JSON station report written to: {}", path.display());
        }
        None => {
            println!("{}", json_string);
        }
    }

    Ok(())
}

/// Generate CSV station report
fn generate_csv_station_report(
    args: &StationsArgs,
    registry: &StationRegistry,
    _load_stats: &crate::app::services::station_registry::LoadStats,
) -> Result<()> {
    let stations = registry.stations();
    let filtered_stations = apply_station_filters(args, &stations)?;

    let mut csv = String::new();
    csv.push_str("src_id,name,latitude,longitude,elevation_m,county,authority,start_date,end_date,east_grid_ref,north_grid_ref,grid_ref_type\n");

    let mut sorted_stations = filtered_stations;
    sorted_stations.sort_by_key(|s| s.src_id);

    for station in sorted_stations {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{}\n",
            station.src_id,
            csv_escape(&station.src_name),
            station.high_prcn_lat,
            station.high_prcn_lon,
            station.height_meters,
            csv_escape(&station.historic_county),
            csv_escape(&station.authority),
            station.src_bgn_date.format("%Y-%m-%d"),
            station.src_end_date.format("%Y-%m-%d"),
            station
                .east_grid_ref
                .map_or_else(|| "".to_string(), |v| v.to_string()),
            station
                .north_grid_ref
                .map_or_else(|| "".to_string(), |v| v.to_string()),
            station.grid_ref_type.as_deref().unwrap_or("")
        ));
    }

    match &args.output_file {
        Some(path) => {
            std::fs::write(path, &csv).map_err(|e| {
                Error::configuration(format!(
                    "Failed to write CSV report to {}: {}",
                    path.display(),
                    e
                ))
            })?;
            info!("CSV station report written to: {}", path.display());
        }
        None => {
            println!("{}", csv);
        }
    }

    Ok(())
}

/// Apply filters to station list based on command arguments
fn apply_station_filters<'a>(
    args: &StationsArgs,
    stations: &'a [&'a crate::app::models::Station],
) -> Result<Vec<&'a crate::app::models::Station>> {
    let mut filtered = stations.to_vec();

    // Apply geographic region filter
    if let Some(region) = &args.region {
        let (min_lat, max_lat, min_lon, max_lon) = args.parse_region(region)?;
        filtered.retain(|station| {
            station.high_prcn_lat >= min_lat
                && station.high_prcn_lat <= max_lat
                && station.high_prcn_lon >= min_lon
                && station.high_prcn_lon <= max_lon
        });
    }

    // Apply active period filter
    if let Some(active_period) = &args.active_period {
        let (start_date, end_date) = args.parse_active_period(active_period)?;
        filtered.retain(|station| {
            // Station is active if its operational period overlaps with the query period
            station.src_bgn_date <= end_date && station.src_end_date >= start_date
        });
    }

    Ok(filtered)
}

/// Escape CSV field values
fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::models::Station;
    use chrono::Utc;

    fn create_test_station(id: i32, name: &str, county: &str) -> Station {
        let start_date = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let end_date = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();

        Station::new(
            id,
            name.to_string(),
            51.5074,      // London latitude
            -0.1278,      // London longitude
            Some(532445), // East grid reference
            Some(181680), // North grid reference
            Some("GB_GRID".to_string()),
            start_date,
            end_date,
            "Met Office".to_string(),
            county.to_string(),
            25.0,
        )
        .unwrap()
    }

    #[test]
    fn test_csv_escape() {
        assert_eq!(csv_escape("simple"), "simple");
        assert_eq!(csv_escape("with,comma"), "\"with,comma\"");
        assert_eq!(csv_escape("with\"quote"), "\"with\"\"quote\"");
        assert_eq!(csv_escape("with\nnewline"), "\"with\nnewline\"");
    }

    #[test]
    fn test_apply_station_filters_no_filters() {
        let station1 = create_test_station(1, "Station 1", "County A");
        let station2 = create_test_station(2, "Station 2", "County B");
        let stations = vec![&station1, &station2];

        let args = StationsArgs {
            cache_path: None,
            datasets: None,
            output_format: OutputFormat::Human,
            output_file: None,
            region: None,
            active_period: None,
            detailed: false,
            verbose: 0,
        };

        let filtered = apply_station_filters(&args, &stations).unwrap();
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_generate_csv_station_report() {
        // Create a mock registry for testing
        use crate::app::services::station_registry::StationRegistry;
        use std::path::PathBuf;

        let cache_path = PathBuf::from("/tmp/test");
        let mut registry = StationRegistry::new(cache_path);

        let station = create_test_station(123, "Test Station", "Test County");
        registry.add_station(station);

        let args = StationsArgs {
            cache_path: None,
            datasets: None,
            output_format: OutputFormat::Csv,
            output_file: None,
            region: None,
            active_period: None,
            detailed: false,
            verbose: 0,
        };

        let load_stats = crate::app::services::station_registry::LoadStats::new();

        // Should not panic
        let result = generate_csv_station_report(&args, &registry, &load_stats);
        assert!(result.is_ok());
    }
}
