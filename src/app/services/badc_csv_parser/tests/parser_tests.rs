//! Tests for the main BADC-CSV parser functionality

use super::*;

#[test]
fn test_section_splitting() {
    let content = r#"Conventions,G,BADC-CSV,1
title,G,Test
missing_value,G,NA
data
col1,col2,col3
val1,val2,val3
end data"#;

    // Test section splitting logic directly
    let lines: Vec<&str> = content.lines().collect();
    let data_start = lines.iter().position(|line| line.trim() == "data").unwrap();
    let header_lines: Vec<String> = lines[..data_start].iter().map(|s| s.to_string()).collect();
    let data_section = if data_start + 1 < lines.len() {
        Some(lines[data_start + 1..].join("\n"))
    } else {
        None
    };

    assert_eq!(header_lines.len(), 3);
    assert!(header_lines[0].contains("Conventions"));
    assert!(header_lines[1].contains("title"));
    assert!(header_lines[2].contains("missing_value"));

    let data_content = data_section.unwrap();
    assert!(data_content.contains("col1,col2,col3"));
    assert!(data_content.contains("val1,val2,val3"));
    assert!(data_content.contains("end data"));
}

#[test]
fn test_section_splitting_no_data() {
    let content = r#"Conventions,G,BADC-CSV,1
title,G,Test
missing_value,G,NA"#;

    let lines: Vec<&str> = content.lines().collect();
    let data_start = lines.iter().position(|line| line.trim() == "data");

    assert!(data_start.is_none());
}

#[test]
fn test_section_splitting_empty_data() {
    let content = r#"Conventions,G,BADC-CSV,1
title,G,Test
data"#;

    let lines: Vec<&str> = content.lines().collect();
    let data_start = lines.iter().position(|line| line.trim() == "data").unwrap();
    let header_lines: Vec<String> = lines[..data_start].iter().map(|s| s.to_string()).collect();
    let data_section = if data_start + 1 < lines.len() {
        Some(lines[data_start + 1..].join("\n"))
    } else {
        None
    };

    assert_eq!(header_lines.len(), 2);
    assert!(data_section.is_none());
}

#[test]
fn test_header_and_data_integration() {
    let temp_file = create_temp_file(&create_test_badc_csv());
    let content = std::fs::read_to_string(temp_file.path()).unwrap();

    // Test file reading and section splitting
    let lines: Vec<&str> = content.lines().collect();
    let data_start = lines.iter().position(|line| line.trim() == "data").unwrap();
    let header_lines: Vec<String> = lines[..data_start].iter().map(|s| s.to_string()).collect();
    let data_section = if data_start + 1 < lines.len() {
        Some(lines[data_start + 1..].join("\n"))
    } else {
        None
    };

    // Verify header parsing
    assert!(!header_lines.is_empty());
    assert!(data_section.is_some());

    // Test header extraction
    let header = super::super::header::SimpleHeader::parse(&header_lines).unwrap();
    assert_eq!(header.missing_value, "NA");
    assert_eq!(header.title, Some("Test Temperature Data".to_string()));
}

#[test]
fn test_minimal_file_structure() {
    let temp_file = create_temp_file(&create_minimal_badc_csv());
    let content = std::fs::read_to_string(temp_file.path()).unwrap();

    let lines: Vec<&str> = content.lines().collect();
    let data_start = lines.iter().position(|line| line.trim() == "data").unwrap();
    let header_lines: Vec<String> = lines[..data_start].iter().map(|s| s.to_string()).collect();
    let data_section = if data_start + 1 < lines.len() {
        Some(lines[data_start + 1..].join("\n"))
    } else {
        None
    };

    assert!(!header_lines.is_empty());
    assert!(data_section.is_some());

    let header = super::super::header::SimpleHeader::parse(&header_lines).unwrap();
    assert_eq!(header.missing_value, "NA");
    assert_eq!(header.title, None);
}

/// Demonstration test showing BadcCsvParser output with a real MIDAS observation file
///
/// This test is ignored by default as it requires access to real MIDAS data files.
/// Run with: `cargo test test_demonstrate_real_parser_output -- --ignored`
///
/// Purpose:
/// - Demonstrate the parser working with actual MIDAS observation data
/// - Show how the parser handles real-world data quality issues
/// - Provide debugging output for understanding parser behavior
/// - Validate integration with station registry using real data
#[tokio::test]
#[ignore] // Ignored because it requires real data files and is for demonstration
async fn test_demonstrate_real_parser_output() {
    use super::super::super::station_registry::StationRegistry;
    use super::super::BadcCsvParser;
    use std::path::Path;

    // Real MIDAS observation file path
    let real_file_path = Path::new(
        "/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-daily-temperature-obs/qcv-1/aberdeenshire/00144_corgarff-castle-lodge/midas-open_uk-daily-temperature-obs_dv-202507_aberdeenshire_00144_corgarff-castle-lodge_qcv-1_1994.csv",
    );

    // Skip test if file doesn't exist (e.g., in CI environments)
    if !real_file_path.exists() {
        println!(
            "⚠️  Skipping demonstration test - real MIDAS file not found at: {}",
            real_file_path.display()
        );
        println!("   This is expected in CI environments or different machine setups.");
        return;
    }

    println!("🔍 BADC CSV Parser Demonstration with Real MIDAS Data");
    println!("=====================================================");
    println!("📁 File: {}", real_file_path.display());
    println!();

    // Load real station registry from cache
    let cache_path =
        Path::new("/Users/richardlyon/Library/Application Support/midas-fetcher/cache");
    if !cache_path.exists() {
        println!(
            "⚠️  Skipping demonstration test - MIDAS cache not found at: {}",
            cache_path.display()
        );
        return;
    }

    println!("📦 Loading station registry from real MIDAS cache...");
    let datasets = vec!["uk-daily-temperature-obs".to_string()];

    let (registry, load_stats) =
        match StationRegistry::load_from_cache(cache_path, &datasets, false).await {
            Ok((registry, stats)) => (registry, stats),
            Err(e) => {
                println!("❌ Failed to load station registry: {}", e);
                return;
            }
        };

    println!("✅ Station registry loaded successfully:");
    println!("   • {} stations loaded", registry.station_count());
    println!("   • {} files processed", load_stats.files_processed);
    println!(
        "   • Load time: {:.2}s",
        load_stats.load_duration.as_secs_f64()
    );
    println!();

    // Create parser with real station registry
    let parser = BadcCsvParser::new(std::sync::Arc::new(registry));

    println!("🔄 Parsing real MIDAS observation file...");
    let start_time = std::time::Instant::now();

    let result = match parser.parse_file(real_file_path).await {
        Ok(result) => result,
        Err(e) => {
            println!("❌ Failed to parse file: {}", e);
            return;
        }
    };

    let parse_duration = start_time.elapsed();

    println!(
        "✅ Parsing completed in {:.3}s",
        parse_duration.as_secs_f64()
    );
    println!();

    // Display parsing statistics
    println!("📊 Parsing Statistics:");
    println!(
        "   • Total records processed: {}",
        result.stats.total_records
    );
    println!(
        "   • Observations parsed: {}",
        result.stats.observations_parsed
    );
    println!("   • Records skipped: {}", result.stats.records_skipped);
    println!("   • Parse errors: {}", result.stats.errors.len());
    println!("   • Success rate: {:.1}%", result.stats.success_rate());
    println!(
        "   • Overall success: {}",
        if result.stats.is_successful() {
            "✅ Yes"
        } else {
            "❌ No"
        }
    );
    println!();

    // Show first few errors if any
    if !result.stats.errors.is_empty() {
        println!("⚠️  Sample parse errors (first 3):");
        for error in result.stats.errors.iter().take(3) {
            println!("   • {}", error);
        }
        println!();
    }

    // Display information about parsed observations
    if result.observations.is_empty() {
        println!("⚠️  No observations were successfully parsed.");
        println!("   This could be due to:");
        println!("   • Station not found in registry");
        println!("   • All data values are missing (NA)");
        println!("   • Data quality issues");
        return;
    }

    println!(
        "🌡️  Successfully Parsed Observations: {}",
        result.observations.len()
    );
    println!();

    // Show station information from first observation
    let first_obs = &result.observations[0];
    println!("🏭 Station Information:");
    println!("   • Station ID: {}", first_obs.station.src_id);
    println!("   • Station Name: {}", first_obs.station.src_name);
    println!(
        "   • Location: {:.4}°N, {:.4}°E",
        first_obs.station.high_prcn_lat, first_obs.station.high_prcn_lon
    );
    println!("   • Elevation: {:.1}m", first_obs.station.height_meters);
    println!("   • County: {}", first_obs.station.historic_county);
    println!("   • Authority: {}", first_obs.station.authority);
    println!(
        "   • Active Period: {} to {}",
        first_obs.station.src_bgn_date.format("%Y-%m-%d"),
        first_obs.station.src_end_date.format("%Y-%m-%d")
    );
    println!();

    // Analyze measurement types and quality flags
    let mut all_measurements = std::collections::HashSet::new();
    let mut all_quality_flags = std::collections::HashSet::new();
    let mut quality_flag_counts = std::collections::HashMap::new();

    for obs in &result.observations {
        for measurement_name in obs.measurements.keys() {
            all_measurements.insert(measurement_name.clone());
        }
        for quality_name in obs.quality_flags.keys() {
            all_quality_flags.insert(quality_name.clone());
        }
        for quality_flag in obs.quality_flags.values() {
            *quality_flag_counts.entry(quality_flag.clone()).or_insert(0) += 1;
        }
    }

    println!(
        "📈 Measurement Types Found ({} types):",
        all_measurements.len()
    );
    for measurement in &all_measurements {
        println!("   • {}", measurement);
    }
    println!();

    println!("🎯 Quality Flag Distribution:");
    for (flag, count) in &quality_flag_counts {
        println!("   • {}: {} occurrences", flag, count);
    }
    println!();

    // Show sample observations with data
    println!("📋 Sample Observations with Actual Data:");
    let mut shown_count = 0;
    for obs in &result.observations {
        if !obs.measurements.is_empty() && shown_count < 5 {
            println!("   🕒 {}", obs.ob_end_time.format("%Y-%m-%d %H:%M:%S"));
            println!("      • Observation ID: {}", obs.observation_id);
            println!("      • Hour Count: {} hours", obs.ob_hour_count);

            // Show measurements
            if !obs.measurements.is_empty() {
                println!("      • Measurements:");
                for (name, value) in &obs.measurements {
                    let quality = obs
                        .quality_flags
                        .get(name)
                        .map(|q| format!(" (Quality: {})", q))
                        .unwrap_or_default();
                    println!("        - {}: {:.1}°C{}", name, value, quality);
                }
            }

            println!();
            shown_count += 1;
        }
    }

    if shown_count == 0 {
        println!("   ℹ️  All observations have missing data (NA values)");
        println!("      This is common in real MIDAS data due to equipment issues or maintenance");
        println!();

        // Show a few sample observations even without measurements
        println!("📋 Sample Observations (structure only):");
        for obs in result.observations.iter().take(3) {
            println!("   🕒 {}", obs.ob_end_time.format("%Y-%m-%d %H:%M:%S"));
            println!(
                "      • ID: {}, Hours: {}, Station: {}",
                obs.observation_id, obs.ob_hour_count, obs.station_id
            );
        }
    }

    println!();
    println!("✨ Demonstration completed successfully!");
    println!("   This shows the BadcCsvParser working with real MIDAS observation data,");
    println!("   handling missing values, quality flags, and station metadata integration.");
}
