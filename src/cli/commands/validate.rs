//! Validate command implementation for MIDAS processor CLI
//!
//! This module contains the comprehensive validation system for testing
//! the processing pipeline using real MIDAS data and generating reports.

use super::shared::{ProcessingStats, setup_validate_logging};
use crate::cli::args::{OutputFormat, ValidateArgs};
use crate::{Error, Result};
use indicatif::{HumanDuration, ProgressBar, ProgressStyle};
use std::time::Instant;
use tracing::{debug, info};

/// Validate command runner for MIDAS processor
///
/// This function runs comprehensive validation tests on the processing pipeline
/// using real MIDAS data from the cache to identify issues and generate reports.
pub async fn run_validate(args: ValidateArgs) -> Result<ProcessingStats> {
    let start_time = Instant::now();

    // Set up logging
    setup_validate_logging(&args)?;

    info!("Starting MIDAS processing pipeline validation");
    debug!("Validation arguments: {:?}", args);

    // Validate arguments
    args.validate()?;

    // Get cache and output paths
    let cache_path = args.get_cache_path();
    let output_dir = args.get_output_dir();

    info!("Cache path: {}", cache_path.display());
    info!("Output directory: {}", output_dir.display());

    // Create integration test configuration
    use crate::app::services::integration_test::{IntegrationTestConfig, IntegrationTestFramework};
    use crate::config::QualityControlConfig;

    let test_config = IntegrationTestConfig {
        cache_path,
        max_files: Some(args.max_files),
        datasets: args.get_datasets().unwrap_or_default(),
        output_dir,
        continue_on_error: args.continue_on_error,
        max_processing_time_per_file: args.max_processing_time,
        max_memory_per_file: 1024, // 1GB default
        min_file_size: args.min_file_size,
        quality_control: QualityControlConfig {
            require_station_metadata: !args.allow_missing_stations,
            exclude_empty_measurements: !args.include_empty_measurements,
        },
    };

    // Initialize test framework
    info!("Initializing validation test framework");
    let mut framework = IntegrationTestFramework::new(test_config).await?;

    // Run validation tests with progress reporting
    let progress_bar = if args.show_progress() {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} [{elapsed_precise}] {msg}")
                .unwrap(),
        );
        pb.set_message("Running validation tests...");
        pb.enable_steady_tick(std::time::Duration::from_millis(100));
        Some(pb)
    } else {
        None
    };

    info!("Running validation tests");
    let test_result = framework.run_tests().await?;

    if let Some(pb) = &progress_bar {
        pb.finish_with_message("Validation tests completed");
    }

    // Save validation results
    info!("Saving validation results");
    framework.save_results(&test_result).await?;

    // Generate final report
    generate_validation_report(&args, &test_result)?;

    // Convert to processing stats for consistency
    let stats = ProcessingStats {
        datasets_processed: test_result.detailed_stats.dataset_issues.len(),
        files_processed: test_result.files_processed,
        stations_loaded: 0, // Not tracked in validation
        observations_processed: test_result.total_records,
        errors_encountered: test_result.files_failed,
        processing_time: start_time.elapsed(),
        output_sizes: vec![
            ("detailed_stats.json".to_string(), 0), // Size would need to be calculated
            ("test_summary.md".to_string(), 0),
            ("issues.csv".to_string(), 0),
        ],
    };

    info!(
        "Validation completed in {:.2}s: {} files processed, {:.1}% success rate",
        stats.processing_time.as_secs_f64(),
        test_result.files_processed,
        test_result.success_rate()
    );

    Ok(stats)
}

/// Generate validation report based on output format
fn generate_validation_report(
    args: &ValidateArgs,
    result: &crate::app::services::integration_test::IntegrationTestResult,
) -> Result<()> {
    match args.output_format {
        OutputFormat::Human => generate_human_validation_report(result),
        OutputFormat::Json => generate_json_validation_report(result),
        OutputFormat::Csv => generate_csv_validation_report(result),
    }
}

/// Generate human-readable validation report
fn generate_human_validation_report(
    result: &crate::app::services::integration_test::IntegrationTestResult,
) -> Result<()> {
    let duration = HumanDuration(std::time::Duration::from_secs_f64(
        result.total_processing_time_seconds,
    ));

    println!("\n🧪 MIDAS Processing Pipeline Validation Results");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Overall status
    if result.success {
        println!("✅ Overall Status: PASS");
    } else {
        println!("❌ Overall Status: FAIL");
    }

    println!("\n📊 Test Summary:");
    println!(
        "   • Files processed: {} ({} failed)",
        result.files_processed, result.files_failed
    );
    println!(
        "   • Records processed: {} ({:.1}% success rate)",
        result.total_records,
        result.success_rate()
    );
    println!("   • Processing time: {}", duration);

    if let Some(memory) = result.peak_memory_mb {
        println!("   • Peak memory usage: {:.1}MB", memory);
    }

    // Issue summary
    if !result.detailed_stats.issue_counts.is_empty() {
        println!("\n⚠️  Issues Identified:");
        for (issue_type, count) in &result.detailed_stats.issue_counts {
            println!("   • {:?}: {} occurrences", issue_type, count);
        }
    } else {
        println!("\n✅ No issues identified during validation");
    }

    // Critical issues
    let critical_files = result.detailed_stats.files_with_critical_issues();
    if !critical_files.is_empty() {
        println!("\n🚨 Files with Critical Issues: {}", critical_files.len());
        for file in critical_files.iter().take(5) {
            println!(
                "   • {}: {} critical issues",
                file.file_path.display(),
                file.issues_by_severity(
                    crate::app::services::record_processor::detailed_stats::IssueSeverity::Critical
                )
                .len()
            );
        }
        if critical_files.len() > 5 {
            println!(
                "   • ... and {} more files with critical issues",
                critical_files.len() - 5
            );
        }
    }

    // Low success rate files
    let low_success_files = result.detailed_stats.files_with_low_success_rate(80.0);
    if !low_success_files.is_empty() {
        println!(
            "\n📉 Files with Low Success Rate (<80%): {}",
            low_success_files.len()
        );
        for file in low_success_files.iter().take(5) {
            println!(
                "   • {}: {:.1}% success rate",
                file.file_path.display(),
                file.success_rate()
            );
        }
        if low_success_files.len() > 5 {
            println!(
                "   • ... and {} more files with low success rates",
                low_success_files.len() - 5
            );
        }
    }

    // Top problematic files
    let problematic_files = result.detailed_stats.most_problematic_files(5);
    if !problematic_files.is_empty() {
        println!("\n🔍 Most Problematic Files:");
        for file in problematic_files {
            println!(
                "   • {}: {} total issues",
                file.file_path.display(),
                file.issues.len()
            );
        }
    }

    // Recommendations
    println!("\n💡 Recommendations:");
    if result.success {
        println!("   • Processing pipeline is working correctly");
        println!("   • Ready to proceed with production processing");
    } else {
        println!("   • Review critical issues before production use");
        println!("   • Consider adjusting quality control parameters");
        println!("   • Investigate files with low success rates");
    }

    println!("\n📁 Detailed results saved to validation output directory");
    println!();

    Ok(())
}

/// Generate JSON validation report
fn generate_json_validation_report(
    result: &crate::app::services::integration_test::IntegrationTestResult,
) -> Result<()> {
    let json_result = serde_json::to_string_pretty(result).map_err(|e| {
        Error::configuration(format!("Failed to serialize validation result: {}", e))
    })?;

    println!("{}", json_result);
    Ok(())
}

/// Generate CSV validation report
fn generate_csv_validation_report(
    result: &crate::app::services::integration_test::IntegrationTestResult,
) -> Result<()> {
    println!("metric,value");
    println!("overall_success,{}", result.success);
    println!("files_processed,{}", result.files_processed);
    println!("files_failed,{}", result.files_failed);
    println!(
        "file_success_rate_percent,{:.2}",
        result.file_success_rate()
    );
    println!("total_records,{}", result.total_records);
    println!("successful_records,{}", result.successful_records);
    println!("record_success_rate_percent,{:.2}", result.success_rate());
    println!(
        "processing_time_seconds,{:.2}",
        result.total_processing_time_seconds
    );

    if let Some(memory) = result.peak_memory_mb {
        println!("peak_memory_mb,{:.2}", memory);
    }

    // Issue counts
    for (issue_type, count) in &result.detailed_stats.issue_counts {
        println!("issue_count_{:?},{}", issue_type, count);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    // Simple unit tests for basic functionality without complex mocking
    #[test]
    fn test_module_compiles() {
        // Basic test to ensure the module compiles correctly
        assert!(true);
    }
}
