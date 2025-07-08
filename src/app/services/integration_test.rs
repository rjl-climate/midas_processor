//! Integration test framework for processing pipeline validation
//!
//! This module provides a comprehensive testing framework that processes real MIDAS
//! data from cache and generates detailed logs for analysis. It's designed to help
//! identify issues in the processing pipeline using actual data complexity.

use crate::Result;
use crate::app::services::badc_csv_parser::BadcCsvParser;
use crate::app::services::cache_scanner::{CacheScanConfig, CacheScanner};
use crate::app::services::record_processor::detailed_stats::{IssueSeverity, IssueType};
use crate::app::services::record_processor::{
    DetailedProcessingStats, FileProcessingStats, ProcessingIssueBuilder, RecordProcessor,
};
use crate::app::services::station_registry::StationRegistry;
use crate::config::QualityControlConfig;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info};

/// Configuration for integration testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationTestConfig {
    /// Path to the MIDAS data cache
    pub cache_path: PathBuf,
    /// Maximum number of files to process
    pub max_files: Option<usize>,
    /// Specific datasets to test (empty = all)
    pub datasets: Vec<String>,
    /// Output directory for test results
    pub output_dir: PathBuf,
    /// Whether to continue processing after errors
    pub continue_on_error: bool,
    /// Maximum processing time per file in seconds
    pub max_processing_time_per_file: u64,
    /// Memory limit per file in MB
    pub max_memory_per_file: usize,
    /// Minimum file size to include in bytes
    pub min_file_size: u64,
    /// Quality control configuration
    pub quality_control: QualityControlConfig,
}

impl Default for IntegrationTestConfig {
    fn default() -> Self {
        Self {
            cache_path: PathBuf::from("./cache"),
            max_files: Some(1000),
            datasets: Vec::new(),
            output_dir: PathBuf::from("./validation_output"),
            continue_on_error: true,
            max_processing_time_per_file: 300, // 5 minutes
            max_memory_per_file: 1024,         // 1GB
            min_file_size: 1024,               // 1KB
            quality_control: QualityControlConfig::default(),
        }
    }
}

/// Result of integration testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrationTestResult {
    /// Overall test success status
    pub success: bool,
    /// Total files processed
    pub files_processed: usize,
    /// Total files that failed processing
    pub files_failed: usize,
    /// Total records processed across all files
    pub total_records: usize,
    /// Total records that were successfully processed
    pub successful_records: usize,
    /// Total processing time in seconds
    pub total_processing_time_seconds: f64,
    /// Peak memory usage during testing
    pub peak_memory_mb: Option<f64>,
    /// Detailed processing statistics
    pub detailed_stats: DetailedProcessingStats,
    /// Summary of key findings
    pub summary: String,
}

impl IntegrationTestResult {
    /// Get overall success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_records == 0 {
            100.0
        } else {
            (self.successful_records as f64 / self.total_records as f64) * 100.0
        }
    }

    /// Get file processing success rate (based on actual file problems, not empty files)
    pub fn file_success_rate(&self) -> f64 {
        self.detailed_stats.file_success_rate()
    }
}

/// Integration test framework for processing pipeline validation
pub struct IntegrationTestFramework {
    config: IntegrationTestConfig,
    // Station registries are now loaded per-dataset as needed
    station_registries: std::collections::HashMap<String, Arc<StationRegistry>>,
}

impl IntegrationTestFramework {
    /// Create new integration test framework
    pub async fn new(config: IntegrationTestConfig) -> Result<Self> {
        info!("Initializing integration test framework");

        // Create output directory
        if !config.output_dir.exists() {
            std::fs::create_dir_all(&config.output_dir)?;
            info!("Created output directory: {}", config.output_dir.display());
        }

        // Station registries will be loaded on-demand per dataset
        let station_registries = std::collections::HashMap::new();

        Ok(Self {
            config,
            station_registries,
        })
    }

    /// Get or load station registry for a specific dataset
    async fn get_registry_for_dataset(&mut self, dataset: &str) -> Result<Arc<StationRegistry>> {
        // Check if we already have this registry loaded
        if let Some(registry) = self.station_registries.get(dataset) {
            return Ok(registry.clone());
        }

        // Load the registry for this dataset
        info!("Loading station registry for dataset: {}", dataset);
        let (registry, load_stats) = StationRegistry::load_for_dataset(
            &self.config.cache_path,
            dataset,
            false, // Don't show progress in automated tests
        )
        .await?;

        info!(
            "Station registry loaded for '{}': {} stations from {} files in {:.2}s",
            dataset,
            load_stats.stations_loaded,
            load_stats.files_processed,
            load_stats.load_duration.as_secs_f64()
        );

        let registry_arc = Arc::new(registry);
        self.station_registries
            .insert(dataset.to_string(), registry_arc.clone());

        Ok(registry_arc)
    }

    /// Run integration tests on real MIDAS data
    pub async fn run_tests(&mut self) -> Result<IntegrationTestResult> {
        info!("Starting integration tests");
        let start_time = Instant::now();

        // Discover files to process
        let files = self.discover_test_files().await?;
        info!("Discovered {} files for testing", files.len());

        // Initialize result tracking
        let mut detailed_stats = DetailedProcessingStats::new();
        let mut files_processed = 0;
        let mut files_failed = 0;
        let mut total_records = 0;
        let mut successful_records = 0;
        let mut peak_memory_mb = None;

        // Process each file
        for (i, file_info) in files.iter().enumerate() {
            info!(
                "Processing file {}/{}: {}",
                i + 1,
                files.len(),
                file_info.path.display()
            );

            match self.process_single_file_with_dataset(file_info).await {
                Ok(file_stats) => {
                    files_processed += 1;
                    total_records += file_stats.total_records;
                    successful_records += file_stats.quality_passed_records;

                    if let Some(memory) = file_stats.peak_memory_bytes {
                        let memory_mb = memory as f64 / 1024.0 / 1024.0;
                        peak_memory_mb = Some(peak_memory_mb.unwrap_or(0.0_f64).max(memory_mb));
                    }

                    detailed_stats.add_file_stats(file_stats);
                }
                Err(e) => {
                    files_failed += 1;
                    error!("Failed to process file {}: {}", file_info.path.display(), e);

                    // Create error file stats
                    let mut error_stats = FileProcessingStats::new(file_info.path.clone());
                    let issue = ProcessingIssueBuilder::new(
                        IssueType::ParseError,
                        IssueSeverity::Critical,
                        format!("File processing failed: {}", e),
                    )
                    .with_file_path(file_info.path.clone())
                    .build();
                    error_stats.add_issue(issue);
                    detailed_stats.add_file_stats(error_stats);

                    if !self.config.continue_on_error {
                        break;
                    }
                }
            }

            // Log progress periodically
            if (i + 1) % 50 == 0 {
                info!("Progress: {}/{} files processed", i + 1, files.len());
            }
        }

        let total_time = start_time.elapsed();
        let total_processing_time_seconds = total_time.as_secs_f64();

        // Generate summary
        let summary = self.generate_test_summary(
            files_processed,
            files_failed,
            total_records,
            successful_records,
            total_processing_time_seconds,
            &detailed_stats,
        );

        let result = IntegrationTestResult {
            success: files_failed == 0
                && detailed_stats
                    .severity_counts
                    .get(&IssueSeverity::Critical)
                    .unwrap_or(&0)
                    == &0,
            files_processed,
            files_failed,
            total_records,
            successful_records,
            total_processing_time_seconds,
            peak_memory_mb,
            detailed_stats,
            summary,
        };

        info!(
            "Integration tests completed in {:.1}s",
            total_processing_time_seconds
        );
        info!("Test summary: {}", result.summary);

        Ok(result)
    }

    /// Discover files to test from cache
    async fn discover_test_files(
        &self,
    ) -> Result<Vec<crate::app::services::cache_scanner::CacheFileInfo>> {
        let scan_config = CacheScanConfig {
            max_files: self.config.max_files,
            datasets: self.config.datasets.clone(),
            min_file_size: self.config.min_file_size,
            include_capability: true, // Need both for testing
            ..Default::default()
        };

        let scanner = CacheScanner::with_config(scan_config);
        let files = scanner.scan_cache(&self.config.cache_path)?;

        info!("Cache scan completed: {} files discovered", files.len());

        // Log dataset distribution
        let mut dataset_counts = std::collections::HashMap::new();
        for file in &files {
            *dataset_counts.entry(file.dataset.clone()).or_insert(0) += 1;
        }

        for (dataset, count) in dataset_counts {
            info!("  {}: {} files", dataset, count);
        }

        Ok(files)
    }

    /// Process a single file with dataset-specific station registry
    async fn process_single_file_with_dataset(
        &mut self,
        file_info: &crate::app::services::cache_scanner::CacheFileInfo,
    ) -> Result<FileProcessingStats> {
        let start_time = Instant::now();
        let mut file_stats = FileProcessingStats::new(file_info.path.clone());

        // Handle capability files separately - they don't need observation processing
        if file_info.is_capability {
            debug!("Processing capability file: {}", file_info.path.display());
            file_stats.update_file_type();
            return Ok(file_stats);
        }

        // Get or load the station registry for this file's dataset
        let station_registry = self.get_registry_for_dataset(&file_info.dataset).await?;

        // Create a parser for this file (parser doesn't need station registry)
        let parser = BadcCsvParser::new(station_registry.clone());

        // Parse CSV file
        let parse_result = match parser.parse_file(&file_info.path).await {
            Ok(result) => result,
            Err(e) => {
                let issue = ProcessingIssueBuilder::new(
                    IssueType::ParseError,
                    IssueSeverity::Critical,
                    format!("CSV parsing failed: {}", e),
                )
                .with_file_path(file_info.path.clone())
                .build();
                file_stats.add_issue(issue);
                return Ok(file_stats);
            }
        };

        file_stats.total_records = parse_result.stats.total_records;
        file_stats.parsed_records = parse_result.stats.observations_parsed;
        file_stats.failed_records =
            parse_result.stats.total_records - parse_result.stats.observations_parsed;

        // Add parsing issues
        for error in &parse_result.stats.errors {
            let issue = ProcessingIssueBuilder::new(
                IssueType::ParseError,
                IssueSeverity::Warning,
                error.clone(),
            )
            .with_file_path(file_info.path.clone())
            .build();
            file_stats.add_issue(issue);
        }

        // Create a processor for this dataset with the appropriate station registry
        let processor = RecordProcessor::new(station_registry, self.config.quality_control.clone());

        // Process observations
        let processing_result = match processor
            .process_observations(parse_result.observations, false)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                let issue = ProcessingIssueBuilder::new(
                    IssueType::Configuration,
                    IssueSeverity::Critical,
                    format!("Processing failed: {}", e),
                )
                .with_file_path(file_info.path.clone())
                .build();
                file_stats.add_issue(issue);
                return Ok(file_stats);
            }
        };

        file_stats.enriched_records = processing_result.stats.enriched;
        file_stats.deduplicated_records = processing_result.stats.deduplicated;
        file_stats.quality_passed_records = processing_result.stats.final_output;

        // Add processing issues
        for error in &processing_result.stats.error_messages {
            let issue = ProcessingIssueBuilder::new(
                IssueType::DataConsistency,
                IssueSeverity::Warning,
                error.clone(),
            )
            .with_file_path(file_info.path.clone())
            .build();
            file_stats.add_issue(issue);
        }

        // Record processing time
        file_stats.processing_time_ms = start_time.elapsed().as_millis() as u64;

        // Update file type based on processing results
        file_stats.update_file_type();

        // Check for performance issues
        if file_stats.processing_time_ms > self.config.max_processing_time_per_file * 1000 {
            let issue = ProcessingIssueBuilder::new(
                IssueType::Performance,
                IssueSeverity::Warning,
                format!(
                    "File took {:.1}s to process (limit: {}s)",
                    file_stats.processing_time_ms as f64 / 1000.0,
                    self.config.max_processing_time_per_file
                ),
            )
            .with_file_path(file_info.path.clone())
            .build();
            file_stats.add_issue(issue);
        }

        Ok(file_stats)
    }

    /// Generate test summary
    fn generate_test_summary(
        &self,
        files_processed: usize,
        files_failed: usize,
        total_records: usize,
        successful_records: usize,
        processing_time: f64,
        detailed_stats: &DetailedProcessingStats,
    ) -> String {
        let mut summary = String::new();

        summary.push_str(&format!(
            "Integration Test Results: {} files processed, {} failed ({:.1}% file success rate)\n",
            files_processed,
            files_failed,
            if files_processed > 0 {
                ((files_processed - files_failed) as f64 / files_processed as f64) * 100.0
            } else {
                100.0
            }
        ));

        summary.push_str(&format!(
            "Records: {} total, {} successful ({:.1}% success rate)\n",
            total_records,
            successful_records,
            if total_records > 0 {
                (successful_records as f64 / total_records as f64) * 100.0
            } else {
                100.0
            }
        ));

        summary.push_str(&format!(
            "Processing time: {:.1}s ({:.1} files/sec)\n",
            processing_time,
            if processing_time > 0.0 {
                files_processed as f64 / processing_time
            } else {
                0.0
            }
        ));

        // Add issue summary
        if !detailed_stats.issue_counts.is_empty() {
            summary.push_str("\nIssue Summary:\n");
            for (issue_type, count) in &detailed_stats.issue_counts {
                summary.push_str(&format!("  {:?}: {}\n", issue_type, count));
            }
        }

        // Add top problematic files
        let problematic_files = detailed_stats.most_problematic_files(5);
        if !problematic_files.is_empty() {
            summary.push_str("\nMost Problematic Files:\n");
            for file in problematic_files {
                summary.push_str(&format!(
                    "  {}: {} issues\n",
                    file.file_path.display(),
                    file.issues.len()
                ));
            }
        }

        summary
    }

    /// Save test results to output directory
    pub async fn save_results(&self, result: &IntegrationTestResult) -> Result<()> {
        info!(
            "Saving test results to {}",
            self.config.output_dir.display()
        );

        // Save detailed statistics as JSON
        let stats_file = self.config.output_dir.join("detailed_stats.json");
        let stats_json = serde_json::to_string_pretty(&result.detailed_stats).map_err(|e| {
            crate::Error::configuration(format!("Failed to serialize stats: {}", e))
        })?;
        std::fs::write(&stats_file, stats_json)?;
        info!("Saved detailed statistics to {}", stats_file.display());

        // Save summary report as markdown
        let summary_file = self.config.output_dir.join("test_summary.md");
        let summary_content = self.generate_markdown_report(result)?;
        std::fs::write(&summary_file, summary_content)?;
        info!("Saved summary report to {}", summary_file.display());

        // Save issues as CSV for analysis
        let issues_file = self.config.output_dir.join("issues.csv");
        self.save_issues_csv(&result.detailed_stats, &issues_file)?;
        info!("Saved issues CSV to {}", issues_file.display());

        Ok(())
    }

    /// Generate markdown report
    fn generate_markdown_report(&self, result: &IntegrationTestResult) -> Result<String> {
        let mut report = String::new();

        report.push_str("# MIDAS Processing Pipeline Integration Test Report\n\n");
        report.push_str(&format!(
            "Generated: {}\n\n",
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        ));

        report.push_str("## Executive Summary\n\n");
        report.push_str(&format!(
            "- **Overall Success**: {}\n",
            if result.success {
                "✅ PASS"
            } else {
                "❌ FAIL"
            }
        ));
        report.push_str(&format!(
            "- **Files Processed**: {} ({} failed)\n",
            result.files_processed, result.files_failed
        ));
        report.push_str(&format!(
            "- **File Success Rate**: {:.1}%\n",
            result.file_success_rate()
        ));
        report.push_str(&format!(
            "- **Records Processed**: {} ({:.1}% success rate)\n",
            result.total_records,
            result.success_rate()
        ));
        report.push_str(&format!(
            "- **Processing Time**: {:.1}s\n",
            result.total_processing_time_seconds
        ));
        if let Some(memory) = result.peak_memory_mb {
            report.push_str(&format!("- **Peak Memory Usage**: {:.1}MB\n", memory));
        }
        report.push('\n');

        // Add file type breakdown
        let breakdown = result.detailed_stats.file_type_breakdown();
        if !breakdown.is_empty() {
            report.push_str("## File Type Breakdown\n\n");
            report.push_str("| File Type | Count |\n");
            report.push_str("|-----------|-------|\n");
            for (file_type, count) in &breakdown {
                report.push_str(&format!("| {} | {} |\n", file_type.description(), count));
            }
            report.push('\n');
        }

        report.push_str("## Issue Analysis\n\n");
        if result.detailed_stats.issue_counts.is_empty() {
            report.push_str("No issues detected during processing.\n\n");
        } else {
            report.push_str("| Issue Type | Count |\n");
            report.push_str("|------------|-------|\n");
            for (issue_type, count) in &result.detailed_stats.issue_counts {
                report.push_str(&format!("| {:?} | {} |\n", issue_type, count));
            }
            report.push('\n');
        }

        report.push_str("## File Processing Results\n\n");
        let problematic_files = result.detailed_stats.most_problematic_files(10);
        if problematic_files.is_empty() {
            report.push_str("No problematic files identified.\n\n");
        } else {
            report.push_str("| File | Type | Status | Issues |\n");
            report.push_str("|------|------|--------|--------|\n");
            for file in problematic_files {
                report.push_str(&format!(
                    "| {} | {} | {} | {} |\n",
                    file.file_path.display(),
                    file.file_type.description(),
                    file.validation_status(),
                    file.issues.len()
                ));
            }
            report.push('\n');
        }

        report.push_str("## Dataset Performance\n\n");
        if result.detailed_stats.dataset_issues.is_empty() {
            report.push_str("No dataset-specific issues identified.\n\n");
        } else {
            report.push_str("| Dataset | Issues |\n");
            report.push_str("|---------|--------|\n");
            for (dataset, issues) in &result.detailed_stats.dataset_issues {
                report.push_str(&format!("| {} | {} |\n", dataset, issues.len()));
            }
            report.push('\n');
        }

        report.push_str("## Recommendations\n\n");
        if result.success {
            report.push_str("- Processing pipeline is working correctly\n");
            report.push_str("- Ready to proceed with production processing\n");
        } else {
            report.push_str("- Review critical issues before production use\n");
            report.push_str("- Consider adjusting quality control parameters\n");
            report.push_str("- Investigate files with low success rates\n");
        }

        Ok(report)
    }

    /// Save issues as CSV for analysis
    fn save_issues_csv(&self, stats: &DetailedProcessingStats, file_path: &Path) -> Result<()> {
        let mut csv_content = String::new();
        csv_content.push_str("file_path,issue_type,severity,description,station_id,timestamp\n");

        for file_stats in &stats.file_stats {
            for issue in &file_stats.issues {
                csv_content.push_str(&format!(
                    "{},{:?},{:?},{},{},{}\n",
                    file_stats.file_path.display(),
                    issue.issue_type,
                    issue.severity,
                    issue.description.replace(',', ";"), // Escape commas
                    issue
                        .station_id
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "".to_string()),
                    issue.timestamp.format("%Y-%m-%d %H:%M:%S")
                ));
            }
        }

        std::fs::write(file_path, csv_content)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integration_test_config() {
        let config = IntegrationTestConfig::default();
        assert_eq!(config.max_files, Some(1000));
        assert!(config.continue_on_error);
        assert_eq!(config.min_file_size, 1024);
    }

    #[test]
    fn test_integration_test_result() {
        // Create detailed stats with file statistics that match the expected rates
        let mut detailed_stats = DetailedProcessingStats::new();

        // Add 100 files total, with 5 problematic files to achieve 95% file success rate
        for i in 0..95 {
            let mut file_stats =
                FileProcessingStats::new(PathBuf::from(format!("good_file_{}.csv", i)));
            file_stats.total_records = 100;
            file_stats.quality_passed_records = 100; // All good files are fully successful
            file_stats.update_file_type(); // Ensure correct file type detection
            detailed_stats.add_file_stats(file_stats);
        }

        for i in 0..5 {
            let mut file_stats =
                FileProcessingStats::new(PathBuf::from(format!("problematic_file_{}.csv", i)));
            file_stats.total_records = 100;
            file_stats.quality_passed_records = 50; // Problematic files have issues (50% success rate < 90% threshold)
            file_stats.update_file_type(); // Ensure correct file type detection
            // Add a critical issue to make it even more clearly problematic
            let issue = ProcessingIssueBuilder::new(
                IssueType::ParseError,
                IssueSeverity::Critical,
                "Test critical error".to_string(),
            )
            .build();
            file_stats.add_issue(issue);
            detailed_stats.add_file_stats(file_stats);
        }

        let result = IntegrationTestResult {
            success: true,
            files_processed: 100,
            files_failed: 5,
            total_records: 10000,
            successful_records: 9500,
            total_processing_time_seconds: 60.0,
            peak_memory_mb: Some(512.0),
            detailed_stats,
            summary: "Test summary".to_string(),
        };

        assert_eq!(result.success_rate(), 95.0);
        assert_eq!(result.file_success_rate(), 95.0);
    }
}
