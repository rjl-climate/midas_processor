//! Detailed processing statistics with granular issue tracking
//!
//! This module extends the basic ProcessingStats to provide comprehensive
//! tracking of issues encountered during the processing pipeline. It's designed
//! to help identify patterns and systematically address data quality problems.

use crate::app::models::QualityFlag;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Detailed error information for a specific processing issue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingIssue {
    /// Type of issue encountered
    pub issue_type: IssueType,
    /// Severity level of the issue
    pub severity: IssueSeverity,
    /// Human-readable description of the issue
    pub description: String,
    /// File path where the issue occurred
    pub file_path: Option<PathBuf>,
    /// Station ID associated with the issue (if applicable)
    pub station_id: Option<i32>,
    /// Timestamp when the issue was detected
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Additional context information
    pub context: HashMap<String, String>,
}

/// Types of issues that can be encountered during processing
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IssueType {
    /// CSV parsing errors
    ParseError,
    /// Invalid or malformed date/time values
    InvalidDateTime,
    /// Invalid numeric values or out-of-range measurements
    InvalidNumeric,
    /// Missing required fields
    MissingField,
    /// Station metadata enrichment failure
    StationEnrichmentFailed,
    /// Quality control flag interpretation issues
    QualityFlagIssue,
    /// Deduplication conflicts or unexpected patterns
    DeduplicationIssue,
    /// Data consistency problems
    DataConsistency,
    /// Performance or memory issues
    Performance,
    /// Configuration or setup issues
    Configuration,
}

/// Severity levels for processing issues
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum IssueSeverity {
    /// Low severity - informational, doesn't affect processing
    Info,
    /// Medium severity - warning, may affect data quality
    Warning,
    /// High severity - error, affects processing results
    Error,
    /// Critical severity - fatal error, stops processing
    Critical,
}

/// Types of MIDAS files for validation classification
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FileType {
    /// Capability files containing station metadata (no observation data expected)
    Capability,
    /// Observation data files with actual measurement records
    ObservationData,
    /// Files with headers but no observation data (not a processing failure)
    EmptyData,
    /// Station metadata files (centralized metadata, no observation data)
    StationMetadata,
}

impl FileType {
    /// Detect file type from path and content characteristics
    pub fn detect(
        file_path: &std::path::Path,
        total_records: usize,
        has_observation_data: bool,
    ) -> Self {
        let filename = file_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");

        if filename.contains("capability") {
            FileType::Capability
        } else if filename.contains("station-metadata") {
            FileType::StationMetadata
        } else if total_records == 0 || !has_observation_data {
            FileType::EmptyData
        } else {
            FileType::ObservationData
        }
    }

    /// Check if this file type should contribute to data processing success rates
    pub fn should_count_for_data_processing(&self) -> bool {
        matches!(self, FileType::ObservationData)
    }

    /// Get a human-readable description of the file type
    pub fn description(&self) -> &'static str {
        match self {
            FileType::Capability => "Station capability file",
            FileType::ObservationData => "Observation data file",
            FileType::EmptyData => "Empty data file",
            FileType::StationMetadata => "Station metadata file",
        }
    }
}

/// Statistics for a specific file processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileProcessingStats {
    /// Path to the processed file
    pub file_path: PathBuf,
    /// Type of file for validation classification
    pub file_type: FileType,
    /// Total records in the file
    pub total_records: usize,
    /// Successfully parsed records
    pub parsed_records: usize,
    /// Records that failed parsing
    pub failed_records: usize,
    /// Records successfully enriched with station metadata
    pub enriched_records: usize,
    /// Records after deduplication
    pub deduplicated_records: usize,
    /// Records that passed quality filtering
    pub quality_passed_records: usize,
    /// Processing time in milliseconds
    pub processing_time_ms: u64,
    /// Memory usage peak during processing (bytes)
    pub peak_memory_bytes: Option<usize>,
    /// Issues encountered during processing
    pub issues: Vec<ProcessingIssue>,
}

impl FileProcessingStats {
    /// Create new file processing statistics
    pub fn new(file_path: PathBuf) -> Self {
        // Detect file type based on path (will be updated later with content info)
        let file_type = FileType::detect(&file_path, 0, false);

        Self {
            file_path,
            file_type,
            total_records: 0,
            parsed_records: 0,
            failed_records: 0,
            enriched_records: 0,
            deduplicated_records: 0,
            quality_passed_records: 0,
            processing_time_ms: 0,
            peak_memory_bytes: None,
            issues: Vec::new(),
        }
    }

    /// Add an issue to the statistics
    pub fn add_issue(&mut self, issue: ProcessingIssue) {
        self.issues.push(issue);
    }

    /// Update file type based on processing results
    pub fn update_file_type(&mut self) {
        // Re-detect file type with actual processing results
        let has_observation_data = self.parsed_records > 0 || self.quality_passed_records > 0;
        self.file_type =
            FileType::detect(&self.file_path, self.total_records, has_observation_data);
    }

    /// Get issues by severity level
    pub fn issues_by_severity(&self, severity: IssueSeverity) -> Vec<&ProcessingIssue> {
        self.issues
            .iter()
            .filter(|i| i.severity == severity)
            .collect()
    }

    /// Get issues by type
    pub fn issues_by_type(&self, issue_type: IssueType) -> Vec<&ProcessingIssue> {
        self.issues
            .iter()
            .filter(|i| i.issue_type == issue_type)
            .collect()
    }

    /// Calculate success rate for this file based on file type
    pub fn success_rate(&self) -> f64 {
        match self.file_type {
            FileType::Capability | FileType::StationMetadata => {
                // Capability and metadata files are successful if no critical issues
                if self.has_critical_issues() {
                    0.0
                } else {
                    100.0
                }
            }
            FileType::EmptyData => {
                // Empty data files are not failures - they just have no data
                100.0
            }
            FileType::ObservationData => {
                // Only observation data files use traditional success rate calculation
                if self.total_records == 0 {
                    100.0
                } else {
                    (self.quality_passed_records as f64 / self.total_records as f64) * 100.0
                }
            }
        }
    }

    /// Check if processing was successful (>90% success rate, no critical issues)
    pub fn is_successful(&self) -> bool {
        self.success_rate() > 90.0 && !self.has_critical_issues()
    }

    /// Check if there are any critical issues
    pub fn has_critical_issues(&self) -> bool {
        self.issues
            .iter()
            .any(|i| i.severity == IssueSeverity::Critical)
    }

    /// Check if this file should be included in data processing metrics
    pub fn counts_for_data_processing(&self) -> bool {
        self.file_type.should_count_for_data_processing()
    }

    /// Check if this file represents a validation problem (not just empty data)
    pub fn is_problematic(&self) -> bool {
        match self.file_type {
            FileType::ObservationData => self.success_rate() < 90.0 || self.has_critical_issues(),
            FileType::Capability | FileType::StationMetadata => self.has_critical_issues(),
            FileType::EmptyData => false, // Empty data is not problematic
        }
    }

    /// Get validation status description
    pub fn validation_status(&self) -> String {
        match self.file_type {
            FileType::EmptyData => "No Data".to_string(),
            FileType::Capability | FileType::StationMetadata => {
                if self.has_critical_issues() {
                    "Failed".to_string()
                } else {
                    "OK".to_string()
                }
            }
            FileType::ObservationData => {
                if self.has_critical_issues() {
                    "Failed".to_string()
                } else {
                    format!("{:.1}%", self.success_rate())
                }
            }
        }
    }

    /// Get summary string for this file
    pub fn summary(&self) -> String {
        format!(
            "{} ({}): {} -> {} records ({}) | {} issues",
            self.file_path.display(),
            self.file_type.description(),
            self.total_records,
            self.quality_passed_records,
            self.validation_status(),
            self.issues.len()
        )
    }
}

/// Detailed processing statistics with issue tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailedProcessingStats {
    /// Statistics for each processed file
    pub file_stats: Vec<FileProcessingStats>,
    /// Global issue counters by type
    pub issue_counts: HashMap<IssueType, usize>,
    /// Global issue counters by severity
    pub severity_counts: HashMap<IssueSeverity, usize>,
    /// Station-specific issue tracking
    pub station_issues: HashMap<i32, Vec<ProcessingIssue>>,
    /// Dataset-specific issue tracking
    pub dataset_issues: HashMap<String, Vec<ProcessingIssue>>,
    /// Quality flag distribution analysis
    pub quality_flag_distribution: HashMap<String, HashMap<QualityFlag, usize>>,
    /// Temporal coverage analysis
    pub temporal_coverage: HashMap<u32, usize>, // year -> record count
    /// Processing performance metrics
    pub total_processing_time_ms: u64,
    /// Peak memory usage across all files
    pub peak_memory_bytes: Option<usize>,
}

impl DetailedProcessingStats {
    /// Create new detailed processing statistics
    pub fn new() -> Self {
        Self {
            file_stats: Vec::new(),
            issue_counts: HashMap::new(),
            severity_counts: HashMap::new(),
            station_issues: HashMap::new(),
            dataset_issues: HashMap::new(),
            quality_flag_distribution: HashMap::new(),
            temporal_coverage: HashMap::new(),
            total_processing_time_ms: 0,
            peak_memory_bytes: None,
        }
    }

    /// Add file processing statistics
    pub fn add_file_stats(&mut self, file_stats: FileProcessingStats) {
        // Update global counters
        for issue in &file_stats.issues {
            *self
                .issue_counts
                .entry(issue.issue_type.clone())
                .or_insert(0) += 1;
            *self
                .severity_counts
                .entry(issue.severity.clone())
                .or_insert(0) += 1;

            // Track station-specific issues
            if let Some(station_id) = issue.station_id {
                self.station_issues
                    .entry(station_id)
                    .or_default()
                    .push(issue.clone());
            }

            // Track dataset-specific issues (extract from file path)
            if let Some(dataset) = self.extract_dataset_from_path(&file_stats.file_path) {
                self.dataset_issues
                    .entry(dataset)
                    .or_default()
                    .push(issue.clone());
            }
        }

        // Update timing
        self.total_processing_time_ms += file_stats.processing_time_ms;

        // Update peak memory
        if let Some(file_memory) = file_stats.peak_memory_bytes {
            self.peak_memory_bytes = Some(self.peak_memory_bytes.unwrap_or(0).max(file_memory));
        }

        self.file_stats.push(file_stats);
    }

    /// Add quality flag observation for distribution analysis
    pub fn add_quality_flag_observation(&mut self, measurement: &str, flag: QualityFlag) {
        *self
            .quality_flag_distribution
            .entry(measurement.to_string())
            .or_default()
            .entry(flag)
            .or_insert(0) += 1;
    }

    /// Add temporal coverage observation
    pub fn add_temporal_observation(&mut self, year: u32) {
        *self.temporal_coverage.entry(year).or_insert(0) += 1;
    }

    /// Extract dataset name from file path
    fn extract_dataset_from_path(&self, path: &Path) -> Option<String> {
        let path_str = path.to_string_lossy();
        let components: Vec<&str> = path_str.split('/').collect();

        for component in components {
            if component.ends_with("-obs") || component.ends_with("-weather") {
                return Some(component.to_string());
            }
        }
        None
    }

    /// Get total number of files processed
    pub fn total_files(&self) -> usize {
        self.file_stats.len()
    }

    /// Get total number of records processed across all files
    pub fn total_records(&self) -> usize {
        self.file_stats.iter().map(|f| f.total_records).sum()
    }

    /// Get total number of successfully processed records
    pub fn total_successful_records(&self) -> usize {
        self.file_stats
            .iter()
            .map(|f| f.quality_passed_records)
            .sum()
    }

    /// Calculate overall success rate
    pub fn overall_success_rate(&self) -> f64 {
        let total = self.total_records();
        if total == 0 {
            100.0
        } else {
            (self.total_successful_records() as f64 / total as f64) * 100.0
        }
    }

    /// Calculate file-level success rate (percentage of files without problems)
    pub fn file_success_rate(&self) -> f64 {
        if self.file_stats.is_empty() {
            100.0
        } else {
            let problematic_files = self
                .file_stats
                .iter()
                .filter(|f| f.is_problematic())
                .count();
            let successful_files = self.file_stats.len() - problematic_files;
            (successful_files as f64 / self.file_stats.len() as f64) * 100.0
        }
    }

    /// Get breakdown by file type
    pub fn file_type_breakdown(&self) -> std::collections::HashMap<FileType, usize> {
        let mut breakdown = std::collections::HashMap::new();
        for file_stats in &self.file_stats {
            *breakdown.entry(file_stats.file_type.clone()).or_insert(0) += 1;
        }
        breakdown
    }

    /// Get files with critical issues
    pub fn files_with_critical_issues(&self) -> Vec<&FileProcessingStats> {
        self.file_stats
            .iter()
            .filter(|f| f.has_critical_issues())
            .collect()
    }

    /// Get files with low success rates
    pub fn files_with_low_success_rate(&self, threshold: f64) -> Vec<&FileProcessingStats> {
        self.file_stats
            .iter()
            .filter(|f| f.success_rate() < threshold)
            .collect()
    }

    /// Get top N most problematic files by issue count
    pub fn most_problematic_files(&self, n: usize) -> Vec<&FileProcessingStats> {
        let mut files = self.file_stats.iter().collect::<Vec<_>>();
        files.sort_by(|a, b| b.issues.len().cmp(&a.issues.len()));
        files.into_iter().take(n).collect()
    }

    /// Get stations with the most issues
    pub fn most_problematic_stations(&self, n: usize) -> Vec<(i32, usize)> {
        let mut stations: Vec<(i32, usize)> = self
            .station_issues
            .iter()
            .map(|(id, issues)| (*id, issues.len()))
            .collect();
        stations.sort_by(|a, b| b.1.cmp(&a.1));
        stations.into_iter().take(n).collect()
    }

    /// Generate comprehensive summary report
    pub fn generate_summary(&self) -> String {
        let mut summary = String::new();

        summary.push_str(&format!(
            "Processing Summary: {} files, {} records total, {} successful ({:.1}% success rate)\n",
            self.total_files(),
            self.total_records(),
            self.total_successful_records(),
            self.overall_success_rate()
        ));

        summary.push_str(&format!(
            "File-level success rate: {:.1}%\n",
            self.file_success_rate()
        ));

        // Add file type breakdown
        let breakdown = self.file_type_breakdown();
        if !breakdown.is_empty() {
            summary.push_str("File type breakdown:\n");
            for (file_type, count) in &breakdown {
                summary.push_str(&format!("  {}: {} files\n", file_type.description(), count));
            }
        }

        summary.push_str(&format!(
            "Processing time: {:.1}s",
            self.total_processing_time_ms as f64 / 1000.0
        ));

        if let Some(memory) = self.peak_memory_bytes {
            summary.push_str(&format!(
                ", Peak memory: {:.1}MB",
                memory as f64 / 1024.0 / 1024.0
            ));
        }
        summary.push('\n');

        // Issue summary
        summary.push_str("\nIssue Summary:\n");
        for (issue_type, count) in &self.issue_counts {
            summary.push_str(&format!("  {:?}: {} issues\n", issue_type, count));
        }

        // Severity summary
        summary.push_str("\nSeverity Summary:\n");
        for (severity, count) in &self.severity_counts {
            summary.push_str(&format!("  {:?}: {} issues\n", severity, count));
        }

        // Critical issues
        let critical_files = self.files_with_critical_issues();
        if !critical_files.is_empty() {
            summary.push_str(&format!(
                "\nFiles with critical issues: {}\n",
                critical_files.len()
            ));
        }

        // Low success rate files
        let low_success_files = self.files_with_low_success_rate(80.0);
        if !low_success_files.is_empty() {
            summary.push_str(&format!(
                "Files with <80% success rate: {}\n",
                low_success_files.len()
            ));
        }

        summary
    }
}

impl Default for DetailedProcessingStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for creating processing issues
pub struct ProcessingIssueBuilder {
    issue_type: IssueType,
    severity: IssueSeverity,
    description: String,
    file_path: Option<PathBuf>,
    station_id: Option<i32>,
    context: HashMap<String, String>,
}

impl ProcessingIssueBuilder {
    /// Create new issue builder
    pub fn new(issue_type: IssueType, severity: IssueSeverity, description: String) -> Self {
        Self {
            issue_type,
            severity,
            description,
            file_path: None,
            station_id: None,
            context: HashMap::new(),
        }
    }

    /// Set file path for the issue
    pub fn with_file_path(mut self, path: PathBuf) -> Self {
        self.file_path = Some(path);
        self
    }

    /// Set station ID for the issue
    pub fn with_station_id(mut self, station_id: i32) -> Self {
        self.station_id = Some(station_id);
        self
    }

    /// Add context information
    pub fn with_context(mut self, key: String, value: String) -> Self {
        self.context.insert(key, value);
        self
    }

    /// Build the processing issue
    pub fn build(self) -> ProcessingIssue {
        ProcessingIssue {
            issue_type: self.issue_type,
            severity: self.severity,
            description: self.description,
            file_path: self.file_path,
            station_id: self.station_id,
            timestamp: chrono::Utc::now(),
            context: self.context,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_file_processing_stats() {
        let mut stats = FileProcessingStats::new(PathBuf::from("test.csv"));
        stats.total_records = 100;
        stats.quality_passed_records = 90;
        stats.update_file_type(); // Re-detect file type based on actual processing results

        assert_eq!(stats.success_rate(), 90.0);
        assert!(!stats.is_successful()); // 90% = boundary, need >90% for success
        assert!(!stats.has_critical_issues());

        // Add a critical issue
        let issue = ProcessingIssueBuilder::new(
            IssueType::ParseError,
            IssueSeverity::Critical,
            "Test critical error".to_string(),
        )
        .build();
        stats.add_issue(issue);

        assert!(!stats.is_successful());
        assert!(stats.has_critical_issues());
    }

    #[test]
    fn test_detailed_processing_stats() {
        let mut stats = DetailedProcessingStats::new();

        let mut file_stats = FileProcessingStats::new(PathBuf::from("test.csv"));
        file_stats.total_records = 100;
        file_stats.quality_passed_records = 90;

        let issue = ProcessingIssueBuilder::new(
            IssueType::ParseError,
            IssueSeverity::Warning,
            "Test warning".to_string(),
        )
        .with_station_id(12345)
        .build();
        file_stats.add_issue(issue);

        stats.add_file_stats(file_stats);

        assert_eq!(stats.total_files(), 1);
        assert_eq!(stats.total_records(), 100);
        assert_eq!(stats.total_successful_records(), 90);
        assert_eq!(stats.overall_success_rate(), 90.0);
        assert_eq!(stats.issue_counts.get(&IssueType::ParseError), Some(&1));
        assert_eq!(stats.severity_counts.get(&IssueSeverity::Warning), Some(&1));
        assert_eq!(stats.station_issues.get(&12345).unwrap().len(), 1);
    }

    #[test]
    fn test_issue_builder() {
        let issue = ProcessingIssueBuilder::new(
            IssueType::InvalidNumeric,
            IssueSeverity::Error,
            "Invalid temperature value".to_string(),
        )
        .with_file_path(PathBuf::from("data.csv"))
        .with_station_id(12345)
        .with_context("value".to_string(), "-999.9".to_string())
        .build();

        assert_eq!(issue.issue_type, IssueType::InvalidNumeric);
        assert_eq!(issue.severity, IssueSeverity::Error);
        assert_eq!(issue.description, "Invalid temperature value");
        assert_eq!(issue.file_path, Some(PathBuf::from("data.csv")));
        assert_eq!(issue.station_id, Some(12345));
        assert_eq!(issue.context.get("value"), Some(&"-999.9".to_string()));
    }
}
