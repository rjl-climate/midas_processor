//! Data models for audit results and reporting
//!
//! This module contains structures for representing audit findings, statistics,
//! and comprehensive reports of MIDAS cache integrity and data quality issues.

use crate::app::services::badc_csv_parser::ParseStats;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

// =============================================================================
// Audit Report Structure
// =============================================================================

/// Comprehensive audit report containing all findings and statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditReport {
    /// Overall audit summary with high-level statistics
    pub summary: AuditSummary,

    /// File-level issues found during structural checks
    pub file_issues: Vec<FileIssue>,

    /// Parsing-related issues found in BADC-CSV files
    pub parsing_issues: Vec<ParsingIssue>,

    /// Data consistency issues found during cross-validation
    pub data_issues: Vec<DataIssue>,

    /// Station metadata issues
    pub station_issues: Vec<StationIssue>,

    /// Cross-dataset consistency issues
    pub consistency_issues: Vec<ConsistencyIssue>,

    /// Performance metrics from the audit process
    pub performance_metrics: PerformanceMetrics,

    /// Timestamp when audit was completed
    pub audit_timestamp: DateTime<Utc>,

    /// Audit configuration used for this run
    pub audit_config: AuditConfig,
}

/// High-level audit summary statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditSummary {
    /// Total number of datasets audited
    pub datasets_audited: usize,

    /// Total number of files examined
    pub files_examined: usize,

    /// Number of files successfully parsed
    pub files_parsed_successfully: usize,

    /// Number of files with parsing errors
    pub files_with_parsing_errors: usize,

    /// Total number of issues found
    pub total_issues: usize,

    /// Issues categorized by severity
    pub issues_by_severity: HashMap<IssueSeverity, usize>,

    /// Issues categorized by type
    pub issues_by_type: HashMap<IssueType, usize>,

    /// Overall audit result
    pub overall_result: AuditResult,

    /// Total time taken for audit
    pub audit_duration: Duration,

    /// Cache health score (0-100)
    pub health_score: f32,
}

/// Overall audit result classification
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AuditResult {
    /// No issues found, cache is healthy
    Healthy,
    /// Minor issues found but cache is usable
    Warning,
    /// Significant issues found that may affect data quality
    Degraded,
    /// Critical issues found, cache requires attention
    Critical,
}

// =============================================================================
// Issue Types
// =============================================================================

/// File structure and access issues
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileIssue {
    /// Issue unique identifier
    pub id: String,

    /// Severity of the issue
    pub severity: IssueSeverity,

    /// Type of file issue
    pub issue_type: FileIssueType,

    /// Path to the problematic file
    pub file_path: PathBuf,

    /// Dataset this file belongs to
    pub dataset: String,

    /// Detailed description of the issue
    pub description: String,

    /// Suggested remediation steps
    pub remediation: Option<String>,

    /// Additional context or metadata
    pub metadata: HashMap<String, String>,
}

/// Types of file structure issues
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum FileIssueType {
    /// File is missing from expected location
    Missing,
    /// File exists but is not readable
    NotReadable,
    /// File has unexpected permissions
    PermissionDenied,
    /// File size is suspicious (too small/large)
    SuspiciousSize,
    /// File has unexpected naming convention
    NamingConvention,
    /// Orphaned file with no corresponding capability/observation pair
    Orphaned,
    /// File appears to be corrupted or truncated
    Corrupted,
}

/// BADC-CSV parsing issues
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsingIssue {
    /// Issue unique identifier
    pub id: String,

    /// Severity of the issue
    pub severity: IssueSeverity,

    /// Type of parsing issue
    pub issue_type: ParsingIssueType,

    /// Path to the problematic file
    pub file_path: PathBuf,

    /// Dataset this file belongs to
    pub dataset: String,

    /// Line number where issue occurred (if applicable)
    pub line_number: Option<usize>,

    /// Column name or position where issue occurred (if applicable)
    pub column: Option<String>,

    /// Detailed description of the parsing issue
    pub description: String,

    /// Raw content that caused the issue (if available)
    pub raw_content: Option<String>,

    /// Parse statistics for this file
    pub parse_stats: Option<ParseStats>,

    /// Additional context or metadata
    pub metadata: HashMap<String, String>,
}

/// Types of parsing issues
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ParsingIssueType {
    /// BADC header section is malformed or missing
    InvalidHeader,
    /// BADC data section marker not found
    MissingDataSection,
    /// Column definitions are inconsistent or invalid
    InvalidColumnDefinitions,
    /// CSV structure is malformed
    MalformedCsv,
    /// Data types don't match column definitions
    TypeMismatch,
    /// Required metadata fields are missing
    MissingMetadata,
    /// Character encoding issues
    EncodingError,
    /// File appears to be truncated
    Truncated,
    /// Data quality issues during parsing
    DataQualityIssue,
}

/// Data consistency and quality issues
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataIssue {
    /// Issue unique identifier
    pub id: String,

    /// Severity of the issue
    pub severity: IssueSeverity,

    /// Type of data issue
    pub issue_type: DataIssueType,

    /// Primary file path involved
    pub file_path: PathBuf,

    /// Related file path (for cross-file issues)
    pub related_file_path: Option<PathBuf>,

    /// Dataset this issue affects
    pub dataset: String,

    /// Station ID involved (if applicable)
    pub station_id: Option<i32>,

    /// Detailed description of the data issue
    pub description: String,

    /// Sample of problematic data
    pub data_sample: Option<String>,

    /// Statistical information about the issue
    pub statistics: Option<HashMap<String, f64>>,

    /// Additional context or metadata
    pub metadata: HashMap<String, String>,
}

/// Types of data consistency issues
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DataIssueType {
    /// Station ID mismatch between observation and capability files
    StationIdMismatch,
    /// Date ranges are inconsistent
    DateRangeInconsistency,
    /// Missing observations for active stations
    MissingObservations,
    /// Duplicate records found
    DuplicateRecords,
    /// Quality flag distribution is suspicious
    SuspiciousQualityFlags,
    /// Data values are outside expected ranges
    OutOfRangeValues,
    /// Systematic missing data patterns
    SystematicMissingData,
    /// Temporal gaps in observation sequences
    TemporalGaps,
}

/// Station metadata issues
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationIssue {
    /// Issue unique identifier
    pub id: String,

    /// Severity of the issue
    pub severity: IssueSeverity,

    /// Type of station issue
    pub issue_type: StationIssueType,

    /// Station ID involved
    pub station_id: i32,

    /// Station name (if available)
    pub station_name: Option<String>,

    /// File path containing station metadata
    pub file_path: PathBuf,

    /// Dataset this station belongs to
    pub dataset: String,

    /// Detailed description of the station issue
    pub description: String,

    /// Expected vs actual values (if applicable)
    pub expected_actual: Option<(String, String)>,

    /// Additional context or metadata
    pub metadata: HashMap<String, String>,
}

/// Types of station metadata issues
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum StationIssueType {
    /// Station coordinates are invalid or suspicious
    InvalidCoordinates,
    /// Station operational dates are inconsistent
    InconsistentDates,
    /// Required metadata fields are missing
    MissingMetadata,
    /// Station appears in multiple datasets with different metadata
    ConflictingMetadata,
    /// Station height/elevation is suspicious
    SuspiciousElevation,
    /// Station appears to be a duplicate
    DuplicateStation,
    /// Station has no associated observation data
    NoObservationData,
}

/// Cross-dataset consistency issues
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsistencyIssue {
    /// Issue unique identifier
    pub id: String,

    /// Severity of the issue
    pub severity: IssueSeverity,

    /// Type of consistency issue
    pub issue_type: ConsistencyIssueType,

    /// Primary dataset involved
    pub primary_dataset: String,

    /// Secondary dataset involved (if applicable)
    pub secondary_dataset: Option<String>,

    /// Station ID involved (if applicable)
    pub station_id: Option<i32>,

    /// Detailed description of the consistency issue
    pub description: String,

    /// Data comparison results
    pub comparison_results: Option<HashMap<String, String>>,

    /// Additional context or metadata
    pub metadata: HashMap<String, String>,
}

/// Types of cross-dataset consistency issues
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ConsistencyIssueType {
    /// Station appears in multiple datasets with different coordinates
    CoordinateMismatch,
    /// Station operational periods differ across datasets
    OperationalPeriodMismatch,
    /// Station metadata conflicts between datasets
    MetadataMismatch,
    /// Quality control versions are inconsistent
    QualityControlMismatch,
    /// Data coverage patterns are inconsistent
    CoverageMismatch,
}

// =============================================================================
// Supporting Types
// =============================================================================

/// Issue severity levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum IssueSeverity {
    /// Informational - no action required
    Info,
    /// Warning - minor issue that should be noted
    Warning,
    /// Error - significant issue that may affect data quality
    Error,
    /// Critical - serious issue requiring immediate attention
    Critical,
}

impl std::fmt::Display for IssueSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IssueSeverity::Info => write!(f, "INFO"),
            IssueSeverity::Warning => write!(f, "WARNING"),
            IssueSeverity::Error => write!(f, "ERROR"),
            IssueSeverity::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// High-level issue type categories
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum IssueType {
    /// File structure and access issues
    File,
    /// BADC-CSV parsing issues
    Parsing,
    /// Data consistency and quality issues
    Data,
    /// Station metadata issues
    Station,
    /// Cross-dataset consistency issues
    Consistency,
}

/// Performance metrics from audit execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    /// Total audit duration
    pub total_duration: Duration,

    /// Time spent on each audit phase
    pub phase_durations: HashMap<String, Duration>,

    /// Files processed per second
    pub files_per_second: f64,

    /// Data processed in bytes
    pub bytes_processed: u64,

    /// Peak memory usage in MB
    pub peak_memory_mb: Option<usize>,

    /// Number of worker threads used
    pub worker_threads: usize,

    /// Total number of files examined
    pub files_examined: Option<usize>,

    /// Number of files parsed successfully
    pub files_parsed_successfully: Option<usize>,
}

/// Audit configuration snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    /// Cache path that was audited
    pub cache_path: PathBuf,

    /// Datasets that were included in audit
    pub datasets: Vec<String>,

    /// Audit level used
    pub audit_level: String,

    /// Types of checks performed
    pub checks_performed: Vec<String>,

    /// Number of parallel workers
    pub parallel_workers: usize,

    /// Version of the auditor tool
    pub auditor_version: String,
}

// =============================================================================
// Dataset-Level Results
// =============================================================================

/// Audit results for a specific dataset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetAuditResult {
    /// Dataset name
    pub dataset_name: String,

    /// Number of files examined in this dataset
    pub files_examined: usize,

    /// Number of files successfully processed
    pub files_processed: usize,

    /// Issues specific to this dataset
    pub issues: Vec<DatasetIssue>,

    /// Statistics about this dataset
    pub statistics: DatasetStatistics,

    /// Overall health assessment for this dataset
    pub health_assessment: DatasetHealth,

    /// Processing duration for this dataset
    pub processing_duration: Duration,
}

/// Dataset-specific issue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetIssue {
    /// Issue severity
    pub severity: IssueSeverity,

    /// Issue category
    pub category: String,

    /// Issue description
    pub description: String,

    /// Number of occurrences
    pub count: usize,

    /// Sample file paths affected
    pub sample_files: Vec<PathBuf>,
}

/// Statistical information about a dataset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetStatistics {
    /// Number of stations found
    pub station_count: usize,

    /// Date range covered by observations
    pub date_range: Option<(DateTime<Utc>, DateTime<Utc>)>,

    /// Total number of observations
    pub observation_count: usize,

    /// Quality flag distribution
    pub quality_flag_distribution: HashMap<String, usize>,

    /// Data completeness percentage
    pub completeness_percentage: f64,

    /// Average observations per station
    pub avg_observations_per_station: f64,
}

/// Dataset health assessment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetHealth {
    /// Overall health score (0-100)
    pub score: f32,

    /// Health status
    pub status: AuditResult,

    /// Key factors affecting health
    pub factors: Vec<String>,

    /// Recommendations for improvement
    pub recommendations: Vec<String>,
}

// =============================================================================
// Helper Functions
// =============================================================================

impl AuditReport {
    /// Create a new empty audit report
    pub fn new(audit_config: AuditConfig) -> Self {
        Self {
            summary: AuditSummary::default(),
            file_issues: Vec::new(),
            parsing_issues: Vec::new(),
            data_issues: Vec::new(),
            station_issues: Vec::new(),
            consistency_issues: Vec::new(),
            performance_metrics: PerformanceMetrics::default(),
            audit_timestamp: Utc::now(),
            audit_config,
        }
    }

    /// Calculate total number of issues
    pub fn total_issues(&self) -> usize {
        self.file_issues.len()
            + self.parsing_issues.len()
            + self.data_issues.len()
            + self.station_issues.len()
            + self.consistency_issues.len()
    }

    /// Get issues by severity level
    pub fn issues_by_severity(&self, severity: IssueSeverity) -> usize {
        let mut count = 0;
        count += self
            .file_issues
            .iter()
            .filter(|i| i.severity == severity)
            .count();
        count += self
            .parsing_issues
            .iter()
            .filter(|i| i.severity == severity)
            .count();
        count += self
            .data_issues
            .iter()
            .filter(|i| i.severity == severity)
            .count();
        count += self
            .station_issues
            .iter()
            .filter(|i| i.severity == severity)
            .count();
        count += self
            .consistency_issues
            .iter()
            .filter(|i| i.severity == severity)
            .count();
        count
    }

    /// Calculate health score based on issues
    pub fn calculate_health_score(&self) -> f32 {
        if self.total_issues() == 0 {
            return 100.0;
        }

        let critical_weight = 25.0;
        let error_weight = 10.0;
        let warning_weight = 3.0;
        let info_weight = 1.0;

        let critical_score =
            self.issues_by_severity(IssueSeverity::Critical) as f32 * critical_weight;
        let error_score = self.issues_by_severity(IssueSeverity::Error) as f32 * error_weight;
        let warning_score = self.issues_by_severity(IssueSeverity::Warning) as f32 * warning_weight;
        let info_score = self.issues_by_severity(IssueSeverity::Info) as f32 * info_weight;

        let total_penalty = critical_score + error_score + warning_score + info_score;
        let max_possible_score = 100.0;

        (max_possible_score - total_penalty).clamp(0.0, 100.0)
    }

    /// Determine overall audit result
    pub fn determine_result(&self) -> AuditResult {
        let critical_count = self.issues_by_severity(IssueSeverity::Critical);
        let error_count = self.issues_by_severity(IssueSeverity::Error);
        let warning_count = self.issues_by_severity(IssueSeverity::Warning);

        if critical_count > 0 || error_count > 5 {
            AuditResult::Critical
        } else if error_count > 0 || warning_count > 10 {
            AuditResult::Degraded
        } else if warning_count > 0 {
            AuditResult::Warning
        } else {
            AuditResult::Healthy
        }
    }
}

impl Default for AuditSummary {
    fn default() -> Self {
        Self {
            datasets_audited: 0,
            files_examined: 0,
            files_parsed_successfully: 0,
            files_with_parsing_errors: 0,
            total_issues: 0,
            issues_by_severity: HashMap::new(),
            issues_by_type: HashMap::new(),
            overall_result: AuditResult::Healthy,
            audit_duration: Duration::from_secs(0),
            health_score: 100.0,
        }
    }
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            total_duration: Duration::from_secs(0),
            phase_durations: HashMap::new(),
            files_per_second: 0.0,
            bytes_processed: 0,
            peak_memory_mb: None,
            worker_threads: 1,
            files_examined: Some(0),
            files_parsed_successfully: Some(0),
        }
    }
}
