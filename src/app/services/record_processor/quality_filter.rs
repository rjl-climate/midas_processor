//! Quality control filtering for MIDAS observations
//!
//! This module provides simple quality control filtering based on configuration settings.
//! Note: This does NOT interpret original MIDAS QC flags, only applies basic filtering
//! based on configuration and processing metadata.

use crate::app::models::{Observation, ProcessingFlag};
use crate::config::QualityControlConfig;
use tracing::{debug, info};

use super::stats::ProcessingStats;

/// Apply quality control filters to observations
///
/// Filters observations based on the quality control configuration.
/// Note: This does NOT interpret original MIDAS QC flags, only applies
/// simple filtering based on configuration settings.
///
/// # Arguments
///
/// * `observations` - Input observations to filter
/// * `quality_config` - Quality control configuration
/// * `stats` - Mutable reference to processing statistics
///
/// # Returns
///
/// Vector of observations that pass quality control filters
pub fn apply_quality_filters(
    observations: Vec<Observation>,
    quality_config: &QualityControlConfig,
    stats: &mut ProcessingStats,
) -> Vec<Observation> {
    let mut filtered = Vec::new();
    let mut filtered_out = 0;

    for observation in observations {
        if passes_quality_filters(&observation, quality_config, stats) {
            filtered.push(observation);
        } else {
            filtered_out += 1;
        }
    }

    info!(
        "Quality filtering complete: {} -> {} observations ({} filtered out)",
        stats.deduplicated,
        filtered.len(),
        filtered_out
    );

    filtered
}

/// Check if an observation passes quality control filters
///
/// # Arguments
///
/// * `observation` - Observation to check
/// * `quality_config` - Quality control configuration
/// * `stats` - Mutable reference to processing statistics for error tracking
///
/// # Returns
///
/// True if observation passes all quality filters
pub fn passes_quality_filters(
    observation: &Observation,
    quality_config: &QualityControlConfig,
    _stats: &mut ProcessingStats,
) -> bool {
    // Check quality control version requirement
    if observation.version_num < quality_config.min_quality_version {
        debug!(
            "Observation {} filtered out: version {} below minimum {}",
            observation.observation_id, observation.version_num, quality_config.min_quality_version
        );
        return false;
    }

    // Check if observation has any usable measurements
    // We apply minimal filtering - most QC interpretation is left to downstream
    if observation.measurements.is_empty() {
        debug!(
            "Observation {} filtered out: no measurements",
            observation.observation_id
        );
        return false;
    }

    // Check for critical processing errors
    if observation.get_processing_flag("station") == Some(ProcessingFlag::StationMissing) {
        // Allow observations with missing stations through if configured to be lenient
        // This could be made configurable in the future
        debug!(
            "Observation {} has missing station metadata but allowed through",
            observation.observation_id
        );
    }

    true
}

/// Check if an observation has critical processing errors that should exclude it
///
/// # Arguments
///
/// * `observation` - Observation to check
///
/// # Returns
///
/// True if observation has critical errors
pub fn has_critical_processing_errors(observation: &Observation) -> bool {
    // Check for processing flags that indicate critical failures
    for (field, flag) in observation.get_all_processing_flags() {
        match flag {
            ProcessingFlag::ParseFailed => {
                // Critical: data couldn't be parsed
                debug!(
                    "Critical error in observation {}: parse failed for field {}",
                    observation.observation_id, field
                );
                return true;
            }
            _ => {
                // Other flags are not considered critical
                continue;
            }
        }
    }

    false
}

/// Get quality filtering statistics for a collection of observations
///
/// # Arguments
///
/// * `observations` - Observations to analyze
/// * `quality_config` - Quality control configuration
///
/// # Returns
///
/// Tuple of (total, would_pass, version_failures, no_measurements, critical_errors)
pub fn get_quality_filter_stats(
    observations: &[Observation],
    quality_config: &QualityControlConfig,
) -> (usize, usize, usize, usize, usize) {
    let total = observations.len();
    let mut would_pass = 0;
    let mut version_failures = 0;
    let mut no_measurements = 0;
    let mut critical_errors = 0;

    for observation in observations {
        // Check version requirement
        if observation.version_num < quality_config.min_quality_version {
            version_failures += 1;
            continue;
        }

        // Check measurements
        if observation.measurements.is_empty() {
            no_measurements += 1;
            continue;
        }

        // Check critical errors
        if has_critical_processing_errors(observation) {
            critical_errors += 1;
            continue;
        }

        would_pass += 1;
    }

    (
        total,
        would_pass,
        version_failures,
        no_measurements,
        critical_errors,
    )
}

/// Check if observation has sufficient quality for analysis
///
/// This is a more permissive check than `passes_quality_filters` and can be used
/// for determining if an observation might be useful for some types of analysis.
///
/// # Arguments
///
/// * `observation` - Observation to check
///
/// # Returns
///
/// True if observation has basic quality for analysis
pub fn has_analysis_quality(observation: &Observation) -> bool {
    // Must have at least one measurement
    if observation.measurements.is_empty() {
        return false;
    }

    // Must have valid station metadata (even if missing initially)
    if observation.station.src_name.starts_with("UNKNOWN_STATION_") {
        return false;
    }

    // Must not have critical processing errors
    if has_critical_processing_errors(observation) {
        return false;
    }

    true
}

/// Get summary of quality issues in a collection of observations
///
/// # Arguments
///
/// * `observations` - Observations to analyze
///
/// # Returns
///
/// String summary of quality issues found
pub fn get_quality_summary(observations: &[Observation]) -> String {
    let total = observations.len();
    let with_measurements = observations
        .iter()
        .filter(|obs| !obs.measurements.is_empty())
        .count();
    let with_station_data = observations
        .iter()
        .filter(|obs| !obs.station.src_name.starts_with("UNKNOWN_STATION_"))
        .count();
    let with_processing_errors = observations
        .iter()
        .filter(|obs| has_critical_processing_errors(obs))
        .count();

    format!(
        "Quality Summary: {total} observations | \
         {with_measurements} have measurements ({:.1}%) | \
         {with_station_data} have station data ({:.1}%) | \
         {with_processing_errors} have critical errors ({:.1}%)",
        (with_measurements as f64 / total as f64) * 100.0,
        (with_station_data as f64 / total as f64) * 100.0,
        (with_processing_errors as f64 / total as f64) * 100.0
    )
}
