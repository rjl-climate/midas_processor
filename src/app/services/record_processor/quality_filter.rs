//! Processing quality filtering for MIDAS observations
//!
//! This module provides processing-level quality control filtering that only removes
//! observations with critical processing errors. ALL MIDAS data-level quality indicators
//! (version_num, quality flags, rec_st_ind) are preserved and passed through without
//! interpretation, in accordance with the pass-through directive.

use crate::app::models::{Observation, ProcessingFlag};
use crate::config::QualityControlConfig;
use indicatif::ProgressBar;
use tracing::{debug, info};

use super::stats::ProcessingStats;

/// Apply processing quality filters to observations
///
/// Filters observations based ONLY on critical processing errors.
/// ALL MIDAS data-level quality indicators are preserved without interpretation.
///
/// # Arguments
///
/// * `observations` - Input observations to filter
/// * `quality_config` - Quality control configuration (for processing requirements only)
/// * `stats` - Mutable reference to processing statistics
/// * `progress_bar` - Optional progress bar for tracking progress
///
/// # Returns
///
/// Vector of observations that pass processing quality filters
pub fn apply_processing_filters(
    observations: Vec<Observation>,
    quality_config: &QualityControlConfig,
    stats: &mut ProcessingStats,
    progress_bar: Option<&ProgressBar>,
) -> Vec<Observation> {
    let mut filtered = Vec::new();
    let mut filtered_out = 0;
    let total_observations = observations.len();

    for (index, observation) in observations.into_iter().enumerate() {
        // Update progress bar
        if let Some(pb) = progress_bar {
            pb.set_position(index as u64);
            if index % 1000 == 0 || index == total_observations - 1 {
                pb.set_message(format!(
                    "Filtering observation {} of {}",
                    index + 1,
                    total_observations
                ));
            }
        }

        if passes_processing_filters(&observation, quality_config, stats) {
            filtered.push(observation);
        } else {
            filtered_out += 1;
        }

        // Increment progress
        if let Some(pb) = progress_bar {
            pb.inc(1);
        }
    }

    info!(
        "Processing filtering complete: {} -> {} observations ({} filtered out)",
        stats.deduplicated,
        filtered.len(),
        filtered_out
    );

    filtered
}

/// Check if an observation passes processing quality filters
///
/// Only filters based on critical processing errors. ALL MIDAS data-level quality
/// indicators (version_num, quality flags, rec_st_ind) are preserved.
///
/// # Arguments
///
/// * `observation` - Observation to check
/// * `quality_config` - Quality control configuration (processing requirements only)
/// * `stats` - Mutable reference to processing statistics for error tracking
///
/// # Returns
///
/// True if observation passes all processing filters
pub fn passes_processing_filters(
    observation: &Observation,
    quality_config: &QualityControlConfig,
    _stats: &mut ProcessingStats,
) -> bool {
    // Check for critical processing errors only
    if has_critical_processing_errors(observation) {
        debug!(
            "Observation {} filtered out: critical processing error",
            observation.observation_id
        );
        return false;
    }

    // Check if observation has any measurements at all (if configured)
    // (Empty measurements indicate a parsing or processing failure)
    if quality_config.exclude_empty_measurements && observation.measurements.is_empty() {
        debug!(
            "Observation {} filtered out: no measurements (processing failure)",
            observation.observation_id
        );
        return false;
    }

    // Check station metadata requirement (if configured)
    if quality_config.require_station_metadata
        && observation.get_processing_flag("station") == Some(ProcessingFlag::StationMissing)
    {
        debug!(
            "Observation {} filtered out: missing station metadata (required by config)",
            observation.observation_id
        );
        return false;
    }

    // All MIDAS quality indicators (version_num, quality flags) are preserved

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

/// Get processing filter statistics for a collection of observations
///
/// # Arguments
///
/// * `observations` - Observations to analyze
/// * `_quality_config` - Quality control configuration (unused for MIDAS data pass-through)
///
/// # Returns
///
/// Tuple of (total, would_pass, no_measurements, critical_errors)
pub fn get_processing_filter_stats(
    observations: &[Observation],
    _quality_config: &QualityControlConfig,
) -> (usize, usize, usize, usize) {
    let total = observations.len();
    let mut would_pass = 0;
    let mut no_measurements = 0;
    let mut critical_errors = 0;

    for observation in observations {
        // Check measurements (empty indicates processing failure)
        if observation.measurements.is_empty() {
            no_measurements += 1;
            continue;
        }

        // Check critical processing errors
        if has_critical_processing_errors(observation) {
            critical_errors += 1;
            continue;
        }

        would_pass += 1;
    }

    (total, would_pass, no_measurements, critical_errors)
}

/// Check if observation has sufficient processing quality for analysis
///
/// This is a more permissive check than `passes_processing_filters` and can be used
/// for determining if an observation might be useful for some types of analysis.
/// Note: This only checks processing quality, not MIDAS data quality indicators.
///
/// # Arguments
///
/// * `observation` - Observation to check
///
/// # Returns
///
/// True if observation has basic processing quality for analysis
pub fn has_analysis_quality(observation: &Observation) -> bool {
    // Must have at least one measurement (processing requirement)
    if observation.measurements.is_empty() {
        return false;
    }

    // Station metadata is now guaranteed to be valid by strict parsing
    // No need to check for placeholder patterns

    // Must not have critical processing errors
    if has_critical_processing_errors(observation) {
        return false;
    }

    // All MIDAS data quality indicators are preserved regardless

    true
}

/// Get summary of processing quality issues in a collection of observations
///
/// # Arguments
///
/// * `observations` - Observations to analyze
///
/// # Returns
///
/// String summary of processing quality issues found (data quality preserved)
pub fn get_processing_quality_summary(observations: &[Observation]) -> String {
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
        "Processing Quality Summary: {total} observations | \
         {with_measurements} have measurements ({:.1}%) | \
         {with_station_data} have station data ({:.1}%) | \
         {with_processing_errors} have critical processing errors ({:.1}%) | \
         ALL MIDAS data quality indicators preserved",
        (with_measurements as f64 / total as f64) * 100.0,
        (with_station_data as f64 / total as f64) * 100.0,
        (with_processing_errors as f64 / total as f64) * 100.0
    )
}
