//! Record deduplication logic for MIDAS observations
//!
//! This module implements deduplication based on MIDAS record status and quality priorities.
//! It handles complex cases with multiple instruments and quality levels while following
//! MIDAS record status priority rules.

use crate::app::models::{Observation, ProcessingFlag};
use crate::constants::record_status;
use indicatif::ProgressBar;
use std::collections::HashMap;
use tracing::{debug, info};

use super::stats::ProcessingStats;

/// Deduplicate observations based on MIDAS record status and quality priorities
///
/// This function implements multi-phase deduplication:
/// 1. Group observations by (observation_id, station_id, ob_end_time)
/// 2. Apply MIDAS record status priority rules (corrected supersedes original)
/// 3. Handle complex cases with multiple instruments or quality levels
///
/// # Arguments
///
/// * `observations` - Input observations to deduplicate
/// * `stats` - Mutable reference to processing statistics
/// * `progress_bar` - Optional progress bar for tracking progress
///
/// # Returns
///
/// Vector of deduplicated observations
pub fn deduplicate_observations(
    observations: Vec<Observation>,
    _stats: &mut ProcessingStats,
    progress_bar: Option<&ProgressBar>,
) -> Vec<Observation> {
    let mut groups: HashMap<(String, i32, chrono::DateTime<chrono::Utc>), Vec<Observation>> =
        HashMap::new();

    // Group observations by their identifying key
    for observation in observations {
        let key = (
            observation.observation_id.clone(),
            observation.station_id,
            observation.ob_end_time,
        );
        groups.entry(key).or_default().push(observation);
    }

    let mut deduplicated = Vec::new();
    let mut duplicates_removed = 0;

    // For each group, apply deduplication rules
    let total_groups = groups.len();
    for (index, (key, group_observations)) in groups.into_iter().enumerate() {
        // Update progress bar
        if let Some(pb) = progress_bar {
            pb.set_position(index as u64);
            if index % 100 == 0 || index == total_groups - 1 {
                pb.set_message(format!(
                    "Processing group {} of {}",
                    index + 1,
                    total_groups
                ));
            }
        }

        if group_observations.len() == 1 {
            // No duplicates, keep the single observation
            let mut observation = group_observations.into_iter().next().unwrap();
            observation.set_processing_flag("record".to_string(), ProcessingFlag::Original);
            deduplicated.push(observation);
        } else {
            // Apply deduplication rules
            let group_size = group_observations.len();
            let best_observation = select_best_observation(group_observations);
            deduplicated.push(best_observation);
            duplicates_removed += 1;

            debug!(
                "Deduplicated observation group {:?}: resolved {} duplicates",
                key,
                group_size - 1
            );
        }

        // Increment progress
        if let Some(pb) = progress_bar {
            pb.inc(1);
        }
    }

    info!(
        "Deduplication complete: resolved {} duplicate groups, {} observations remaining",
        duplicates_removed,
        deduplicated.len()
    );

    deduplicated
}

/// Select the best observation from a group of duplicates
///
/// Applies MIDAS priority rules:
/// 1. Record status priority (corrected > original, modern processing stages)
/// 2. Quality control version (higher is better)
/// 3. Processing completeness (more measurements is better)
///
/// # Arguments
///
/// * `observations` - Group of duplicate observations
///
/// # Returns
///
/// The best observation from the group
pub fn select_best_observation(mut observations: Vec<Observation>) -> Observation {
    // Sort by priority (highest priority first)
    observations.sort_by(|a, b| {
        // Primary: Record status priority
        let status_cmp = compare_record_priority(a.rec_st_ind, b.rec_st_ind);
        if status_cmp != std::cmp::Ordering::Equal {
            return status_cmp;
        }

        // Secondary: Quality control version (higher is better)
        let version_cmp = b.version_num.cmp(&a.version_num);
        if version_cmp != std::cmp::Ordering::Equal {
            return version_cmp;
        }

        // Tertiary: Number of measurements (more is better)
        b.measurements.len().cmp(&a.measurements.len())
    });

    let mut best_observation = observations.into_iter().next().unwrap();
    best_observation.set_processing_flag("record".to_string(), ProcessingFlag::DuplicateResolved);
    best_observation
}

/// Compare record status priorities for deduplication
///
/// Implements the MIDAS record status priority system
///
/// # Arguments
///
/// * `a` - First record status
/// * `b` - Second record status
///
/// # Returns
///
/// Ordering for sorting (highest priority first)
pub fn compare_record_priority(a: i32, b: i32) -> std::cmp::Ordering {
    let priority_a = get_record_status_priority(a);
    let priority_b = get_record_status_priority(b);
    priority_a.cmp(&priority_b)
}

/// Get priority value for record status (lower = higher priority)
///
/// # Arguments
///
/// * `status` - Record status value
///
/// # Returns
///
/// Priority value (lower numbers = higher priority)
pub fn get_record_status_priority(status: i32) -> i32 {
    // Define constant values for pattern matching
    const CORRECTED: i32 = record_status::CORRECTED as i32;
    const ORIGINAL: i32 = record_status::ORIGINAL as i32;

    match status {
        // Highest priority: corrected/processed data
        CORRECTED => 1,
        s if s == record_status::PROCESSED => 2,
        s if s == record_status::REVISED => 3,
        s if s == record_status::ARCHIVED => 4,
        s if s == record_status::ALTERNATE_PROCESSING => 5,

        // Lower priority: original and other processing stages
        ORIGINAL => 100,

        // Default: use the status value itself (lower = higher priority)
        _ => status,
    }
}

/// Analyze duplicate patterns in a collection of observations
///
/// # Arguments
///
/// * `observations` - Observations to analyze
///
/// # Returns
///
/// Tuple of (total_groups, duplicate_groups, total_duplicates)
pub fn analyze_duplicate_patterns(observations: &[Observation]) -> (usize, usize, usize) {
    let mut groups: HashMap<(String, i32, chrono::DateTime<chrono::Utc>), usize> = HashMap::new();

    // Count observations per group
    for observation in observations {
        let key = (
            observation.observation_id.clone(),
            observation.station_id,
            observation.ob_end_time,
        );
        *groups.entry(key).or_insert(0) += 1;
    }

    let total_groups = groups.len();
    let duplicate_groups = groups.values().filter(|&&count| count > 1).count();
    let total_duplicates = groups.values().map(|&count| count.saturating_sub(1)).sum();

    (total_groups, duplicate_groups, total_duplicates)
}

/// Get deduplication effectiveness metrics
///
/// # Arguments
///
/// * `input_count` - Number of observations before deduplication
/// * `output_count` - Number of observations after deduplication
///
/// # Returns
///
/// Tuple of (reduction_percentage, duplicates_removed)
pub fn get_deduplication_metrics(input_count: usize, output_count: usize) -> (f64, usize) {
    let duplicates_removed = input_count.saturating_sub(output_count);
    let reduction_percentage = if input_count > 0 {
        (duplicates_removed as f64 / input_count as f64) * 100.0
    } else {
        0.0
    };

    (reduction_percentage, duplicates_removed)
}

/// Check if two observations are considered duplicates
///
/// # Arguments
///
/// * `obs1` - First observation
/// * `obs2` - Second observation
///
/// # Returns
///
/// True if observations are duplicates (same key)
pub fn are_duplicates(obs1: &Observation, obs2: &Observation) -> bool {
    obs1.observation_id == obs2.observation_id
        && obs1.station_id == obs2.station_id
        && obs1.ob_end_time == obs2.ob_end_time
}
