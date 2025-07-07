//! Station metadata re-enrichment for observations
//!
//! This module handles re-enriching observations that may have placeholder or missing
//! station metadata from the CSV parsing stage. It attempts to lookup proper station
//! data and updates processing flags accordingly.

use crate::Result;
use crate::app::models::{Observation, ProcessingFlag};
use crate::app::services::station_registry::StationRegistry;
use tracing::debug;

use super::stats::ProcessingStats;

/// Re-enrich observations with station metadata from the registry
///
/// This function handles observations that may have placeholder or missing station metadata
/// from the CSV parsing stage. It attempts to lookup proper station data and updates
/// the processing flags accordingly.
///
/// # Arguments
///
/// * `observations` - Input observations to re-enrich
/// * `station_registry` - Registry for station metadata lookups
/// * `stats` - Mutable reference to processing statistics
///
/// # Returns
///
/// Vector of observations with proper station metadata
pub async fn re_enrich_station_metadata(
    observations: Vec<Observation>,
    station_registry: &StationRegistry,
    stats: &mut ProcessingStats,
) -> Result<Vec<Observation>> {
    let mut enriched = Vec::with_capacity(observations.len());

    for mut observation in observations {
        match re_enrich_single_observation(&mut observation, station_registry).await {
            Ok(()) => {
                enriched.push(observation);
            }
            Err(e) => {
                let error_msg = format!(
                    "Failed to re-enrich observation {} for station {}: {}",
                    observation.observation_id, observation.station_id, e
                );
                debug!("{}", error_msg);
                stats.add_error(error_msg);
                // Continue processing with original observation
                enriched.push(observation);
            }
        }
    }

    debug!(
        "Station metadata re-enrichment: {} -> {} observations",
        stats.total_input,
        enriched.len()
    );

    Ok(enriched)
}

/// Re-enrich a single observation with station metadata
///
/// # Arguments
///
/// * `observation` - Mutable observation to enrich
/// * `station_registry` - Registry for station metadata lookups
///
/// # Returns
///
/// Result indicating success or failure
pub async fn re_enrich_single_observation(
    observation: &mut Observation,
    station_registry: &StationRegistry,
) -> Result<()> {
    // Check if observation already has proper station metadata
    if observation.get_processing_flag("station") == Some(ProcessingFlag::StationFound) {
        // Station metadata is already valid
        return Ok(());
    }

    // Station metadata might be missing or placeholder, try to re-lookup
    if let Some(station) = station_registry.get_station(observation.station_id) {
        // Update with correct station metadata
        observation.station = station.clone();
        observation.set_processing_flag("station".to_string(), ProcessingFlag::StationFound);
        debug!(
            "Re-enriched observation {} with station metadata for station {}",
            observation.observation_id, observation.station_id
        );
    } else {
        // Still no station found - this is a processing issue
        observation.set_processing_flag("station".to_string(), ProcessingFlag::StationMissing);
        debug!(
            "Station {} still not found for observation {}",
            observation.station_id, observation.observation_id
        );
    }

    Ok(())
}

/// Check if an observation needs station re-enrichment
///
/// # Arguments
///
/// * `observation` - Observation to check
///
/// # Returns
///
/// True if the observation needs station re-enrichment
pub fn needs_station_enrichment(observation: &Observation) -> bool {
    // Check if station processing flag indicates missing or placeholder data
    match observation.get_processing_flag("station") {
        Some(ProcessingFlag::StationFound) => false,
        Some(ProcessingFlag::StationMissing) => true,
        None => true, // No processing flag means it hasn't been checked
        _ => false,   // Other flags don't indicate station issues
    }
}

/// Get statistics about station enrichment needs in a collection of observations
///
/// # Arguments
///
/// * `observations` - Observations to analyze
///
/// # Returns
///
/// Tuple of (total_count, needs_enrichment_count, missing_station_count)
pub fn analyze_enrichment_needs(observations: &[Observation]) -> (usize, usize, usize) {
    let total = observations.len();
    let needs_enrichment = observations
        .iter()
        .filter(|obs| needs_station_enrichment(obs))
        .count();
    let missing_stations = observations
        .iter()
        .filter(|obs| obs.get_processing_flag("station") == Some(ProcessingFlag::StationMissing))
        .count();

    (total, needs_enrichment, missing_stations)
}
