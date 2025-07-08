//! Main record processor implementation and pipeline orchestration
//!
//! This module contains the main RecordProcessor struct and coordinates the complete
//! processing pipeline for MIDAS observations, including station re-enrichment,
//! deduplication, and quality control filtering.

use crate::Result;
use crate::app::models::Observation;
use crate::app::services::station_registry::StationRegistry;
use crate::config::QualityControlConfig;
use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;
use tracing::{debug, info};

use super::{
    deduplication::deduplicate_observations,
    enrichment::re_enrich_station_metadata,
    quality_filter::apply_processing_filters,
    stats::{ProcessingResult, ProcessingStats},
};

/// Record processor for MIDAS observation data
///
/// The RecordProcessor handles the post-parsing processing pipeline for MIDAS observations.
/// It takes observations (typically from the BADC CSV parser) and applies station re-enrichment,
/// deduplication, and quality control filtering.
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
/// use midas_processor::app::services::record_processor::RecordProcessor;
/// use midas_processor::app::services::station_registry::StationRegistry;
/// use midas_processor::config::QualityControlConfig;
///
/// # async fn example(observations: Vec<midas_processor::app::models::Observation>) -> midas_processor::Result<()> {
/// let registry = Arc::new(StationRegistry::new(std::path::PathBuf::from("/cache")));
/// let qc_config = QualityControlConfig::default();
/// let processor = RecordProcessor::new(registry, qc_config);
///
/// let result = processor.process_observations(observations).await?;
/// println!("Processed {} observations", result.observations.len());
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct RecordProcessor {
    /// Station registry for metadata lookups
    station_registry: Arc<StationRegistry>,
    /// Quality control configuration
    quality_config: QualityControlConfig,
}

impl RecordProcessor {
    /// Create a new record processor with station registry and quality control configuration
    ///
    /// # Arguments
    ///
    /// * `station_registry` - Registry for O(1) station metadata lookups
    /// * `quality_config` - Configuration for quality control filtering
    pub fn new(
        station_registry: Arc<StationRegistry>,
        quality_config: QualityControlConfig,
    ) -> Self {
        Self {
            station_registry,
            quality_config,
        }
    }

    /// Process a collection of observations through the full pipeline
    ///
    /// This method applies the complete processing pipeline:
    /// 1. Station metadata re-enrichment (fix placeholder/missing stations)
    /// 2. Deduplication based on record status priorities
    /// 3. Processing quality filtering (MIDAS data quality preserved)
    ///
    /// # Arguments
    ///
    /// * `observations` - Input observations to process
    /// * `show_progress` - Whether to show progress bars for processing steps
    ///
    /// # Returns
    ///
    /// A `ProcessingResult` containing the processed observations and statistics
    pub async fn process_observations(
        &self,
        observations: Vec<Observation>,
        show_progress: bool,
    ) -> Result<ProcessingResult> {
        let mut stats = ProcessingStats::new();
        stats.total_input = observations.len();

        info!(
            "Starting record processing pipeline for {} observations",
            observations.len()
        );

        // Step 1: Re-enrich observations with missing/placeholder station metadata
        let enrichment_pb = if show_progress {
            let pb = Self::create_processing_progress_bar(
                observations.len() as u64,
                "Station enrichment",
            );
            Some(pb)
        } else {
            None
        };

        let enriched_observations = re_enrich_station_metadata(
            observations,
            &self.station_registry,
            &mut stats,
            enrichment_pb.as_ref(),
        )
        .await?;
        stats.enriched = enriched_observations.len();

        if let Some(pb) = enrichment_pb {
            pb.finish_with_message(format!(
                "Station enrichment complete: {} observations",
                enriched_observations.len()
            ));
        }

        // Step 2: Deduplicate observations based on record status and quality
        let dedup_pb = if show_progress {
            let pb = Self::create_processing_progress_bar(
                enriched_observations.len() as u64,
                "Deduplication",
            );
            Some(pb)
        } else {
            None
        };

        let deduplicated_observations =
            deduplicate_observations(enriched_observations, &mut stats, dedup_pb.as_ref());
        stats.deduplicated = deduplicated_observations.len();

        if let Some(pb) = dedup_pb {
            pb.finish_with_message(format!(
                "Deduplication complete: {} observations",
                deduplicated_observations.len()
            ));
        }

        // Step 3: Apply processing quality filtering (MIDAS data quality preserved)
        let filter_pb = if show_progress {
            let pb = Self::create_processing_progress_bar(
                deduplicated_observations.len() as u64,
                "Quality filtering",
            );
            Some(pb)
        } else {
            None
        };

        let filtered_observations = apply_processing_filters(
            deduplicated_observations,
            &self.quality_config,
            &mut stats,
            filter_pb.as_ref(),
        );
        stats.quality_filtered = filtered_observations.len();
        stats.final_output = filtered_observations.len();

        if let Some(pb) = filter_pb {
            pb.finish_with_message(format!(
                "Quality filtering complete: {} observations",
                filtered_observations.len()
            ));
        }

        info!(
            "Record processing complete: {} -> {} observations ({}% success rate)",
            stats.total_input,
            stats.final_output,
            stats.success_rate()
        );

        if !stats.is_successful() {
            // Use debug level to avoid interfering with progress bars
            // This information is still available with verbose logging (-v)
            debug!(
                "Low success rate in record processing: {}% ({} errors)",
                stats.success_rate(),
                stats.errors
            );
        }

        Ok(ProcessingResult::new(filtered_observations, stats))
    }

    /// Get the station registry used by this processor
    pub fn station_registry(&self) -> &StationRegistry {
        &self.station_registry
    }

    /// Get the quality control configuration used by this processor
    pub fn quality_config(&self) -> &QualityControlConfig {
        &self.quality_config
    }

    /// Create a progress bar for processing operations
    fn create_processing_progress_bar(total: u64, operation: &str) -> ProgressBar {
        let pb = ProgressBar::new(total);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg} [{per_sec}] ETA: {eta}")
                .unwrap()
                .progress_chars("#>-"),
        );
        pb.set_message(operation.to_string());
        pb
    }

    /// Process observations with custom pipeline steps
    ///
    /// This method allows for more granular control over the processing pipeline
    /// by allowing individual steps to be skipped or customized.
    ///
    /// # Arguments
    ///
    /// * `observations` - Input observations to process
    /// * `skip_enrichment` - Skip station re-enrichment step
    /// * `skip_deduplication` - Skip deduplication step
    /// * `skip_quality_filter` - Skip quality filtering step
    ///
    /// # Returns
    ///
    /// A `ProcessingResult` containing the processed observations and statistics
    pub async fn process_observations_custom(
        &self,
        observations: Vec<Observation>,
        skip_enrichment: bool,
        skip_deduplication: bool,
        skip_quality_filter: bool,
    ) -> Result<ProcessingResult> {
        let mut stats = ProcessingStats::new();
        stats.total_input = observations.len();

        info!(
            "Starting custom record processing pipeline for {} observations \
             (enrichment: {}, deduplication: {}, quality_filter: {})",
            observations.len(),
            !skip_enrichment,
            !skip_deduplication,
            !skip_quality_filter
        );

        let mut current_observations = observations;

        // Step 1: Station re-enrichment (optional)
        if !skip_enrichment {
            current_observations = re_enrich_station_metadata(
                current_observations,
                &self.station_registry,
                &mut stats,
                None,
            )
            .await?;
        }
        stats.enriched = current_observations.len();

        // Step 2: Deduplication (optional)
        if !skip_deduplication {
            current_observations = deduplicate_observations(current_observations, &mut stats, None);
        }
        stats.deduplicated = current_observations.len();

        // Step 3: Quality filtering (optional)
        if !skip_quality_filter {
            current_observations = apply_processing_filters(
                current_observations,
                &self.quality_config,
                &mut stats,
                None,
            );
        }
        stats.quality_filtered = current_observations.len();
        stats.final_output = current_observations.len();

        info!(
            "Custom record processing complete: {} -> {} observations ({}% success rate)",
            stats.total_input,
            stats.final_output,
            stats.success_rate()
        );

        Ok(ProcessingResult::new(current_observations, stats))
    }

    /// Validate observations before processing
    ///
    /// This method performs basic validation checks on observations before
    /// processing to catch obvious issues early.
    ///
    /// # Arguments
    ///
    /// * `observations` - Observations to validate
    ///
    /// # Returns
    ///
    /// Result indicating validation success or first validation error found
    pub fn validate_observations(&self, observations: &[Observation]) -> Result<()> {
        if observations.is_empty() {
            return Err(crate::Error::data_validation(
                "Cannot process empty observation collection".to_string(),
            ));
        }

        // Check for basic data integrity
        for (i, observation) in observations.iter().enumerate() {
            if observation.observation_id.trim().is_empty() {
                return Err(crate::Error::data_validation(format!(
                    "Observation at index {} has empty observation_id",
                    i
                )));
            }

            if observation.station_id <= 0 {
                return Err(crate::Error::data_validation(format!(
                    "Observation at index {} has invalid station_id {}",
                    i, observation.station_id
                )));
            }
        }

        Ok(())
    }
}
