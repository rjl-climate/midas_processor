use std::collections::HashMap;
use midas_processor::app::models::{Observation, ProcessingFlag, Station};
use midas_processor::app::services::record_processor::tests::*;
use midas_processor::app::services::record_processor::quality_filter::*;
use midas_processor::app::services::record_processor::stats::ProcessingStats;

fn main() {
    let mut stats = ProcessingStats::new();
    let config = create_permissive_quality_config();
    
    let observations = vec![
        create_observation_with_good_station("obs1", 123),
        create_observation_with_missing_station("obs2", 999),
        create_observation_with_parse_failures("obs3", 124),
    ];

    println!("=== Permissive Config ===");
    println!("require_station_metadata: {}", config.require_station_metadata);
    println!("exclude_empty_measurements: {}", config.exclude_empty_measurements);
    println!();

    for (i, obs) in observations.iter().enumerate() {
        println!("=== Observation {} ===", i + 1);
        println!("ID: {}", obs.observation_id);
        println!("Station ID: {}", obs.station_id);
        println!("Measurements: {:?}", obs.measurements);
        println!("Processing flags: {:?}", obs.processing_flags);
        println!("Has measurements: {}", !obs.measurements.is_empty());
        println!("Has critical errors: {}", has_critical_processing_errors(obs));
        println!("Station processing flag: {:?}", obs.get_processing_flag("station"));
        println!("Passes filters: {}", passes_processing_filters(obs, &config, &mut stats));
        println!();
    }

    let result = apply_processing_filters(observations, &config, &mut stats);
    println!("Final result length: {}", result.len());
    println!("Stats errors: {}", stats.errors);
}