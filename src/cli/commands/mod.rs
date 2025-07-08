//! Command implementations for MIDAS processor CLI
//!
//! This module contains the main command execution logic, progress reporting,
//! and error handling for the CLI interface. Each command is implemented in
//! its own module for better organization and maintainability.

pub mod observation_stream;
pub mod parallel_processor;
pub mod process;
pub mod shared;
pub mod stations;
pub mod validate;

// Re-export the main types and functions for backward compatibility
pub use shared::ProcessingStats;

use crate::Result;
use crate::cli::args::{Args, Commands};

/// Main command runner for MIDAS processor
///
/// This function dispatches to the appropriate subcommand handler based on CLI args.
/// Each command is implemented in its own module:
/// - `process`: Data processing workflow with Parquet output
/// - `stations`: Station registry analysis and reporting
/// - `validate`: Pipeline validation with comprehensive testing
pub async fn run(args: Args) -> Result<ProcessingStats> {
    match args.get_command() {
        Commands::Process(process_args) => process::run_process(process_args).await,
        Commands::Stations(stations_args) => stations::run_stations(stations_args).await,
        Commands::Validate(validate_args) => validate::run_validate(validate_args).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_processing_stats_re_export() {
        // Verify that ProcessingStats is properly re-exported
        let stats = ProcessingStats::default();
        assert_eq!(stats.datasets_processed, 0);
        assert_eq!(stats.total_output_size(), 0);
    }
}
