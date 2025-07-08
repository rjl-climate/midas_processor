use clap::Parser;
use midas_processor::cli::{args::Args, commands};
use std::process;
use tokio_util::sync::CancellationToken;

fn main() {
    // Parse command line arguments
    let args = Args::parse();

    // If no subcommand was provided, show help and available commands
    if args.command.is_none() {
        show_help_and_commands();
        process::exit(0);
    }

    // Create async runtime and run the main command logic with signal handling
    let runtime = tokio::runtime::Runtime::new().unwrap_or_else(|e| {
        eprintln!("Failed to create async runtime: {}", e);
        process::exit(1);
    });

    let result = runtime.block_on(async {
        // Create cancellation token for coordinating graceful shutdown
        let cancellation_token = CancellationToken::new();

        // Set up graceful shutdown handling
        let shutdown_signal = async {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to install CTRL+C signal handler");

            // Cancel all operations when Ctrl+C is received
            cancellation_token.cancel();
        };

        // Run the main command with cancellation support
        tokio::select! {
            result = commands::run(args, cancellation_token.clone()) => {
                result
            }
            _ = shutdown_signal => {
                eprintln!("\nReceived CTRL+C, shutting down gracefully...");
                Err(midas_processor::Error::processing_interrupted(
                    "Processing interrupted by user".to_string()
                ))
            }
        }
    });

    match result {
        Ok(_stats) => {
            // Success - stats have already been reported by the command
            process::exit(0);
        }
        Err(error) => {
            // Error occurred - print to stderr and exit with error code
            eprintln!("Error: {:#}", error);
            process::exit(1);
        }
    }
}

/// Show help information and available commands when no subcommand is provided
fn show_help_and_commands() {
    println!("MIDAS Processor - UK Met Office Weather Data Converter");
    println!("====================================================");
    println!();
    println!("Convert UK Met Office MIDAS weather observation data from CSV format");
    println!("into optimized Apache Parquet files for fast Python data analysis.");
    println!();
    println!("USAGE:");
    println!("    midas-processor <COMMAND> [OPTIONS]");
    println!();
    println!("COMMANDS:");
    println!("    process     Process MIDAS data from CSV to Parquet format (main command)");
    println!("    stations    Generate station registry reports and visualizations");
    println!("    help        Show this help message or help for specific commands");
    println!();
    println!("OPTIONS:");
    println!("    -h, --help       Show help information");
    println!("    -V, --version    Show version information");
    println!();
    println!("EXAMPLES:");
    println!("    # Process default datasets (temperature and rainfall):");
    println!("    midas-processor process");
    println!();
    println!("    # Process specific datasets with custom paths:");
    println!(
        "    midas-processor process --input /path/to/midas/cache --output /path/to/output \\"
    );
    println!("                            --datasets uk-daily-temperature-obs,uk-daily-rain-obs");
    println!();
    println!("    # Generate station registry report:");
    println!("    midas-processor stations --detailed --format json");
    println!();
    println!("    # Get help for specific commands:");
    println!("    midas-processor process --help");
    println!("    midas-processor stations --help");
    println!();
    println!("For detailed help on any command, use:");
    println!("    midas-processor <COMMAND> --help");
}
