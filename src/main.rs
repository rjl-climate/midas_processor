use clap::Parser;
use midas_processor::cli::{args::Args, commands};
use std::process;

fn main() {
    // Parse command line arguments
    let args = Args::parse();

    // Create async runtime and run the main command logic
    let runtime = tokio::runtime::Runtime::new().unwrap_or_else(|e| {
        eprintln!("Failed to create async runtime: {}", e);
        process::exit(1);
    });

    let result = runtime.block_on(commands::run(args));
    
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
