//! User input utilities for interactive CLI prompts
//!
//! This module provides functions for interactive user input, including
//! dataset selection menus and confirmation prompts.

use crate::{Error, Result};
use std::io::{self, Write};

/// Display an interactive dataset selection menu and get user choice
///
/// Returns the selected datasets or all datasets if "all" is chosen
pub fn prompt_dataset_selection(available_datasets: &[String]) -> Result<Vec<String>> {
    if available_datasets.is_empty() {
        return Err(Error::configuration(
            "No datasets available for selection".to_string(),
        ));
    }

    // Display menu
    println!("\nAvailable datasets:");
    for (i, dataset) in available_datasets.iter().enumerate() {
        println!("  {}. {}", i + 1, dataset);
    }
    println!("  {}. all (default)", available_datasets.len() + 1);
    println!();

    // Get user input
    print!(
        "Select datasets to process [{}]: ",
        available_datasets.len() + 1
    );
    io::stdout()
        .flush()
        .map_err(|e| Error::io("Failed to flush stdout".to_string(), e))?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .map_err(|e| Error::io("Failed to read user input".to_string(), e))?;

    let input = input.trim();

    // Handle empty input (default to "all")
    if input.is_empty() {
        return Ok(available_datasets.to_vec());
    }

    // Parse input
    if input == "all" || input == (available_datasets.len() + 1).to_string() {
        return Ok(available_datasets.to_vec());
    }

    // Handle single selection
    if let Ok(choice) = input.parse::<usize>() {
        if choice >= 1 && choice <= available_datasets.len() {
            return Ok(vec![available_datasets[choice - 1].clone()]);
        }
    }

    // Handle comma-separated selections
    let mut selected = Vec::new();
    for part in input.split(',') {
        let part = part.trim();
        if let Ok(choice) = part.parse::<usize>() {
            if choice >= 1 && choice <= available_datasets.len() {
                selected.push(available_datasets[choice - 1].clone());
            } else {
                return Err(Error::data_validation(format!(
                    "Invalid selection '{}'. Please choose 1-{} or 'all'",
                    choice,
                    available_datasets.len()
                )));
            }
        } else {
            return Err(Error::data_validation(format!(
                "Invalid input '{}'. Please enter numbers separated by commas, or 'all'",
                part
            )));
        }
    }

    if selected.is_empty() {
        return Err(Error::data_validation(
            "No valid datasets selected".to_string(),
        ));
    }

    Ok(selected)
}

/// Get user confirmation for an action
pub fn prompt_confirmation(message: &str, default_yes: bool) -> Result<bool> {
    let default_text = if default_yes { "Y/n" } else { "y/N" };
    print!("{} [{}]: ", message, default_text);

    io::stdout()
        .flush()
        .map_err(|e| Error::io("Failed to flush stdout".to_string(), e))?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .map_err(|e| Error::io("Failed to read user input".to_string(), e))?;

    let input = input.trim().to_lowercase();

    if input.is_empty() {
        return Ok(default_yes);
    }

    match input.as_str() {
        "y" | "yes" => Ok(true),
        "n" | "no" => Ok(false),
        _ => {
            println!("Please enter 'y' for yes or 'n' for no.");
            prompt_confirmation(message, default_yes)
        }
    }
}

/// Prompt user to continue or exit
pub fn prompt_continue(message: &str) -> Result<bool> {
    prompt_confirmation(message, true)
}

/// Display a progress message and wait for user acknowledgment
pub fn wait_for_acknowledgment(message: &str) -> Result<()> {
    println!("{}", message);
    print!("Press Enter to continue...");

    io::stdout()
        .flush()
        .map_err(|e| Error::io("Failed to flush stdout".to_string(), e))?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .map_err(|e| Error::io("Failed to read user input".to_string(), e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test dataset selection validation handles edge cases
    /// Ensures robust input parsing and error handling
    #[test]
    fn test_dataset_selection_validation() {
        let _datasets = [
            "uk-daily-temperature-obs".to_string(),
            "uk-daily-rain-obs".to_string(),
        ];

        // Empty datasets should return error
        let result = prompt_dataset_selection(&[]);
        assert!(result.is_err());
    }

    /// Test confirmation prompt parsing handles various inputs
    #[test]
    fn test_confirmation_parsing() {
        // Note: These tests can't actually test user input without mocking stdin
        // But we can test the logic structure
        // Placeholder for input validation logic when stdin mocking is implemented
    }
}
