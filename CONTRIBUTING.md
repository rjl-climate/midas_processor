# Contributing to MIDAS Converter

Thank you for your interest in contributing to MIDAS Converter! This document provides guidelines and information for contributors.

## Code of Conduct

This project adheres to a code of conduct that we expect all contributors to follow. Please be respectful and constructive in all interactions.

## How to Contribute

### Reporting Issues

Before creating an issue, please:
1. **Search existing issues** to avoid duplicates
2. **Use the issue templates** when available
3. **Provide clear reproduction steps** for bugs
4. **Include system information** (OS, Rust version, dataset details)

### Suggesting Features

We welcome feature suggestions! Please:
1. **Check if similar features exist** or are planned
2. **Describe the use case** and why it would be valuable
3. **Consider the impact** on performance and complexity
4. **Propose an implementation approach** if possible

### Development Workflow

1. **Fork the repository** and create a feature branch
2. **Make your changes** following our coding standards
3. **Add tests** for new functionality
4. **Update documentation** as needed
5. **Submit a pull request** with a clear description

## Development Setup

### Prerequisites

- **Rust 1.70+** with Rust 2024 edition support
- **Git** for version control
- **Test datasets** (use midas-fetcher to download samples)

### Local Development

```bash
# Clone your fork
git clone https://github.com/your-username/midas-converter
cd midas-converter

# Build and test
cargo build
cargo test
cargo clippy
cargo fmt --check

# Run with sample data
cargo run -- --help
```

### Testing

We use several types of tests:

**Unit Tests**
```bash
cargo test
```

**Integration Tests**
```bash
cargo test --test integration
```

**Benchmarks**
```bash
cargo bench
```

**Manual Testing**
```bash
# Test with real datasets
cargo run -- /path/to/test/dataset --verbose
```

## Coding Standards

### Code Style

- **Use `cargo fmt`** for consistent formatting
- **Run `cargo clippy`** and fix all warnings
- **Follow Rust naming conventions** (snake_case, CamelCase, etc.)
- **Write clear, self-documenting code** with meaningful variable names

### Documentation

- **Add rustdoc comments** to all public APIs
- **Include examples** in documentation where helpful
- **Update README.md** for user-facing changes
- **Add inline comments** for complex logic

### Error Handling

- **Use `Result<T, E>`** for fallible operations
- **Provide context** with `.context()` for error chains
- **Use specific error types** rather than generic errors
- **Handle errors gracefully** with helpful user messages

### Performance

- **Benchmark performance-critical changes**
- **Use profiling tools** to identify bottlenecks
- **Consider memory usage** for large datasets
- **Test with realistic data sizes**

## Architecture Guidelines

### Code Organization

```
src/
├── main.rs          # CLI entry point
├── lib.rs           # Public library interface
├── cli.rs           # Command-line interface
├── config.rs        # Configuration management
├── processor.rs     # Main processing engine
├── schema.rs        # Schema detection and management
├── header.rs        # BADC-CSV header parsing
├── models.rs        # Data structures
└── error.rs         # Error types
```

### Design Principles

1. **Separation of concerns** - Each module has a clear responsibility
2. **Dependency injection** - Components accept dependencies for testability
3. **Error propagation** - Use `?` operator and error chains
4. **Async by default** - Use async/await for I/O operations
5. **Memory efficiency** - Stream data when possible

### Adding New Features

When adding features, consider:

1. **Backward compatibility** - Don't break existing workflows
2. **Configuration** - Make features configurable when appropriate
3. **Testing** - Add comprehensive tests including edge cases
4. **Documentation** - Update user and developer documentation
5. **Performance** - Measure impact on processing speed and memory

## Testing Guidelines

### Test Structure

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_feature_works() {
        // Arrange
        let input = create_test_input();
        
        // Act
        let result = function_under_test(input);
        
        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_value);
    }
}
```

### Test Data

- **Use minimal test datasets** in `tests/data/`
- **Create synthetic data** when real data isn't available
- **Clean up temporary files** in test teardown
- **Use consistent test fixtures** across tests

### Async Testing

```rust
#[tokio::test]
async fn test_async_function() {
    let result = async_function().await;
    assert!(result.is_ok());
}
```

## Performance Optimization

### Profiling

Use these tools to identify performance bottlenecks:

```bash
# CPU profiling
cargo install flamegraph
sudo cargo flamegraph --bin midas -- test-dataset

# Memory profiling
cargo install heaptrack
heaptrack cargo run -- test-dataset

# Benchmarking
cargo bench
```

### Common Optimizations

1. **Reduce allocations** - Use references and iterators
2. **Batch operations** - Process multiple items together
3. **Parallel processing** - Use rayon for CPU-intensive tasks
4. **Streaming** - Process data without loading everything into memory
5. **Caching** - Cache expensive computations

## Documentation

### Code Documentation

```rust
/// Processes a MIDAS dataset and converts it to Parquet format.
///
/// # Arguments
/// * `dataset_path` - Path to the MIDAS dataset directory
/// * `config` - Processing configuration options
///
/// # Returns
/// * `Ok(stats)` - Processing statistics on success
/// * `Err(error)` - Error details on failure
///
/// # Example
/// ```
/// let stats = process_dataset(&path, &config).await?;
/// println!("Processed {} files", stats.files_processed);
/// ```
pub async fn process_dataset(
    dataset_path: &Path,
    config: &Config,
) -> Result<ProcessingStats> {
    // Implementation
}
```

### User Documentation

Update these files for user-facing changes:
- `README.md` - Main documentation
- `CHANGELOG.md` - Version history
- `docs/` - Detailed guides (if applicable)

## Pull Request Guidelines

### Before Submitting

- [ ] **All tests pass** (`cargo test`)
- [ ] **No clippy warnings** (`cargo clippy`)
- [ ] **Code is formatted** (`cargo fmt`)
- [ ] **Documentation is updated**
- [ ] **CHANGELOG.md is updated** (for significant changes)

### PR Description Template

```markdown
## Summary
Brief description of changes

## Changes
- List of specific changes made
- Include any breaking changes

## Testing
- How the changes were tested
- Any manual testing performed

## Related Issues
- Fixes #123
- Related to #456
```

### Review Process

1. **Automated checks** must pass (CI/CD)
2. **Code review** by maintainers
3. **Testing** with real datasets (if applicable)
4. **Documentation review** for clarity
5. **Merge** after approval

## Release Process

### Version Numbering

We follow [Semantic Versioning](https://semver.org/):
- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

### Release Checklist

1. **Update version** in `Cargo.toml`
2. **Update CHANGELOG.md** with release notes
3. **Tag release** in git
4. **Build and test** release artifacts
5. **Publish to crates.io** (maintainers only)

## Community

### Getting Help

- **GitHub Discussions** - Ask questions and share ideas
- **GitHub Issues** - Report bugs and request features
- **Documentation** - Check existing docs first

### Staying Updated

- **Watch the repository** for notifications
- **Follow the changelog** for new features
- **Join discussions** to influence direction

## Recognition

We recognize contributors in several ways:
- **Contributor list** in README.md
- **Release notes** mentioning significant contributions
- **GitHub contributor graph** showing activity

Thank you for contributing to MIDAS Converter!