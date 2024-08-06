//! Comprehensive Example Verification Tests
//!
//! This test suite verifies that ALL examples in the project work correctly:
//! - All Rust examples compile and execute
//! - All Datalog test examples have valid syntax
//! - All test examples have corresponding .out snapshot files

use std::fs;
use std::path::Path;

/// Helper to find all .dl files recursively in a directory
fn find_dl_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut dl_files = Vec::new();

    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_dir() {
                dl_files.extend(find_dl_files(&path));
            } else if path.extension().and_then(|s| s.to_str()) == Some("dl") {
                dl_files.push(path);
            }
        }
    }

    dl_files.sort();
    dl_files
}

// Datalog Example Structure Tests
#[test]
fn test_all_datalog_examples_present() {
    let examples_dir = Path::new("examples/datalog");

    if !examples_dir.exists() {
        panic!("examples/datalog directory does not exist!");
    }

    let dl_files = find_dl_files(examples_dir);

    // Expected test categories
    let expected_categories = vec![
        "01_knowledge_graph",
        "02_relations",
        "04_session",
        "06_joins",
        "07_filters",
        "08_negation",
        "09_recursion",
        "10_edge_cases",
        "11_types",
        "12_errors",
    ];

    // Verify each category directory exists
    for category in &expected_categories {
        let category_path = examples_dir.join(category);
        assert!(
            category_path.exists(),
            "Missing test category directory: {}",
            category
        );
    }

    // Should have at least one test file per category (26 total based on current structure)
    assert!(
        dl_files.len() >= 20,
        "Expected at least 20 test files, found {}",
        dl_files.len()
    );

    println!(
        "Found {} datalog test files across {} categories",
        dl_files.len(),
        expected_categories.len()
    );
}

#[test]
fn test_all_test_files_have_output_snapshots() {
    let examples_dir = Path::new("examples/datalog");
    let dl_files = find_dl_files(examples_dir);

    let mut missing_outputs = Vec::new();

    for dl_file in &dl_files {
        let out_file = dl_file.with_extension("dl.out");
        if !out_file.exists() {
            missing_outputs.push(dl_file.display().to_string());
        }
    }

    assert!(
        missing_outputs.is_empty(),
        "The following test files are missing .dl.out snapshot files:\n  {}",
        missing_outputs.join("\n  ")
    );
}

#[test]
fn test_all_rust_examples_present() {
    let examples_dir = Path::new("examples/rust");

    if !examples_dir.exists() {
        panic!("examples/rust directory does not exist!");
    }

    let entries = fs::read_dir(examples_dir).expect("Failed to read examples/rust directory");

    let mut rs_files: Vec<String> = entries
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension()? == "rs" {
                Some(path.file_name()?.to_string_lossy().to_string())
            } else {
                None
            }
        })
        .collect();

    rs_files.sort();

    // Should have at least 4 Rust examples
    assert!(
        rs_files.len() >= 4,
        "Expected at least 4 Rust examples, found {}. Files: {:?}",
        rs_files.len(),
        rs_files
    );
}

// Example Content Validation
#[test]
