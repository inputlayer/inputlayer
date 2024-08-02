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
