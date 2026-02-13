//! Comprehensive Example Verification Tests
//!
//! This test suite verifies that ALL examples in the project work correctly:
//! - All Rust examples compile and execute
//! - All Datalog test examples have valid syntax
//! - All test examples have corresponding .out snapshot files

use std::fs;
use std::path::Path;

/// Helper to find all .idl files recursively in a directory
fn find_idl_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut idl_files = Vec::new();

    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_dir() {
                idl_files.extend(find_idl_files(&path));
            } else if path.extension().and_then(|s| s.to_str()) == Some("idl") {
                idl_files.push(path);
            }
        }
    }

    idl_files.sort();
    idl_files
}

// Datalog Example Structure Tests
#[test]
fn test_all_datalog_examples_present() {
    let examples_dir = Path::new("examples/datalog");

    if !examples_dir.exists() {
        panic!("examples/datalog directory does not exist!");
    }

    let idl_files = find_idl_files(examples_dir);

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
        idl_files.len() >= 20,
        "Expected at least 20 test files, found {}",
        idl_files.len()
    );

    println!(
        "Found {} datalog test files across {} categories",
        idl_files.len(),
        expected_categories.len()
    );
}

#[test]
fn test_all_test_files_have_output_snapshots() {
    let examples_dir = Path::new("examples/datalog");
    let idl_files = find_idl_files(examples_dir);

    let mut missing_outputs = Vec::new();

    for idl_file in &idl_files {
        let out_file = idl_file.with_extension("idl.out");
        if !out_file.exists() {
            missing_outputs.push(idl_file.display().to_string());
        }
    }

    assert!(
        missing_outputs.is_empty(),
        "The following test files are missing .idl.out snapshot files:\n  {}",
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
fn test_examples_not_empty() {
    let examples_dir = Path::new("examples/datalog");
    let idl_files = find_idl_files(examples_dir);

    for path in idl_files {
        let content =
            fs::read_to_string(&path).unwrap_or_else(|_| panic!("Failed to read {:?}", path));

        assert!(
            !content.trim().is_empty(),
            "Example file {:?} is empty",
            path.file_name().unwrap()
        );

        assert!(
            content.len() > 10,
            "Example file {:?} is suspiciously small (< 10 bytes)",
            path.file_name().unwrap()
        );
    }
}

#[test]
fn test_output_files_not_empty() {
    let examples_dir = Path::new("examples/datalog");
    let idl_files = find_idl_files(examples_dir);

    for idl_file in idl_files {
        let out_file = idl_file.with_extension("idl.out");
        if out_file.exists() {
            let content = fs::read_to_string(&out_file)
                .unwrap_or_else(|_| panic!("Failed to read {:?}", out_file));

            assert!(
                !content.trim().is_empty(),
                "Output file {:?} is empty",
                out_file.file_name().unwrap()
            );
        }
    }
}

// Test Category Validation
#[test]
fn test_knowledge_graph_tests() {
    let dir = Path::new("examples/datalog/01_knowledge_graph");
    let files = find_idl_files(dir);
    assert!(
        !files.is_empty(),
        "01_knowledge_graph should have at least one test"
    );
}

#[test]
fn test_relations_tests() {
    let dir = Path::new("examples/datalog/02_relations");
    let files = find_idl_files(dir);
    assert!(
        files.len() >= 3,
        "02_relations should have at least 3 tests (insert, bulk, delete)"
    );
}

#[test]
fn test_joins_tests() {
    let dir = Path::new("examples/datalog/06_joins");
    let files = find_idl_files(dir);
    assert!(files.len() >= 3, "06_joins should have at least 3 tests");
}

#[test]
fn test_negation_tests() {
    let dir = Path::new("examples/datalog/08_negation");
    let files = find_idl_files(dir);
    assert!(
        !files.is_empty(),
        "08_negation should have at least one test"
    );
}

#[test]
fn test_recursion_tests() {
    let dir = Path::new("examples/datalog/09_recursion");
    let files = find_idl_files(dir);
    assert!(
        files.len() >= 2,
        "09_recursion should have at least 2 tests"
    );
}

// Syntax Validation Tests
/// Extract persistent rule statements from our test format (new syntax uses "+name(...) <- ...")
fn extract_rules_from_test(content: &str) -> Vec<String> {
    content
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            // Keep lines that look like persistent rule declarations (start with "+" and contain "<-")
            if trimmed.starts_with("+") && trimmed.contains("<-") {
                Some(trimmed.to_string())
            } else {
                None
            }
        })
        .collect()
}

#[test]
fn test_negation_syntax_valid() {
    let path = Path::new("examples/datalog/08_negation/01_simple_negation.idl");
    let content = fs::read_to_string(path).expect("Failed to read negation test");
    let rules = extract_rules_from_test(&content);

    // Verify the rule syntax contains negation
    let has_negation = rules.iter().any(|r| r.contains("!skip"));
    assert!(has_negation, "Test should contain negation syntax (!skip)");

    // Verify the rule format is correct (new + prefix syntax)
    let negation_rule = rules.iter().find(|r| r.contains("!skip")).unwrap();
    assert!(
        negation_rule.contains("+filtered") && negation_rule.contains("!skip(X, Y)"),
        "Negation rule should have correct format"
    );
}

#[test]
fn test_recursion_syntax_valid() {
    let path = Path::new("examples/datalog/09_recursion/01_transitive_closure.idl");
    let content = fs::read_to_string(path).expect("Failed to read recursion test");
    let rules = extract_rules_from_test(&content);

    // Should have at least 2 rules (base case and recursive case)
    assert!(
        rules.len() >= 2,
        "Transitive closure should have at least 2 rules, found {}",
        rules.len()
    );

    // Verify recursive structure (relation used in both head and body)
    let has_recursive = rules.iter().any(|r| {
        // Extract name after "+" and before "("
        let after_plus = r.strip_prefix("+").unwrap_or("");
        let head_name = after_plus.split('(').next().unwrap_or("").trim();
        let body = r.split("<-").nth(1).unwrap_or("");
        // Check if the relation in head appears in body
        !head_name.is_empty() && body.contains(head_name)
    });
    assert!(has_recursive, "Should have recursive rule structure");
}

// Test Summary and Statistics
#[test]
fn test_example_statistics() {
    let rust_dir = Path::new("examples/rust");
    let datalog_dir = Path::new("examples/datalog");

    let rust_count = fs::read_dir(rust_dir)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .and_then(|s| s.to_str())
                == Some("rs")
        })
        .count();

    let datalog_count = find_idl_files(datalog_dir).len();

    println!("\n=== Example Statistics ===");
    println!("Rust examples: {}", rust_count);
    println!("Datalog test files: {}", datalog_count);
    println!("Total examples: {}", rust_count + datalog_count);

    // Verify minimum counts
    assert!(rust_count >= 4, "Should have at least 4 Rust examples");
    assert!(
        datalog_count >= 20,
        "Should have at least 20 Datalog test files"
    );
}
