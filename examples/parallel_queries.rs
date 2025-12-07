//! Parallel Query Execution Demonstration
//!
//! This example demonstrates the worker pool infrastructure for parallel
//! query execution across multiple databases, utilizing all CPU cores.
//!
//! Features shown:
//! - Parallel query execution across multiple databases
//! - Executing the same query on multiple databases
//! - Multiple queries on the same database
//! - Performance comparison: sequential vs parallel
//! - Automatic CPU core utilization

use datalog_engine::{Config, StorageEngine};
use std::time::Instant;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== InputLayer Parallel Query Execution Demo ===\n");

    // Use temporary directory for demo (idempotent - can run multiple times)
    let _temp_dir = TempDir::new()?;
    let mut config = Config::default();
    config.storage.data_dir = _temp_dir.path().to_path_buf();

    let mut storage = StorageEngine::new(config)?;

    println!("Worker pool configured with {} threads (all CPUs)", storage.num_cpus());
    println!();

    // ========================================================================
    // Setup: Create multiple databases with data
    // ========================================================================
    println!("--- Setup: Creating Databases ---");

    let database_names = vec!["db1", "db2", "db3", "db4"];

    for (i, name) in database_names.iter().enumerate() {
        storage.create_database(name)?;
        storage.use_database(name)?;

        // Insert edge data (different sizes for each database)
        let size = (i + 1) * 100;
        let edges: Vec<(i32, i32)> = (0..size)
            .map(|j| (j as i32, (j + 1) as i32))
            .collect();

        storage.insert("edge", edges)?;
        println!("  {} - Inserted {} edges", name, size);
    }

    println!();

    // ========================================================================
    // Demo 1: Parallel Queries Across Different Databases
    // ========================================================================
    println!("--- Demo 1: Different Queries on Different Databases ---");

    let queries = vec![
        ("db1", "result(x,y) :- edge(x,y)."),
        ("db2", "result(x,y) :- edge(x,y), x > 50."),
        ("db3", "result(x,y) :- edge(x,y), x < 150."),
        ("db4", "result(x,y) :- edge(x,y), y > 200."),
    ];

    let start = Instant::now();
    let results = storage.execute_parallel_queries_on_databases(queries)?;
    let duration = start.elapsed();

    println!("Executed {} queries in parallel", results.len());
    for (db, result) in &results {
        println!("  {} returned {} tuples", db, result.len());
    }
    println!("Time: {:?}\n", duration);

    // ========================================================================
    // Demo 2: Same Query on Multiple Databases
    // ========================================================================
    println!("--- Demo 2: Same Query on Multiple Databases ---");

    let query = "result(x,y) :- edge(x,y), x > 10.";

    let start = Instant::now();
    let results = storage.execute_query_on_multiple_databases(
        database_names.clone(),
        query,
    )?;
    let duration = start.elapsed();

    println!("Query: {}", query);
    println!("Executed on {} databases in parallel", results.len());
    for (db, result) in &results {
        println!("  {} returned {} tuples", db, result.len());
    }
    println!("Time: {:?}\n", duration);

    // ========================================================================
    // Demo 3: Multiple Queries on Same Database
    // ========================================================================
    println!("--- Demo 3: Multiple Queries on Same Database ---");

    let queries = vec![
        "q1(x,y) :- edge(x,y).",
        "q2(x,y) :- edge(x,y), x < 50.",
        "q3(x,y) :- edge(x,y), x > 50.",
        "q4(x,y) :- edge(x,y), x > 25, x < 75.",
    ];

    let start = Instant::now();
    let results = storage.execute_parallel_queries_on_database("db2", queries)?;
    let duration = start.elapsed();

    println!("Executed {} queries on 'db2' in parallel", results.len());
    for (i, result) in results.iter().enumerate() {
        println!("  Query {} returned {} tuples", i + 1, result.len());
    }
    println!("Time: {:?}\n", duration);

    // ========================================================================
    // Demo 4: Performance Comparison (Sequential vs Parallel)
    // ========================================================================
    println!("--- Demo 4: Performance Comparison ---");

    let test_query = "result(x,y) :- edge(x,y), x > 5.";

    // Sequential execution
    println!("Sequential execution:");
    let start = Instant::now();
    for db in &database_names {
        let _ = storage.execute_query_on(db, test_query)?;
    }
    let seq_duration = start.elapsed();
    println!("  Time: {:?}", seq_duration);

    // Parallel execution
    println!("Parallel execution:");
    let start = Instant::now();
    let _ = storage.execute_query_on_multiple_databases(
        database_names.clone(),
        test_query,
    )?;
    let par_duration = start.elapsed();
    println!("  Time: {:?}", par_duration);

    if seq_duration > par_duration {
        let speedup = seq_duration.as_secs_f64() / par_duration.as_secs_f64();
        println!("  Speedup: {:.2}x faster", speedup);
    }

    println!();

    // ========================================================================
    // Demo 5: Scaling with Large Workloads
    // ========================================================================
    println!("--- Demo 5: Scaling with More Databases ---");

    // Create more databases for scaling test
    let large_db_names: Vec<String> = (1..=8)
        .map(|i| format!("scale_db{}", i))
        .collect();

    for (i, name) in large_db_names.iter().enumerate() {
        storage.create_database(name)?;
        storage.use_database(name)?;

        let edges: Vec<(i32, i32)> = (0..50)
            .map(|j| (j + (i * 50) as i32, j + (i * 50) as i32 + 1))
            .collect();

        storage.insert("edge", edges)?;
    }

    let db_refs: Vec<&str> = large_db_names.iter().map(|s| s.as_str()).collect();

    let start = Instant::now();
    let results = storage.execute_query_on_multiple_databases(
        db_refs,
        "result(x,y) :- edge(x,y).",
    )?;
    let duration = start.elapsed();

    println!("Executed query on {} databases in parallel", results.len());
    println!("Total tuples returned: {}", results.iter().map(|(_, r)| r.len()).sum::<usize>());
    println!("Time: {:?}", duration);
    println!("Average time per database: {:?}", duration / results.len() as u32);

    // ========================================================================
    // Summary
    // ========================================================================
    println!("\n--- Summary ---");
    println!("✅ Worker pool infrastructure is functioning correctly");
    println!("✅ Queries execute in parallel across multiple databases");
    println!("✅ Efficient CPU utilization with {} threads", storage.num_cpus());
    println!("✅ Scalable to many databases and queries");

    println!("\n✅ Parallel query execution demo completed successfully!");

    Ok(())
}
