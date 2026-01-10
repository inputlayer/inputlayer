//! Storage Engine Demonstration
//!
//! This example demonstrates the multi-database storage engine with persistence.
//!
//! Features shown:
//! - Creating multiple databases
//! - Switching between databases
//! - Inserting and querying data
//! - Persisting data to disk
//! - Loading data from disk

use inputlayer::{Config, StorageEngine};
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== InputLayer Storage Engine Demo ===\n");

    // Use temporary directory for demo (idempotent - can run multiple times)
    let temp_dir = TempDir::new()?;
    let mut config = Config::default();
    config.storage.data_dir = temp_dir.path().to_path_buf();

    println!("Data directory: {:?}", config.storage.data_dir);
    println!(
        "Thread pool size: {} (0 = all CPUs)\n",
        config.storage.performance.num_threads
    );

    // Create storage engine
    let mut storage = StorageEngine::new(config)?;

    println!(
        "Available CPUs for parallel execution: {}\n",
        storage.num_cpus()
    );

    // ========================================================================
    // Demo 1: Create Multiple Databases
    // ========================================================================
    println!("--- Demo 1: Creating Multiple Databases ---");

    storage.create_knowledge_graph("analytics")?;
    storage.create_knowledge_graph("staging")?;
    storage.create_knowledge_graph("production")?;

    let databases = storage.list_knowledge_graphs();
    println!("Created databases: {:?}\n", databases);

    // ========================================================================
    // Demo 2: Insert Data into Analytics Database
    // ========================================================================
    println!("--- Demo 2: Working with Analytics Database ---");

    storage.use_knowledge_graph("analytics")?;
    println!(
        "Switched to database: {}",
        storage.current_knowledge_graph().unwrap()
    );

    // Insert edge data
    storage.insert(
        "edge",
        vec![(1, 2), (2, 3), (3, 4), (4, 5), (2, 6), (6, 7), (7, 8)],
    )?;
    println!("Inserted 7 edges into 'edge' relation");

    // Insert person data
    storage.insert(
        "person",
        vec![(1, 100), (2, 200), (3, 300), (4, 400), (5, 500)],
    )?;
    println!("Inserted 5 tuples into 'person' relation");

    // Query edge data
    let results = storage.execute_query("result(x,y) :- edge(x,y).")?;
    println!("\nQuery: result(x,y) :- edge(x,y).");
    println!("Results: {} tuples", results.len());
    println!("Sample: {:?}", &results[0..3.min(results.len())]);

    // Query with filter
    let results = storage.execute_query("result(x,y) :- edge(x,y), x > 2.")?;
    println!("\nQuery: result(x,y) :- edge(x,y), x > 2.");
    println!("Results: {} tuples", results.len());
    println!("Sample: {:?}", results);

    // ========================================================================
    // Demo 3: Database Isolation
    // ========================================================================
    println!("\n--- Demo 3: Database Isolation ---");

    storage.create_knowledge_graph("isolated")?;
    storage.use_knowledge_graph("isolated")?;
    println!(
        "Switched to database: {}",
        storage.current_knowledge_graph().unwrap()
    );

    // Try to query edge (should fail - no data in this database)
    match storage.execute_query("result(x,y) :- edge(x,y).") {
        Ok(results) => println!("Unexpected success with {} results", results.len()),
        Err(e) => println!("Expected error (database isolation works): {}", e),
    }

    // Add some data to isolated database
    storage.insert("test", vec![(10, 20), (20, 30)])?;
    let results = storage.execute_query("result(x,y) :- test(x,y).")?;
    println!(
        "Inserted and queried 'test' relation: {} tuples",
        results.len()
    );

    // ========================================================================
    // Demo 4: Persistence
    // ========================================================================
    println!("\n--- Demo 4: Persistence ---");

    storage.use_knowledge_graph("analytics")?;
    storage.save_knowledge_graph("analytics")?;
    println!("Saved 'analytics' database to disk");

    storage.save_all()?;
    println!("Saved all databases to disk");

    // ========================================================================
    // Demo 5: Explicit Database Parameter API
    // ========================================================================
    println!("\n--- Demo 5: Explicit Database API ---");

    // Insert into specific database without switching
    storage.insert_into("staging", "data", vec![(1, 1), (2, 2), (3, 3)])?;
    println!("Inserted into 'staging' database without switching");

    // Query specific database
    let results = storage.execute_query_on("staging", "result(x,y) :- data(x,y).")?;
    println!("Queried 'staging' database: {} tuples", results.len());

    println!(
        "\nCurrent database is still: {}",
        storage.current_knowledge_graph().unwrap()
    );

    // ========================================================================
    // Summary
    // ========================================================================
    println!("\n--- Summary ---");
    let databases = storage.list_knowledge_graphs();
    println!("Total databases: {}", databases.len());
    for db in &databases {
        println!("  - {}", db);
    }

    println!("\n✅ Storage engine demo completed successfully!");

    Ok(())
}
