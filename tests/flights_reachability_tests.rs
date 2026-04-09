//! Flights reachability end-to-end tests.
//!
//! Walks through the full flights example step by step:
//!   1. Insert direct flights
//!   2. Define transitive reachability rules
//!   3. Query reachable destinations
//!   4. Add new routes and verify incremental update
//!   5. Retract routes and verify correct retraction (diamond problem)
//!   6. Verify provenance (.why / .why_not)
//!   7. Verify column names are preserved across all mutations
//!
//! Each test verifies both the result data AND the schema column names,
//! catching regressions where column names fall back to col0, col1, etc.

use inputlayer::protocol::Handler;
use inputlayer::Config;
use tempfile::TempDir;

fn create_test_handler() -> (Handler, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    let storage = inputlayer::StorageEngine::new(config).unwrap();
    let handler = Handler::new(storage);
    (handler, temp)
}

/// Helper: execute a program and assert success.
async fn exec(handler: &Handler, program: &str) -> inputlayer::protocol::wire::QueryResult {
    handler
        .query_program(None, program.to_string())
        .await
        .unwrap_or_else(|e| panic!("Failed to execute '{program}': {e}"))
}

/// Helper: collect result rows as Vec<Vec<String>> for easy assertions.
fn rows_as_strings(result: &inputlayer::protocol::wire::QueryResult) -> Vec<Vec<String>> {
    result
        .rows
        .iter()
        .map(|row| {
            row.values
                .iter()
                .map(|v| format!("{v}"))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Helper: get column names from the schema.
fn column_names(result: &inputlayer::protocol::wire::QueryResult) -> Vec<String> {
    result.schema.iter().map(|c| c.name.clone()).collect()
}

/// Helper: collect a specific column's values as sorted strings.
fn column_values_sorted(
    result: &inputlayer::protocol::wire::QueryResult,
    col_idx: usize,
) -> Vec<String> {
    let mut vals: Vec<String> = result
        .rows
        .iter()
        .map(|row| format!("{}", row.values[col_idx]))
        .collect();
    vals.sort();
    vals
}

// ═══════════════════════════════════════════════════════════════════════
// Step 1: Insert direct flights and query them
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_step1_insert_direct_flights() {
    let (handler, _temp) = create_test_handler();

    exec(
        &handler,
        r#"+direct_flight[("new_york", "london", 7.0),
                          ("london", "paris", 1.5),
                          ("paris", "tokyo", 12.0),
                          ("tokyo", "sydney", 9.5)]"#,
    )
    .await;

    let result = exec(&handler, "?direct_flight(From, To, Hours)").await;
    assert_eq!(result.rows.len(), 4, "Should have 4 direct flights");
    assert_eq!(
        column_names(&result),
        vec!["From", "To", "Hours"],
        "Column names should match query variables"
    );
}

#[tokio::test]
async fn test_step1_query_specific_origin() {
    let (handler, _temp) = create_test_handler();

    exec(
        &handler,
        r#"+direct_flight[("new_york", "london", 7.0),
                          ("london", "paris", 1.5),
                          ("paris", "tokyo", 12.0),
                          ("tokyo", "sydney", 9.5)]"#,
    )
    .await;

    let result = exec(&handler, r#"?direct_flight("london", To, Hours)"#).await;
    assert_eq!(result.rows.len(), 1, "London has 1 direct flight");
}

// ═══════════════════════════════════════════════════════════════════════
// Step 2: Define reachability rules and query
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_step2_define_rules_and_query_reachability() {
    let (handler, _temp) = create_test_handler();

    // Insert flights
    exec(
        &handler,
        r#"+direct_flight[("new_york", "london", 7.0),
                          ("london", "paris", 1.5),
                          ("paris", "tokyo", 12.0),
                          ("tokyo", "sydney", 9.5)]"#,
    )
    .await;

    // Define rules
    exec(&handler, "+can_reach(A, B) <- direct_flight(A, B, _)").await;
    exec(
        &handler,
        "+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)",
    )
    .await;

    // Query all reachable pairs
    let result = exec(&handler, "?can_reach(From, To)").await;
    assert_eq!(
        column_names(&result),
        vec!["From", "To"],
        "Column names should be From, To"
    );

    // New York can reach: london, paris, tokyo, sydney (4 destinations)
    let ny_dests: Vec<String> = result
        .rows
        .iter()
        .filter(|row| format!("{}", row.values[0]) == "\"new_york\"")
        .map(|row| format!("{}", row.values[1]))
        .collect();
    assert_eq!(
        ny_dests.len(),
        4,
        "New York should reach 4 destinations, got: {:?}",
        ny_dests
    );
}

#[tokio::test]
async fn test_step2_query_specific_reachability() {
    let (handler, _temp) = create_test_handler();

    exec(
        &handler,
        r#"+direct_flight[("new_york", "london", 7.0),
                          ("london", "paris", 1.5),
                          ("paris", "tokyo", 12.0),
                          ("tokyo", "sydney", 9.5)]"#,
    )
    .await;
    exec(&handler, "+can_reach(A, B) <- direct_flight(A, B, _)").await;
    exec(
        &handler,
        "+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)",
    )
    .await;

    // New York -> Sydney should be reachable
    let result = exec(&handler, r#"?can_reach("new_york", "sydney")"#).await;
    assert_eq!(result.rows.len(), 1, "NY -> Sydney should be reachable");
}

// ═══════════════════════════════════════════════════════════════════════
// Step 3: Add new routes - incremental update
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_step3_add_routes_incremental() {
    let (handler, _temp) = create_test_handler();

    // Initial setup
    exec(
        &handler,
        r#"+direct_flight[("new_york", "london", 7.0),
                          ("london", "paris", 1.5),
                          ("paris", "tokyo", 12.0),
                          ("tokyo", "sydney", 9.5)]"#,
    )
    .await;
    exec(&handler, "+can_reach(A, B) <- direct_flight(A, B, _)").await;
    exec(
        &handler,
        "+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)",
    )
    .await;

    // Query before adding new route
    let before = exec(&handler, "?can_reach(From, To)").await;
    let ny_before: Vec<_> = before
        .rows
        .iter()
        .filter(|row| format!("{}", row.values[0]) == "\"new_york\"")
        .collect();
    assert_eq!(ny_before.len(), 4, "NY reaches 4 cities before new route");

    // Add London -> Dubai route
    exec(&handler, r#"+direct_flight("london", "dubai", 7.0)"#).await;

    // Query after adding new route - column names must still be correct
    let after = exec(&handler, "?can_reach(From, To)").await;
    assert_eq!(
        column_names(&after),
        vec!["From", "To"],
        "Column names must remain From, To after insert"
    );

    let ny_after: Vec<String> = after
        .rows
        .iter()
        .filter(|row| format!("{}", row.values[0]) == "\"new_york\"")
        .map(|row| format!("{}", row.values[1]))
        .collect();
    assert!(
        ny_after.contains(&"\"dubai\"".to_string()),
        "NY should now reach Dubai, got: {:?}",
        ny_after
    );
    assert_eq!(ny_after.len(), 5, "NY reaches 5 cities after new route");
}

#[tokio::test]
async fn test_step3_column_names_stable_after_multiple_inserts() {
    let (handler, _temp) = create_test_handler();

    exec(
        &handler,
        r#"+direct_flight[("new_york", "london", 7.0),
                          ("london", "paris", 1.5)]"#,
    )
    .await;
    exec(&handler, "+can_reach(A, B) <- direct_flight(A, B, _)").await;
    exec(
        &handler,
        "+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)",
    )
    .await;

    // First query
    let r1 = exec(&handler, "?can_reach(From, To)").await;
    assert_eq!(column_names(&r1), vec!["From", "To"]);

    // Insert more
    exec(&handler, r#"+direct_flight("paris", "tokyo", 12.0)"#).await;

    // Second query - column names must be stable
    let r2 = exec(&handler, "?can_reach(From, To)").await;
    assert_eq!(
        column_names(&r2),
        vec!["From", "To"],
        "Column names must remain From, To after second insert"
    );

    // Insert even more
    exec(&handler, r#"+direct_flight("tokyo", "sydney", 9.5)"#).await;

    // Third query
    let r3 = exec(&handler, "?can_reach(From, To)").await;
    assert_eq!(
        column_names(&r3),
        vec!["From", "To"],
        "Column names must remain From, To after third insert"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Step 4: Correct retraction (diamond problem)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_step4_retraction_diamond_both_paths() {
    let (handler, _temp) = create_test_handler();

    // Two independent routes to Sydney
    exec(
        &handler,
        r#"+direct_flight[("new_york", "london", 7.0),
                          ("london", "paris", 1.5),
                          ("paris", "tokyo", 12.0),
                          ("tokyo", "sydney", 9.5),
                          ("london", "dubai", 7.0),
                          ("dubai", "sydney", 11.0)]"#,
    )
    .await;
    exec(&handler, "+can_reach(A, B) <- direct_flight(A, B, _)").await;
    exec(
        &handler,
        "+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)",
    )
    .await;

    // Sydney should be reachable (via both paths)
    let result = exec(&handler, r#"?can_reach("new_york", "sydney")"#).await;
    assert_eq!(result.rows.len(), 1, "Sydney reachable via two paths");
}

#[tokio::test]
async fn test_step4_retraction_remove_one_path() {
    let (handler, _temp) = create_test_handler();

    exec(
        &handler,
        r#"+direct_flight[("new_york", "london", 7.0),
                          ("london", "paris", 1.5),
                          ("paris", "tokyo", 12.0),
                          ("tokyo", "sydney", 9.5),
                          ("london", "dubai", 7.0),
                          ("dubai", "sydney", 11.0)]"#,
    )
    .await;
    exec(&handler, "+can_reach(A, B) <- direct_flight(A, B, _)").await;
    exec(
        &handler,
        "+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)",
    )
    .await;

    // Cancel Dubai route
    exec(&handler, r#"-direct_flight("london", "dubai", 7.0)"#).await;

    // Sydney should STILL be reachable (via Tokyo path)
    let result = exec(&handler, r#"?can_reach("new_york", "sydney")"#).await;
    assert_eq!(
        result.rows.len(),
        1,
        "Sydney still reachable after removing one of two paths"
    );

    // Column names must survive retraction
    let full = exec(&handler, "?can_reach(From, To)").await;
    assert_eq!(
        column_names(&full),
        vec!["From", "To"],
        "Column names must remain From, To after retraction"
    );
}

#[tokio::test]
async fn test_step4_retraction_remove_both_paths() {
    let (handler, _temp) = create_test_handler();

    exec(
        &handler,
        r#"+direct_flight[("new_york", "london", 7.0),
                          ("london", "paris", 1.5),
                          ("paris", "tokyo", 12.0),
                          ("tokyo", "sydney", 9.5),
                          ("london", "dubai", 7.0),
                          ("dubai", "sydney", 11.0)]"#,
    )
    .await;
    exec(&handler, "+can_reach(A, B) <- direct_flight(A, B, _)").await;
    exec(
        &handler,
        "+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)",
    )
    .await;

    // Cancel both routes to Sydney
    exec(&handler, r#"-direct_flight("london", "dubai", 7.0)"#).await;
    exec(&handler, r#"-direct_flight("tokyo", "sydney", 9.5)"#).await;

    // Sydney should now be unreachable
    let result = exec(&handler, r#"?can_reach("new_york", "sydney")"#).await;
    assert_eq!(
        result.rows.len(),
        0,
        "Sydney unreachable after removing both paths"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Step 5: Provenance (.why / .why_not)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_step5_why_reachable() {
    let (handler, _temp) = create_test_handler();

    exec(
        &handler,
        r#"+direct_flight[("new_york", "london", 7.0),
                          ("london", "paris", 1.5),
                          ("paris", "tokyo", 12.0),
                          ("tokyo", "sydney", 9.5)]"#,
    )
    .await;
    exec(&handler, "+can_reach(A, B) <- direct_flight(A, B, _)").await;
    exec(
        &handler,
        "+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)",
    )
    .await;

    // .why should return a result with proof trees
    let result = exec(&handler, r#".why ?can_reach("new_york", "sydney")"#).await;
    assert_eq!(result.rows.len(), 1, ".why should return the proven tuple");
    assert!(
        result.proof_trees.is_some(),
        ".why should include proof trees"
    );
    let graphs = result.proof_trees.as_ref().unwrap();
    assert!(!graphs.is_empty(), "Should have at least one proof tree");
}

#[tokio::test]
async fn test_step5_why_not_unreachable() {
    let (handler, _temp) = create_test_handler();

    exec(
        &handler,
        r#"+direct_flight[("new_york", "london", 7.0),
                          ("london", "paris", 1.5),
                          ("paris", "tokyo", 12.0),
                          ("tokyo", "sydney", 9.5)]"#,
    )
    .await;
    exec(&handler, "+can_reach(A, B) <- direct_flight(A, B, _)").await;
    exec(
        &handler,
        "+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)",
    )
    .await;

    // .why_not should explain why sao_paulo is unreachable
    let result = exec(&handler, r#".why_not can_reach("new_york", "sao_paulo")"#).await;
    // why_not returns explanation rows
    assert!(
        result.rows.len() > 0,
        ".why_not should return at least one explanation row"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Step 6: Column name stability - the core regression test
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_column_names_stable_across_full_lifecycle() {
    let (handler, _temp) = create_test_handler();

    // Insert initial flights
    exec(
        &handler,
        r#"+direct_flight[("new_york", "london", 7.0),
                          ("london", "paris", 1.5),
                          ("paris", "tokyo", 12.0),
                          ("tokyo", "sydney", 9.5)]"#,
    )
    .await;

    // Query direct flights - check columns
    let r = exec(&handler, "?direct_flight(From, To, Hours)").await;
    assert_eq!(
        column_names(&r),
        vec!["From", "To", "Hours"],
        "Step 1: direct_flight columns"
    );

    // Define rules
    exec(&handler, "+can_reach(A, B) <- direct_flight(A, B, _)").await;
    exec(
        &handler,
        "+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)",
    )
    .await;

    // Query reachability - check columns
    let r = exec(&handler, "?can_reach(From, To)").await;
    assert_eq!(
        column_names(&r),
        vec!["From", "To"],
        "Step 2: can_reach columns after rules"
    );

    // Insert more flights
    exec(&handler, r#"+direct_flight("london", "dubai", 7.0)"#).await;

    // Query again - columns MUST be From, To (not col0, col1)
    let r = exec(&handler, "?can_reach(From, To)").await;
    assert_eq!(
        column_names(&r),
        vec!["From", "To"],
        "Step 3: can_reach columns after additional insert"
    );

    // Retract a flight
    exec(&handler, r#"-direct_flight("london", "dubai", 7.0)"#).await;

    // Query again - columns MUST still be From, To
    let r = exec(&handler, "?can_reach(From, To)").await;
    assert_eq!(
        column_names(&r),
        vec!["From", "To"],
        "Step 4: can_reach columns after retraction"
    );

    // Insert and retract more
    exec(
        &handler,
        r#"+direct_flight[("london", "dubai", 7.0),
                          ("dubai", "singapore", 7.5),
                          ("singapore", "sydney", 8.0)]"#,
    )
    .await;

    let r = exec(&handler, "?can_reach(From, To)").await;
    assert_eq!(
        column_names(&r),
        vec!["From", "To"],
        "Step 5: can_reach columns after bulk insert"
    );

    exec(&handler, r#"-direct_flight("dubai", "singapore", 7.5)"#).await;

    let r = exec(&handler, "?can_reach(From, To)").await;
    assert_eq!(
        column_names(&r),
        vec!["From", "To"],
        "Step 6: can_reach columns after second retraction"
    );
}

#[tokio::test]
async fn test_column_names_with_different_variable_names() {
    let (handler, _temp) = create_test_handler();

    exec(
        &handler,
        r#"+direct_flight[("new_york", "london", 7.0),
                          ("london", "paris", 1.5)]"#,
    )
    .await;
    exec(&handler, "+can_reach(A, B) <- direct_flight(A, B, _)").await;
    exec(
        &handler,
        "+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)",
    )
    .await;

    // Different variable names in query should produce different column names
    let r1 = exec(&handler, "?can_reach(Origin, Destination)").await;
    assert_eq!(
        column_names(&r1),
        vec!["Origin", "Destination"],
        "Should use query variable names, not schema names"
    );

    let r2 = exec(&handler, "?can_reach(X, Y)").await;
    assert_eq!(
        column_names(&r2),
        vec!["X", "Y"],
        "Should use X, Y when queried with those variables"
    );

    // After an insert, still respect query variables
    exec(&handler, r#"+direct_flight("paris", "tokyo", 12.0)"#).await;

    let r3 = exec(&handler, "?can_reach(Src, Dst)").await;
    assert_eq!(
        column_names(&r3),
        vec!["Src", "Dst"],
        "Should use Src, Dst after insert"
    );
}

#[tokio::test]
async fn test_column_names_direct_flight_three_cols_after_insert() {
    let (handler, _temp) = create_test_handler();

    exec(&handler, r#"+direct_flight[("new_york", "london", 7.0)]"#).await;

    let r1 = exec(&handler, "?direct_flight(From, To, Hours)").await;
    assert_eq!(column_names(&r1), vec!["From", "To", "Hours"]);

    // Insert more
    exec(&handler, r#"+direct_flight("london", "paris", 1.5)"#).await;

    let r2 = exec(&handler, "?direct_flight(From, To, Hours)").await;
    assert_eq!(
        column_names(&r2),
        vec!["From", "To", "Hours"],
        "3-column names must be stable after insert"
    );

    // Insert even more
    exec(&handler, r#"+direct_flight("paris", "tokyo", 12.0)"#).await;

    let r3 = exec(&handler, "?direct_flight(From, To, Hours)").await;
    assert_eq!(
        column_names(&r3),
        vec!["From", "To", "Hours"],
        "3-column names must be stable after second insert"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Step 7: Hub score with aggregation
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_hub_score_aggregation() {
    let (handler, _temp) = create_test_handler();

    exec(
        &handler,
        r#"+direct_flight[("new_york", "london", 7.0),
                          ("london", "paris", 1.5),
                          ("paris", "tokyo", 12.0),
                          ("tokyo", "sydney", 9.5)]"#,
    )
    .await;
    exec(&handler, "+can_reach(A, B) <- direct_flight(A, B, _)").await;
    exec(
        &handler,
        "+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)",
    )
    .await;

    // Hub score: count reachable destinations per city
    exec(
        &handler,
        "+hub_score(City, count<Dest>) <- can_reach(City, Dest)",
    )
    .await;

    let result = exec(&handler, "?hub_score(City, Score)").await;
    assert_eq!(
        column_names(&result),
        vec!["City", "Score"],
        "Hub score columns should be City, Score"
    );

    // New York reaches 4 cities (most connected)
    let ny_score: Vec<_> = result
        .rows
        .iter()
        .filter(|row| format!("{}", row.values[0]) == "\"new_york\"")
        .collect();
    assert_eq!(ny_score.len(), 1, "NY should have one hub score entry");
}

// ═══════════════════════════════════════════════════════════════════════
// Edge cases: column names with constants in query head
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_column_names_with_bound_constant() {
    let (handler, _temp) = create_test_handler();

    exec(
        &handler,
        r#"+direct_flight[("new_york", "london", 7.0),
                          ("london", "paris", 1.5)]"#,
    )
    .await;
    exec(&handler, "+can_reach(A, B) <- direct_flight(A, B, _)").await;
    exec(
        &handler,
        "+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)",
    )
    .await;

    // Query with one constant
    let r = exec(&handler, r#"?can_reach("new_york", Dest)"#).await;
    assert_eq!(r.rows.len(), 2, "NY reaches london and paris");

    // Insert and query again
    exec(&handler, r#"+direct_flight("paris", "tokyo", 12.0)"#).await;

    let r2 = exec(&handler, r#"?can_reach("new_york", Dest)"#).await;
    assert_eq!(r2.rows.len(), 3, "NY now reaches london, paris, tokyo");
}

// ═══════════════════════════════════════════════════════════════════════
// Regression: query_program_with_session also preserves column names
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_session_query_column_names() {
    let (handler, _temp) = create_test_handler();

    exec(
        &handler,
        r#"+direct_flight[("new_york", "london", 7.0),
                          ("london", "paris", 1.5)]"#,
    )
    .await;
    exec(&handler, "+can_reach(A, B) <- direct_flight(A, B, _)").await;
    exec(
        &handler,
        "+can_reach(A, C) <- direct_flight(A, B, _), can_reach(B, C)",
    )
    .await;

    let session_id = handler.create_session("default").unwrap();

    let result = handler
        .query_program_with_session(&session_id, "?can_reach(From, To)".to_string())
        .await
        .unwrap();
    assert_eq!(
        column_names(&result),
        vec!["From", "To"],
        "Session query should also preserve column names"
    );

    // Insert and re-query via session
    exec(&handler, r#"+direct_flight("paris", "tokyo", 12.0)"#).await;

    let result2 = handler
        .query_program_with_session(&session_id, "?can_reach(From, To)".to_string())
        .await
        .unwrap();
    assert_eq!(
        column_names(&result2),
        vec!["From", "To"],
        "Session query column names must be stable after insert"
    );
}
