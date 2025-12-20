//! InputLayer Client Binary - Datalog-Native Syntax
//!
//! Interactive client for InputLayer. Supports both local and RPC modes.
//!
//! ## Usage
//!
//! ```bash
//! # Local mode (direct storage engine access)
//! cargo run --bin inputlayer-client
//!
//! # Execute a Datalog script
//! cargo run --bin inputlayer-client -- --script examples/datalog/basic/same_component.dl
//!
//! # Execute script and then open REPL
//! cargo run --bin inputlayer-client -- --script examples/datalog/basic/same_component.dl --repl
//!
//! # RPC mode (connect to server)
//! cargo run --bin inputlayer-client -- --server 192.168.1.100:5433
//! ```
//!
//! ## Syntax
//!
//! ### Meta Commands (dot-prefix)
//! - `.db` - Show current database
//! - `.db list` - List all databases
//! - `.db create <name>` - Create a database
//! - `.db use <name>` - Switch to database
//! - `.db drop <name>` - Drop a database
//! - `.rel` - List relations
//! - `.rel <name>` - Describe relation
//! - `.view` - List views
//! - `.view <name>` - Describe view
//! - `.view drop <name>` - Drop view
//! - `.save` - Flush to disk
//! - `.status` - System status
//! - `.help` - Show help
//! - `.quit` - Exit
//!
//! ### Data Manipulation
//! - `+edge(1, 2).` - Insert fact
//! - `+edge[(1,2), (3,4)].` - Bulk insert
//! - `-edge(1, 2).` - Delete fact
//! - `-edge(X, Y) :- X > 5.` - Conditional delete
//!
//! ### Views (Persistent Rules)
//! - `path(X, Y) := edge(X, Y).` - Define persistent view
//!
//! ### Transient Rules & Queries
//! - `result(X, Y) :- edge(X, Y), X < Y.` - Transient rule
//! - `?- path(1, X).` - Query

use inputlayer::{
    statement::{parse_statement, DeletePattern, MetaCommand, Statement},
    value::{Tuple, Value},
    Config, StorageEngine,
};
use datalog_ast::{Rule, Term};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use std::env;
use std::fs;
use std::path::PathBuf;

/// Command-line arguments
struct Args {
    /// Path to a Datalog script to execute
    script: Option<String>,
    /// Whether to open REPL after script execution
    repl: bool,
    /// Server address for RPC mode (not yet implemented)
    server: Option<String>,
}

struct ReplState {
    storage: StorageEngine,
    /// Session-scoped transient rules (cleared on exit or database switch)
    session_rules: Vec<datalog_ast::Rule>,
}

impl ReplState {
    fn new(config: Config) -> Result<Self, String> {
        let storage = StorageEngine::new(config)
            .map_err(|e| format!("Failed to create storage engine: {}", e))?;
        Ok(Self {
            storage,
            session_rules: Vec::new(),
        })
    }

    fn prompt(&self) -> String {
        let session_indicator = if self.session_rules.is_empty() {
            ""
        } else {
            "*"  // Indicate there are session rules
        };
        match self.storage.current_database() {
            Some(db) => format!("{}{}> ", db, session_indicator),
            None => "inputlayer> ".to_string(),
        }
    }
}

fn parse_args() -> Args {
    let args: Vec<String> = env::args().collect();
    let mut result = Args {
        script: None,
        repl: false,
        server: None,
    };

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--script" | "-s" => {
                if i + 1 < args.len() {
                    result.script = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Error: --script requires a file path");
                    std::process::exit(1);
                }
            }
            "--repl" | "-r" => {
                result.repl = true;
                i += 1;
            }
            "--server" => {
                if i + 1 < args.len() {
                    result.server = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Error: --server requires an address");
                    std::process::exit(1);
                }
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            arg if arg.ends_with(".dl") => {
                // Allow script path without --script flag
                result.script = Some(arg.to_string());
                i += 1;
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                print_usage();
                std::process::exit(1);
            }
        }
    }

    result
}

fn print_usage() {
    println!("InputLayer Datalog Client");
    println!();
    println!("USAGE:");
    println!("  inputlayer-client [OPTIONS] [SCRIPT.dl]");
    println!();
    println!("OPTIONS:");
    println!("  -s, --script <FILE>   Execute a Datalog script file");
    println!("  -r, --repl            Open REPL after script execution");
    println!("      --server <ADDR>   Connect to server (not yet implemented)");
    println!("  -h, --help            Show this help message");
    println!();
    println!("EXAMPLES:");
    println!("  inputlayer-client                              # Start REPL");
    println!("  inputlayer-client script.dl                    # Execute script");
    println!("  inputlayer-client --script script.dl           # Execute script");
    println!("  inputlayer-client --script script.dl --repl    # Execute script, then REPL");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();

    // Check for RPC mode (not yet implemented)
    if args.server.is_some() {
        eprintln!("RPC mode not yet implemented. Use local mode for now.");
        eprintln!("Running in local mode...");
    }

    // Load configuration
    let config = Config::load().unwrap_or_else(|_| Config::default());
    let mut state = ReplState::new(config)?;

    // If a script is provided, execute it
    if let Some(script_path) = &args.script {
        // Always use portable relative path for reproducible CI/CD output
        // Extract "examples/datalog/..." portion if present, otherwise show basename
        let display_path = if let Some(pos) = script_path.find("examples/datalog/") {
            script_path[pos..].to_string()
        } else if let Some(pos) = script_path.find("examples/") {
            script_path[pos..].to_string()
        } else {
            // Fallback to just the filename
            std::path::Path::new(script_path)
                .file_name()
                .map(|f| f.to_string_lossy().to_string())
                .unwrap_or_else(|| script_path.clone())
        };
        println!("Executing script: {}", display_path);
        println!();

        match execute_script(&mut state, script_path) {
            Ok(()) => {
                if !args.repl {
                    // Exit after script execution unless --repl is specified
                    return Ok(());
                }
                println!();
                println!("Script completed. Entering REPL...");
                println!();
            }
            Err(e) => {
                println!("Script error: {}", e);
                if !args.repl {
                    std::process::exit(1);
                }
                println!();
                println!("Entering REPL despite errors...");
                println!();
            }
        }
    } else {
        // No script - show banner
        println!("InputLayer Datalog Client");
        println!("=========================");
        println!();
        println!("Data directory: {:?}", state.storage.config().storage.data_dir);
        println!("Current database: {:?}", state.storage.current_database());
        println!();
        println!("Type .help for syntax reference.");
        println!("Use arrow keys ↑/↓ to navigate command history.");
        println!();
    }

    // Run REPL
    run_repl(&mut state)
}

/// Strip inline comments (// ...) from a line, respecting string literals
fn strip_inline_comment(line: &str) -> &str {
    let mut in_string = false;
    let mut escape_next = false;
    let bytes = line.as_bytes();

    for i in 0..bytes.len() {
        if escape_next {
            escape_next = false;
            continue;
        }

        let c = bytes[i] as char;

        if c == '\\' && in_string {
            escape_next = true;
            continue;
        }

        if c == '"' {
            in_string = !in_string;
            continue;
        }

        // Check for // outside of string
        if !in_string && c == '/' && i + 1 < bytes.len() && bytes[i + 1] as char == '/' {
            return line[..i].trim_end();
        }
    }

    line
}

/// Check if a line looks like a complete statement (ends with . or is a meta command)
fn is_complete_statement(line: &str) -> bool {
    let stripped = strip_inline_comment(line).trim();
    if stripped.is_empty() {
        return false;
    }
    // Meta commands are complete on one line
    if stripped.starts_with('.') {
        return true;
    }
    // Regular statements end with .
    stripped.ends_with('.')
}

/// Execute a Datalog script file
fn execute_script(state: &mut ReplState, path: &str) -> Result<(), String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read script '{}': {}", path, e))?;

    let mut accumulated_line = String::new();
    let mut start_line_num = 0;

    for (line_num, line) in content.lines().enumerate() {
        let line = line.trim();

        // Skip empty lines and full-line comments
        if line.is_empty() || line.starts_with("//") || line.starts_with('#') {
            continue;
        }

        // Strip inline comment for processing
        let stripped = strip_inline_comment(line);

        // If we're starting a new statement, record the line number
        if accumulated_line.is_empty() {
            start_line_num = line_num + 1;
            accumulated_line = stripped.to_string();
        } else {
            // Continuation of a multi-line statement
            accumulated_line.push(' ');
            accumulated_line.push_str(stripped.trim());
        }

        // Check if statement is complete
        if is_complete_statement(&accumulated_line) {
            // Echo the accumulated statement
            println!("> {}", accumulated_line);

            // Parse and execute
            match parse_statement(&accumulated_line) {
                Ok(stmt) => {
                    if let Err(e) = handle_statement(state, stmt) {
                        return Err(format!("Line {}: {}", start_line_num, e));
                    }
                }
                Err(e) => {
                    return Err(format!("Line {}: Parse error: {}", start_line_num, e));
                }
            }

            // Reset for next statement
            accumulated_line.clear();
        }
    }

    // Handle any remaining incomplete statement
    if !accumulated_line.is_empty() {
        return Err(format!("Line {}: Incomplete statement (missing '.')", start_line_num));
    }

    Ok(())
}

/// Run the interactive REPL
fn run_repl(state: &mut ReplState) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize rustyline editor with history
    let mut rl = DefaultEditor::new()?;

    // Load history from file if it exists
    let history_path = get_history_path();
    if history_path.exists() {
        let _ = rl.load_history(&history_path);
    }

    loop {
        let prompt = state.prompt();
        match rl.readline(&prompt) {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                // Add to history
                let _ = rl.add_history_entry(line);

                // Parse the statement
                match parse_statement(line) {
                    Ok(stmt) => {
                        if let Err(e) = handle_statement(state, stmt) {
                            println!("Error: {}", e);
                        }
                    }
                    Err(e) => {
                        println!("Parse error: {}", e);
                        println!("Type .help for syntax reference.");
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C: just show new prompt
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                // Ctrl-D: exit
                println!("Goodbye!");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    // Save history
    let _ = rl.save_history(&history_path);

    Ok(())
}

/// Get the path to the history file
fn get_history_path() -> PathBuf {
    // Try to use ~/.inputlayer/history, fallback to current directory
    if let Some(home) = dirs_home() {
        let config_dir = home.join(".inputlayer");
        let _ = std::fs::create_dir_all(&config_dir);
        config_dir.join("history")
    } else {
        PathBuf::from(".inputlayer_history")
    }
}

/// Get home directory
fn dirs_home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

fn handle_statement(state: &mut ReplState, stmt: Statement) -> Result<(), String> {
    match stmt {
        Statement::Meta(cmd) => handle_meta_command(state, cmd),
        Statement::Insert(op) => handle_insert(state, op),
        Statement::Delete(op) => handle_delete(state, op),
        Statement::Update(op) => handle_update(state, op),
        Statement::TypeDecl(decl) => handle_type_decl(state, decl),
        Statement::RelDecl(decl) => handle_rel_decl(state, decl),
        Statement::ViewDecl(decl) => handle_view_decl(state, decl),
        Statement::SessionRule(rule) => handle_session_rule(state, rule),
        Statement::Fact(rule) => handle_fact(state, rule),
        Statement::Query(goal) => handle_query(state, goal),
    }
}

fn handle_type_decl(_state: &mut ReplState, decl: inputlayer::statement::TypeDecl) -> Result<(), String> {
    // TODO: Register type in catalog/type registry
    println!("Type '{}' declared.", decl.name);
    Ok(())
}

fn handle_rel_decl(_state: &mut ReplState, decl: inputlayer::statement::RelationDecl) -> Result<(), String> {
    // TODO: Register relation schema in catalog
    let col_str = decl.columns.iter()
        .map(|c| format!("{}: {:?}", c.name, c.col_type))
        .collect::<Vec<_>>()
        .join(", ");
    println!("Relation '{}' schema declared: ({})", decl.name, col_str);
    Ok(())
}

fn handle_view_decl(state: &mut ReplState, decl: inputlayer::statement::ViewDecl) -> Result<(), String> {
    // Convert ViewDecl to legacy ViewDef for existing handling
    let view_def = inputlayer::statement::ViewDef {
        name: decl.name.clone(),
        rule: decl.rule.clone(),
    };
    handle_view(state, view_def)
}

fn handle_session_rule(state: &mut ReplState, rule: datalog_ast::Rule) -> Result<(), String> {
    // Session rules are added as transient rules (not materialized)
    handle_transient_rule(state, rule)
}

fn handle_fact(state: &mut ReplState, rule: datalog_ast::Rule) -> Result<(), String> {
    // Facts are rules with empty body
    // Convert to insert operation
    let head = &rule.head;
    // TODO: For now, treat facts as transient rules
    handle_transient_rule(state, rule)
}

fn handle_meta_command(state: &mut ReplState, cmd: MetaCommand) -> Result<(), String> {
    match cmd {
        MetaCommand::DbShow => {
            if let Some(db) = state.storage.current_database() {
                println!("Current database: {}", db);
            } else {
                println!("No database selected. Use .db use <name> or .db create <name>");
            }
        }

        MetaCommand::DbList => {
            let databases = state.storage.list_databases();
            if databases.is_empty() {
                println!("No databases found.");
            } else {
                println!("Databases:");
                for db in databases {
                    let marker = if state.storage.current_database() == Some(&db) {
                        " *"
                    } else {
                        ""
                    };
                    println!("  {}{}", db, marker);
                }
            }
        }

        MetaCommand::DbCreate(name) => {
            state.storage.create_database(&name)
                .map_err(|e| format!("{}", e))?;
            println!("Database '{}' created.", name);
            state.storage.use_database(&name)
                .map_err(|e| format!("{}", e))?;
            println!("Switched to database: {}", name);
        }

        MetaCommand::DbUse(name) => {
            state.storage.use_database(&name)
                .map_err(|e| format!("{}", e))?;

            // Clear session rules when switching databases
            if !state.session_rules.is_empty() {
                let count = state.session_rules.len();
                state.session_rules.clear();
                println!("Switched to database: {}", name);
                println!("(Cleared {} session rule(s))", count);
            } else {
                println!("Switched to database: {}", name);
            }
        }

        MetaCommand::DbDrop(name) => {
            if state.storage.current_database() == Some(&name) {
                return Err("Cannot drop current database. Switch to another first.".to_string());
            }
            state.storage.drop_database(&name)
                .map_err(|e| format!("{}", e))?;
            println!("Database '{}' dropped.", name);
        }

        MetaCommand::RelList => {
            let relations = state.storage.list_relations()
                .map_err(|e| format!("{}", e))?;
            if relations.is_empty() {
                println!("No relations in current database.");
            } else {
                println!("Relations:");
                for rel in relations {
                    println!("  {}", rel);
                }
            }
        }

        MetaCommand::RelDescribe(name) => {
            match state.storage.describe_relation(&name)
                .map_err(|e| format!("{}", e))? {
                Some(desc) => println!("{}", desc),
                None => println!("Relation '{}' not found.", name),
            }
        }

        MetaCommand::ViewList => {
            let views = state.storage.list_views()
                .map_err(|e| format!("{}", e))?;
            if views.is_empty() {
                println!("No views defined.");
            } else {
                println!("Views:");
                for view in views {
                    println!("  {}", view);
                }
            }
        }

        MetaCommand::ViewQuery(name) => {
            // .view <name> shows definition AND computed results preview
            match state.storage.describe_view(&name)
                .map_err(|e| format!("{}", e))? {
                Some(desc) => {
                    println!("{}", desc);

                    // Also show computed results (up to 10 rows)
                    let query = format!("__result__(X, Y) :- {}(X, Y).", name);
                    match state.storage.execute_query_with_views(&query) {
                        Ok(results) => {
                            if results.is_empty() {
                                println!("Results: (empty - no base data or view not computable)");
                            } else {
                                let _show_count = std::cmp::min(results.len(), 10);
                                println!("Results: {} rows{}", results.len(),
                                    if results.len() > 10 { " (showing first 10)" } else { "" });
                                for (a, b) in results.iter().take(10) {
                                    println!("  ({}, {})", a, b);
                                }
                            }
                        }
                        Err(e) => {
                            println!("Results: (error computing: {})", e);
                        }
                    }
                }
                None => println!("View '{}' not found.", name),
            }
        }

        MetaCommand::ViewDef(name) => {
            // .view def <name> shows only the definition (no results)
            match state.storage.describe_view(&name)
                .map_err(|e| format!("{}", e))? {
                Some(desc) => println!("{}", desc),
                None => println!("View '{}' not found.", name),
            }
        }

        MetaCommand::ViewDrop(name) => {
            state.storage.drop_view(&name)
                .map_err(|e| format!("{}", e))?;
            println!("View '{}' dropped.", name);
        }

        MetaCommand::ViewEdit { name, index, rule_text } => {
            // Parse the rule text as a view declaration
            // The rule_text should be like: view connected(x: int, y: int) :- edge(x, y), connected(y, z).
            use inputlayer::statement::{parse_statement, Statement};

            match parse_statement(&rule_text) {
                Ok(Statement::ViewDecl(view_decl)) => {
                    // Verify the view name matches
                    if view_decl.name != name {
                        return Err(format!(
                            "Rule head '{}' doesn't match view name '{}'. Use: view {} :- ...",
                            view_decl.name, name, name
                        ));
                    }

                    state.storage.replace_view_rule(&name, index, view_decl.rule)
                        .map_err(|e| format!("{}", e))?;
                    println!("Rule {} of view '{}' replaced.", index + 1, name);

                    // Show updated view
                    if let Ok(Some(desc)) = state.storage.describe_view(&name) {
                        println!("\nUpdated view:");
                        println!("{}", desc);
                    }
                }
                Ok(_) => {
                    return Err("Invalid rule syntax. Use: view name(col: type) :- body.".to_string());
                }
                Err(e) => {
                    return Err(format!("Failed to parse rule: {}", e));
                }
            }
        }

        MetaCommand::ViewClear(name) => {
            // Clear all rules from the view, ready for re-registration
            state.storage.clear_view(&name)
                .map_err(|e| format!("{}", e))?;
            println!("View '{}' cleared. You can now re-register its rules with :=", name);
            println!("Example: {}(X, Y) := edge(X, Y).", name);
        }

        MetaCommand::SessionList => {
            if state.session_rules.is_empty() {
                println!("No session rules defined.");
                println!("Use :- to define transient rules (e.g., foo(X, Y) :- bar(X, Y).)");
            } else {
                println!("Session rules ({}):", state.session_rules.len());
                for (i, rule) in state.session_rules.iter().enumerate() {
                    println!("  {}. {}", i + 1, format_rule(rule));
                }
                println!();
                println!("Use .session clear to clear all, .session drop <n> to remove one.");
            }
        }

        MetaCommand::SessionClear => {
            let count = state.session_rules.len();
            state.session_rules.clear();
            if count == 0 {
                println!("No session rules to clear.");
            } else {
                println!("Cleared {} session rule(s).", count);
            }
        }

        MetaCommand::SessionDrop(index) => {
            if index >= state.session_rules.len() {
                return Err(format!(
                    "Rule index {} out of bounds. Session has {} rule(s).",
                    index + 1,
                    state.session_rules.len()
                ));
            }
            let removed = state.session_rules.remove(index);
            println!("Removed rule {}: {}", index + 1, format_rule(&removed));
            println!("(session: {} rule(s) remaining)", state.session_rules.len());
        }

        MetaCommand::Compact => {
            state.storage.compact_all()
                .map_err(|e| format!("{}", e))?;
            println!("Compaction complete. WAL flushed to batch files.");
        }

        MetaCommand::Status => {
            println!("InputLayer Status");
            println!("  Current database: {:?}", state.storage.current_database());
            println!("  Databases: {}", state.storage.list_databases().len());
            println!("  Data directory: {:?}", state.storage.config().storage.data_dir);
        }

        MetaCommand::Help => print_help(),

        MetaCommand::Quit => {
            // No need to save - operations are already durable via WAL
            println!("Goodbye!");
            std::process::exit(0);
        }
    }

    Ok(())
}

fn handle_insert(
    state: &mut ReplState,
    op: inputlayer::statement::InsertOp,
) -> Result<(), String> {
    // Check if any tuple needs the production API (vectors, strings, floats)
    let needs_production_api = op.tuples.iter().any(|t| tuple_needs_production_api(t));

    if needs_production_api {
        // Use production API with full Value types
        let tuples: Result<Vec<Tuple>, String> = op
            .tuples
            .iter()
            .map(|tuple| {
                let values: Result<Vec<Value>, String> = tuple
                    .iter()
                    .map(|term| {
                        term_to_value(term).ok_or_else(|| {
                            format!("Cannot convert term {:?} to storage value", term)
                        })
                    })
                    .collect();
                Ok(Tuple::new(values?))
            })
            .collect();

        let tuples = tuples?;

        if tuples.is_empty() {
            return Err("No valid tuples to insert".to_string());
        }

        let (new_count, dup_count) = state.storage.insert_tuples(&op.relation, tuples)
            .map_err(|e| format!("{}", e))?;

        print_insert_result(new_count, dup_count, &op.relation);
    } else {
        // Use legacy API for simple integer tuples (backward compatibility)
        let tuples: Vec<(i32, i32)> = op
            .tuples
            .iter()
            .filter_map(|tuple| {
                if tuple.len() >= 2 {
                    let a = term_to_i32(&tuple[0])?;
                    let b = term_to_i32(&tuple[1])?;
                    Some((a, b))
                } else {
                    None
                }
            })
            .collect();

        if tuples.is_empty() {
            return Err("No valid tuples to insert (requires 2-element tuples with integer values)".to_string());
        }

        let (new_count, dup_count) = state.storage.insert(&op.relation, tuples)
            .map_err(|e| format!("{}", e))?;

        print_insert_result(new_count, dup_count, &op.relation);
    }

    Ok(())
}

fn print_insert_result(new_count: usize, dup_count: usize, relation: &str) {
    if dup_count == 0 {
        if new_count == 1 {
            println!("Inserted 1 fact into '{}'.", relation);
        } else {
            println!("Inserted {} facts into '{}'.", new_count, relation);
        }
    } else if new_count == 0 {
        if dup_count == 1 {
            println!("No facts inserted (1 duplicate skipped).");
        } else {
            println!("No facts inserted ({} duplicates skipped).", dup_count);
        }
    } else {
        println!("Inserted {} new fact(s) into '{}' ({} duplicate(s) skipped).",
                 new_count, relation, dup_count);
    }
}

fn handle_delete(
    state: &mut ReplState,
    op: inputlayer::statement::DeleteOp,
) -> Result<(), String> {
    match op.pattern {
        DeletePattern::SingleTuple(terms) => {
            if terms.len() < 2 {
                return Err("Delete requires at least 2 arguments".to_string());
            }

            let a = term_to_i32(&terms[0])
                .ok_or_else(|| "First argument must be an integer".to_string())?;
            let b = term_to_i32(&terms[1])
                .ok_or_else(|| "Second argument must be an integer".to_string())?;

            state.storage.delete(&op.relation, vec![(a, b)])
                .map_err(|e| format!("{}", e))?;

            println!("Deleted fact from '{}'.", op.relation);
            Ok(())
        }

        DeletePattern::Conditional { head_args, body, constraints } => {
            // For conditional deletes, execute a query to find matching tuples
            let body_str = format_body(&body, &constraints);
            let head_str = format_args(&head_args);
            let query_program = format!("__delete_result__({}) :- {}.", head_str, body_str);

            let results = state.storage.execute_query(&query_program)
                .map_err(|e| format!("{}", e))?;

            if results.is_empty() {
                println!("No matching tuples to delete.");
                return Ok(());
            }

            let count = results.len();
            state.storage.delete(&op.relation, results)
                .map_err(|e| format!("{}", e))?;

            println!("Deleted {} facts from '{}'.", count, op.relation);
            Ok(())
        }
    }
}

fn handle_update(
    state: &mut ReplState,
    op: inputlayer::statement::UpdateOp,
) -> Result<(), String> {
    // Build query to find matching tuples
    let body_str = format_body(&op.body, &op.constraints);

    // Collect all variables
    let mut all_vars: Vec<String> = Vec::new();
    for del in &op.deletes {
        for arg in &del.args {
            if let Term::Variable(v) = arg {
                if !all_vars.contains(v) {
                    all_vars.push(v.clone());
                }
            }
        }
    }
    for ins in &op.inserts {
        for arg in &ins.args {
            if let Term::Variable(v) = arg {
                if !all_vars.contains(v) {
                    all_vars.push(v.clone());
                }
            }
        }
    }

    let head_vars = all_vars.join(", ");
    let query_program = format!("__update_result__({}) :- {}.", head_vars, body_str);

    let results = state.storage.execute_query(&query_program)
        .map_err(|e| format!("{}", e))?;

    if results.is_empty() {
        println!("No matching tuples for update.");
        return Ok(());
    }

    let num_matches = results.len();

    // For each result, perform deletes and inserts
    for result in results {
        // Build variable bindings from result
        let bindings: std::collections::HashMap<String, i32> = if all_vars.len() >= 2 {
            let mut map = std::collections::HashMap::new();
            if let Some(v) = all_vars.get(0) {
                map.insert(v.clone(), result.0);
            }
            if let Some(v) = all_vars.get(1) {
                map.insert(v.clone(), result.1);
            }
            map
        } else {
            std::collections::HashMap::new()
        };

        // Perform deletes
        for del in &op.deletes {
            if del.args.len() >= 2 {
                let a = substitute_term_i32(&del.args[0], &bindings)?;
                let b = substitute_term_i32(&del.args[1], &bindings)?;
                let _ = state.storage.delete(&del.relation, vec![(a, b)]);
            }
        }

        // Perform inserts
        for ins in &op.inserts {
            if ins.args.len() >= 2 {
                let a = substitute_term_i32(&ins.args[0], &bindings)?;
                let b = substitute_term_i32(&ins.args[1], &bindings)?;
                let _ = state.storage.insert(&ins.relation, vec![(a, b)]);
            }
        }
    }

    println!(
        "Updated {} tuples ({} deletes, {} inserts per match).",
        num_matches,
        op.deletes.len(),
        op.inserts.len()
    );
    Ok(())
}

fn handle_view(
    state: &mut ReplState,
    def: inputlayer::statement::ViewDef,
) -> Result<(), String> {
    use inputlayer::view_catalog::ViewRegisterResult;

    let result = state.storage.register_view(&def)
        .map_err(|e| format!("{}", e))?;

    match result {
        ViewRegisterResult::Created => {
            println!("View '{}' registered.", def.name);
        }
        ViewRegisterResult::RuleAdded(rule_count) => {
            println!("Rule added to view '{}' ({} rules total).", def.name, rule_count);
        }
    }
    Ok(())
}

fn handle_transient_rule(
    state: &mut ReplState,
    rule: datalog_ast::Rule,
) -> Result<(), String> {
    // Get head relation name before moving rule
    let head_relation = rule.head.relation.clone();

    // Add to session rules
    state.session_rules.push(rule);

    // Build combined program: all session rules + query for head relation
    let program = build_session_program(&state.session_rules, &head_relation);

    // Execute with persistent views included
    let results = state.storage.execute_query_with_views(&program)
        .map_err(|e| format!("{}", e))?;

    // Show results
    if results.is_empty() {
        println!("No results for '{}'.", head_relation);
    } else {
        println!("{} rows:", results.len());
        for (a, b) in results {
            println!("  ({}, {})", a, b);
        }
    }
    println!("(session: {} rule(s))", state.session_rules.len());

    Ok(())
}

/// Build a program from session rules with a query for a specific relation
fn build_session_program(session_rules: &[Rule], query_relation: &str) -> String {
    let mut program = String::new();

    // Add all session rules
    for rule in session_rules {
        let formatted = format_rule(rule);
        if std::env::var("DATALOG_DEBUG").is_ok() {
            eprintln!("DEBUG build_session_program: formatted rule = {}", formatted);
        }
        program.push_str(&formatted);
        program.push('\n');
    }

    // Add query to extract results for the head relation
    program.push_str(&format!("__session_result__(X, Y) :- {}(X, Y).", query_relation));

    program
}

fn handle_query(
    state: &mut ReplState,
    goal: inputlayer::statement::QueryGoal,
) -> Result<(), String> {
    // Transform query: replace constants with temp variables, add equality constraints
    // This avoids "Constants in rule head not yet supported" error
    let mut head_vars = Vec::new();
    let mut extra_constraints = Vec::new();

    let transformed_args: Vec<String> = goal
        .goal
        .args
        .iter()
        .enumerate()
        .map(|(i, term)| match term {
            Term::Variable(v) => {
                head_vars.push(v.clone());
                v.clone()
            }
            Term::Constant(val) => {
                let temp = format!("_c{}", i);
                head_vars.push(temp.clone());
                extra_constraints.push(format!("{} = {}", temp, val));
                temp
            }
            Term::FloatConstant(val) => {
                let temp = format!("_c{}", i);
                head_vars.push(temp.clone());
                extra_constraints.push(format!("{} = {}", temp, val));
                temp
            }
            Term::StringConstant(s) => {
                let temp = format!("_c{}", i);
                head_vars.push(temp.clone());
                extra_constraints.push(format!("{} = \"{}\"", temp, s));
                temp
            }
            Term::Placeholder => {
                let temp = format!("_p{}", i);
                head_vars.push(temp.clone());
                temp
            }
            _ => {
                let temp = format!("_t{}", i);
                head_vars.push(temp.clone());
                temp
            }
        })
        .collect();

    // Build body atom with transformed args
    let body_atom = format!("{}({})", goal.goal.relation, transformed_args.join(", "));

    let mut body_parts = vec![body_atom];
    body_parts.extend(extra_constraints);

    // Add other body predicates and constraints from the query
    for pred in &goal.body {
        body_parts.push(format_body_pred(pred));
    }
    for constraint in &goal.constraints {
        body_parts.push(format_constraint(constraint));
    }

    // Build program with session rules prepended
    let mut program = String::new();

    // Add session rules first (so query can reference them)
    for rule in &state.session_rules {
        program.push_str(&format_rule(rule));
        program.push('\n');
    }

    // Add the query
    program.push_str(&format!("__query__({}) :- {}.", head_vars.join(", "), body_parts.join(", ")));

    // Use tuple-based execution to support arbitrary arity
    let results = state.storage.execute_query_with_views_tuples(&program)
        .map_err(|e| format!("{}", e))?;

    if results.is_empty() {
        println!("No results.");
    } else {
        println!("{} rows:", results.len());
        for tuple in results {
            // Format tuple values as comma-separated list
            let values: Vec<String> = tuple.values()
                .iter()
                .map(|v| format!("{}", v))
                .collect();
            println!("  ({})", values.join(", "));
        }
    }

    Ok(())
}

// ============================================================================
// Helper Functions
// ============================================================================

fn term_to_i32(term: &Term) -> Option<i32> {
    match term {
        Term::Constant(n) => Some(*n as i32),
        Term::FloatConstant(f) => Some(*f as i32),
        _ => None,
    }
}

/// Convert a Term to a Value for storage
fn term_to_value(term: &Term) -> Option<Value> {
    use std::sync::Arc;
    match term {
        Term::Constant(n) => Some(Value::Int64(*n)),
        Term::FloatConstant(f) => Some(Value::Float64(*f)),
        Term::StringConstant(s) => Some(Value::String(Arc::from(s.as_str()))),
        Term::VectorLiteral(vals) => {
            let f32_vals: Vec<f32> = vals.iter().map(|v| *v as f32).collect();
            Some(Value::Vector(Arc::new(f32_vals)))
        }
        _ => None,
    }
}

/// Check if a tuple contains any non-simple types (vectors, strings, floats)
/// that require the production API instead of the legacy Tuple2 API
fn tuple_needs_production_api(tuple: &[Term]) -> bool {
    // Use production API for tuples with more than 2 elements (legacy API only handles 2 columns)
    // or for tuples containing vectors, strings, or floats
    tuple.len() != 2 || tuple.iter().any(|t| matches!(t, Term::VectorLiteral(_) | Term::StringConstant(_) | Term::FloatConstant(_)))
}

fn substitute_term_i32(
    term: &Term,
    bindings: &std::collections::HashMap<String, i32>,
) -> Result<i32, String> {
    match term {
        Term::Variable(v) => bindings
            .get(v)
            .copied()
            .ok_or_else(|| format!("Variable '{}' not bound", v)),
        Term::Constant(n) => Ok(*n as i32),
        Term::FloatConstant(f) => Ok(*f as i32),
        _ => Err("Unsupported term type".to_string()),
    }
}

fn format_rule(rule: &datalog_ast::Rule) -> String {
    let head = format_atom(&rule.head);

    if rule.body.is_empty() && rule.constraints.is_empty() {
        return format!("{}.", head);
    }

    let mut body_parts = Vec::new();
    for pred in &rule.body {
        body_parts.push(format_body_pred(pred));
    }
    for constraint in &rule.constraints {
        body_parts.push(format_constraint(constraint));
    }

    format!("{} :- {}.", head, body_parts.join(", "))
}

fn format_atom(atom: &datalog_ast::Atom) -> String {
    let args: Vec<String> = atom.args.iter().map(format_term).collect();
    format!("{}({})", atom.relation, args.join(", "))
}

fn format_term(term: &Term) -> String {
    match term {
        Term::Variable(name) => name.clone(),
        Term::Constant(val) => val.to_string(),
        Term::StringConstant(s) => format!("\"{}\"", s),
        Term::FloatConstant(f) => f.to_string(),
        Term::Placeholder => "_".to_string(),
        Term::Arithmetic(expr) => format_arith_expr(expr),
        Term::Aggregate(func, var) => format_aggregate(func, var),
        Term::FunctionCall(func, args) => {
            let formatted_args: Vec<String> = args.iter().map(format_term).collect();
            format!("{}({})", func.as_str(), formatted_args.join(", "))
        }
        Term::VectorLiteral(vals) => {
            let formatted: Vec<String> = vals.iter().map(|v| v.to_string()).collect();
            format!("[{}]", formatted.join(", "))
        }
        Term::FieldAccess(base, field) => {
            format!("{}.{}", format_term(base), field)
        }
        Term::RecordPattern(fields) => {
            let formatted: Vec<String> = fields
                .iter()
                .map(|(name, term)| format!("{}: {}", name, format_term(term)))
                .collect();
            format!("{{ {} }}", formatted.join(", "))
        }
    }
}

/// Format an ArithExpr as a Datalog string
fn format_arith_expr(expr: &datalog_ast::ArithExpr) -> String {
    match expr {
        datalog_ast::ArithExpr::Variable(name) => name.clone(),
        datalog_ast::ArithExpr::Constant(val) => val.to_string(),
        datalog_ast::ArithExpr::Binary { op, left, right } => {
            format!("{}{}{}", format_arith_expr(left), op.as_str(), format_arith_expr(right))
        }
    }
}

/// Format an AggregateFunc as a Datalog string
fn format_aggregate(func: &datalog_ast::AggregateFunc, var: &str) -> String {
    match func {
        datalog_ast::AggregateFunc::Count => format!("count<{}>", var),
        datalog_ast::AggregateFunc::Sum => format!("sum<{}>", var),
        datalog_ast::AggregateFunc::Min => format!("min<{}>", var),
        datalog_ast::AggregateFunc::Max => format!("max<{}>", var),
        datalog_ast::AggregateFunc::Avg => format!("avg<{}>", var),
        datalog_ast::AggregateFunc::TopK { k, order_var, descending } => {
            if *descending {
                format!("top_k<{}, {}, desc>", k, order_var)
            } else {
                format!("top_k<{}, {}>", k, order_var)
            }
        }
        datalog_ast::AggregateFunc::TopKThreshold { k, order_var, threshold, descending } => {
            if *descending {
                format!("top_k_threshold<{}, {}, {}, desc>", k, order_var, threshold)
            } else {
                format!("top_k_threshold<{}, {}, {}>", k, order_var, threshold)
            }
        }
        datalog_ast::AggregateFunc::WithinRadius { distance_var, max_distance } => {
            format!("within_radius<{}, {}>", distance_var, max_distance)
        }
    }
}

fn format_body_pred(pred: &datalog_ast::BodyPredicate) -> String {
    match pred {
        datalog_ast::BodyPredicate::Positive(atom) => format_atom(atom),
        datalog_ast::BodyPredicate::Negated(atom) => format!("!{}", format_atom(atom)),
    }
}

fn format_constraint(constraint: &datalog_ast::Constraint) -> String {
    match constraint {
        datalog_ast::Constraint::Equal(l, r) => format!("{} = {}", format_term(l), format_term(r)),
        datalog_ast::Constraint::NotEqual(l, r) => format!("{} != {}", format_term(l), format_term(r)),
        datalog_ast::Constraint::LessThan(l, r) => format!("{} < {}", format_term(l), format_term(r)),
        datalog_ast::Constraint::LessOrEqual(l, r) => format!("{} <= {}", format_term(l), format_term(r)),
        datalog_ast::Constraint::GreaterThan(l, r) => format!("{} > {}", format_term(l), format_term(r)),
        datalog_ast::Constraint::GreaterOrEqual(l, r) => format!("{} >= {}", format_term(l), format_term(r)),
    }
}

fn format_body(body: &[datalog_ast::BodyPredicate], constraints: &[datalog_ast::Constraint]) -> String {
    let mut parts = Vec::new();
    for pred in body {
        parts.push(format_body_pred(pred));
    }
    for constraint in constraints {
        parts.push(format_constraint(constraint));
    }
    parts.join(", ")
}

fn format_args(args: &[Term]) -> String {
    args.iter().map(format_term).collect::<Vec<_>>().join(", ")
}

fn print_help() {
    println!("InputLayer Datalog Cheatsheet");
    println!("=============================");
    println!();
    println!("Meta Commands:");
    println!("  .db                  Show current database");
    println!("  .db list             List all databases");
    println!("  .db create <name>    Create database");
    println!("  .db use <name>       Switch to database");
    println!("  .db drop <name>      Drop database");
    println!("  .rel                 List relations");
    println!("  .rel <name>          Describe relation");
    println!("  .view                List views");
    println!("  .view <name>         Query view (show computed data)");
    println!("  .view def <name>     Show view definition");
    println!("  .view edit <name> <n> <rule>   Replace rule #n");
    println!("  .view clear <name>   Clear all rules for re-registration");
    println!("  .view drop <name>    Drop view");
    println!("  .session             List session rules");
    println!("  .session clear       Clear all session rules");
    println!("  .session drop <n>    Remove session rule #n");
    println!("  .compact             Compact WAL and consolidate batches");
    println!("  .status              System status");
    println!("  .help                Show this help");
    println!("  .quit                Exit");
    println!();
    println!("Data Manipulation:");
    println!("  +edge(1, 2).                   Insert fact");
    println!("  +edge[(1,2), (3,4)].           Bulk insert");
    println!("  -edge(1, 2).                   Delete fact");
    println!("  -edge(X, Y) :- X > 5.          Conditional delete");
    println!();
    println!("Views (Persistent Rules - saved to disk):");
    println!("  path(X, Y) := edge(X, Y).      Define persistent view");
    println!();
    println!("Session Rules (transient - cleared on exit/db switch):");
    println!("  foo(X, Y) :- bar(X, Y).        Add rule to session");
    println!("  foo(X, Z) :- foo(X, Y), foo(Y, Z).   Rules accumulate & evaluate together");
    println!();
    println!("Queries:");
    println!("  ?- path(1, X).                 Query (uses session rules + views)");
    println!();
    println!("Operator Reference:");
    println!("  +   Insert fact                Persisted to disk");
    println!("  -   Delete fact                Persisted to disk");
    println!("  :=  Define view                Persisted to disk");
    println!("  :-  Session rule               Memory only (session)");
    println!("  ?-  Query                      Memory only (one-shot)");
    println!();
}
