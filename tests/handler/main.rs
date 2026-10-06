//! Handler component tests: programs run through `Handler` in process, with
//! no socket. Statement outcomes, limits, authorization, provenance,
//! indexes, evaluation regressions and server startup, one module per
//! subject.

mod harness;

mod aggregate_correctness;
mod atomic_program;
mod authz_multistatement;
mod bootstrap_secret;
mod client_handler;
mod comparison;
mod error_handling;
mod expression_comparison;
mod fact_program;
mod fastpath_regressions;
mod hnsw_index;
mod input_validation;
mod magic_sets_regressions;
mod memory_limit;
mod multi_clause_rule;
mod mutual_recursion;
mod optimizer_config;
mod pinned_proof;
mod result_limit;
mod server_startup;
mod session_schema;
mod size_limit;
mod snapshot_sharing;
mod statement_counts;
mod statement_status;
mod string_literal;
mod typed_arithmetic;
mod why_not;
mod why_provenance;
mod wildcard_rule;
