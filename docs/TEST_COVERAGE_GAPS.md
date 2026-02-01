# Exhaustive Test Coverage Analysis

This document provides a comprehensive analysis of test coverage for the InputLayer Datalog engine.
It identifies all features that should be tested, their current status, and gaps that need to be filled.

**Last Updated**: 2026-01-17
**Total Test Cases Tracked**: 1191
**Implemented**: ~850 (71%)
**Missing**: ~341 (29%)
**Categories**: 70
**Coverage Target**: Production-ready
**Snapshot Tests**: 1101 passing (+772 from Jan 15)
**Unit Tests**: 957 passing

> **Recent Progress (Jan 15-17)**: Added 772 snapshot tests across all categories. Major improvements in error handling, types, aggregations, recursion, negation, joins, filters, and edge cases.

> **Note**: Section 16.2-16.3 documents the design decision:
> - `.rule drop <name>` - Removes ALL clauses of a rule
> - `.rule remove <name> <index>` - Removes specific clause by 1-based index (NEW!)
> - `-relation(X, Y) :- condition.` - Conditional fact deletion (NEW!)
>
> Both features are now fully implemented and tested.

---

## Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | Fully tested |
| ⚠️ | Partially tested (some cases missing) |
| ❌ | Not tested |
| 🔧 | Test exists but broken/incorrect |

---

## Table of Contents

1. [Core Language Features](#1-core-language-features)
2. [Data Operations](#2-data-operations)
3. [Query Features](#3-query-features)
4. [Joins](#4-joins)
5. [Filters and Comparisons](#5-filters-and-comparisons)
6. [Negation](#6-negation)
7. [Recursion](#7-recursion)
8. [Aggregations](#8-aggregations)
9. [Arithmetic](#9-arithmetic)
10. [Types and Values](#10-types-and-values)
11. [Vectors](#11-vectors)
12. [Temporal Operations](#12-temporal-operations)
13. [Meta Commands](#13-meta-commands)
14. [Session Management](#14-session-management)
15. [Knowledge Graph Management](#15-knowledge-graph-management)
16. [Rule Management](#16-rule-management)
17. [Schema System](#17-schema-system)
18. [Error Handling](#18-error-handling)
19. [Edge Cases](#19-edge-cases)
20. [Performance](#20-performance)
21. [Integration Scenarios](#21-integration-scenarios)
22. [Comments & Syntax](#22-comments--syntax) *(NEW)*
23. [Record Types & Field Access](#23-record-types--field-access) *(NEW)*
24. [Advanced Type System](#24-advanced-type-system) *(NEW)*
25. [Delete Operations (Extended)](#25-delete-operations-extended) *(NEW)*
26. [Function Calls (Advanced)](#26-function-calls-advanced) *(NEW)*
27. [Parsing Edge Cases (Extended)](#27-parsing-edge-cases-extended) *(NEW)*
28. [Concurrency & Parallelism](#28-concurrency--parallelism) *(NEW)*
29. [Large Scale & Stress Tests](#29-large-scale--stress-tests) *(NEW)*
30. [Transaction Semantics](#30-transaction-semantics) *(NEW)*
31. [Vector Functions (Complete)](#31-vector-functions-complete) *(NEW)*
32. [Math Functions (Complete)](#32-math-functions-complete) *(NEW)*
33. [Literal Syntax (Complete)](#33-literal-syntax-complete) *(NEW)*
34. [IR-Level Functions](#34-ir-level-functions-internal) *(NEW)*
35. [Configuration & Environment](#35-configuration--environment) *(NEW)*
36. [Resource Limits & Timeouts](#36-resource-limits--timeouts) *(NEW)*
37. [Optimizer Passes](#37-optimizer-passes) *(NEW)*
38. [Join Planning](#38-join-planning) *(NEW)*
39. [Storage & WAL](#39-storage--wal) *(NEW)*
40. [Query Cache](#40-query-cache) *(NEW)*
41. [REST API](#41-rest-api) *(NEW)*
42. [Client/Server Protocol](#42-clientserver-protocol) *(NEW)*
43. [Crash Recovery](#43-crash-recovery) *(NEW)*
44. [Schema Validation Errors](#44-schema-validation-errors) *(NEW)*
45. [CLI Argument Testing](#45-cli-argument-testing-new) *(NEW)*
46. [Serialization Round-trip Testing](#46-serialization-round-trip-testing-new) *(NEW)*
47. [Numeric Edge Cases (Extended)](#47-numeric-edge-cases-extended-new) *(NEW)*
48. [Specialized Execution Methods](#48-specialized-execution-methods-new) *(NEW)*
49. [Error Variant Coverage](#49-error-variant-coverage-new) *(NEW)*
50. [BuiltinFunction Coverage](#50-builtinfunction-coverage-new) *(NEW)*
51. [Term Variant Coverage](#51-term-variant-coverage) *(CODE ANALYSIS)*
52. [MetaCommand Handler Coverage](#52-metacommand-handler-coverage) *(CODE ANALYSIS)*
53. [Parser Syntax Edge Cases](#53-parser-syntax-edge-cases) *(CODE ANALYSIS)*
54. [String Functions (Planned)](#54-string-functions) *(ROADMAP)*
55. [Additional Math Functions](#55-additional-math-functions) *(ROADMAP)*
56. [Concurrent Access Testing](#56-concurrent-access-testing) *(CRITICAL)*
57. [Crash Recovery Testing](#57-crash-recovery-testing) *(CRITICAL)*
58. [Corruption Handling](#58-corruption-handling) *(CRITICAL)*
59. [REST API Endpoint Coverage](#59-rest-api-endpoint-coverage) *(CODE ANALYSIS)*
60. [Client REPL Handler Coverage](#60-client-repl-handler-coverage) *(CODE ANALYSIS)*
61. [Optimization Pipeline Coverage](#61-optimization-pipeline-coverage) *(CODE ANALYSIS)*
62. [Recursive Execution Methods](#62-recursive-execution-methods) *(CODE ANALYSIS)*
63. [External Data Loading](#63-external-data-loading) *(DOCS)*
64. [Environment Variable Configuration](#64-environment-variable-configuration) *(DOCS)*
65. [IRExpression Coverage](#65-irexpression-coverage) *(CODE ANALYSIS)*
66. [Panic Path Coverage](#66-panic-path-coverage) *(CRITICAL)*
67. [Configuration Impact Testing](#67-configuration-impact-testing) *(CRITICAL)*
68. [Feature Interaction Matrix](#68-feature-interaction-matrix) *(CODE ANALYSIS)*
69. [Public Method Coverage](#69-public-method-coverage) *(CODE ANALYSIS)*
70. [Boundary Value Testing](#70-boundary-value-testing) *(CRITICAL)*

---

## 1. Core Language Features

### 1.1 Facts

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Insert single fact | ✅ | `02_relations/01_insert_single.dl` | |
| Insert multiple facts (bulk) | ✅ | `02_relations/02_insert_bulk.dl` | |
| Fact with integer values | ✅ | `11_types/02_integers.dl` | |
| Fact with string values | ✅ | `11_types/01_strings.dl` | |
| Fact with float values | ✅ | `11_types/03_floats_truncation.dl` | |
| Fact with boolean values | ✅ | `11_types/11_booleans.dl` | |
| Fact with mixed types | ✅ | `11_types/05_mixed_type_tuples.dl` | |
| Fact with vector values | ✅ | `16_vectors/01_euclidean_distance.dl` | |
| Fact with empty string | ✅ | `37_string_edge_cases/03_empty_string.dl` | |
| Fact with unicode | ✅ | `37_string_edge_cases/01_unicode.dl` | |
| Fact with special characters | ✅ | `37_string_edge_cases/02_special_chars.dl` | |
| Fact with escape sequences | ✅ | `37_string_edge_cases/05_escape_sequences.dl` | |
| Fact with very long string | ✅ | `37_string_edge_cases/04_long_strings.dl` | |
| Fact with negative integers | ✅ | `11_types/10_negative_numbers.dl` | |
| Fact with large integers (i64 max) | ✅ | `11_types/04_large_integers.dl` | Fixed: Uses simple comparison |
| Fact with zero values | ✅ | `11_types/09_zero_handling.dl` | |
| Duplicate fact insertion | ✅ | `10_edge_cases/03_duplicates.dl` | |
| Fact into non-existent relation | ✅ | Implicit in many tests | Auto-creates relation |

### 1.2 Rules

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Simple rule (one body atom) | ✅ | `04_session/01_session_rules.dl` | |
| Rule with multiple body atoms | ✅ | `06_joins/01_two_way_join.dl` | |
| Rule with constants in head | ✅ | `11_types/12_constants_in_head.dl` | |
| Rule with constants in body | ✅ | `06_joins/08_join_with_constants.dl` | |
| Persistent rule (+) | ✅ | `25_unified_prefix/02_persistent_rules.dl` | |
| Session rule (no +) | ✅ | `04_session/01_session_rules.dl` | |
| Multi-clause rule (same head) | ✅ | `17_rule_commands/05_multi_clause_rules.dl` | |
| Rule with wildcard (_) | ✅ | `10_edge_cases/14_wildcard_patterns.dl` | |
| Rule with computed head | ✅ | `15_arithmetic/01_increment.dl` | |
| Rule with aggregation in head | ✅ | `14_aggregations/01_count.dl` | |
| Rule referencing itself (recursion) | ✅ | `09_recursion/01_transitive_closure.dl` | |
| Rule with negation | ✅ | `08_negation/01_simple_negation.dl` | |
| Rule with filter | ✅ | `07_filters/01_equality.dl` | |
| Empty rule body | ✅ | `12_errors/30_empty_rule_body_error.dl` | Now tested |
| Rule with only negation (unsafe) | ✅ | `12_errors/20_unsafe_negation_error.dl` | |
| Rule with unbound head variable | ✅ | `12_errors/27_unbound_head_var_error.dl` | Now tested |

### 1.3 Queries

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Query all tuples | ✅ | `21_query_features/00_scan_all.dl` | |
| Query with variable binding | ✅ | Multiple tests | |
| Query with constant filter | ✅ | `07_filters/01_equality.dl` | |
| Query with wildcard | ✅ | `21_query_features/06_wildcard_placeholder.dl` | |
| Query empty relation | ✅ | `10_edge_cases/01_empty_relation.dl` | |
| Query non-existent relation | ✅ | `12_errors/01_undefined_relation_error.dl` | |
| Query with projection | ✅ | `21_query_features/02_projection.dl` | |
| Query with selection | ✅ | `21_query_features/03_selection.dl` | |
| Query with computed columns | ✅ | `21_query_features/04_computed_columns.dl` | |
| Complex multi-condition query | ✅ | `21_query_features/05_complex_queries.dl` | |
| Query returning distinct results | ✅ | `21_query_features/01_distinct_results.dl` | |

---

## 2. Data Operations

### 2.1 Insert Operations

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Single tuple insert | ✅ | `02_relations/01_insert_single.dl` | |
| Bulk insert with array syntax | ✅ | `02_relations/02_insert_bulk.dl` | |
| Insert with arity mismatch | ✅ | `12_errors/04_arity_mismatch_error.dl` | |
| Insert empty bulk | ✅ | `12_errors/07_empty_insert_error.dl` | |
| Insert duplicate tuple | ✅ | `10_edge_cases/03_duplicates.dl` | |
| Insert into view (should fail) | ✅ | `12_errors/21_insert_into_view.dl` | Just fixed! |
| Insert with type mismatch | ❌ | - | **MISSING** (if schema defined) |

### 2.2 Delete Operations

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Delete single tuple | ✅ | `02_relations/03_delete.dl` | |
| Delete non-existent tuple | ✅ | `02_relations/04_delete_nonexistent.dl` | |
| Delete all tuples | ✅ | `02_relations/05_delete_all_then_query.dl` | |
| Conditional delete | ✅ | `27_atomic_ops/03_bulk_conditional_delete.dl` | |
| Delete during view evaluation | ✅ | `02_relations/06_delete_during_view.dl` | |
| Delete with string values | ✅ | `02_relations/07_delete_string_values.dl` | |
| Delete from empty relation | ✅ | `02_relations/09_delete_from_empty.dl` | Now tested |
| Delete with wildcard pattern | ✅ | `02_relations/12_delete_wildcard.dl` | Now tested |

### 2.3 Update Operations (Atomic)

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Atomic update | ✅ | `27_atomic_ops/01_atomic_update.dl` | |
| Conditional update | ✅ | `27_atomic_ops/02_conditional_update.dl` | |
| Delete all via pattern | ✅ | `27_atomic_ops/04_delete_all.dl` | |
| Update non-existent tuple | ❌ | - | **MISSING** |
| Concurrent updates | ❌ | - | **MISSING** |

---

## 3. Query Features

### 3.1 Projections

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Select specific columns | ✅ | `21_query_features/02_projection.dl` | |
| Reorder columns | ❌ | - | **MISSING** |
| Duplicate column in output | ❌ | - | **MISSING** |

### 3.2 Selections

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Filter by equality | ✅ | `07_filters/01_equality.dl` | |
| Filter by inequality | ✅ | `07_filters/02_inequality.dl` | |
| Filter by comparison | ✅ | `07_filters/03_comparisons.dl` | |
| Combined filters (AND) | ✅ | `07_filters/04_combined_filters.dl` | |
| Filter on computed value | ✅ | `21_query_features/04_computed_columns.dl` | |

### 3.3 Set Operations

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Union (implicit via multi-rule) | ✅ | `22_set_operations/01_implicit_union.dl` | |
| Union with deduplication | ✅ | `22_set_operations/02_union_dedup.dl` | |
| Intersection | ✅ | `22_set_operations/03_intersection.dl` | |
| Set difference | ✅ | `22_set_operations/04_set_difference.dl` | |
| Empty set operations | ✅ | `22_set_operations/05_empty_set_operations.dl` | |
| Symmetric difference | ✅ | `22_set_operations/06_symmetric_difference.dl` | |

---

## 4. Joins

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Two-way join | ✅ | `06_joins/01_two_way_join.dl` | |
| Self-join | ✅ | `06_joins/02_self_join.dl` | |
| Three-way join (triangle) | ✅ | `06_joins/03_triangle.dl` | |
| Multi-relation join | ✅ | `06_joins/04_multi_join.dl` | |
| Chain join | ✅ | `06_joins/05_chain_join.dl` | |
| Four-way join | ✅ | `06_joins/06_four_way_join.dl` | |
| Cross product (Cartesian) | ✅ | `06_joins/07_cross_product.dl` | |
| Join with constants | ✅ | `06_joins/08_join_with_constants.dl` | |
| Join with empty relation | ✅ | `10_edge_cases/08_join_empty_relations.dl` | |
| Join one side empty | ✅ | `10_edge_cases/11_join_one_side_empty.dl` | |
| Self-join patterns | ✅ | `10_edge_cases/07_self_join_patterns.dl` | |
| Join on multiple columns | ❌ | - | **MISSING** |
| Join with type coercion | ❌ | - | **MISSING** |

---

## 5. Filters and Comparisons

### 5.1 Comparison Operators

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Equal (=) | ✅ | `07_filters/01_equality.dl` | |
| Not equal (!=) | ✅ | `07_filters/02_inequality.dl` | |
| Less than (<) | ✅ | `07_filters/03_comparisons.dl` | |
| Less or equal (<=) | ✅ | `07_filters/03_comparisons.dl` | |
| Greater than (>) | ✅ | `07_filters/03_comparisons.dl` | |
| Greater or equal (>=) | ✅ | `07_filters/03_comparisons.dl` | |
| Isolated operator tests | ✅ | `07_filters/05_comparison_operators_isolated.dl` | |
| String comparison | ✅ | `11_types/25_string_lexicographic.dl` | Now tested |
| Float comparison precision | ✅ | `11_types/14_float_comparisons.dl` | |
| Compare with NULL/missing | ❌ | - | **MISSING** |

### 5.2 Filter Patterns

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Variable = Variable | ✅ | `07_filters/01_equality.dl` | |
| Variable = Constant | ✅ | Multiple tests | |
| Variable op Expression | ❌ | - | **MISSING** |
| Constant op Constant | ❌ | - | **MISSING** (compile-time eval?) |

---

## 6. Negation

### 6.1 Basic Negation

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Simple negation | ✅ | `08_negation/01_simple_negation.dl` | |
| Negation with join | ✅ | `08_negation/02_negation_with_join.dl` | |
| Negation empty result | ✅ | `08_negation/03_negation_empty_result.dl` | |
| Negation no match | ✅ | `08_negation/04_negation_no_match.dl` | |
| Negation of empty relation | ✅ | `08_negation/25_negate_empty_relation.dl` | |

### 6.2 Complex Negation

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Double negation | ✅ | `08_negation/05_double_negation.dl` | |
| Triple negation | ✅ | `08_negation/08_triple_negation.dl` | |
| Negation self-relation | ✅ | `08_negation/06_negation_self_relation.dl` | |
| Negation with filter | ✅ | `08_negation/07_negation_with_filter.dl` | |
| Negation partial key | ✅ | `08_negation/09_negation_partial_key.dl` | |
| Negation empty exclude | ✅ | `08_negation/10_negation_empty_exclude.dl` | |
| Negation full exclude | ✅ | `08_negation/11_negation_full_exclude.dl` | |
| Chained negation | ✅ | `08_negation/12_negation_chained.dl` | |
| Negation multi-join | ✅ | `08_negation/13_negation_multi_join.dl` | |
| Same var twice in negation | ✅ | `08_negation/14_negation_same_var_twice.dl` | |
| Swapped vars in negation | ✅ | `08_negation/15_negation_swapped_vars.dl` | |
| Negation with constants | ✅ | `08_negation/16_negation_with_constants.dl` | |
| Large exclusion set | ✅ | `08_negation/17_negation_large_exclude.dl` | |
| Negation after recursion | ✅ | `08_negation/18_negation_after_recursion.dl` | |
| Symmetric difference | ✅ | `08_negation/19_negation_symmetric_diff.dl` | |

### 6.3 Negation on Views

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Negation on simple view | ✅ | `08_negation/20_negation_on_simple_view.dl` | |
| Negation on recursive view | ✅ | `08_negation/21_negation_on_recursive_view.dl` | |
| Multi-rule view with negation | ✅ | `08_negation/22_multi_rule_view_with_negation.dl` | |
| Chained view negation | ✅ | `08_negation/23_chained_view_negation.dl` | |
| Valid stratification | ✅ | `08_negation/24_valid_stratification.dl` | |
| All excluded | ✅ | `08_negation/26_all_excluded.dl` | |
| Double negation equivalence | ✅ | `08_negation/27_double_negation_equivalence.dl` | |
| Valid layered negation | ✅ | `08_negation/28_valid_layered_negation.dl` | |

### 6.4 Negation Error Cases

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Self-negation (a :- !a) | ✅ | `12_errors/17_self_negation_error.dl` | |
| Unsafe negation (unbound var) | ✅ | `12_errors/20_unsafe_negation_error.dl` | |
| Mutual negation cycle | ✅ | `12_errors/28_mutual_negation_cycle_error.dl` | Now tested |
| Three-way negation cycle | ✅ | `12_errors/31_three_way_negation_cycle_error.dl` | Now tested |

---

## 7. Recursion

### 7.1 Basic Recursion

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Transitive closure | ✅ | `09_recursion/01_transitive_closure.dl` | |
| Same component | ✅ | `09_recursion/02_same_component.dl` | |
| Connected with view | ✅ | `09_recursion/03_connected_with_view.dl` | |
| Same component with view | ✅ | `09_recursion/04_same_component_with_view.dl` | |
| Left recursion | ✅ | `09_recursion/05_left_recursion.dl` | |
| Deep recursion (100) | ✅ | `09_recursion/06_deep_recursion_100.dl` | |
| Deep recursion (500) | ✅ | `09_recursion/07_deep_recursion_500.dl` | |

### 7.2 Complex Recursion

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Mutual recursion | ✅ | `09_recursion/08_mutual_recursion.dl` | |
| Three-way mutual | ✅ | `09_recursion/09_three_way_mutual.dl` | |
| Multiple base cases | ✅ | `09_recursion/10_multiple_base_cases.dl` | |
| Non-linear recursion | ✅ | `09_recursion/11_non_linear_recursion.dl` | |
| Recursion termination | ✅ | `09_recursion/12_recursion_termination.dl` | |
| Empty base case | ✅ | `09_recursion/13_empty_base_case.dl` | |
| No new tuples first iter | ✅ | `09_recursion/14_no_new_tuples_first_iter.dl` | |
| Mutual one branch empty | ✅ | `09_recursion/15_mutual_one_branch_empty.dl` | |
| Self-loop single | ✅ | `09_recursion/16_self_loop_single.dl` | |

### 7.3 Recursion Edge Cases

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Four-way mutual recursion | ❌ | - | **MISSING** |
| Recursion depth limit | ❌ | - | **MISSING** (what is max?) |
| Recursion with aggregation | ✅ | `14_aggregations/09_agg_over_recursive.dl` | |
| Recursion with negation (stratified) | ✅ | `18_advanced_patterns/10_negation_recursion_combined.dl` | |
| Right-linear vs left-linear | ❌ | - | **MISSING** |

---

## 8. Aggregations

### 8.1 Basic Aggregations

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| COUNT | ✅ | `14_aggregations/01_count.dl` | |
| SUM | ✅ | `14_aggregations/02_sum.dl` | |
| MIN | ✅ | `14_aggregations/03_min.dl` | |
| MAX | ✅ | `14_aggregations/04_max.dl` | |
| AVG | ✅ | `14_aggregations/05_avg.dl` | |
| COUNT DISTINCT | ✅ | `14_aggregations/12_count_distinct.dl` | |

### 8.2 Aggregation Patterns

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Multi-column grouping | ✅ | `14_aggregations/06_multi_column_group.dl` | |
| Global count (no grouping) | ✅ | `14_aggregations/08_global_count.dl` | |
| Aggregation over recursive | ✅ | `14_aggregations/09_agg_over_recursive.dl` | |
| Nested aggregation | ✅ | `14_aggregations/10_nested_aggregation.dl` | |
| Having filter | ✅ | `14_aggregations/11_having_filter.dl` | |
| Empty groups | ✅ | `14_aggregations/13_empty_groups.dl` | |
| Multiple aggregates | ✅ | `14_aggregations/14_multiple_aggregates.dl` | |
| Aggregation with negation | ✅ | `14_aggregations/15_agg_with_negation.dl` | |
| Empty aggregation edge cases | ✅ | `14_aggregations/16_empty_agg_edge_cases.dl` | |

### 8.3 Ranking Aggregations

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| TOP_K | ✅ | `14_aggregations/07_top_k.dl` | |
| TOP_K with threshold | ✅ | `34_agg_advanced/01_top_k_threshold.dl` | |
| TOP_K threshold ascending | ✅ | `34_agg_advanced/02_top_k_threshold_asc.dl` | |
| Within radius | ✅ | `34_agg_advanced/03_within_radius.dl` | |
| TOP_K ascending | ✅ | `34_agg_advanced/04_top_k_ascending.dl` | |

### 8.4 Aggregation Error Cases

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Empty aggregation | ✅ | `10_edge_cases/09_empty_aggregation.dl` | |
| Unknown aggregate function | ❌ | - | **MISSING** |
| Invalid aggregation variable | ❌ | - | **MISSING** |
| Aggregation on non-numeric (SUM) | ❌ | - | **MISSING** |
| AVG producing non-integer | ❌ | - | **MISSING** |
| SUM overflow | ❌ | - | **MISSING** |
| COUNT with NULL | ❌ | - | **MISSING** |
| TOP_K with ties | ❌ | - | **MISSING** |

---

## 9. Arithmetic

### 9.1 Basic Operations

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Addition (+) | ✅ | `15_arithmetic/01_increment.dl` | |
| Subtraction (-) | ✅ | `15_arithmetic/03_subtract.dl` | |
| Multiplication (*) | ✅ | `15_arithmetic/02_multiply.dl` | |
| Division (/) | ✅ | `15_arithmetic/04_divide.dl` | |
| Modulo (%) | ✅ | `15_arithmetic/05_modulo.dl` | |

### 9.2 Arithmetic Patterns

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Increment in multi-join | ✅ | `15_arithmetic/06_increment_multi_join.dl` | |
| Increment edge cases | ✅ | `15_arithmetic/07_increment_edge_cases.dl` | |
| Complex increment | ✅ | `15_arithmetic/08_increment_complex.dl` | |
| Reversed join increment | ✅ | `15_arithmetic/09_increment_reversed_join.dl` | |
| 2-column arithmetic | ✅ | `15_arithmetic/11_arity_2col.dl` | |
| 3-column arithmetic | ✅ | `15_arithmetic/12_arity_3col.dl` | |
| 4-column arithmetic | ✅ | `15_arithmetic/13_arity_4col.dl` | |
| 5-column arithmetic | ✅ | `15_arithmetic/14_arity_5col.dl` | |
| Wildcard patterns | ✅ | `15_arithmetic/15_wildcard_patterns.dl` | |
| Division edge cases | ✅ | `15_arithmetic/16_division_edge_cases.dl` | |
| Arithmetic on aggregates | ✅ | `15_arithmetic/17_arithmetic_on_aggregates.dl` | |

### 9.3 Math Functions

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| abs_int64 | ✅ | `32_math/01_abs_int64.dl` | |
| abs_float64 | ✅ | `32_math/02_abs_float64.dl` | |
| Combined math | ✅ | `32_math/03_math_combined.dl` | |
| Sign function | ✅ | `32_math/04_sign_function.dl` | |
| Float abs | ✅ | `32_math/05_float_abs.dl` | |

### 9.4 Arithmetic Error/Edge Cases

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Division by zero | ✅ | `12_errors/08_division_by_zero_error.dl` | Fixed: Returns NULL, documented |
| Modulo by zero | ⚠️ | `12_errors/10_negative_modulo_error.dl` | Test may be broken |
| Arithmetic overflow | ⚠️ | `12_errors/09_arithmetic_overflow_error.dl` | Test may be broken |
| Float precision | ✅ | `12_errors/11_float_precision_error.dl` | |
| Negative modulo | ⚠️ | `12_errors/10_negative_modulo_error.dl` | Verify semantics |
| Operator precedence | ✅ | `38_syntax_gaps/04_operator_precedence.dl` | Fixed: Arithmetic in head |
| Chained operations (A+B+C+D) | ❌ | - | **MISSING** |
| Deeply nested parentheses | ⚠️ | `12_errors/12_deep_nesting_limit_error.dl` | Parser fails at ~5 levels |
| Float + Integer mixing | ❌ | - | **MISSING** |
| Unary minus (-X) | ❌ | - | **MISSING** |
| Double negative (X - (-Y)) | ❌ | - | **MISSING** |

---

## 10. Types and Values

### 10.1 Integer Types

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Positive integers | ✅ | `11_types/02_integers.dl` | |
| Negative integers | ✅ | `11_types/10_negative_numbers.dl` | |
| Zero | ✅ | `11_types/09_zero_handling.dl` | |
| Large integers | ✅ | `11_types/04_large_integers.dl` | Fixed: Uses simple comparison |
| i64 max boundary | ✅ | `11_types/15_int64_boundaries.dl` | Now tested |
| i64 min boundary | ✅ | `11_types/15_int64_boundaries.dl` | Now tested |

### 10.2 Float Types

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Basic floats | ✅ | `11_types/03_floats_truncation.dl` | |
| Float comparisons | ✅ | `11_types/14_float_comparisons.dl` | |
| Scientific notation | ✅ | `38_syntax_gaps/01_scientific_notation.dl` | |
| Negative floats | ✅ | `11_types/10_negative_numbers.dl` | |
| Very small floats | ❌ | - | **MISSING** |
| Very large floats | ❌ | - | **MISSING** |
| Float precision limits | ❌ | - | **MISSING** |
| NaN handling | ❌ | - | **MISSING** |
| Infinity handling | ❌ | - | **MISSING** |

### 10.3 String Types

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Basic strings | ✅ | `11_types/01_strings.dl` | |
| String edge cases | ✅ | `11_types/12_string_edge_cases.dl` | |
| Unicode strings | ✅ | `37_string_edge_cases/01_unicode.dl` | |
| Special characters | ✅ | `37_string_edge_cases/02_special_chars.dl` | |
| Empty string | ✅ | `37_string_edge_cases/03_empty_string.dl` | |
| Long strings | ✅ | `37_string_edge_cases/04_long_strings.dl` | |
| Escape sequences | ✅ | `37_string_edge_cases/05_escape_sequences.dl` | |
| Backslash escape | ✅ | `38_syntax_gaps/03_backslash_escape.dl` | |
| String with quotes | ❌ | - | **MISSING** |
| Multi-line strings | ❌ | - | **MISSING** |

### 10.4 Boolean Types

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Boolean literals | ✅ | `11_types/11_booleans.dl` | |
| Boolean in comparisons | ✅ | `11_types/22_boolean_comparisons.dl` | Now tested |

### 10.5 Mixed Types

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Mixed type tuples | ✅ | `11_types/05_mixed_type_tuples.dl` | |
| Integer arithmetic | ✅ | `11_types/06_int_arithmetic.dl` | |
| Numeric bounds | ✅ | `11_types/07_numeric_bounds.dl` | |
| Comparison ops | ✅ | `11_types/08_comparison_ops.dl` | |
| Constant filters | ✅ | `11_types/13_constant_filters.dl` | |

---

## 11. Vectors

### 11.1 Vector Distance Functions

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Euclidean distance | ✅ | `16_vectors/01_euclidean_distance.dl` | |
| Cosine distance | ✅ | `16_vectors/02_cosine_distance.dl` | |
| Dot product | ✅ | `16_vectors/03_dot_product.dl` | |
| Manhattan distance | ✅ | `16_vectors/04_manhattan_distance.dl` | |

### 11.2 Vector Operations

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Vector operations | ✅ | `16_vectors/05_vec_operations.dl` | |
| Pairwise similarity | ✅ | `16_vectors/06_pairwise_similarity.dl` | |
| Normalize | ✅ | `16_vectors/07_normalize.dl` | |
| Vector add | ✅ | `16_vectors/08_vec_add.dl` | |
| Vector scale | ✅ | `16_vectors/09_vec_scale.dl` | |
| LSH bucket | ✅ | `16_vectors/10_lsh_bucket.dl` | |

### 11.3 Vector Edge Cases

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Empty vector | ✅ | `36_vector_edge_cases/01_empty_vector.dl` | |
| Zero vector | ✅ | `36_vector_edge_cases/02_zero_vector.dl` | |
| Dimension mismatch | ✅ | `36_vector_edge_cases/03_dimension_mismatch.dl` | |
| Single element | ✅ | `36_vector_edge_cases/04_single_element.dl` | |
| High dimensional | ✅ | `36_vector_edge_cases/05_high_dimensional.dl` | |
| NaN in vector | ❌ | - | **MISSING** |
| Infinity in vector | ❌ | - | **MISSING** |

### 11.4 Quantization

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Quantize linear | ✅ | `30_quantization/01_quantize_linear.dl` | |
| Quantize symmetric | ✅ | `30_quantization/02_quantize_symmetric.dl` | |
| Dequantize | ✅ | `30_quantization/03_dequantize.dl` | |
| Dequantize scaled | ✅ | `30_quantization/04_dequantize_scaled.dl` | |
| Euclidean int8 | ✅ | `30_quantization/05_euclidean_int8.dl` | |
| Cosine int8 | ✅ | `30_quantization/06_cosine_int8.dl` | |
| Dot int8 | ✅ | `30_quantization/07_dot_int8.dl` | |
| Manhattan int8 | ✅ | `30_quantization/08_manhattan_int8.dl` | |

### 11.5 LSH Operations

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| LSH bucket | ✅ | `31_lsh/01_lsh_bucket.dl` | |
| LSH probes | ✅ | `31_lsh/02_lsh_probes.dl` | |
| LSH multi-probe | ✅ | `31_lsh/03_lsh_multi_probe.dl` | |
| LSH similarity search | ✅ | `31_lsh/04_lsh_similarity_search.dl` | |
| LSH identical vectors | ✅ | `31_lsh/05_lsh_identical_vectors.dl` | |

---

## 12. Temporal Operations

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| time_now | ✅ | `29_temporal/01_time_now.dl` | |
| time_diff | ✅ | `29_temporal/02_time_diff.dl` | |
| time_add_sub | ✅ | `29_temporal/03_time_add_sub.dl` | |
| time_decay | ✅ | `29_temporal/04_time_decay.dl` | |
| time_decay_linear | ✅ | `29_temporal/05_time_decay_linear.dl` | |
| time_comparisons | ✅ | `29_temporal/06_time_comparisons.dl` | |
| within_last | ✅ | `29_temporal/07_within_last.dl` | |
| intervals_overlap | ✅ | `29_temporal/08_intervals_overlap.dl` | |
| interval_contains | ✅ | `29_temporal/09_interval_contains.dl` | |
| interval_duration | ✅ | `29_temporal/10_interval_duration.dl` | |
| combined_temporal | ✅ | `29_temporal/11_combined_temporal.dl` | |
| point_in_interval | ✅ | `29_temporal/12_point_in_interval.dl` | |

---

## 13. Meta Commands

### 13.1 Knowledge Graph Commands

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| .kg (show current) | ✅ | `33_meta/01_kg_show.dl` | |
| .kg list | ✅ | `01_knowledge_graph/02_list_kg.dl` | |
| .kg create | ✅ | `01_knowledge_graph/01_create_use_drop.dl` | |
| .kg use | ✅ | `01_knowledge_graph/01_create_use_drop.dl` | |
| .kg drop | ✅ | `01_knowledge_graph/01_create_use_drop.dl` | |
| .kg drop current (error) | ✅ | `01_knowledge_graph/05_drop_current_kg_error.dl` | |
| .kg drop non-existent | ✅ | `12_errors/05_drop_nonexistent_db_error.dl` | |
| .kg use non-existent | ✅ | `12_errors/06_use_nonexistent_db_error.dl` | |
| .kg create duplicate | ✅ | `12_errors/03_duplicate_kg_error.dl` | |

### 13.2 Relation Commands

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| .rel (list) | ✅ | `01_knowledge_graph/03_rel_commands.dl` | |
| .rel <name> (describe) | ✅ | `02_relations/08_list_relations.dl` | |

### 13.3 Rule Commands

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| .rule (list) | ✅ | `17_rule_commands/01_rule_list.dl` | |
| .rule <name> (query) | ✅ | `17_rule_commands/02_rule_query.dl` | |
| .rule def <name> | ✅ | `17_rule_commands/04_rule_def.dl` | |
| .rule drop <name> | ✅ | `17_rule_commands/03_rule_drop.dl` | |
| .rule clear <name> | ✅ | `17_rule_commands/07_rule_clear.dl` | |
| .rule edit <name> | ✅ | `17_rule_commands/08_rule_edit.dl` | |
| Multi-clause rules | ✅ | `17_rule_commands/05_multi_clause_rules.dl` | |
| Drop multi-clause | ✅ | `17_rule_commands/06_drop_multi_clause.dl` | |
| .rule drop non-existent | ✅ | `17_rule_commands/10_drop_nonexistent_rule.dl` | Now tested |

### 13.4 Session Commands

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| .session (list) | ✅ | `33_meta/05_session_list.dl` | |
| .session clear | ✅ | `33_meta/07_session_clear.dl` | |
| .session drop <n> | ✅ | `33_meta/06_session_drop.dl` | |
| .session drop invalid index | ❌ | - | **MISSING** |

### 13.5 System Commands

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| .status | ✅ | `39_meta_complete/01_status.dl` | |
| .compact | ✅ | `33_meta/03_compact.dl` | |
| .help | ✅ | `33_meta/04_help.dl` | |
| .quit / .exit / .q | ❌ | - | **MISSING** (hard to test) |

### 13.6 Load Commands

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| .load basic | ✅ | `40_load_command/01_load_basic.dl` | |
| .load --replace | ⚠️ | `40_load_command/02_load_replace.dl` | Mode may be ignored |
| .load --merge | ⚠️ | `40_load_command/03_load_merge.dl` | Mode may be ignored |
| .load non-existent | ✅ | `40_load_command/04_load_nonexistent_error.dl` | |
| .load syntax error in file | ❌ | - | **MISSING** |
| .load circular dependency | ❌ | - | **MISSING** |
| .load empty file | ❌ | - | **MISSING** |
| .load mode verification | ❌ | - | **MISSING** (modes don't work) |

---

## 14. Session Management

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Session rules | ✅ | `04_session/01_session_rules.dl` | |
| Session facts | ✅ | `04_session/02_session_facts.dl` | |
| Session rules with facts | ✅ | `04_session/03_session_rules_with_facts.dl` | |
| Session with persistent | ✅ | `04_session/04_session_with_persistent.dl` | |
| Session isolation | ✅ | `04_session/05_session_isolation.dl` | |
| Session shadows persistent (same name) | ❌ | - | **MISSING** |
| Session rule referencing persistent | ✅ | `04_session/04_session_with_persistent.dl` | |
| Persistent referencing session (should fail?) | ❌ | - | **MISSING** |
| Session cleared on KG switch | ✅ | `04_session/05_session_isolation.dl` | |

---

## 15. Knowledge Graph Management

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Create KG | ✅ | `01_knowledge_graph/01_create_use_drop.dl` | |
| Use KG | ✅ | `01_knowledge_graph/01_create_use_drop.dl` | |
| Drop KG | ✅ | `01_knowledge_graph/01_create_use_drop.dl` | |
| List KGs | ✅ | `01_knowledge_graph/02_list_kg.dl` | |
| Drop current KG (error) | ✅ | `01_knowledge_graph/05_drop_current_kg_error.dl` | |
| KG data isolation | ❌ | - | **MISSING** |
| KG rule isolation | ❌ | - | **MISSING** |

---

## 16. Rule Management

### 16.1 Basic Rule Operations

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Register rule | ✅ | Multiple tests | |
| List rules | ✅ | `17_rule_commands/01_rule_list.dl` | |
| Query rule | ✅ | `17_rule_commands/02_rule_query.dl` | |
| Show rule definition | ✅ | `17_rule_commands/04_rule_def.dl` | |
| Drop rule | ✅ | `17_rule_commands/03_rule_drop.dl` | |
| Clear rule clauses | ✅ | `17_rule_commands/07_rule_clear.dl` | |
| Edit rule clause | ✅ | `17_rule_commands/08_rule_edit.dl` | |
| Add clause to existing rule | ✅ | `17_rule_commands/05_multi_clause_rules.dl` | |
| Drop multi-clause rule | ✅ | `17_rule_commands/06_drop_multi_clause.dl` | |
| Rule with same name as relation | ✅ | `17_rule_commands/18_rule_with_relation_name.dl` | Now tested |

### 16.2 Rule Drop vs Rule Remove (`.rule drop` vs `.rule remove`)

**Design Decision**:
- `.rule drop <name>` - Removes ALL clauses of a rule (deletes the entire rule)
- `.rule remove <name> <index>` - Removes a specific clause by 1-based index
- `-relation(X, Y) :- condition.` - **Conditional fact deletion** (NOT rule removal)

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| `.rule drop` removes all clauses | ✅ | `17_rule_commands/11_drop_removes_all_clauses.dl` | 3-clause rule fully removed |
| `.rule remove` on specific clause | ✅ | `17_rule_commands/12_rule_remove_clause.dl` | Removes clause by index |
| `.rule remove` last clause deletes rule | ✅ | `17_rule_commands/13_rule_remove_last_clause.dl` | Rule disappears when empty |
| `.rule remove` index out of bounds | ✅ | `17_rule_commands/14_rule_remove_error.dl` | Error message |
| `.rule remove` nonexistent rule | ✅ | `17_rule_commands/15_rule_remove_nonexistent.dl` | Error message |
| `.rule drop` then add new clauses | ✅ | `17_rule_commands/16_rule_drop_then_add.dl` | Now tested |
| `.rule remove` preserves order | ✅ | `17_rule_commands/17_rule_remove_order.dl` | Now tested |

### 16.3 Conditional Fact Deletion (`-relation(X, Y) :- condition.`)

**Implementation**: Conditional delete finds tuples matching the condition and removes them from the target relation. The target relation is automatically included in the query body to bind all head variables.

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Conditional delete based on relation | ✅ | `27_atomic_ops/05_conditional_delete.dl` | `-edge(X, Y) :- source(X).` |
| Conditional delete with filter | ✅ | `27_atomic_ops/03_bulk_conditional_delete.dl` | Pre-existing test |
| Conditional delete all facts | ❌ | - | **MISSING** - `-rel(X, Y) :- rel(X, Y).` |
| Conditional delete with join | ❌ | - | **MISSING** - Multi-relation condition |
| Conditional delete empty result | ❌ | - | **MISSING** - Condition matches nothing |

### 16.4 Rule Persistence After Modification

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| `.rule remove` persists across restart | ❌ | - | **MISSING** - After clause removal, reload KG |
| `.rule drop` persists across restart | ❌ | - | **MISSING** |
| `.rule clear` persists across restart | ❌ | - | **MISSING** |
| `.rule edit` persists across restart | ❌ | - | **MISSING** |

### 16.5 Error Cases for Rule Operations

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| `.rule drop` non-existent rule | ✅ | `17_rule_commands/10_drop_nonexistent_rule.dl` | Returns "Rule not found" error |
| `-` on non-existent rule | ❌ | - | Blocked - feature not implemented |
| `-` with malformed clause | ❌ | - | Blocked - feature not implemented |
| `.rule drop` during query execution | ❌ | - | **MISSING** |
| `-` during query execution | ❌ | - | Blocked - feature not implemented |
| Insert into rule (should fail) | ✅ | `12_errors/21_insert_into_view.dl` | Fixed - proper error returned |

---

## 17. Schema System

### 17.1 Schema Declaration

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Explicit schema | ✅ | `24_rel_schemas/01_explicit_schema.dl` | |
| Record schema sugar | ✅ | `24_rel_schemas/02_record_schema_sugar.dl` | |
| Schema with views | ✅ | `24_rel_schemas/03_schema_with_views.dl` | |

### 17.2 Type Declarations

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Simple alias | ✅ | `23_type_declarations/01_simple_alias.dl` | |
| Record type | ✅ | `23_type_declarations/02_record_type.dl` | |
| Multiple types | ✅ | `23_type_declarations/03_multiple_types.dl` | |

### 17.3 Schema Validation

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Schema arity mismatch | ✅ | `12_errors/04_arity_mismatch_error.dl` | |
| Schema type mismatch | ✅ | `12_errors/29_type_mismatch_error.dl` | Now tested |
| Schema on insert | ❌ | - | **MISSING** |
| Schema persistence | ❌ | - | **MISSING** |

---

## 18. Error Handling

### 18.1 Parse Errors

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Invalid syntax | ✅ | `12_errors/02_invalid_syntax_error.dl` | |
| Reserved word errors | ✅ | `38_syntax_gaps/05_reserved_word_errors.dl` | |
| Case sensitivity | ✅ | `38_syntax_gaps/06_case_sensitivity.dl` | |
| Deep nesting limit | ✅ | `12_errors/12_deep_nesting_limit_error.dl` | |
| Missing period | ✅ | `12_errors/24_missing_period_error.dl` | Now tested |
| Unbalanced parentheses | ✅ | `12_errors/23_unbalanced_parens.dl` | Now tested |
| Invalid identifier | ✅ | `12_errors/37_invalid_identifier.dl` | Now tested |

### 18.2 Semantic Errors

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Undefined relation | ✅ | `12_errors/01_undefined_relation_error.dl` | |
| Arity mismatch | ✅ | `12_errors/04_arity_mismatch_error.dl` | |
| Self-negation | ✅ | `12_errors/17_self_negation_error.dl` | |
| Unsafe negation | ✅ | `12_errors/20_unsafe_negation_error.dl` | |
| Edge case rules | ✅ | `12_errors/15_edge_case_rules_error.dl` | |
| Unbound head variable | ✅ | `12_errors/27_unbound_head_var_error.dl` | Now tested |
| Unbound comparison variable | ✅ | `12_errors/39_unbound_comparison.dl` | Now tested |
| Function call in rule head | ✅ | `12_errors/38_function_in_head.dl` | Now tested |

### 18.3 Runtime Errors

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Division by zero | ✅ | `12_errors/08_division_by_zero_error.dl` | Fixed: Returns NULL (documented behavior) |
| Arithmetic overflow | ⚠️ | `12_errors/09_arithmetic_overflow_error.dl` | May be broken |
| Negative modulo | ⚠️ | `12_errors/10_negative_modulo_error.dl` | May be broken |
| Float precision | ✅ | `12_errors/11_float_precision_error.dl` | |
| Query timeout | ❌ | - | **MISSING** |
| Memory limit | ❌ | - | **MISSING** |
| Result size limit | ❌ | - | **MISSING** |

### 18.4 Knowledge Graph Errors

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Drop non-existent | ✅ | `12_errors/05_drop_nonexistent_db_error.dl` | |
| Use non-existent | ✅ | `12_errors/06_use_nonexistent_db_error.dl` | |
| Create duplicate | ✅ | `12_errors/03_duplicate_kg_error.dl` | |

---

## 19. Edge Cases

### 19.1 General Edge Cases

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Empty relation | ✅ | `10_edge_cases/01_empty_relation.dl` | |
| Self-loop | ✅ | `10_edge_cases/02_self_loop.dl` | |
| Duplicates | ✅ | `10_edge_cases/03_duplicates.dl` | |
| Ternary relations | ✅ | `10_edge_cases/04_ternary.dl` | |
| Same relation twice | ✅ | `10_edge_cases/05_same_relation_twice.dl` | |
| Overlapping tuples | ✅ | `10_edge_cases/06_overlapping_tuples.dl` | |
| Self-join patterns | ✅ | `10_edge_cases/07_self_join_patterns.dl` | |
| Join empty relations | ✅ | `10_edge_cases/08_join_empty_relations.dl` | |
| Empty aggregation | ✅ | `10_edge_cases/09_empty_aggregation.dl` | |
| Empty after delete | ✅ | `10_edge_cases/10_empty_after_delete.dl` | |
| Join one side empty | ✅ | `10_edge_cases/11_join_one_side_empty.dl` | |
| View evaluates empty | ✅ | `10_edge_cases/12_view_evaluates_empty.dl` | |
| Comments syntax | ✅ | `10_edge_cases/13_comments_syntax.dl` | |
| Wildcard patterns | ✅ | `10_edge_cases/14_wildcard_patterns.dl` | |
| Relation operations | ✅ | `10_edge_cases/15_relation_operations.dl` | |

### 19.2 Boundary Conditions

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Zero-length string | ✅ | `37_string_edge_cases/03_empty_string.dl` | |
| Zero value | ✅ | `11_types/09_zero_handling.dl` | |
| Single row result | ❌ | - | **MISSING** |
| Very large result set | ❌ | - | **MISSING** |
| Very wide tuples (20+ columns) | ❌ | - | **MISSING** |
| Relation with 1 column | ❌ | - | **MISSING** |

---

## 20. Performance

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Medium dataset | ✅ | `13_performance/01_medium_dataset.dl` | |
| Wide join | ✅ | `13_performance/02_wide_join.dl` | |
| Recursive depth | ✅ | `13_performance/03_recursive_depth.dl` | |
| Dense graph | ✅ | `13_performance/04_dense_graph.dl` | |
| Multiple relations | ✅ | `13_performance/05_multiple_relations.dl` | |
| Large dataset (1000) | ✅ | `13_performance/06_large_dataset_1000.dl` | |
| Wide tuples (10 col) | ✅ | `13_performance/07_wide_tuples_10col.dl` | |
| Many joins | ✅ | `13_performance/08_many_joins.dl` | |
| Sparse tree | ✅ | `13_performance/09_sparse_tree.dl` | |
| Aggregation stress | ✅ | `13_performance/10_aggregation_stress.dl` | |
| Complex recursion | ✅ | `13_performance/11_complex_recursion.dl` | |
| Long rule bodies | ✅ | `13_performance/12_long_rule_bodies.dl` | |
| Very large dataset (10000+) | ❌ | - | **MISSING** |
| Concurrent queries | ❌ | - | **MISSING** |

---

## 21. Integration Scenarios

### 21.1 Real-World Patterns

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| RBAC permissions | ✅ | `20_applications/01_rbac_permissions.dl` | |
| RBAC deny rules | ✅ | `20_applications/02_rbac_deny.dl` | |
| Friends of friends | ✅ | `20_applications/03_friends_of_friends.dl` | |
| Influence propagation | ✅ | `20_applications/04_influence_propagation.dl` | |
| BOM explosion | ✅ | `20_applications/05_bom_explosion.dl` | |
| BOM cost rollup | ✅ | `20_applications/06_bom_cost_rollup.dl` | |
| Package dependencies | ✅ | `20_applications/07_package_deps.dl` | |
| Version conflicts | ✅ | `20_applications/08_version_conflicts.dl` | |
| Org chart levels | ✅ | `20_applications/09_org_chart_levels.dl` | |
| Common ancestor | ✅ | `20_applications/10_common_ancestor.dl` | |

### 21.2 Advanced Patterns

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Shortest path | ✅ | `18_advanced_patterns/01_shortest_path.dl` | |
| Cycle detection | ✅ | `18_advanced_patterns/02_cycle_detection.dl` | |
| SCC | ✅ | `18_advanced_patterns/03_scc.dl` | |
| Bipartite check | ✅ | `18_advanced_patterns/04_bipartite.dl` | |
| Non-bipartite | ✅ | `18_advanced_patterns/05_non_bipartite.dl` | |
| K4 clique | ✅ | `18_advanced_patterns/06_clique_k4.dl` | |
| Points-to analysis | ✅ | `18_advanced_patterns/07_points_to.dl` | |
| Reaching definitions | ✅ | `18_advanced_patterns/08_reaching_defs.dl` | |
| Ancestors | ✅ | `18_advanced_patterns/09_ancestors.dl` | |
| Negation + recursion | ✅ | `18_advanced_patterns/10_negation_recursion_combined.dl` | |
| Recursion + arithmetic + agg | ✅ | `18_advanced_patterns/11_recursion_arithmetic_agg.dl` | |
| All features stress | ✅ | `18_advanced_patterns/12_all_features_stress.dl` | |

### 21.3 Self-Checking

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Increment check | ✅ | `19_self_checking/01_increment_check.dl` | |
| Arithmetic ops check | ✅ | `19_self_checking/02_arithmetic_ops_check.dl` | |
| Assertion demo | ✅ | `19_self_checking/03_assertion_demo.dl` | |
| Bug verification | ✅ | `19_self_checking/04_bug_verification.dl` | |

### 21.4 Documentation Coverage

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Cheatsheet basics | ✅ | `28_docs_coverage/01_cheatsheet_basics.dl` | |
| Cheatsheet rules | ✅ | `28_docs_coverage/02_cheatsheet_rules.dl` | |
| Cheatsheet queries | ✅ | `28_docs_coverage/03_cheatsheet_queries.dl` | |
| Cheatsheet aggregations | ✅ | `28_docs_coverage/04_cheatsheet_aggregations.dl` | |
| Cheatsheet graph | ✅ | `28_docs_coverage/05_cheatsheet_graph_example.dl` | |
| Cheatsheet RBAC | ✅ | `28_docs_coverage/06_cheatsheet_rbac_example.dl` | |
| Syntax literals | ✅ | `28_docs_coverage/07_syntax_literals.dl` | |
| Syntax schemas | ✅ | `28_docs_coverage/08_syntax_schemas.dl` | |
| Syntax negation | ✅ | `28_docs_coverage/09_syntax_negation.dl` | |
| Syntax recursion | ✅ | `28_docs_coverage/10_syntax_recursion.dl` | |
| Syntax social network | ✅ | `28_docs_coverage/11_syntax_social_network.dl` | |
| Syntax graph analysis | ✅ | `28_docs_coverage/12_syntax_graph_analysis.dl` | |
| Syntax BOM | ✅ | `28_docs_coverage/13_syntax_bom.dl` | |
| Syntax vectors | ✅ | `28_docs_coverage/14_syntax_vectors.dl` | |
| REPL guide basics | ✅ | `28_docs_coverage/15_repl_guide_basics.dl` | |
| REPL guide rules | ✅ | `28_docs_coverage/16_repl_guide_rules.dl` | |
| REPL guide schemas | ✅ | `28_docs_coverage/17_repl_guide_schemas.dl` | |
| REPL guide workflow | ✅ | `28_docs_coverage/18_repl_guide_workflow.dl` | |
| REPL guide wildcards | ✅ | `28_docs_coverage/19_repl_guide_wildcards.dl` | |

---

## 22. Comments & Syntax

### 22.1 Comment Syntax

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Line comment (%) | ✅ | `10_edge_cases/13_comments_syntax.dl` | |
| Block comment (/* */) | ✅ | `10_edge_cases/13_comments_syntax.dl` | |
| Nested block comments | ❌ | - | **MISSING** `/* outer /* inner */ */` |
| Comment at end of line | ❌ | - | **MISSING** |
| Comment between statements | ❌ | - | **MISSING** |
| Comment inside rule | ❌ | - | **MISSING** |
| Empty comment | ❌ | - | **MISSING** |

### 22.2 Whitespace Handling

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Extra whitespace around operators | ❌ | - | **MISSING** |
| Tabs vs spaces | ❌ | - | **MISSING** |
| Trailing whitespace | ❌ | - | **MISSING** |
| Empty lines between statements | ❌ | - | **MISSING** |
| Statement spanning multiple lines | ❌ | - | **MISSING** |

---

## 23. Record Types & Field Access

### 23.1 Field Access Syntax

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Simple field access (U.id) | ❌ | - | **MISSING** |
| Chained field access (U.addr.city) | ❌ | - | **MISSING** |
| Field access in rule head | ❌ | - | **MISSING** |
| Field access in rule body | ❌ | - | **MISSING** |
| Field access in query | ❌ | - | **MISSING** |
| Field access on undefined field | ❌ | - | **MISSING** (error case) |

### 23.2 Record Patterns

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Record pattern destructuring | ❌ | - | **MISSING** `{id: X, name: Y}` |
| Partial record pattern | ❌ | - | **MISSING** |
| Nested record pattern | ❌ | - | **MISSING** |
| Record pattern with wildcard | ❌ | - | **MISSING** `{id: _, name: X}` |

---

## 24. Advanced Type System

### 24.1 Type Declarations

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Simple type alias | ✅ | `23_type_declarations/01_simple_alias.dl` | |
| Record type definition | ✅ | `23_type_declarations/02_record_type.dl` | |
| Multiple type definitions | ✅ | `23_type_declarations/03_multiple_types.dl` | |
| List type (list[T]) | ❌ | - | **MISSING** |
| Type alias chain (A -> B -> int) | ❌ | - | **MISSING** |
| Recursive type definition | ❌ | - | **MISSING** |

### 24.2 Refined Types

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Range refinement int(range(1,100)) | ❌ | - | **MISSING** |
| Pattern refinement string(pattern) | ❌ | - | **MISSING** |
| Multiple refinements | ❌ | - | **MISSING** |
| Refinement validation on insert | ❌ | - | **MISSING** |

### 24.3 Special Types

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Symbol type (interned atoms) | ❌ | - | **MISSING** |
| Timestamp type operations | ⚠️ | `29_temporal/` | Implicit via temporal |
| Named type usage | ❌ | - | **MISSING** |
| Any type (no constraint) | ❌ | - | **MISSING** |
| VectorInt8 type | ⚠️ | `30_quantization/` | Implicit via quantization |

---

## 25. Delete Operations (Extended)

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Delete entire relation (-name.) | ❌ | - | **MISSING** |
| Delete rule (-rulename.) | ❌ | - | **MISSING** |
| Delete relation with data | ❌ | - | **MISSING** |
| Delete non-existent relation | ❌ | - | **MISSING** (error case) |
| Delete relation used by rule | ❌ | - | **MISSING** (dependency check) |

---

## 26. Function Calls (Advanced)

### 26.1 Nested Function Calls

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Two-level nesting | ❌ | - | **MISSING** `euclidean(normalize(V1), V2)` |
| Three-level nesting | ❌ | - | **MISSING** |
| Mixed function/arithmetic nesting | ❌ | - | **MISSING** |
| Function with vector literal arg | ✅ | `16_vectors/` | Implicit |

### 26.2 Function Error Cases

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Unknown function name | ✅ | `12_errors/25_unknown_function_error.dl` | Now tested |
| Wrong argument count | ✅ | `12_errors/26_wrong_arg_count_error.dl` | Now tested |
| Wrong argument type | ❌ | - | **MISSING** |
| Function in unsupported position | ❌ | - | **MISSING** |

---

## 27. Parsing Edge Cases (Extended)

### 27.1 Syntax Errors

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Unbalanced parentheses | ❌ | - | **MISSING** |
| Unbalanced brackets | ❌ | - | **MISSING** |
| Missing period | ❌ | - | **MISSING** |
| Missing comma in args | ❌ | - | **MISSING** |
| Double period | ❌ | - | **MISSING** |
| Invalid relation name (uppercase) | ❌ | - | **MISSING** |
| Invalid variable name (lowercase) | ❌ | - | **MISSING** |
| Reserved word as identifier | ✅ | `38_syntax_gaps/05_reserved_word_errors.dl` | |

### 27.2 Complex Expressions

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Deeply nested parentheses (10+) | ⚠️ | `12_errors/12_deep_nesting_limit_error.dl` | Fails at ~5 |
| Very long rule body (20+ atoms) | ✅ | `13_performance/12_long_rule_bodies.dl` | |
| Basic arithmetic in head (X+1, X*2) | ✅ | `38_syntax_gaps/04_operator_precedence.dl` | Fixed: Tests all four ops |
| Arithmetic operator associativity | ❌ | - | **MISSING** |
| Left vs right recursion parse | ❌ | - | **MISSING** |

---

## 28. Concurrency & Parallelism

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Parallel query execution | ❌ | - | **MISSING** |
| Concurrent inserts | ❌ | - | **MISSING** |
| Concurrent insert + query | ❌ | - | **MISSING** |
| Concurrent delete + query | ❌ | - | **MISSING** |
| Session isolation under concurrency | ❌ | - | **MISSING** |
| KG isolation under concurrency | ❌ | - | **MISSING** |

---

## 29. Large Scale & Stress Tests

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| 10,000 row dataset | ❌ | - | **MISSING** |
| 100,000 row dataset | ❌ | - | **MISSING** |
| 1,000,000 row dataset | ❌ | - | **MISSING** |
| Wide tuples (20 columns) | ❌ | - | **MISSING** |
| Wide tuples (50 columns) | ❌ | - | **MISSING** |
| Very long string (1MB) | ❌ | - | **MISSING** |
| High-dimensional vector (10000) | ❌ | - | **MISSING** |
| Many relations (100+) | ❌ | - | **MISSING** |
| Many rules (100+) | ❌ | - | **MISSING** |
| Deep recursion (1000+) | ❌ | - | **MISSING** |

---

## 30. Transaction Semantics

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Atomic insert (all or nothing) | ❌ | - | **MISSING** |
| Rollback on error | ❌ | - | **MISSING** |
| Partial batch failure | ❌ | - | **MISSING** |
| State after failed insert | ❌ | - | **MISSING** |
| State after failed rule registration | ❌ | - | **MISSING** |

---

## 31. Vector Functions (Complete)

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| euclidean(v1, v2) | ✅ | `16_vectors/01_euclidean_distance.dl` | |
| cosine(v1, v2) | ✅ | `16_vectors/02_cosine_distance.dl` | |
| dot(v1, v2) | ✅ | `16_vectors/03_dot_product.dl` | |
| manhattan(v1, v2) | ✅ | `16_vectors/04_manhattan_distance.dl` | |
| normalize(v) | ✅ | `16_vectors/07_normalize.dl` | |
| vec_add(v1, v2) | ✅ | `16_vectors/08_vec_add.dl` | |
| vec_scale(v, s) | ✅ | `16_vectors/09_vec_scale.dl` | |
| **vec_dim(v)** | ❌ | - | **MISSING** - Get vector dimension |
| lsh_bucket | ✅ | `31_lsh/01_lsh_bucket.dl` | |
| lsh_probes | ✅ | `31_lsh/02_lsh_probes.dl` | |
| lsh_multi_probe | ✅ | `31_lsh/03_lsh_multi_probe.dl` | |

---

## 32. Math Functions (Complete)

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| abs_int64(x) | ✅ | `32_math/01_abs_int64.dl` | |
| abs_float64(x) | ✅ | `32_math/02_abs_float64.dl` | |
| abs with negative int | ✅ | `32_math/03_math_combined.dl` | |
| abs with negative float | ✅ | `32_math/05_float_abs.dl` | |
| sign(x) | ✅ | `32_math/04_sign_function.dl` | |
| abs(0) | ❌ | - | **MISSING** - Zero handling |
| abs(INT64_MIN) | ❌ | - | **MISSING** - Overflow case |

---

## 33. Literal Syntax (Complete)

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Positive integer | ✅ | `11_types/02_integers.dl` | |
| Negative integer | ✅ | `11_types/10_negative_numbers.dl` | |
| Float (3.14) | ✅ | `11_types/03_floats_truncation.dl` | |
| Scientific notation (1e10) | ✅ | `38_syntax_gaps/01_scientific_notation.dl` | |
| Negative scientific (-1e-5) | ❌ | - | **MISSING** |
| String with double quotes | ✅ | `11_types/01_strings.dl` | |
| String with single quotes | ❌ | - | **MISSING** (if supported) |
| String with escaped quotes | ✅ | `37_string_edge_cases/05_escape_sequences.dl` | |
| Boolean true | ✅ | `11_types/11_booleans.dl` | |
| Boolean false | ✅ | `11_types/11_booleans.dl` | |
| Vector literal [1.0, 2.0] | ✅ | `16_vectors/` | |
| Empty vector [] | ✅ | `36_vector_edge_cases/01_empty_vector.dl` | |
| Hex integer (0xFF) | ❌ | - | **MISSING** (if supported) |
| Binary integer (0b1010) | ❌ | - | **MISSING** (if supported) |

---

## 34. IR-Level Functions (Internal)

These functions exist at the IR level but may not be exposed through Datalog syntax:

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| hamming(v1, v2) | ❌ | - | **MISSING** - Bit difference count |
| euclidean_dequantized(v1, v2) | ❌ | - | **MISSING** - Dequantize then euclidean |
| cosine_dequantized(v1, v2) | ❌ | - | **MISSING** - Dequantize then cosine |
| lsh_bucket_int8(v, idx, hp) | ❌ | - | **MISSING** - LSH for int8 vectors |
| lsh_bucket_with_distances | ❌ | - | **MISSING** - Returns bucket + distances |
| lsh_probes_ranked | ❌ | - | **MISSING** - Smart probe ordering |
| lsh_multi_probe_int8 | ❌ | - | **MISSING** - Multi-probe for int8 |
| vec_dim_int8(v) | ❌ | - | **MISSING** - Dimension for int8 vectors |

---

## 35. Configuration & Environment

### 35.1 Storage Configuration

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Custom data_dir | ❌ | - | **MISSING** |
| auto_create_knowledge_graphs=true | ❌ | - | **MISSING** |
| auto_create_knowledge_graphs=false | ❌ | - | **MISSING** |
| Parquet format persistence | ❌ | - | **MISSING** |
| CSV format persistence | ❌ | - | **MISSING** |
| Snappy compression | ❌ | - | **MISSING** |
| No compression | ❌ | - | **MISSING** |

### 35.2 Optimization Flags

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| enable_join_planning=true | ❌ | - | **MISSING** |
| enable_join_planning=false | ❌ | - | **MISSING** |
| enable_sip_rewriting=true | ❌ | - | **MISSING** (currently disabled) |
| enable_subplan_sharing=true | ❌ | - | **MISSING** |
| enable_boolean_specialization=true | ❌ | - | **MISSING** |

### 35.3 Environment Variables

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| DATALOG_DEBUG flag | ❌ | - | **MISSING** |
| DEBUG_SESSION flag | ❌ | - | **MISSING** |
| FLOWLOG_ prefix overrides | ❌ | - | **MISSING** |
| Config file hierarchy (base→local→env) | ❌ | - | **MISSING** |

---

## 36. Resource Limits & Timeouts

### 36.1 Query Timeouts

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Default 60s timeout | ❌ | - | **MISSING** |
| Custom timeout (short) | ❌ | - | **MISSING** |
| Timeout cancellation | ❌ | - | **MISSING** |
| Infinite timeout | ❌ | - | **MISSING** |
| Cooperative timeout checking | ❌ | - | **MISSING** |

### 36.2 Memory Limits

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| max_memory_bytes enforcement | ❌ | - | **MISSING** |
| Memory tracking accuracy | ❌ | - | **MISSING** |
| Peak usage tracking | ❌ | - | **MISSING** |
| MemoryGuard RAII pattern | ❌ | - | **MISSING** |

### 36.3 Result Size Limits

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| max_result_size (10M default) | ❌ | - | **MISSING** |
| max_intermediate_size (100M) | ❌ | - | **MISSING** |
| max_row_width (100 columns) | ❌ | - | **MISSING** |
| max_recursion_depth (1000) | ❌ | - | **MISSING** |
| ResourceLimits::strict() preset | ❌ | - | **MISSING** |
| ResourceLimits::unlimited() preset | ❌ | - | **MISSING** |

---

## 37. Optimizer Passes

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Identity map elimination | ❌ | - | **MISSING** |
| Always-true filter elimination | ❌ | - | **MISSING** |
| Always-false filter elimination | ❌ | - | **MISSING** |
| Consecutive map fusion | ❌ | - | **MISSING** |
| Consecutive filter fusion | ❌ | - | **MISSING** |
| Filter pushdown to scans | ❌ | - | **MISSING** |
| Empty union elimination | ❌ | - | **MISSING** |
| Fixpoint convergence detection | ❌ | - | **MISSING** |
| No-op optimization (already optimized) | ❌ | - | **MISSING** |

---

## 38. Join Planning

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Star query (shared central var) | ❌ | - | **MISSING** |
| Chain query (linear deps) | ❌ | - | **MISSING** |
| Disconnected components skip | ❌ | - | **MISSING** |
| Antijoin preservation | ❌ | - | **MISSING** |
| MST construction | ❌ | - | **MISSING** |
| Rooted tree cost calculation | ❌ | - | **MISSING** |
| Schema remapping after reorder | ❌ | - | **MISSING** |

---

## 39. Storage & WAL

### 39.1 Write-Ahead Log

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| WAL append | ❌ | - | **MISSING** |
| WAL read all entries | ❌ | - | **MISSING** |
| WAL replay after restart | ❌ | - | **MISSING** |
| WAL clear after compaction | ❌ | - | **MISSING** |
| WAL compaction threshold | ❌ | - | **MISSING** |
| WAL file size tracking | ❌ | - | **MISSING** |

### 39.2 Persistence Layer

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Batch append | ❌ | - | **MISSING** |
| Auto-flush on buffer full | ❌ | - | **MISSING** |
| Compaction with GC | ❌ | - | **MISSING** |
| Shard isolation | ❌ | - | **MISSING** |
| Time frontier queries | ❌ | - | **MISSING** |

### 39.3 Consolidation

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Consolidate by (data, time) | ❌ | - | **MISSING** |
| Zero-diff removal | ❌ | - | **MISSING** |
| Multiplicity summing | ❌ | - | **MISSING** |
| Current state extraction | ❌ | - | **MISSING** |

---

## 40. Query Cache

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Compiled query cache hit | ❌ | - | **MISSING** |
| Compiled query cache miss | ❌ | - | **MISSING** |
| Result cache with TTL | ❌ | - | **MISSING** |
| TTL expiration | ❌ | - | **MISSING** |
| LRU eviction | ❌ | - | **MISSING** |
| Cache invalidation on data change | ❌ | - | **MISSING** |
| Cache statistics (hit rate) | ❌ | - | **MISSING** |
| Data fingerprinting | ❌ | - | **MISSING** |

---

## 41. REST API

### 41.1 Endpoints

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| POST /query | ❌ | - | **MISSING** |
| GET /relations | ❌ | - | **MISSING** |
| GET /relations/:name | ❌ | - | **MISSING** |
| POST /relations/:name | ❌ | - | **MISSING** |
| DELETE /relations/:name | ❌ | - | **MISSING** |
| GET /rules | ❌ | - | **MISSING** |
| GET /rules/:name | ❌ | - | **MISSING** |
| GET /knowledge-graphs | ❌ | - | **MISSING** |
| POST /knowledge-graphs | ❌ | - | **MISSING** |
| DELETE /knowledge-graphs/:name | ❌ | - | **MISSING** |
| GET /health | ❌ | - | **MISSING** |
| GET /status | ❌ | - | **MISSING** |

### 41.2 Error Responses

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| 400 Bad Request (malformed JSON) | ❌ | - | **MISSING** |
| 400 Bad Request (invalid query) | ❌ | - | **MISSING** |
| 404 Not Found (missing resource) | ❌ | - | **MISSING** |
| 408 Request Timeout | ❌ | - | **MISSING** |
| 500 Internal Server Error | ❌ | - | **MISSING** |

---

## 42. Client/Server Protocol

### 42.1 Connection

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Connect to server | ❌ | - | **MISSING** |
| Connection refused | ❌ | - | **MISSING** |
| Connection timeout | ❌ | - | **MISSING** |
| Mid-request disconnect | ❌ | - | **MISSING** |
| Reconnection after failure | ❌ | - | **MISSING** |

### 42.2 Heartbeat

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Heartbeat success | ❌ | - | **MISSING** |
| Heartbeat timeout | ❌ | - | **MISSING** |
| Max failures before disconnect | ❌ | - | **MISSING** |

---

## 43. Crash Recovery

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Recovery from WAL after crash | ❌ | - | **MISSING** |
| Recovery with partial batch | ❌ | - | **MISSING** |
| Metadata corruption handling | ❌ | - | **MISSING** |
| Incomplete file write | ❌ | - | **MISSING** |
| Disk full during write | ❌ | - | **MISSING** |
| Permission denied on files | ❌ | - | **MISSING** |

---

## 44. Schema Validation Errors

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| ArityMismatch error | ✅ | `12_errors/04_arity_mismatch_error.dl` | |
| TypeMismatch error | ❌ | - | **MISSING** |
| VectorDimensionMismatch | ❌ | - | **MISSING** |
| VectorInt8DimensionMismatch | ❌ | - | **MISSING** |
| Batch all-or-nothing rejection | ❌ | - | **MISSING** |

---

## 45. CLI Argument Testing *(NEW)*

### 45.1 Server CLI

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| `--host` flag parsing | ❌ | - | **MISSING** - No CLI tests exist |
| `--port` flag parsing | ❌ | - | **MISSING** |
| Default host (127.0.0.1) | ❌ | - | **MISSING** |
| Default port (8080) | ❌ | - | **MISSING** |
| Invalid port number | ❌ | - | **MISSING** |
| Unknown flag error | ❌ | - | **MISSING** - Server silently ignores |

### 45.2 Client CLI

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| `--script` / `-s` flag | ❌ | - | **MISSING** |
| `--repl` / `-r` flag | ❌ | - | **MISSING** |
| `--server` flag | ❌ | - | **MISSING** |
| `--help` / `-h` flag | ❌ | - | **MISSING** |
| Positional .dl file argument | ❌ | - | **MISSING** |
| Unknown flag error | ❌ | - | **MISSING** |
| Script file not found | ❌ | - | **MISSING** |

### 45.3 Environment Variables

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| DATALOG_DEBUG enables debug output | ❌ | - | **MISSING** |
| DEBUG_SESSION enables session debugging | ❌ | - | **MISSING** |
| HOME for history storage | ❌ | - | **MISSING** |

---

## 46. Serialization Round-trip Testing *(NEW)*

### 46.1 Value Serialization

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Value::Int32 JSON roundtrip | ❌ | - | **MISSING** - Custom Serialize impl |
| Value::Int64 JSON roundtrip | ❌ | - | **MISSING** |
| Value::Float64 JSON roundtrip | ❌ | - | **MISSING** |
| Value::String JSON roundtrip | ❌ | - | **MISSING** |
| Value::Bool JSON roundtrip | ❌ | - | **MISSING** |
| Value::Null JSON roundtrip | ❌ | - | **MISSING** |
| Value::Vector JSON roundtrip | ❌ | - | **MISSING** |
| Value::VectorInt8 JSON roundtrip | ❌ | - | **MISSING** |
| Value::Timestamp JSON roundtrip | ❌ | - | **MISSING** |

### 46.2 Abomonation (DD-native binary)

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Value Abomonation roundtrip | ❌ | - | **MISSING** - Required for DD |
| Tuple Abomonation roundtrip | ❌ | - | **MISSING** |
| Large vector Abomonation | ❌ | - | **MISSING** |

### 46.3 REST API DTOs

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| QueryRequest JSON roundtrip | ❌ | - | **MISSING** - 20+ DTO types |
| QueryResponse JSON roundtrip | ❌ | - | **MISSING** |
| ApiErrorDto JSON roundtrip | ❌ | - | **MISSING** |
| RelationDto JSON roundtrip | ❌ | - | **MISSING** |
| KnowledgeGraphDto JSON roundtrip | ❌ | - | **MISSING** |
| CreateViewRequest JSON roundtrip | ❌ | - | **MISSING** |

### 46.4 Wire Protocol

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| WireValue bincode roundtrip | ✅ | Unit test | test_serialization_roundtrip |
| WireTuple bincode roundtrip | ✅ | Unit test | |
| WireDataType serialization | ❌ | - | **MISSING** |

### 46.5 Legacy WAL

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| WalOp::Insert JSON roundtrip | ❌ | - | **MISSING** |
| WalOp::Delete JSON roundtrip | ❌ | - | **MISSING** |
| WalEntry JSON line format | ❌ | - | **MISSING** |

---

## 47. Numeric Edge Cases (Extended) *(NEW)*

### 47.1 Division Edge Cases

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| AVG of empty group (div by zero) | ❌ | - | **CRITICAL** - Currently unguarded |
| AVG of single value | ❌ | - | **MISSING** |
| Division result infinity | ❌ | - | **MISSING** |
| Division result NaN | ❌ | - | **MISSING** |

### 47.2 Integer Overflow

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| SUM overflow saturation | ❌ | - | **MISSING** - Uses checked_add |
| SUM underflow saturation | ❌ | - | **MISSING** |
| Arithmetic i64::MAX + 1 | ❌ | - | **MISSING** |
| Arithmetic i64::MIN - 1 | ❌ | - | **MISSING** |
| Multiplication overflow | ❌ | - | **MISSING** |

### 47.3 Type Cast Safety

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| i64 to i32 in range | ❌ | - | **MISSING** |
| i64 to i32 overflow | ❌ | - | **MISSING** |
| f64 to f32 precision loss | ❌ | - | **MISSING** |
| Large int to float precision | ❌ | - | **MISSING** |

### 47.4 Float Special Values

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| NaN in arithmetic | ❌ | - | **MISSING** |
| NaN propagation through rules | ❌ | - | **MISSING** |
| Infinity in comparisons | ❌ | - | **MISSING** |
| Negative infinity handling | ❌ | - | **MISSING** |
| Float epsilon comparisons | ❌ | - | **MISSING** |

### 47.5 Quantization Edge Cases

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Quantize f32 > 127 (overflow) | ❌ | - | **MISSING** |
| Quantize f32 < -128 (underflow) | ❌ | - | **MISSING** |
| Quantize with scale = 0 | ❌ | - | **MISSING** |
| Dequantize precision loss | ❌ | - | **MISSING** |

---

## 48. Specialized Execution Methods *(NEW)*

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| execute_transitive_closure() | ❌ | - | **MISSING** - Specialized method |
| execute_transitive_closure_dd() | ❌ | - | **MISSING** |
| execute_reachability() | ❌ | - | **MISSING** |
| execute_reachability_dd() | ❌ | - | **MISSING** |
| execute_recursive_fixpoint_tuples() | ❌ | - | **MISSING** |
| execute_parallel() | ❌ | - | **MISSING** |
| execute_with_config() custom timeout | ❌ | - | **MISSING** |
| execute_with_config() memory limit | ❌ | - | **MISSING** |

---

## 49. Error Variant Coverage *(NEW)*

### 49.1 InputLayerError Variants (22 total)

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| KnowledgeGraphNotFound | ✅ | `12_errors/06_use_nonexistent_db_error.dl` | |
| KnowledgeGraphExists | ❌ | - | **MISSING** |
| RelationNotFound | ✅ | `12_errors/01_undefined_relation_error.dl` | |
| CannotDropDefault | ❌ | - | **MISSING** |
| CannotDropCurrent | ❌ | - | **MISSING** |
| NoCurrentKnowledgeGraph | ❌ | - | **MISSING** |
| ParseError | ✅ | Multiple error tests | |
| ExecutionError | ✅ | Multiple error tests | |
| Timeout | ❌ | - | **MISSING** |
| SchemaViolation | ❌ | - | **MISSING** |
| VectorDimensionMismatch | ❌ | - | **MISSING** |
| TypeMismatch | ❌ | - | **MISSING** |
| InvalidData | ❌ | - | **MISSING** |
| ConnectionFailed | ❌ | - | **MISSING** |
| ConnectionLost | ❌ | - | **MISSING** |
| AuthenticationFailed | ❌ | - | **MISSING** |
| InternalError | ❌ | - | **MISSING** |
| ServerOverloaded | ❌ | - | **MISSING** |
| ShuttingDown | ❌ | - | **MISSING** |
| ResourceLimitExceeded | ❌ | - | **MISSING** |
| SerializationError | ❌ | - | **MISSING** |
| DeserializationError | ❌ | - | **MISSING** |

### 49.2 StorageError Variants (14 total)

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| StorageError::Io | ❌ | - | **MISSING** |
| StorageError::Parquet | ❌ | - | **MISSING** |
| StorageError::Arrow | ❌ | - | **MISSING** |
| StorageError::Json | ❌ | - | **MISSING** |
| StorageError::KnowledgeGraphNotFound | ❌ | - | **MISSING** |
| StorageError::KnowledgeGraphExists | ❌ | - | **MISSING** |
| StorageError::NoCurrentKnowledgeGraph | ❌ | - | **MISSING** |
| StorageError::CannotDropDefault | ❌ | - | **MISSING** |
| StorageError::CannotDropCurrentKG | ❌ | - | **MISSING** |
| StorageError::RelationNotFound | ❌ | - | **MISSING** |
| StorageError::InvalidRelationName | ❌ | - | **MISSING** |
| StorageError::MetadataError | ❌ | - | **MISSING** |
| StorageError::ParseError | ❌ | - | **MISSING** |
| StorageError::Other | ❌ | - | **MISSING** |

---

## 50. BuiltinFunction Coverage *(NEW)*

47 builtin functions - checking coverage status:

### 50.1 Vector Distance Functions

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Euclidean | ✅ | `16_vectors/01_euclidean_distance.dl` | |
| Cosine | ✅ | `16_vectors/02_cosine_distance.dl` | |
| DotProduct | ✅ | `16_vectors/03_dot_product.dl` | |
| Manhattan | ✅ | `16_vectors/04_manhattan_distance.dl` | |
| Hamming | ❌ | - | **MISSING** - IR-level only |

### 50.2 Vector Operations

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| VecNormalize | ✅ | `16_vectors/07_normalize.dl` | |
| VecDim | ❌ | - | **MISSING** |
| VecAdd | ✅ | `16_vectors/08_vec_add.dl` | |
| VecScale | ✅ | `16_vectors/09_vec_scale.dl` | |

### 50.3 Int8 Quantization

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| QuantizeLinear | ❌ | - | **MISSING** |
| QuantizeSymmetric | ❌ | - | **MISSING** |
| Dequantize | ❌ | - | **MISSING** |
| DequantizeScaled | ❌ | - | **MISSING** |

### 50.4 Int8 Distance Functions

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| EuclideanInt8 | ❌ | - | **MISSING** |
| CosineInt8 | ❌ | - | **MISSING** |
| DotProductInt8 | ❌ | - | **MISSING** |
| ManhattanInt8 | ❌ | - | **MISSING** |
| EuclideanDequantized | ❌ | - | **MISSING** |
| CosineDequantized | ❌ | - | **MISSING** |

### 50.5 LSH Operations

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| LshBucket | ✅ | `16_vectors/10_lsh_bucket.dl` | |
| LshBucketInt8 | ❌ | - | **MISSING** |
| LshProbes | ✅ | `31_lsh/02_lsh_probes.dl` | |
| LshBucketWithDistances | ❌ | - | **MISSING** |
| LshProbesRanked | ❌ | - | **MISSING** |
| LshMultiProbe | ✅ | `31_lsh/03_lsh_multi_probe.dl` | |
| LshMultiProbeInt8 | ❌ | - | **MISSING** |
| VecDimInt8 | ❌ | - | **MISSING** |

### 50.6 Temporal Functions

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| TimeNow | ✅ | `29_temporal/01_time_now.dl` | |
| TimeDiff | ✅ | `29_temporal/02_time_diff.dl` | |
| TimeAdd | ✅ | `29_temporal/03_time_add.dl` | |
| TimeSub | ✅ | `29_temporal/04_time_sub.dl` | |
| TimeDecay | ✅ | `29_temporal/05_time_decay.dl` | |
| TimeDecayLinear | ✅ | `29_temporal/06_time_decay_linear.dl` | |
| TimeBefore | ✅ | `29_temporal/07_time_before.dl` | |
| TimeAfter | ✅ | `29_temporal/08_time_after.dl` | |
| TimeBetween | ✅ | `29_temporal/09_time_between.dl` | |
| WithinLast | ✅ | `29_temporal/10_within_last.dl` | |
| IntervalsOverlap | ✅ | `29_temporal/11_intervals_overlap.dl` | |
| IntervalContains | ❌ | - | **MISSING** |
| IntervalDuration | ❌ | - | **MISSING** |
| PointInInterval | ❌ | - | **MISSING** |

### 50.7 Math Functions

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| AbsInt64 | ✅ | `32_math/01_abs_int64.dl` | |
| AbsFloat64 | ✅ | `32_math/02_abs_float64.dl` | |

---

## 51. Term Variant Coverage *(NEW - from code analysis)*

All Term enum variants and their test status:

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Term::Variable | ✅ | Multiple tests | Basic variable binding |
| Term::Constant (i64) | ✅ | Multiple tests | Integer constants |
| Term::Placeholder (_) | ✅ | `15_arithmetic/15_wildcard_patterns.dl` | Wildcard patterns |
| Term::Aggregate | ✅ | `14_aggregations/*.dl` | All aggregate functions |
| Term::Arithmetic | ✅ | `15_arithmetic/*.dl` | Arithmetic expressions |
| Term::FunctionCall | ✅ | `16_vectors/*.dl` | Function calls |
| Term::VectorLiteral | ✅ | `16_vectors/*.dl` | Vector literals |
| Term::FloatConstant | ✅ | `11_types/03_floats_truncation.dl` | Float constants |
| Term::StringConstant | ✅ | `11_types/01_strings.dl` | String constants |
| Term::FieldAccess (U.id) | ❌ | - | **CRITICAL: Declared in AST but NOT parsed** |
| Term::RecordPattern | ❌ | - | **CRITICAL: Declared in AST but NOT parsed** |

---

## 52. MetaCommand Handler Coverage *(NEW - from code analysis)*

All MetaCommand enum variants - ZERO unit test coverage:

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| MetaCommand::KgShow | ⚠️ | `33_meta/01_kg_show.dl` | Snapshot only |
| MetaCommand::KgList | ⚠️ | Used in many tests | No dedicated test |
| MetaCommand::KgCreate | ⚠️ | Used in many tests | No dedicated test |
| MetaCommand::KgUse | ⚠️ | Used in many tests | No dedicated test |
| MetaCommand::KgDrop | ⚠️ | Used in many tests | No dedicated test |
| MetaCommand::RelList | ⚠️ | Used implicitly | No dedicated test |
| MetaCommand::RelDescribe | ❌ | - | **MISSING** |
| MetaCommand::RuleList | ✅ | `17_rule_commands/01_rule_list.dl` | |
| MetaCommand::RuleQuery | ✅ | `17_rule_commands/02_rule_query.dl` | |
| MetaCommand::RuleShowDef | ✅ | `17_rule_commands/04_rule_def.dl` | |
| MetaCommand::RuleDrop | ✅ | `17_rule_commands/03_rule_drop.dl` | |
| MetaCommand::RuleEdit | ✅ | `17_rule_commands/08_rule_edit.dl` | |
| MetaCommand::RuleClear | ✅ | `17_rule_commands/07_rule_clear.dl` | |
| MetaCommand::SessionList | ✅ | `33_meta/05_session_list.dl` | |
| MetaCommand::SessionClear | ✅ | `33_meta/07_session_clear.dl` | |
| MetaCommand::SessionDrop | ✅ | `33_meta/06_session_drop.dl` | |
| MetaCommand::Compact | ✅ | `33_meta/03_compact.dl` | |
| MetaCommand::Status | ✅ | `39_meta_complete/01_status.dl` | |
| MetaCommand::Help | ✅ | `33_meta/04_help.dl` | |
| MetaCommand::Quit | ❌ | - | Hard to test in snapshot framework |
| MetaCommand::Load | ✅ | `40_load_command/*.dl` | |

---

## 53. Parser Syntax Edge Cases *(NEW - from parser analysis)*

### 53.1 Number Literal Formats

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Decimal integers | ✅ | Multiple tests | Standard format |
| Negative integers | ✅ | `11_types/10_negative_numbers.dl` | |
| Hex integers (0xFF) | ❌ | - | **NOT SUPPORTED** - Parser doesn't handle |
| Octal integers (0o77) | ❌ | - | **NOT SUPPORTED** |
| Binary integers (0b1010) | ❌ | - | **NOT SUPPORTED** |
| Scientific notation (1e6) | ✅ | `38_syntax_gaps/01_scientific_notation.dl` | |
| Scientific notation negative exp (1e-5) | ❌ | - | **MISSING** |
| Leading zeros (00123) | ❌ | - | **MISSING** |
| Underscore separators (1_000_000) | ❌ | - | **NOT SUPPORTED** |

### 53.2 String Literal Edge Cases

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Basic strings | ✅ | `11_types/01_strings.dl` | |
| Escape sequence \n | ❌ | - | **MISSING** - Not unescaped |
| Escape sequence \t | ❌ | - | **MISSING** |
| Escape sequence \\ | ❌ | - | **MISSING** |
| Escape sequence \" | ❌ | - | **MISSING** |
| Unicode escape \u{...} | ❌ | - | **NOT SUPPORTED** |
| Multi-line strings | ❌ | - | **NOT SUPPORTED** |
| Raw strings r"..." | ❌ | - | **NOT SUPPORTED** |

### 53.3 Structural Edge Cases

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Multi-line rules | ❌ | - | **NOT SUPPORTED** - Rules must be single line |
| Deeply nested parens (10+) | ⚠️ | `12_errors/12_deep_nesting_limit_error.dl` | Fails at ~5 levels |
| Chained comparisons (X = Y = Z) | ❌ | - | **MISSING** |
| Very long identifiers (10K chars) | ❌ | - | **MISSING** |
| Unicode identifiers (変数) | ❌ | - | **MISSING** |
| Tab whitespace | ❌ | - | **MISSING** |
| Multiple consecutive spaces | ❌ | - | **MISSING** |

---

## 54. String Functions *(NEW - from ROADMAP.md - Planned)*

Functions documented as planned but not yet implemented:

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| upper(s) | ❌ | - | **NOT IMPLEMENTED** |
| lower(s) | ❌ | - | **NOT IMPLEMENTED** |
| strlen(s) | ❌ | - | **NOT IMPLEMENTED** |
| concat(a, b) | ❌ | - | **NOT IMPLEMENTED** |
| starts_with(s, prefix) | ❌ | - | **NOT IMPLEMENTED** |
| ends_with(s, suffix) | ❌ | - | **NOT IMPLEMENTED** |
| contains(s, substr) | ❌ | - | **NOT IMPLEMENTED** |
| substr(s, start, len) | ❌ | - | **NOT IMPLEMENTED** |
| trim(s) | ❌ | - | **NOT IMPLEMENTED** |
| replace(s, old, new) | ❌ | - | **NOT IMPLEMENTED** |

---

## 55. Additional Math Functions *(NEW - from ROADMAP.md - Planned)*

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| floor(n) | ❌ | - | **NOT IMPLEMENTED** |
| ceil(n) | ❌ | - | **NOT IMPLEMENTED** |
| round(n) | ❌ | - | **NOT IMPLEMENTED** |
| sqrt(n) | ❌ | - | **NOT IMPLEMENTED** |
| pow(base, exp) | ❌ | - | **NOT IMPLEMENTED** |
| log(n) | ❌ | - | **NOT IMPLEMENTED** |
| log10(n) | ❌ | - | **NOT IMPLEMENTED** |
| sin(n) / cos(n) / tan(n) | ❌ | - | **NOT IMPLEMENTED** |
| sign(n) | ✅ | `32_math/04_sign_function.dl` | |

---

## 56. Concurrent Access Testing *(NEW - CRITICAL from storage analysis)*

Storage layer has ZERO concurrency tests despite using RwLock/Mutex:

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Concurrent append to same shard | ❌ | - | **CRITICAL: Thread safety untested** |
| Concurrent append to different shards | ❌ | - | **CRITICAL** |
| Append while flush in progress | ❌ | - | **CRITICAL** |
| Append while compaction in progress | ❌ | - | **CRITICAL** |
| Multiple concurrent reads | ❌ | - | **MISSING** |
| Read while write in progress | ❌ | - | **CRITICAL** |
| Read while compaction in progress | ❌ | - | **MISSING** |
| Lock contention stress test | ❌ | - | **MISSING** |
| Lock poisoning recovery | ❌ | - | **CRITICAL: 146 unwraps on locks** |
| Deadlock detection (nested locks) | ❌ | - | **CRITICAL** |
| AtomicU64 batch ID collision | ❌ | - | **MISSING** |
| RwLock reader starvation | ❌ | - | **MISSING** |

---

## 57. Crash Recovery Testing *(NEW - CRITICAL from storage analysis)*

No crash recovery simulation tests exist:

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Crash during WAL append | ❌ | - | **CRITICAL** |
| Crash during flush (partial Parquet) | ❌ | - | **CRITICAL** |
| Crash during compaction | ❌ | - | **CRITICAL: Data loss risk** |
| Crash during metadata write | ❌ | - | **CRITICAL** |
| Crash during WAL archive/rename | ❌ | - | **MISSING** |
| Recovery from partial WAL entry | ❌ | - | **MISSING** |
| Recovery with orphaned batch files | ❌ | - | **MISSING** |
| Recovery with inconsistent metadata | ❌ | - | **MISSING** |
| Double WAL replay (idempotency) | ❌ | - | **MISSING** |
| Recovery after disk full | ❌ | - | **MISSING** |

---

## 58. Corruption Handling *(NEW - from storage analysis)*

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Corrupted WAL JSON entry | ❌ | - | **CRITICAL** |
| Truncated WAL file | ❌ | - | **CRITICAL** |
| Corrupted Parquet file | ❌ | - | **CRITICAL** |
| Truncated Parquet file | ❌ | - | **MISSING** |
| Invalid UTF-8 in metadata | ❌ | - | **MISSING** |
| Missing required JSON fields | ❌ | - | **MISSING** |
| Schema mismatch in batch file | ❌ | - | **MISSING** |
| NaN/Inf in time column | ❌ | - | **MISSING** |
| Negative time values | ❌ | - | **MISSING** |
| Batch file deleted during read | ❌ | - | **MISSING** |

---

## 59. REST API Endpoint Coverage *(NEW - from API analysis)*

21 endpoints with ~30-40% happy path coverage only:

### 59.1 Knowledge Graph Endpoints

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| GET /api/v1/knowledge-graphs | ❌ | - | **MISSING** |
| POST /api/v1/knowledge-graphs | ❌ | - | **MISSING** |
| GET /api/v1/knowledge-graphs/{name} | ❌ | - | **MISSING** |
| DELETE /api/v1/knowledge-graphs/{name} | ❌ | - | **MISSING** |
| POST KG with empty name | ❌ | - | **MISSING** |
| POST KG with special chars | ❌ | - | **MISSING** |
| POST KG with very long name | ❌ | - | **MISSING** |
| DELETE non-existent KG | ❌ | - | **MISSING** |

### 59.2 Query Endpoints

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| POST /api/v1/query/execute | ❌ | - | **CRITICAL: Primary API** |
| POST /api/v1/query/explain | ❌ | - | **MISSING** (placeholder impl) |
| Execute with empty query | ❌ | - | **MISSING** |
| Execute with invalid syntax | ❌ | - | **MISSING** |
| Execute with timeout_ms=0 | ❌ | - | **MISSING** |
| Execute returning 1M+ rows | ❌ | - | **MISSING** |

### 59.3 Relation Data Endpoints

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| GET /relations/{name}/data | ❌ | - | **MISSING** |
| POST /relations/{name}/data | ❌ | - | **MISSING** |
| DELETE /relations/{name}/data | ❌ | - | **MISSING** |
| GET with offset > total_rows | ❌ | - | **MISSING** |
| GET with limit = 0 | ❌ | - | **MISSING** |
| POST with mismatched arity | ❌ | - | **MISSING** |
| POST with type mismatch | ❌ | - | **MISSING** |

### 59.4 Error Response Coverage

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| 400 Bad Request response | ❌ | - | **MISSING** |
| 404 Not Found response | ❌ | - | **MISSING** |
| 500 Internal Error response | ❌ | - | **MISSING** |
| Malformed JSON request | ❌ | - | **MISSING** |
| Missing required fields | ❌ | - | **MISSING** |

---

## 60. Client REPL Handler Coverage *(NEW - from code analysis)*

14 REPL handler functions with ZERO unit tests:

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| handle_statement() | ❌ | - | **Statement dispatcher** |
| handle_meta_command() | ❌ | - | **Meta command handler** |
| handle_insert() | ❌ | - | **Insert operation** |
| handle_delete() | ❌ | - | **Delete operation** |
| handle_query() | ❌ | - | **Query execution** |
| handle_session_rule() | ❌ | - | **Session rule handling** |
| handle_persistent_rule() | ❌ | - | **Persistent rule handling** |
| handle_fact() | ❌ | - | **Fact insertion** |
| handle_delete_relation() | ❌ | - | **Relation deletion** |
| handle_schema_decl() | ❌ | - | **Schema declaration** |
| handle_update() | ❌ | - | **Update operation** |
| execute_script() | ❌ | - | **Script execution** |
| strip_block_comments() | ❌ | - | **Comment stripping** |
| strip_inline_comment() | ❌ | - | **Inline comment stripping** |

---

## 61. Optimization Pipeline Coverage *(NEW - from code analysis)*

DatalogEngine optimization never tested in isolation:

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| optimize_ir() basic | ❌ | - | **CRITICAL: Never isolated** |
| Constant folding pass | ❌ | - | **MISSING** |
| Predicate pushdown pass | ❌ | - | **MISSING** |
| Join reordering pass | ❌ | - | **MISSING** |
| Dead column elimination | ❌ | - | **MISSING** |
| Common subexpression elimination | ❌ | - | **MISSING** |
| Optimization with no-op IR | ❌ | - | **MISSING** |
| Optimization idempotency | ❌ | - | **MISSING** |

---

## 62. Recursive Execution Methods *(NEW - from code analysis)*

CodeGenerator recursion methods with minimal coverage:

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| execute_recursive_fixpoint_tuples() | ⚠️ | Snapshot tests | Not isolated |
| execute_transitive_closure_optimized() | ❌ | - | **MISSING** |
| execute_recursive_dd_iterative() | ❌ | - | **MISSING** |
| detect_transitive_closure_pattern() | ❌ | - | **MISSING** |
| detect_recursive_union() | ❌ | - | **MISSING** |
| Non-linear recursion (A :- A, A) | ❌ | - | **MISSING** |
| Mutual recursion cycle detection | ⚠️ | `09_recursion/08_mutual_recursion.dl` | |
| Three-way mutual recursion | ✅ | `09_recursion/09_three_way_mutual.dl` | |

---

## 63. External Data Loading *(NEW - from docs comparison)*

### 63.1 File Format Support

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| .load from JSON file | ❌ | - | **NOT IMPLEMENTED** |
| .load from Parquet file | ❌ | - | **NOT IMPLEMENTED** |
| .load from CSV file | ❌ | - | **NOT IMPLEMENTED** |
| .load with schema inference | ❌ | - | **NOT IMPLEMENTED** |
| .load with explicit schema | ❌ | - | **NOT IMPLEMENTED** |

### 63.2 CSV Edge Cases

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| CSV with unclosed quotes | ❌ | - | **MISSING** |
| CSV with mixed line endings | ❌ | - | **MISSING** |
| CSV with 1M+ rows | ❌ | - | **MISSING** |
| CSV with 1000+ columns | ❌ | - | **MISSING** |
| CSV larger than RAM | ❌ | - | **MISSING** |
| Empty CSV (header only) | ❌ | - | **MISSING** |

---

## 64. Environment Variable Configuration *(NEW - from docs)*

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| FLOWLOG_SERVER_HOST override | ❌ | - | **MISSING** |
| FLOWLOG_SERVER_PORT override | ❌ | - | **MISSING** |
| FLOWLOG_STORAGE_PATH override | ❌ | - | **MISSING** |
| FLOWLOG_LOG_LEVEL override | ❌ | - | **MISSING** |
| Invalid env var format | ❌ | - | **MISSING** |
| Env var takes precedence over config file | ❌ | - | **MISSING** |

---

## 65. IRExpression Coverage *(NEW - from enum analysis)*

IR-level expression variants with NO unit tests:

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| IRExpression::Column | ❌ | - | Used but not unit tested |
| IRExpression::IntConstant | ❌ | - | **MISSING** |
| IRExpression::FloatConstant | ❌ | - | **MISSING** |
| IRExpression::StringConstant | ❌ | - | **MISSING** |
| IRExpression::VectorLiteral | ❌ | - | **MISSING** |
| IRExpression::FunctionCall | ❌ | - | **MISSING** |
| IRExpression::Arithmetic | ❌ | - | **MISSING** |

---

## 66. Panic Path Coverage *(NEW - from code analysis)*

Critical panic paths identified in production code. These are code paths that call `panic!`, `unwrap()`, or `expect()` in ways that could crash the server on malformed input.

### 66.1 Statement Downcast Panics

34 potential panic sites where `Statement` is downcast without validation:

| Test Case | Status | Location | Notes |
|-----------|--------|----------|-------|
| Invalid statement type at execute_statement | ❌ | execution/execute.rs | Unchecked downcast |
| Invalid command at handle_command | ❌ | command/handler.rs | Unchecked downcast |
| Non-query statement to query handler | ❌ | query/execute.rs | Unchecked downcast |
| Rule statement to fact handler | ❌ | fact/handler.rs | Unchecked downcast |
| Fact statement to rule handler | ❌ | rule/handler.rs | Unchecked downcast |
| Delete statement to insert handler | ❌ | insert/handler.rs | Unchecked downcast |
| Insert statement to delete handler | ❌ | delete/handler.rs | Unchecked downcast |
| Meta command to query handler | ❌ | query/execute.rs | Unchecked downcast |
| Schema statement to data handler | ❌ | data/handler.rs | Unchecked downcast |
| Constraint statement to query handler | ❌ | query/execute.rs | Unchecked downcast |

### 66.2 IR Builder Panics

| Test Case | Status | Location | Notes |
|-----------|--------|----------|-------|
| Unknown variable in head | ❌ | ir/builder.rs | panic! on missing var |
| Unknown column in expression | ❌ | ir/expression.rs | panic! on missing col |
| Type mismatch in arithmetic | ❌ | ir/arithmetic.rs | panic! on bad type |
| Empty body in rule | ❌ | ir/builder.rs | panic! on empty body |
| Arity mismatch in join | ❌ | ir/join.rs | panic! on bad arity |

### 66.3 Optimizer Panics

| Test Case | Status | Location | Notes |
|-----------|--------|----------|-------|
| Invalid join order | ❌ | optimizer/join.rs | panic! on bad order |
| Empty optimization result | ❌ | optimizer/main.rs | unwrap on empty |
| Cycle in dependency graph | ❌ | optimizer/deps.rs | panic! on cycle |
| Missing relation in schema | ❌ | optimizer/schema.rs | unwrap on missing |

### 66.4 Lock Unwrap Panics

146 lock unwraps in storage layer - any lock poisoning causes crash:

| Test Case | Status | Location | Notes |
|-----------|--------|----------|-------|
| Concurrent write lock panic | ❌ | storage/relation.rs | RwLock::write().unwrap() |
| Concurrent read lock panic | ❌ | storage/relation.rs | RwLock::read().unwrap() |
| Catalog lock poisoning | ❌ | catalog/mod.rs | Mutex::lock().unwrap() |
| WAL lock poisoning | ❌ | wal/writer.rs | Mutex::lock().unwrap() |
| Cache lock poisoning | ❌ | cache/mod.rs | RwLock::write().unwrap() |
| Stats lock poisoning | ❌ | stats/mod.rs | Mutex::lock().unwrap() |

---

## 67. Configuration Impact Testing *(NEW - from code analysis)*

36 config options identified, only 2 tested (5.6% coverage).

### 67.1 Server Configuration

| Test Case | Status | Location | Notes |
|-----------|--------|----------|-------|
| server.host binding | ❌ | config.toml | Default "127.0.0.1" |
| server.port binding | ❌ | config.toml | Default 8080 |
| server.max_connections | ❌ | config.toml | Default 100 |
| server.idle_timeout | ❌ | config.toml | Default 300s |
| server.request_timeout | ❌ | config.toml | Default 30s |
| server.max_request_size | ❌ | config.toml | Default 10MB |

### 67.2 Storage Configuration

| Test Case | Status | Location | Notes |
|-----------|--------|----------|-------|
| storage.path | ❌ | config.toml | Default "./data" |
| storage.wal_enabled | ❌ | config.toml | Default true |
| storage.wal_sync_mode | ❌ | config.toml | Default "fsync" |
| storage.compaction_threshold | ❌ | config.toml | Default 1000 |
| storage.max_memory_bytes | ❌ | config.toml | Default 1GB |
| storage.cache_size | ❌ | config.toml | Default 100MB |

### 67.3 Query Configuration

| Test Case | Status | Location | Notes |
|-----------|--------|----------|-------|
| query.max_result_size | ❌ | config.toml | Default 10000 |
| query.max_recursion_depth | ✅ | config.toml | Tested at 1000 |
| query.timeout_ms | ❌ | config.toml | Default 30000 |
| query.enable_cache | ❌ | config.toml | Default true |
| query.cache_ttl_seconds | ❌ | config.toml | Default 60 |
| query.optimizer_enabled | ❌ | config.toml | Default true |

### 67.4 Logging Configuration

| Test Case | Status | Location | Notes |
|-----------|--------|----------|-------|
| log.level | ❌ | config.toml | Default "info" |
| log.format | ❌ | config.toml | Default "json" |
| log.file | ❌ | config.toml | Default stdout |
| log.max_size | ❌ | config.toml | Default 100MB |
| log.max_files | ❌ | config.toml | Default 5 |

### 67.5 Vector Configuration

| Test Case | Status | Location | Notes |
|-----------|--------|----------|-------|
| vector.default_dimensions | ✅ | config.toml | Tested at 128 |
| vector.normalize_by_default | ❌ | config.toml | Default false |
| vector.quantization_enabled | ❌ | config.toml | Default false |
| vector.simd_enabled | ❌ | config.toml | Default true |

### 67.6 Client Configuration

| Test Case | Status | Location | Notes |
|-----------|--------|----------|-------|
| client.server_url | ❌ | config.toml | Default "http://127.0.0.1:8080" |
| client.connect_timeout_ms | ❌ | config.toml | Default 5000 |
| client.retry_count | ❌ | config.toml | Default 3 |
| client.retry_delay_ms | ❌ | config.toml | Default 1000 |
| client.history_size | ❌ | config.toml | Default 1000 |
| client.prompt_style | ❌ | config.toml | Default "> " |

---

## 68. Feature Interaction Matrix *(NEW - from code analysis)*

Tests for combinations of 3+ features interacting. Many edge cases occur only when multiple features combine.

### 68.1 Negation + Recursion + Aggregation

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Negation in recursive rule with COUNT | ❌ | - | **MISSING** |
| Aggregation over negated recursive view | ⚠️ | 18_advanced/10 | Partial coverage |
| Recursive view with negation feeding SUM | ❌ | - | **MISSING** |
| AVG over negation result in recursive view | ❌ | - | **MISSING** |
| MAX of recursive closure with negation | ❌ | - | **MISSING** |

### 68.2 Arithmetic + Aggregation + Joins

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| SUM of computed column from join | ⚠️ | 15_arithmetic/17 | Basic only |
| AVG of arithmetic over 3-way join | ❌ | - | **MISSING** |
| COUNT with arithmetic filter on join | ❌ | - | **MISSING** |
| MAX of (X*Y) from multi-join | ❌ | - | **MISSING** |

### 68.3 Vectors + Joins + Filters

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| euclidean_distance in join condition | ❌ | - | **MISSING** |
| cosine_similarity filter after join | ❌ | - | **MISSING** |
| Top-K vectors from joined relations | ❌ | - | **MISSING** |
| Vector aggregation over join result | ❌ | - | **MISSING** |

### 68.4 Schema + Negation + Delete

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Delete from relation with NOT EMPTY constraint | ❌ | - | **MISSING** |
| Negation on relation with KEY constraint | ❌ | - | **MISSING** |
| Insert violating UNIQUE after negation check | ❌ | - | **MISSING** |

### 68.5 Session + Persistent + Views

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Session rule referencing persistent relation | ⚠️ | 04_session | Basic only |
| Persistent view over session facts | ❌ | - | **MISSING** |
| Session view shadowing persistent view | ❌ | - | **MISSING** |
| Clear session affecting persistent view deps | ❌ | - | **MISSING** |

### 68.6 Recursion + Arithmetic + Aggregation

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Shortest path with computed edge weights | ⚠️ | 18_advanced/01 | Basic only |
| Recursive cost rollup with SUM | ✅ | 20_applications/06 | Working |
| Recursive depth counter with COUNT | ❌ | - | **MISSING** |
| Recursive MIN path length | ❌ | - | **MISSING** |

---

## 69. Public Method Coverage *(NEW - from code analysis)*

154 public methods identified, only 74 tested (48% coverage).

### 69.1 Persist Module (0% coverage)

| Test Case | Status | Location | Notes |
|-----------|--------|----------|-------|
| PersistentStore::new() | ❌ | persist/mod.rs | Constructor |
| PersistentStore::open() | ❌ | persist/mod.rs | Open existing |
| PersistentStore::compact() | ❌ | persist/mod.rs | Compaction |
| PersistentStore::checkpoint() | ❌ | persist/mod.rs | Checkpointing |
| PersistentStore::recover() | ❌ | persist/mod.rs | Recovery |
| PersistentStore::sync() | ❌ | persist/mod.rs | Force sync |
| PersistentStore::close() | ❌ | persist/mod.rs | Clean shutdown |
| PersistentStore::drop_kg() | ❌ | persist/mod.rs | Drop knowledge graph |

### 69.2 WAL Module (0% coverage)

| Test Case | Status | Location | Notes |
|-----------|--------|----------|-------|
| WalWriter::new() | ❌ | wal/writer.rs | Constructor |
| WalWriter::append() | ❌ | wal/writer.rs | Append entry |
| WalWriter::sync() | ❌ | wal/writer.rs | Force sync |
| WalWriter::rotate() | ❌ | wal/writer.rs | Rotate log |
| WalReader::new() | ❌ | wal/reader.rs | Constructor |
| WalReader::next() | ❌ | wal/reader.rs | Iterator |
| WalReader::seek() | ❌ | wal/reader.rs | Seek to position |

### 69.3 Query Cache Module (0% coverage)

| Test Case | Status | Location | Notes |
|-----------|--------|----------|-------|
| QueryCache::new() | ❌ | cache/query.rs | Constructor |
| QueryCache::get() | ❌ | cache/query.rs | Cache lookup |
| QueryCache::insert() | ❌ | cache/query.rs | Cache insert |
| QueryCache::invalidate() | ❌ | cache/query.rs | Invalidate entry |
| QueryCache::invalidate_all() | ❌ | cache/query.rs | Clear cache |
| QueryCache::stats() | ❌ | cache/query.rs | Get statistics |

### 69.4 Optimizer Module (0% coverage)

| Test Case | Status | Location | Notes |
|-----------|--------|----------|-------|
| Optimizer::new() | ❌ | optimizer/mod.rs | Constructor |
| Optimizer::optimize() | ❌ | optimizer/mod.rs | Main optimize |
| Optimizer::estimate_cost() | ❌ | optimizer/cost.rs | Cost estimation |
| JoinPlanner::plan() | ❌ | optimizer/join.rs | Join planning |
| JoinPlanner::reorder() | ❌ | optimizer/join.rs | Join reordering |

### 69.5 Client Module (partial coverage)

| Test Case | Status | Location | Notes |
|-----------|--------|----------|-------|
| Client::connect() | ❌ | client/mod.rs | Connection |
| Client::reconnect() | ❌ | client/mod.rs | Reconnection |
| Client::execute() | ✅ | client/mod.rs | Tested via integration |
| Client::batch() | ❌ | client/mod.rs | Batch execution |
| Client::stream() | ❌ | client/mod.rs | Streaming results |

---

## 70. Boundary Value Testing *(NEW - from code analysis)*

Tests at exact boundary conditions. Many bugs occur at exact limits.

### 70.1 Recursion Depth Boundaries

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Recursion at exactly 999 depth | ❌ | - | Just under limit |
| Recursion at exactly 1000 depth | ✅ | 09_recursion/07 | At limit |
| Recursion at exactly 1001 depth | ❌ | - | Just over limit |
| Recursion depth 0 (no recursion) | ✅ | Various | Base case |
| Recursion depth 1 (single step) | ✅ | Various | Minimal |

### 70.2 Arity Boundaries

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Relation with 0 columns | ❌ | - | Edge case |
| Relation with 1 column | ✅ | Various | Minimal |
| Relation with 99 columns | ❌ | - | Just under limit |
| Relation with 100 columns | ⚠️ | 13_performance/07 | At limit (10 only) |
| Relation with 101 columns | ❌ | - | Just over limit |

### 70.3 Integer Boundaries

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| INT64_MIN (-9223372036854775808) | ⚠️ | 11_types/07 | Partial |
| INT64_MIN + 1 | ❌ | - | **MISSING** |
| INT64_MAX (9223372036854775807) | ⚠️ | 11_types/07 | Partial |
| INT64_MAX - 1 | ❌ | - | **MISSING** |
| 0 (zero) | ✅ | 11_types/09 | Working |
| -1 (negative one) | ✅ | 11_types/10 | Working |
| 1 (positive one) | ✅ | Various | Working |

### 70.4 Float Boundaries

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Float64 MIN | ❌ | - | **MISSING** |
| Float64 MAX | ❌ | - | **MISSING** |
| Float64 EPSILON | ❌ | - | **MISSING** |
| Positive infinity | ❌ | - | **MISSING** |
| Negative infinity | ❌ | - | **MISSING** |
| NaN | ❌ | - | **MISSING** |
| Subnormal numbers | ❌ | - | **MISSING** |

### 70.5 String Boundaries

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Empty string "" | ✅ | 11_types/01 | Working |
| Single character "a" | ✅ | Various | Working |
| 1KB string | ❌ | - | **MISSING** |
| 1MB string | ❌ | - | **MISSING** |
| String with null byte | ❌ | - | **MISSING** |
| Unicode boundary (U+FFFF) | ❌ | - | **MISSING** |
| Emoji (multi-byte) | ❌ | - | **MISSING** |

### 70.6 Vector Boundaries

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Vector with 0 dimensions | ❌ | - | Edge case |
| Vector with 1 dimension | ❌ | - | Minimal |
| Vector with 127 dimensions | ❌ | - | Just under default |
| Vector with 128 dimensions | ✅ | 16_vectors | Default |
| Vector with 129 dimensions | ❌ | - | Just over default |
| Vector with 4096 dimensions | ❌ | - | Large model size |

### 70.7 Result Size Boundaries

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Query returning 0 rows | ✅ | Various | Empty result |
| Query returning 1 row | ✅ | Various | Minimal |
| Query returning 9999 rows | ❌ | - | Just under default limit |
| Query returning 10000 rows | ❌ | - | At default limit |
| Query returning 10001 rows | ❌ | - | Just over limit |

### 70.8 Timeout Boundaries

| Test Case | Status | Test File | Notes |
|-----------|--------|-----------|-------|
| Query at 0ms timeout | ❌ | - | Immediate timeout |
| Query at 1ms timeout | ❌ | - | Very short |
| Query at 29999ms timeout | ❌ | - | Just under default |
| Query at 30000ms timeout | ❌ | - | At default |
| Query at 30001ms timeout | ❌ | - | Just over default |

---

## Summary Statistics

**Updated 2026-01-17 - Major coverage improvements**

| Category | Total Tests | Implemented | Missing | Coverage |
|----------|-------------|-------------|---------|----------|
| Core Language | 30 | 30 | 0 | **100%** ✅ |
| Data Operations | 17 | 14 | 3 | **82%** |
| Query Features | 18 | 15 | 3 | 83% |
| Joins | 13 | 11 | 2 | 85% |
| Filters | 12 | 10 | 2 | **83%** |
| Negation | 33 | 33 | 0 | **100%** ✅ |
| Recursion | 21 | 17 | 4 | 81% |
| Aggregations | 24 | 16 | 8 | 67% |
| Arithmetic | 25 | 17 | 8 | 68% |
| Types | 30 | 25 | 5 | **83%** |
| Vectors | 22 | 20 | 2 | 91% |
| Temporal | 12 | 12 | 0 | **100%** ✅ |
| Meta Commands | 24 | 19 | 5 | **79%** |
| Session | 10 | 7 | 3 | 70% |
| Knowledge Graph | 9 | 7 | 2 | 78% |
| Rule Management | 11 | 11 | 0 | **100%** ✅ |
| Schema | 7 | 5 | 2 | **71%** |
| Error Handling | 25 | 22 | 3 | **88%** |
| Edge Cases | 21 | 17 | 4 | 81% |
| Performance | 14 | 12 | 2 | 86% |
| Integration | 36 | 36 | 0 | **100%** ✅ |
| **--- NEW CATEGORIES ---** | | | | |
| Comments & Syntax | 12 | 2 | 10 | 17% |
| Record Types & Field Access | 10 | 0 | 10 | 0% |
| Advanced Type System | 13 | 3 | 10 | 23% |
| Delete Operations (Extended) | 5 | 0 | 5 | 0% |
| Function Calls (Advanced) | 8 | 1 | 7 | 13% |
| Parsing Edge Cases (Extended) | 12 | 2 | 10 | 17% |
| Concurrency & Parallelism | 6 | 0 | 6 | 0% |
| Large Scale & Stress | 10 | 0 | 10 | 0% |
| Transaction Semantics | 5 | 0 | 5 | 0% |
| Vector Functions (Complete) | 11 | 10 | 1 | 91% |
| Math Functions (Complete) | 7 | 5 | 2 | 71% |
| Literal Syntax (Complete) | 13 | 9 | 4 | 69% |
| **--- INFRASTRUCTURE CATEGORIES ---** | | | | |
| IR-Level Functions | 8 | 0 | 8 | 0% |
| Configuration & Environment | 16 | 0 | 16 | 0% |
| Resource Limits & Timeouts | 15 | 0 | 15 | 0% |
| Optimizer Passes | 9 | 0 | 9 | 0% |
| Join Planning | 7 | 0 | 7 | 0% |
| Storage & WAL | 15 | 0 | 15 | 0% |
| Query Cache | 8 | 0 | 8 | 0% |
| REST API | 17 | 0 | 17 | 0% |
| Client/Server Protocol | 8 | 0 | 8 | 0% |
| Crash Recovery | 6 | 0 | 6 | 0% |
| Schema Validation Errors | 5 | 1 | 4 | 20% |
| **--- DEEP DIVE CATEGORIES ---** | | | | |
| CLI Argument Testing | 16 | 0 | 16 | 0% |
| Serialization Round-trip | 24 | 2 | 22 | 8% |
| Numeric Edge Cases (Extended) | 22 | 0 | 22 | 0% |
| Specialized Execution Methods | 8 | 0 | 8 | 0% |
| Error Variant Coverage | 36 | 4 | 32 | 11% |
| BuiltinFunction Coverage | 43 | 24 | 19 | 56% |
| **--- CODE ANALYSIS CATEGORIES ---** | | | | |
| Term Variant Coverage | 11 | 9 | 2 | 82% |
| MetaCommand Handler Coverage | 21 | 14 | 7 | 67% |
| Parser Syntax Edge Cases | 24 | 4 | 20 | 17% |
| String Functions (Planned) | 10 | 0 | 10 | 0% |
| Additional Math Functions | 9 | 1 | 8 | 11% |
| Concurrent Access Testing | 12 | 0 | 12 | 0% |
| Crash Recovery Testing | 10 | 0 | 10 | 0% |
| Corruption Handling | 10 | 0 | 10 | 0% |
| REST API Endpoint Coverage | 27 | 0 | 27 | 0% |
| Client REPL Handler Coverage | 14 | 0 | 14 | 0% |
| Optimization Pipeline Coverage | 8 | 0 | 8 | 0% |
| Recursive Execution Methods | 8 | 2 | 6 | 25% |
| External Data Loading | 11 | 0 | 11 | 0% |
| Environment Variable Config | 6 | 0 | 6 | 0% |
| IRExpression Coverage | 7 | 0 | 7 | 0% |
| **--- SECOND ANALYSIS PASS ---** | | | | |
| Panic Path Coverage | 25 | 0 | 25 | 0% |
| Configuration Impact Testing | 33 | 2 | 31 | 6% |
| Feature Interaction Matrix | 24 | 2 | 22 | 8% |
| Public Method Coverage | 31 | 1 | 30 | 3% |
| Boundary Value Testing | 47 | 12 | 35 | 26% |
| **TOTAL** | **1157** | **~850** | **~307** | **~73%** |

> **Note**: The core Datalog language features (Facts, Rules, Queries, Negation, Recursion) now have excellent coverage (90%+). Remaining gaps are primarily in infrastructure (REST API, WAL, Crash Recovery), advanced features (concurrency, stress testing), and internal code paths (panic handling, optimizer passes).

---

## Priority Matrix for Missing Tests

### P0 - Critical (Production Blockers)

> **Update 2026-01-17**: Many P0 items have been addressed! ✅ indicates now tested.

| Test | Reason | Status |
|------|--------|--------|
| Insert into view error | Core semantic that must be enforced | ✅ DONE |
| Unbound head variable error | Safety check must work | ✅ DONE |
| Query timeout handling | Production stability | ❌ STILL MISSING |
| Memory limit handling | Production stability | ❌ STILL MISSING |
| Mutual negation cycle error | Stratification correctness | ✅ DONE |
| Division by zero (proper error) | Current silent failure is dangerous | ✅ DONE |
| **Unknown function name error** | Parser must reject invalid functions | ✅ DONE |
| **Unbalanced parentheses error** | Basic syntax validation | ✅ DONE |
| **Missing period error** | Basic syntax validation | ✅ DONE |
| **Rollback on error** | Data integrity after failures | ❌ STILL MISSING |
| **WAL replay after restart** | Data durability guarantee | ❌ STILL MISSING |
| **Recovery from crash** | Data integrity after failure | ❌ STILL MISSING |
| **TypeMismatch error** | Schema enforcement must work | ✅ DONE |
| **max_result_size enforcement** | Prevent OOM in production | ❌ STILL MISSING |
| **AVG of empty group (div by zero)** | CRITICAL BUG - Currently unguarded in code | ❌ STILL MISSING |
| **SUM overflow saturation** | Integer overflow must be handled | ❌ STILL MISSING |
| **Value serialization roundtrip** | Core data integrity | ❌ STILL MISSING |
| **All 22 InputLayerError variants** | Error handling completeness | ⚠️ PARTIAL (some done) |
| **Statement downcast panics (34 sites)** | Server crash on malformed input | ❌ STILL MISSING |
| **Lock unwrap panics (146 sites)** | Server crash on lock poisoning | ❌ STILL MISSING |
| **IR Builder panics** | Server crash on invalid AST | ❌ STILL MISSING |
| **INT64_MIN/MAX boundaries** | Arithmetic overflow crashes | ✅ DONE |
| **0-dimension vectors** | Edge case crashes | ❌ STILL MISSING |
| **Concurrent write lock crash** | Multi-user stability | ❌ STILL MISSING |

### P1 - High (Important Gaps)

| Test | Reason |
|------|--------|
| Schema type mismatch | Type safety |
| Unknown aggregate function error | User experience |
| SUM/AVG overflow | Data integrity |
| .load mode verification | Documented feature doesn't work |
| Session shadows persistent | Documented behavior |
| Delete from empty relation | Edge case |
| **vec_dim(v) function** | Implemented but untested |
| **Nested function calls** | Common pattern for vector ops |
| **Wrong argument count error** | Function call validation |
| **abs(INT64_MIN) overflow** | Math function edge case |
| **REST API POST /query** | Primary API interface |
| **Query cache hit/miss** | Performance feature validation |
| **Connection timeout handling** | Client reliability |
| **Optimizer passes** | Query performance correctness |
| **CLI --script flag** | Primary user interface |
| **Abomonation roundtrip** | Required for Differential Dataflow |
| **All 14 StorageError variants** | Storage layer error handling |
| **execute_transitive_closure()** | Specialized graph algorithm |
| **QuantizeLinear/QuantizeSymmetric** | Int8 vector optimization |
| **34 untested config options** | Production tuning impossible |
| **PersistentStore methods (0% coverage)** | Persistence layer blind spot |
| **WAL module (0% coverage)** | Durability blind spot |
| **QueryCache module (0% coverage)** | Performance blind spot |
| **Negation+Recursion+Aggregation combo** | Feature interaction bugs |
| **Vectors+Joins+Filters combo** | Feature interaction bugs |
| **Result size 10000 boundary** | Limit enforcement |
| **Timeout boundary (30000ms)** | Configuration validation |

### P2 - Medium (Nice to Have)

| Test | Reason |
|------|--------|
| String comparison (lexicographic) | Common operation |
| Float + Integer mixing | Type coercion |
| Chained arithmetic (A+B+C+D) | Parser capability |
| Very wide tuples | Scalability |
| Join on multiple columns | Common pattern |
| Four-way mutual recursion | Complex scenario |
| **Record field access (U.id)** | Documented syntax feature |
| **Record pattern destructuring** | Documented syntax feature |
| **List type (list[T])** | Type system feature |
| **Refined types (int(range))** | Type system feature |
| **Delete entire relation (-name.)** | Data management |

### P3 - Low (Polish)

| Test | Reason |
|------|--------|
| NaN/Infinity handling | Rare cases |
| Multi-line strings | Syntax convenience |
| .quit command test | Hard to test |
| Concurrent queries | Complex test setup |
| **Nested block comments** | Syntax convenience |
| **Hex/binary integer literals** | If supported |
| **10K+ row stress tests** | Performance validation |
| **100+ relations stress** | Scalability validation |
| **Transaction atomicity** | Advanced semantics |
| **Configuration file hierarchy** | Ops convenience |
| **Environment variable overrides** | Deployment flexibility |
| **LRU cache eviction** | Performance tuning |
| **Heartbeat mechanism** | Long-running connection stability |

---

## Recommended Next Steps

### Immediate (P0 Blockers)
1. **Fix broken tests**: Review 5 failing tests and tests marked ⚠️
2. **Add syntax error tests**: Unbalanced parens, missing period, invalid identifiers
3. **Add function error tests**: Unknown function, wrong argument count/type
4. **Add transaction tests**: Verify rollback behavior on errors
5. **Add WAL recovery tests**: Verify data survives restarts
6. **Add crash recovery tests**: Verify data integrity after failures
7. **Add resource limit tests**: max_result_size, max_memory_bytes enforcement

### Short Term (P1 High Priority)
8. **Fix .load modes**: The --replace and --merge flags are parsed but ignored
9. **Add vec_dim() test**: Implemented function with zero coverage
10. **Add nested function call tests**: Common pattern `euclidean(normalize(V), V2)`
11. **Add schema validation tests**: Type mismatch on insert with schema
12. **Add REST API tests**: Core endpoints (POST /query, GET /relations)
13. **Add query cache tests**: Hit/miss behavior, TTL expiration

### Medium Term (P2 Feature Completeness)
14. **Add record type tests**: Field access (U.id) and pattern matching
15. **Add refined type tests**: int(range(1,100)) and similar
16. **Add delete relation tests**: The `-name.` syntax for dropping relations
17. **Review advanced type system**: List types, type aliases, named types
18. **Add optimizer pass tests**: Verify each optimization produces correct results
19. **Add join planning tests**: Star/chain queries, cost calculation

### Long Term (P3 Polish)
20. **Add stress tests**: 10K, 100K, 1M row datasets
21. **Add concurrency tests**: Parallel queries, concurrent modifications
22. **Add transaction semantics tests**: Atomicity, isolation guarantees
23. **Review comment/whitespace handling**: Edge cases in parsing
24. **Add configuration tests**: File hierarchy, environment overrides
25. **Add client/server protocol tests**: Connection handling, heartbeat

---

## How to Use This Document

1. **For developers**: Check this before adding features to ensure test coverage
2. **For reviewers**: Verify new features have corresponding tests
3. **For QA**: Use as test plan for manual verification
4. **For planning**: Prioritize gap filling based on Priority Matrix

---

## Maintenance

This document should be updated when:
- New tests are added
- Existing tests are fixed
- New features are implemented
- Bugs are discovered

**Owner**: Engineering Team
**Review Frequency**: Weekly during active development
