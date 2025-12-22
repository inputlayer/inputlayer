# Learning Path: InputLayer by Example

This guide provides a structured path through the example programs to learn InputLayer progressively.

## How to Use This Guide

1. **Work through examples in order** - They build on each other
2. **Read each `.dl` file** - Examples include comments explaining concepts
3. **Run the examples** - See the output firsthand
4. **Experiment** - Modify examples and observe changes

### Running Examples

```bash
# From the inputlayer directory
cargo run --bin inputlayer-client --release --quiet -- --script examples/datalog/path/to/example.dl
```

Or use the snapshot test runner:
```bash
./scripts/run_snapshot_tests.sh -f "example_name"
```

---

## Stage 1: Foundations

**Goal**: Understand databases, facts, and basic queries.

### 1.1 Database Management

| Example | What You'll Learn |
|---------|-------------------|
| `01_database/01_create_use_drop.dl` | Creating, switching, and dropping databases |

**Key Concepts**:
- `.db create <name>` - Create a new database
- `.db use <name>` - Switch to a database
- `.db drop <name>` - Delete a database

### 1.2 Working with Facts

| Example | What You'll Learn |
|---------|-------------------|
| `02_relations/01_insert_single.dl` | Inserting individual facts |
| `02_relations/02_insert_bulk.dl` | Bulk insertion syntax |
| `02_relations/03_delete.dl` | Deleting facts |
| `02_relations/04_delete_nonexistent.dl` | Behavior when deleting missing data |
| `02_relations/05_delete_all_then_query.dl` | Emptying a relation |

**Key Concepts**:
- `+relation(val1, val2).` - Insert a single fact
- `+relation[(a, b), (c, d)].` - Bulk insert
- `-relation(val1, val2).` - Delete a fact

### 1.3 Simple Queries

| Example | What You'll Learn |
|---------|-------------------|
| `21_query_features/00_scan_all.dl` | Querying all data from a relation |

**Key Concepts**:
- `?- relation(X, Y).` - Query with variables
- Variables (uppercase) bind to values
- `_` ignores a column

**Checkpoint**: You should now be able to create databases, add/remove facts, and query them.

---

## Stage 2: Rules and Derived Data

**Goal**: Create derived relations using rules.

### 2.1 Session Rules

| Example | What You'll Learn |
|---------|-------------------|
| `04_session/01_session_rules.dl` | Transient rules that don't persist |

**Key Concepts**:
- `head(X) :- body(X).` - Session rule (no `+` prefix)
- Session rules exist only for current session

### 2.2 Persistent Rules

| Example | What You'll Learn |
|---------|-------------------|
| `25_unified_prefix/02_persistent_rules.dl` | The `+` prefix for persistence |

**Key Concepts**:
- `+derived(X, Y) :- source(X, Y).` - Persistent rule
- Rules automatically update when facts change

### 2.3 Rule Management

| Example | What You'll Learn |
|---------|-------------------|
| `17_rule_commands/01_rule_list.dl` | Listing defined rules |
| `17_rule_commands/02_rule_query.dl` | Querying rule results |
| `17_rule_commands/03_rule_drop.dl` | Removing rules |
| `17_rule_commands/04_rule_def.dl` | Showing rule definitions |
| `17_rule_commands/05_multi_clause_rules.dl` | Multi-clause rules and inspection |
| `17_rule_commands/06_drop_multi_clause.dl` | Dropping multi-clause rules |
| `17_rule_commands/07_rule_clear.dl` | Clearing rules for re-registration |
| `17_rule_commands/08_rule_edit.dl` | Editing specific clauses |

**Key Concepts**:
- `.rule` / `.rule list` - List all rules
- `.rule <name>` - Query a rule (shows clauses and results)
- `.rule def <name>` - Show definition only
- `.rule drop <name>` - Delete a rule entirely
- `.rule clear <name>` - Clear clauses for re-registration
- `.rule edit <name> <index> <clause>` - Edit a specific clause

**Checkpoint**: You can now create, inspect, modify, and delete rules.

---

## Stage 3: Joins and Filters

**Goal**: Combine data from multiple relations and filter results.

### 3.1 Joins

| Example | What You'll Learn |
|---------|-------------------|
| `06_joins/01_two_way_join.dl` | Joining two relations |
| `06_joins/02_self_join.dl` | Joining a relation with itself |
| `06_joins/03_multi_way_join.dl` | Three or more relations |
| `06_joins/04_join_with_constants.dl` | Mixing variables and constants |
| `06_joins/05_cartesian.dl` | Cross products (careful!) |

**Key Concepts**:
- Shared variables create joins: `a(X, Y), b(Y, Z)`
- Self-join uses same relation twice: `edge(X, Y), edge(Y, Z)`
- Cartesian product when no shared variables (avoid if possible)

### 3.2 Filters and Comparisons

| Example | What You'll Learn |
|---------|-------------------|
| `07_filters/01_equality.dl` | Equality constraints |
| `07_filters/02_inequality.dl` | Not-equal constraints |
| `07_filters/03_less_than.dl` | Less-than comparisons |
| `07_filters/04_greater_than.dl` | Greater-than comparisons |

**Key Concepts**:
- `X = Y` - Equality
- `X != Y` - Inequality
- `X < Y`, `X <= Y`, `X > Y`, `X >= Y` - Comparisons

**Checkpoint**: You can now write rules that join and filter data.

---

## Stage 4: Negation

**Goal**: Express "does not exist" conditions.

### 4.1 Basic Negation

| Example | What You'll Learn |
|---------|-------------------|
| `08_negation/01_simple_negation.dl` | Basic negation syntax |
| `08_negation/02_set_difference.dl` | Finding items in A but not B |
| `08_negation/05_double_negation.dl` | Negating negation |

**Key Concepts**:
- `!relation(X, Y)` - Negation
- Variables in negation must appear positively elsewhere (safety)

### 4.2 Negation Patterns

| Example | What You'll Learn |
|---------|-------------------|
| `08_negation/06_negation_self_relation.dl` | Negating within same relation |
| `08_negation/07_negation_with_filter.dl` | Combining negation with filters |
| `08_negation/09_negation_partial_key.dl` | Partial key matching |
| `08_negation/11_negation_full_exclude.dl` | Complete exclusion |
| `08_negation/12_negation_chained.dl` | Multiple negations |

**Checkpoint**: You can express "not in" conditions safely.

---

## Stage 5: Recursion

**Goal**: Compute transitive closures and graph reachability.

### 5.1 Transitive Closure

| Example | What You'll Learn |
|---------|-------------------|
| `09_recursion/01_transitive_closure.dl` | Basic reachability |
| `09_recursion/02_left_recursion.dl` | Left-recursive style |
| `09_recursion/03_right_recursion.dl` | Right-recursive style |

**Key Concepts**:
```datalog
+reachable(X, Y) :- edge(X, Y).           // Base case
+reachable(X, Z) :- reachable(X, Y), edge(Y, Z).  // Recursive
```

### 5.2 Advanced Recursion

| Example | What You'll Learn |
|---------|-------------------|
| `09_recursion/05_mutual_recursion.dl` | Rules that reference each other |
| `09_recursion/06_chain_multiple.dl` | Long recursive chains |
| `09_recursion/08_linear_chain.dl` | Linear recursion patterns |
| `09_recursion/16_self_loop_single.dl` | Handling self-loops |

### 5.3 Edge Cases

| Example | What You'll Learn |
|---------|-------------------|
| `09_recursion/10_empty_graph.dl` | Recursion with no data |
| `09_recursion/13_empty_base_case.dl` | Empty base case behavior |
| `09_recursion/14_no_new_tuples_first_iter.dl` | Fixpoint termination |

**Checkpoint**: You can compute graph reachability and transitive closures.

---

## Stage 6: Aggregations

**Goal**: Compute counts, sums, min, max, and averages.

### 6.1 Basic Aggregates

| Example | What You'll Learn |
|---------|-------------------|
| `14_aggregations/01_count.dl` | Counting rows |
| `14_aggregations/02_sum.dl` | Summing values |
| `14_aggregations/03_min.dl` | Finding minimum |
| `14_aggregations/04_max.dl` | Finding maximum |
| `14_aggregations/05_avg.dl` | Computing averages |

**Key Concepts**:
```datalog
+result(Group, count<Val>) :- source(Group, Val).
+result(Group, sum<Val>) :- source(Group, Val).
```

### 6.2 Grouping

| Example | What You'll Learn |
|---------|-------------------|
| `14_aggregations/06_multi_column_group.dl` | Grouping by multiple columns |
| `14_aggregations/08_global_count.dl` | Aggregating without groups |

### 6.3 Advanced Aggregation

| Example | What You'll Learn |
|---------|-------------------|
| `14_aggregations/09_agg_over_recursive.dl` | Aggregating recursive results |
| `14_aggregations/10_nested_aggregation.dl` | Nested aggregations |
| `14_aggregations/11_having_filter.dl` | Filtering aggregated results |
| `14_aggregations/12_count_distinct.dl` | Counting unique values |
| `14_aggregations/14_multiple_aggregates.dl` | Multiple aggregates |
| `14_aggregations/15_agg_with_negation.dl` | Combining with negation |

**Checkpoint**: You can perform SQL-like aggregations in Datalog.

---

## Stage 7: Arithmetic and Functions

**Goal**: Use expressions and built-in functions.

### 7.1 Basic Arithmetic

| Example | What You'll Learn |
|---------|-------------------|
| `15_arithmetic/01_increment.dl` | Addition |
| `15_arithmetic/02_multiply.dl` | Multiplication |
| `15_arithmetic/03_subtract.dl` | Subtraction |
| `15_arithmetic/04_divide.dl` | Division |
| `15_arithmetic/05_modulo.dl` | Modulo operation |

**Key Concepts**:
```datalog
+doubled(X, Y) :- nums(X), Y = X * 2.
```

### 7.2 Complex Expressions

| Example | What You'll Learn |
|---------|-------------------|
| `15_arithmetic/06_increment_multi_join.dl` | Arithmetic in joins |
| `15_arithmetic/08_increment_complex.dl` | Complex expressions |
| `15_arithmetic/17_arithmetic_on_aggregates.dl` | Arithmetic on aggregates |

### 7.3 Vector Operations

| Example | What You'll Learn |
|---------|-------------------|
| `16_vectors/01_euclidean_distance.dl` | Euclidean distance |
| `16_vectors/02_cosine_distance.dl` | Cosine similarity |
| `16_vectors/03_dot_product.dl` | Dot product |
| `16_vectors/04_manhattan_distance.dl` | Manhattan distance |
| `16_vectors/05_vec_operations.dl` | Vector operations |

**Checkpoint**: You can use arithmetic and vector functions.

---

## Stage 8: Types and Schemas

**Goal**: Define typed schemas for relations.

### 8.1 Data Types

| Example | What You'll Learn |
|---------|-------------------|
| `11_types/01_strings.dl` | String values |
| `11_types/02_integers.dl` | Integer values |
| `11_types/03_floats.dl` | Float values |
| `11_types/04_mixed_types.dl` | Combining types |

### 8.2 Schema Declarations

| Example | What You'll Learn |
|---------|-------------------|
| `24_rel_schemas/01_simple_schema.dl` | Basic schema |
| `24_rel_schemas/02_schema_with_data.dl` | Schema then data |
| `25_unified_prefix/01_schema_declarations.dl` | Schema syntax |
| `25_unified_prefix/03_schema_vs_data.dl` | Schema vs data distinction |
| `25_unified_prefix/04_schema_with_constraints.dl` | Adding constraints |

**Key Concepts**:
```datalog
+person(id: int, name: string, age: int).
+user(id: int @key, email: string @unique).
```

### 8.3 Type Declarations

| Example | What You'll Learn |
|---------|-------------------|
| `23_type_declarations/01_simple_alias.dl` | Type aliases |
| `23_type_declarations/02_record_type.dl` | Record types |
| `23_type_declarations/03_multiple_types.dl` | Multiple types |

**Checkpoint**: You can define typed schemas for your data.

---

## Stage 9: Advanced Patterns

**Goal**: Apply Datalog to real-world problems.

### 9.1 Graph Algorithms

| Example | What You'll Learn |
|---------|-------------------|
| `18_advanced_patterns/01_shortest_path.dl` | Shortest paths |
| `18_advanced_patterns/02_cycle_detection.dl` | Finding cycles |
| `18_advanced_patterns/03_scc.dl` | Strongly connected components |
| `18_advanced_patterns/04_bipartite.dl` | Bipartite detection |

### 9.2 Program Analysis

| Example | What You'll Learn |
|---------|-------------------|
| `18_advanced_patterns/07_points_to.dl` | Pointer analysis |
| `18_advanced_patterns/08_reaching_defs.dl` | Reaching definitions |
| `18_advanced_patterns/09_ancestors.dl` | Ancestry computation |

### 9.3 Combined Features

| Example | What You'll Learn |
|---------|-------------------|
| `18_advanced_patterns/10_negation_recursion_combined.dl` | Negation + recursion |
| `18_advanced_patterns/11_recursion_arithmetic_agg.dl` | Recursion + arithmetic + aggregation |
| `18_advanced_patterns/12_all_features_stress.dl` | Everything together |

**Checkpoint**: You can apply Datalog to complex real-world problems.

---

## Stage 10: Real-World Applications

**Goal**: See complete, realistic examples.

### 10.1 Application Examples

| Example | What You'll Learn |
|---------|-------------------|
| `20_applications/01_rbac.dl` | Role-based access control |
| `20_applications/02_social_network.dl` | Social network analysis |
| `20_applications/03_supply_chain.dl` | Supply chain management |
| `20_applications/04_dependency_analysis.dl` | Dependency resolution |
| `20_applications/05_recommendation.dl` | Recommendation systems |

### 10.2 Query Features

| Example | What You'll Learn |
|---------|-------------------|
| `21_query_features/01_distinct.dl` | Distinct results |
| `21_query_features/02_projection.dl` | Column projection |
| `21_query_features/03_selection.dl` | Row selection |

### 10.3 Set Operations

| Example | What You'll Learn |
|---------|-------------------|
| `22_set_operations/01_union.dl` | Union of relations |
| `22_set_operations/02_intersection.dl` | Intersection |
| `22_set_operations/03_difference.dl` | Set difference |

---

## Stage 11: Edge Cases and Errors

**Goal**: Understand error handling and edge cases.

### 11.1 Edge Cases

| Example | What You'll Learn |
|---------|-------------------|
| `10_edge_cases/01_empty_relation.dl` | Empty relations |
| `10_edge_cases/02_duplicate_facts.dl` | Duplicate handling |
| `10_edge_cases/03_self_loops.dl` | Self-referential edges |
| `10_edge_cases/04_large_values.dl` | Large numeric values |

### 11.2 Error Examples

| Example | What You'll Learn |
|---------|-------------------|
| `12_errors/01_undefined_relation_error.dl` | Unknown relation errors |
| `12_errors/02_invalid_syntax_error.dl` | Syntax errors |
| `12_errors/03_string_rejection_error.dl` | Type mismatches |
| `12_errors/04_arity_mismatch_error.dl` | Wrong number of columns |
| `12_errors/05_drop_nonexistent_db_error.dl` | Database errors |

---

## Quick Reference: Example Categories

| Category | Examples | Difficulty |
|----------|----------|------------|
| `01_database/` | Database management | Beginner |
| `02_relations/` | Fact operations | Beginner |
| `04_session/` | Session rules | Beginner |
| `06_joins/` | Joining data | Beginner |
| `07_filters/` | Filtering | Beginner |
| `08_negation/` | Negation | Intermediate |
| `09_recursion/` | Recursion | Intermediate |
| `10_edge_cases/` | Edge cases | Intermediate |
| `11_types/` | Data types | Beginner |
| `12_errors/` | Error handling | Beginner |
| `13_performance/` | Performance | Advanced |
| `14_aggregations/` | Aggregations | Intermediate |
| `15_arithmetic/` | Arithmetic | Intermediate |
| `16_vectors/` | Vector ops | Advanced |
| `17_rule_commands/` | Rule management | Beginner |
| `18_advanced_patterns/` | Graph algorithms | Advanced |
| `19_self_checking/` | Assertions | Intermediate |
| `20_applications/` | Real apps | Advanced |
| `21_query_features/` | Query features | Intermediate |
| `22_set_operations/` | Set operations | Intermediate |
| `23_type_declarations/` | Type system | Intermediate |
| `24_rel_schemas/` | Schemas | Beginner |
| `25_unified_prefix/` | Unified syntax | Beginner |

---

## Suggested Time Investment

| Stage | Time | Focus |
|-------|------|-------|
| 1-2 | 1 hour | Core concepts |
| 3-4 | 1-2 hours | Joins, filters, negation |
| 5 | 1-2 hours | Recursion (critical!) |
| 6 | 1 hour | Aggregations |
| 7-8 | 1 hour | Arithmetic, types |
| 9-10 | 2+ hours | Advanced applications |
| 11 | 30 min | Error handling |

**Total**: 8-10 hours for comprehensive understanding

---

## Next Steps After Examples

1. **Read the tutorials**: `docs/tutorials/`
2. **Check the syntax reference**: `docs/reference/syntax.md`
3. **Explore external datasets**: See sections below in this README
4. **Try the REPL**: Interactive exploration with `inputlayer-client`
5. **Build your own**: Apply to your own domain problems

---

## Tips for Learning

1. **Start simple** - Don't skip the basics
2. **Run every example** - See the actual output
3. **Modify and break things** - Learning comes from experimentation
4. **Understand recursion deeply** - It's Datalog's superpower
5. **Read error messages** - They're designed to help
6. **Use `.rule def`** - Inspect how rules are stored
7. **Use session rules first** - Before committing to persistent rules
