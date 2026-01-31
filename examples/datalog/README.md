# Datalog Examples and Snapshot Tests

This directory contains snapshot tests and external benchmark datasets for the InputLayer Datalog engine.

---

## Snapshot Tests

Each `.dl` file is a test script; each `.dl.out` file holds the expected output.

### Running Tests

```bash
# Run all tests
./scripts/run_snapshot_tests.sh

# Verbose (shows diffs on failures)
./scripts/run_snapshot_tests.sh -v

# Filter by pattern
./scripts/run_snapshot_tests.sh -f recursion
./scripts/run_snapshot_tests.sh -f "20_applications"

# Regenerate snapshots (after verifying output is correct)
./scripts/run_snapshot_tests.sh --update
./scripts/run_snapshot_tests.sh --update -f "specific_test"
```

### Test Categories

| Directory | Description | Tests |
|-----------|-------------|-------|
| `01_knowledge_graph/` | Database create, use, drop commands | 1 |
| `02_relations/` | Insert and delete operations | 6 |
| `04_session/` | Session-scoped rules | 1 |
| `06_joins/` | Two-way, self, and multi-way joins | 5 |
| `07_filters/` | Equality, inequality, comparisons | 4 |
| `08_negation/` | Negation patterns | 27 |
| `09_recursion/` | Transitive closure, mutual recursion | 16 |
| `10_edge_cases/` | Empty relations, duplicates, self-loops | 12 |
| `11_types/` | Strings, integers, floats, mixed types | 10 |
| `12_errors/` | Error handling and edge cases | 15 |
| `13_performance/` | Stress tests with larger datasets | 11 |
| `14_aggregations/` | count, sum, min, max, avg | 16 |
| `15_arithmetic/` | Arithmetic in rule heads | 17 |
| `16_vectors/` | Vector distance functions | 5 |
| `17_rule_commands/` | Rule list, query, drop, def, clear, edit | 8 |
| `18_advanced_patterns/` | Graph algorithms, complex patterns | 12 |
| `19_self_checking/` | Tests with embedded assertions | 4 |
| `20_applications/` | Real-world use cases (RBAC, graphs) | 10 |
| `21_query_features/` | Scan, distinct, projection, selection | 6 |
| `22_set_operations/` | Union, intersection, difference | 4 |
| `23_type_declarations/` | Type aliases and record types | 3 |
| `24_rel_schemas/` | Relation schema declarations | 3 |

### Syntax Reference

#### Persistent Rules (Views)

```datalog
// Basic persistent rule
+reachable(X, Y) :- edge(X, Y).

// Recursive rule
+reachable(X, Y) :- edge(X, Z), reachable(Z, Y).

// Multi-rule view (adds to existing view)
+symmetric_edge(A, B) :- edge(A, B).
+symmetric_edge(A, B) :- edge(B, A).

// Aggregation
+total_sales(Dept, sum<Amount>) :- sales(Dept, Amount).

// Arithmetic in head
+doubled(X, X*2) :- nums(X).

// Constants in head
+constant_fact(0, 1) :- trigger(_).
```

#### Schema Declarations

```datalog
// Persistent schema with typed columns
+employee(id: int, name: string, dept_id: int).
+user(id: int, name: string, email: string).
```

#### Fact Operations

```datalog
// Insert facts
+relation[(1, 2), (3, 4)].

// Delete facts
-relation[(1, 2)].
```

#### Queries

```datalog
// Query all results
?- view_name(X, Y).

// Query with constants
?- view_name(1, Y).
```

#### Filters and Negation

```datalog
// Comparison filters
+filtered(X) :- nums(X), X > 10.
+range(X) :- nums(X), X >= 5, X <= 15.

// Negation (stratified)
+not_in_b(X) :- a(X), !b(X).
```

### Embedded Assertions

Tests can include assertions to verify specific properties:

```datalog
// @ASSERT_ROWS: 5
?- my_view(X).

// @ASSERT_CONTAINS: (1, 2)
?- pairs(A, B).

// @ASSERT_NOT_CONTAINS: (0, 0)
?- diagonal(X, Y).

// @ASSERT_EMPTY
?- impossible_result(X).

// @ASSERT_COLUMNS: 3
?- triple(A, B, C).
```

### Adding New Tests

1. Create a `.dl` file in the appropriate category directory
2. Add a header comment:
   ```datalog
   // Test: Descriptive Name
   // Category: category_name
   // Description: What this test verifies
   ```
3. Run and verify the output:
   ```bash
   cargo run --bin inputlayer-client --release --quiet -- --script your_test.dl
   ```
4. Generate the snapshot:
   ```bash
   ./scripts/run_snapshot_tests.sh --update -f "your_test"
   ```
5. Confirm the test passes:
   ```bash
   ./scripts/run_snapshot_tests.sh -f "your_test"
   ```

### Known Limitations

- Vector operations in rule heads produce "Unsafe rule" errors (see `16_vectors/`)
- Some ordering patterns like `A < B, B < C` in certain contexts may produce errors
- Deep recursion tested up to 500 levels; deeper is possible but may hit stack limits

### CI Notes

- Output is normalized (timestamps and `Executing script:` lines stripped)
- Paths are relative for portability
- Build failures abort the run with clear error messages

---

## External Datasets

Each dataset lives in its own folder with CSV files and `.dl` programs.

```
examples/datalog/
  pointsto/        Pointer analysis (Andersen's)
  galen/           Medical ontology reasoning
  borrow/          Rust borrow checker analysis
  batik/           Apache Batik program analysis
  biojava/         BioJava bioinformatics library
  cvc5/            CVC5 SMT solver analysis
  z3/              Z3 SMT solver analysis
  crdt/            CRDT operations analysis
  eclipse/         Eclipse IDE analysis
  xalan/           Apache Xalan XSLT processor
  zxing/           ZXing barcode library
  generic-examples/ Generic examples (graphs, family trees)
```

### Small Datasets (< 10MB)

**pointsto/**  - Pointer analysis. 564K, 4 CSVs.
- `andersen.dl`  - Andersen's points-to analysis

**galen/**  - Medical ontology. 15MB, 6 CSVs.
- `galen.dl`  - Ontology reasoning, transitive closure

**borrow/**  - Rust borrow checker. 40MB, 18 CSVs.
- `borrow.dl`  - Program verification, borrow checking

### Medium Datasets (10-100MB)

**batik/**  - Apache Batik. ~30MB, 20+ CSVs.
**biojava/**  - BioJava. ~30MB, 20+ CSVs.
**cvc5/**  - CVC5 SMT solver. ~15MB, 15+ CSVs.
**z3/**  - Z3 SMT solver. ~15MB, 15+ CSVs.
**crdt/**  - CRDT operations. ~10MB, `crdt.dl` + `crdt_slow.dl`.

### Large Datasets (100MB-1GB)

**eclipse/**  - Eclipse IDE. ~200MB, 20+ CSVs.
**xalan/**  - Apache Xalan. ~200MB, 20+ CSVs.
**zxing/**  - ZXing barcode library. ~200MB, 20+ CSVs.

### Generic Examples

The `generic-examples/` folder has programs that work with generated graph data:

- `tc.dl`, `transitive_closure.dl`  - Transitive closure
- `reach.dl`  - Reachability
- `cc.dl`  - Connected components
- `sssp.dl`  - Single-source shortest path
- `bipartite.dl`  - Bipartite graph detection
- `sg.dl`, `same_generation.dl`  - Same generation
- `csda.dl`, `cspa.dl`  - Context-sensitive analysis
- `dyck.dl`  - Dyck language recognition

Generate test data:

```bash
cd generic-examples
python3 generate_sample_data.py tc.dl
```

### Dataset Reference

| Dataset | Size | .dl Files | CSV Files |
|---------|------|-----------|-----------|
| pointsto | 564K | 1 | 4 |
| galen | 15MB | 1 | 6 |
| borrow | 40MB | 1 | 18 |
| batik | ~30MB | 1 | 20+ |
| biojava | ~30MB | 1 | 20+ |
| cvc5 | ~15MB | 1 | 15+ |
| z3 | ~15MB | 1 | 15+ |
| crdt | ~10MB | 2 | Multiple |
| eclipse | ~200MB | 1 | 20+ |
| xalan | ~200MB | 1 | 20+ |
| zxing | ~200MB | 1 | 20+ |
| generic-examples |  - | 11 | Generated |

### CSV Format

- Comma-separated, no headers, data starts at line 1
- Example: `1,2` means a relation between entity 1 and entity 2

### File Extensions

- `.dl`  - Datalog programs
- `.csv`  - Data files
- `.facts`  - Alternative fact format (same as CSV)
