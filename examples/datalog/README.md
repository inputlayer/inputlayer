# Datalog Examples and Snapshot Tests

**Location**: `course/final-project/examples/datalog/`
**Last Updated**: 2025-12-19

---

## Snapshot Tests

This directory contains 186 snapshot tests for the InputLayer Datalog engine. Each `.dl` file is a test script, and each `.dl.out` file contains the expected output.

### Running Tests

```bash
# Run all tests
./scripts/run_snapshot_tests.sh

# Run with verbose output (shows diffs on failures)
./scripts/run_snapshot_tests.sh -v

# Filter tests by pattern
./scripts/run_snapshot_tests.sh -f recursion
./scripts/run_snapshot_tests.sh -f "20_applications"

# Regenerate snapshots (use after verifying output is correct)
./scripts/run_snapshot_tests.sh --update
./scripts/run_snapshot_tests.sh --update -f "specific_test"
```

### Test Categories

| Directory | Description | Tests |
|-----------|-------------|-------|
| `01_database/` | Database create, use, drop commands | 1 |
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
2. Add header comment describing the test:
   ```datalog
   // Test: Descriptive Name
   // Category: category_name
   // Description: What this test verifies
   ```
3. Run the test to verify output:
   ```bash
   cargo run --bin inputlayer-client --release --quiet -- --script your_test.dl
   ```
4. Verify the output is semantically correct
5. Generate the snapshot:
   ```bash
   ./scripts/run_snapshot_tests.sh --update -f "your_test"
   ```
6. Verify the test passes:
   ```bash
   ./scripts/run_snapshot_tests.sh -f "your_test"
   ```

### Known Limitations

- **Vector functions**: Vector operations in rule heads produce "Unsafe rule" errors (see `16_vectors/`)
- **Ordering constraints**: Some ordering patterns like `A < B, B < C` in certain contexts may produce errors
- **Deep recursion**: Tested up to 500 levels; deeper recursion is possible but may hit stack limits

### CI/CD Notes

- The test runner normalizes output to strip timestamps and `Executing script:` lines
- Paths are relative (`examples/datalog/...`) for portability across machines
- Build failures are detected and abort the test run with clear error messages

---

## External Datasets

**Organization**: One folder per dataset, containing both data and .dl files

---

## 📁 Directory Structure

```
course/final-project/examples/datalog/
├── pointsto/           Pointer analysis (Andersen's)
├── galen/              Medical ontology reasoning
├── borrow/             Rust borrow checker analysis
├── batik/              Apache Batik program analysis
├── biojava/            BioJava bioinformatics library
├── cvc5/               CVC5 SMT solver analysis
├── z3/                 Z3 SMT solver analysis
├── crdt/               CRDT operations analysis
├── eclipse/            Eclipse IDE analysis
├── xalan/              Apache Xalan XSLT processor
├── zxing/              ZXing barcode library
└── generic-examples/   Generic examples (graphs, family trees)
```

Each dataset folder contains:
- **CSV files**: The actual benchmark data
- **.dl files**: Datalog programs that use this data

---

## 📦 Dataset Folders

### Small Datasets (< 10MB)

#### `pointsto/` - Pointer Analysis
- **Size**: 564K
- **CSV Files**: 4
  - `AssignAlloc.csv`
  - `Load.csv`
  - `PrimitiveAssign.csv`
  - `Store.csv`
- **Datalog Programs**:
  - `andersen.dl` - Andersen's points-to analysis
- **Use Case**: Learning pointer analysis fundamentals

#### `galen/` - Medical Ontology
- **Size**: 15MB
- **CSV Files**: 6
  - `C.csv`, `P.csv`, `Q.csv`, `R.csv`, `S.csv`, `U.csv`
- **Datalog Programs**:
  - `galen.dl` - Medical ontology reasoning
- **Use Case**: Ontology reasoning, transitive closure
- **Status**: ✅ Perfect match, ready to use

#### `borrow/` - Rust Borrow Checker
- **Size**: 40MB
- **CSV Files**: 18 (cfg_edge, loan_issued_at, subset_base, etc.)
- **Datalog Programs**:
  - `borrow.dl` - Rust borrow checker analysis
- **Use Case**: Program verification, borrow checking

### Medium Datasets (10-100MB)

#### `batik/` - Apache Batik
- **Size**: ~30MB
- **CSV Files**: 20+ (class hierarchy, method calls, field access, etc.)
- **Datalog Programs**:
  - `batik.dl` - Apache Batik SVG library analysis
- **Use Case**: Large-scale program analysis

#### `biojava/` - BioJava
- **Size**: ~30MB
- **CSV Files**: 20+ (program analysis facts)
- **Datalog Programs**:
  - `biojava.dl` - BioJava bioinformatics library analysis
- **Use Case**: Bioinformatics code analysis

#### `cvc5/` - CVC5 SMT Solver
- **Size**: ~15MB
- **CSV Files**: 15+
- **Datalog Programs**:
  - `cvc5.dl` - CVC5 SMT solver analysis
- **Use Case**: SMT solver codebase analysis

#### `z3/` - Z3 SMT Solver
- **Size**: ~15MB
- **CSV Files**: 15+
- **Datalog Programs**:
  - `z3.dl` - Z3 SMT solver analysis
- **Use Case**: SMT solver codebase analysis

#### `crdt/` - CRDT Operations
- **Size**: ~10MB
- **CSV Files**: Multiple operation logs
- **Datalog Programs**:
  - `crdt.dl` - CRDT operations analysis
  - `crdt_slow.dl` - Alternative CRDT analysis
- **Use Case**: Distributed systems, CRDT verification

### Large Datasets (100MB-1GB)

#### `eclipse/` - Eclipse IDE
- **Size**: ~200MB
- **CSV Files**: 20+ (program analysis at scale)
- **Datalog Programs**:
  - `eclipse.dl` - Eclipse IDE analysis
- **Use Case**: Production-scale program analysis

#### `xalan/` - Apache Xalan
- **Size**: ~200MB
- **CSV Files**: 20+ (XSLT processor analysis)
- **Datalog Programs**:
  - `xalan.dl` - Apache Xalan XSLT processor analysis
- **Use Case**: Large-scale Java program analysis

#### `zxing/` - ZXing Barcode Library
- **Size**: ~200MB
- **CSV Files**: 20+ (barcode library analysis)
- **Datalog Programs**:
  - `zxing.dl` - ZXing barcode library analysis
- **Use Case**: Real-world library analysis

---

## 📝 Generic Examples

### `generic-examples/` - General-Purpose Examples

Examples that work with generated or simple graph data:

**Graph-Based Examples**:
- `tc.dl` - Transitive closure
- `transitive_closure.dl` - Alternative transitive closure
- `reach.dl` - Reachability analysis
- `cc.dl` - Connected components
- `sssp.dl` - Single-source shortest path
- `bipartite.dl` - Bipartite graph detection

**Family Tree Examples**:
- `sg.dl` - Same generation
- `same_generation.dl` - Alternative same generation

**Analysis Examples**:
- `csda.dl` - Context-sensitive data analysis
- `cspa.dl` - Context-sensitive program analysis
- `dyck.dl` - Dyck language recognition

**Helper Files**:
- `generate_sample_data.py` - Generate test CSV data
- `DATA_GENERATION_GUIDE.md` - Guide for data generation

### Generating Test Data

```bash
cd generic-examples

# Generate graph data for transitive closure
python3 generate_sample_data.py tc.dl

# Generate graph data for reachability
python3 generate_sample_data.py reach.dl

# Generate family tree data
python3 generate_sample_data.py sg.dl
```

---

## 🚀 Usage Examples

### Example 1: Run Galen with Soufflé (if installed)

```bash
cd galen
souffle galen.dl -F .
```

This runs the galen.dl program with the CSV files in the current directory.

### Example 2: Run Batik Analysis

```bash
cd batik
souffle batik.dl -F .
```

### Example 3: Use Generic Examples

```bash
cd generic-examples

# Generate sample data
python3 generate_sample_data.py tc.dl

# Run transitive closure (if Soufflé installed)
souffle tc.dl -F .
```

### Example 4: Explore Pointer Analysis

```bash
cd pointsto

# View the data
head AssignAlloc.csv
head Load.csv

# Run analysis (if Soufflé installed)
souffle andersen.dl -F .
```

---

## 📊 Quick Reference Table

| Dataset | Size | .dl Files | CSV Files | Complexity |
|---------|------|-----------|-----------|------------|
| pointsto | 564K | 1 | 4 | Simple ✅ |
| galen | 15MB | 1 | 6 | Simple ✅ |
| borrow | 40MB | 1 | 18 | Medium |
| batik | ~30MB | 1 | 20+ | Medium |
| biojava | ~30MB | 1 | 20+ | Medium |
| cvc5 | ~15MB | 1 | 15+ | Medium |
| z3 | ~15MB | 1 | 15+ | Medium |
| crdt | ~10MB | 2 | Multiple | Medium |
| eclipse | ~200MB | 1 | 20+ | Large |
| xalan | ~200MB | 1 | 20+ | Large |
| zxing | ~200MB | 1 | 20+ | Large |
| generic-examples | - | 11 | Generate | Variable |

---

## 🔍 Exploring Datasets

### View CSV Structure

```bash
cd galen
head -5 P.csv
wc -l *.csv  # Count lines in all CSV files
```

### Count Data Size

```bash
cd batik
du -sh .          # Total size
find . -name "*.csv" | wc -l  # Number of CSV files
```

### Examine Datalog Rules

```bash
cd galen
grep ":-" galen.dl  # Show all rules
```

---

## 🧪 Testing with Our Engine

Our InputLayer reference implementation can parse most of these examples:

```bash
# From the final-project directory
cd ../../final-project

# Parse galen.dl
cargo test test_parse_galen

# Run all example parsing tests
cargo test --test example_verification
```

**Parsing Status**:
- ✅ Simple examples (galen, andersen, etc.): Parse correctly
- ✅ Recursive examples (tc, reach): Parse correctly
- ⏳ Aggregation examples (borrow, cc, sssp): Need Module 10

---

## 📚 Documentation

### Dataset Documentation
- See `../../datasets/DATASETS_README.md` for download instructions
- See `generic-examples/DATA_GENERATION_GUIDE.md` for data generation

### Original Files
- Original .dl files are in: `../../final-project/examples/datalog/`
- Dataset sources are in: `../../datasets/`

---

## 🎯 Recommended Learning Path

### 1. Start with Small Examples

**galen/** (Recommended first!)
- Small dataset (15MB)
- Clean structure (6 CSV files)
- Perfect schema match
- Good for learning

**pointsto/**
- Very small (564K)
- Simple pointer analysis
- 4 CSV files only

### 2. Try Generic Examples

**generic-examples/**
- Generate your own data
- Control size and complexity
- Good for experimentation

### 3. Move to Medium Examples

**batik/**, **biojava/**, **z3/**
- Real program analysis data
- Medium complexity
- Production-like scale

### 4. Advanced: Large Datasets

**eclipse/**, **xalan/**, **zxing/**
- Production scale
- 200MB+ datasets
- Benchmarking and performance testing

---

## 🔧 Maintenance

### Adding New Datasets

To add a new dataset:

1. Create folder: `mkdir my_dataset`
2. Add CSV files: `cp /path/to/*.csv my_dataset/`
3. Add .dl file: `cp /path/to/my_dataset.dl my_dataset/`
4. Update this README

### Regenerating Data

If you need to re-download datasets:

```bash
cd ../../datasets
./download_datasets.sh [category]
# Then re-organize as needed
```

---

## ⚠️ Important Notes

### CSV Format
- All CSV files are **comma-separated**
- **No headers** (data starts at line 1)
- Example: `1,2` means a relation between entity 1 and entity 2

### Soufflé vs Our Engine
- **Soufflé**: Can execute all examples fully
- **Our Engine**:
  - ✅ Parses rules correctly
  - ✅ Executes non-recursive queries
  - ⏳ Recursion needs `.iterative()` implementation (4-6 hours)
  - ⏳ Aggregation needs Module 10 (8-16 hours)

### File Extensions
- `.dl` - Datalog programs (Soufflé format)
- `.csv` - Data files (comma-separated values)
- `.facts` - Alternative fact format (same as CSV)

---

## 📖 References

- **Dataset Source**: https://pages.cs.wisc.edu/~m0riarty/dataset/csv/
- **Soufflé Documentation**: https://souffle-lang.github.io/
- **InputLayer Implementation**: `../../final-project/`
- **Download Script**: `../../datasets/download_datasets.sh`

---

## ✅ Verification Checklist

Before using a dataset:

- [ ] Dataset folder exists with CSV files
- [ ] Corresponding .dl file is present
- [ ] CSV files have data (use `head` to check)
- [ ] Understand the schema (check .dl input declarations)
- [ ] Have Soufflé installed (if executing) or use our engine for parsing

---

**Organization**: One folder per dataset ✅
**Total Datasets**: 11 with data, 1 generic folder
**Total Examples**: 23 .dl files
**Status**: Production Ready
**Last Updated**: 2025-11-24
