# Datalog Examples - Organized by Dataset

**Location**: `course/final-project/examples/datalog/`
**Organization**: One folder per dataset, containing both data and .dl files
**Last Updated**: 2025-11-24

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
