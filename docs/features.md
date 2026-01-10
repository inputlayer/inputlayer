# InputLayer Datalog Engine - Feature List

A production-grade Datalog engine built on Differential Dataflow.

## Core Language Features

### Datalog Syntax
- **Rules**: `head(x,y) :- body1(x,z), body2(z,y).`
- **Facts**: `edge(1, 2).`
- **Variables**: Alphanumeric identifiers starting with lowercase
- **Constants**: Integer literals, quoted strings
- **Comments**: Line comments with `//`

### Predicates & Constraints
- **Positive atoms**: `relation(args...)`
- **Negation**: `!relation(args...)` (stratified negation)
- **Comparison operators**:
  - Equal: `x = y`, `x = 5`
  - Not equal: `x != y`
  - Greater than: `x > y`, `x > 5`
  - Less than: `x < y`
  - Greater or equal: `x >= y`
  - Less or equal: `x <= y`
- **Column-to-column comparisons**: `x > y` where both are variables
- **Column-to-constant comparisons**: `x > 5`

### Arithmetic Expressions
- **Addition**: `x + y`
- **Subtraction**: `x - y`
- **Multiplication**: `x * y`
- **Division**: `x / y`
- **Modulo**: `x % y`
- **Parentheses**: `(x + y) * z`
- **Operator precedence**: Standard mathematical precedence

### Aggregations
- **COUNT**: `count<x>` - Count distinct values
- **SUM**: `sum<x>` - Sum numeric values
- **MIN**: `min<x>` - Minimum value
- **MAX**: `max<x>` - Maximum value
- **AVG**: `avg<x>` - Average value

### Recursion
- **Linear recursion**: `path(x,z) :- path(x,y), edge(y,z).`
- **Mutual recursion**: Multiple predicates depending on each other
- **Stratified negation**: Negation allowed when properly stratified
- **Automatic stratification**: SCC-based strata computation

## Query Processing Pipeline

### Module 04: Parsing
- Datalog source code → Abstract Syntax Tree (AST)
- Lexer with comprehensive token support
- Recursive descent parser
- Error messages with line/column information

### Module 05: IR Construction
- AST → Intermediate Representation (IR)
- Schema inference and catalog management
- Variable position tracking
- Join key computation

### Module 06: Basic Optimizations
- **Identity map elimination**: Remove `Map(x, identity)`
- **Map fusion**: `Map(Map(input, p1), p2)` → `Map(input, p1 ∘ p2)`
- **Filter fusion**: `Filter(Filter(input, p1), p2)` → `Filter(input, And(p1, p2))`
- **Filter pushdown**: Push filters through joins
- **Always-true filter elimination**: Remove `Filter(x, true)`
- **Always-false filter elimination**: `Filter(x, false)` → empty
- **Singleton union elimination**: `Union([x])` → `x`
- **Empty union elimination**: Dead code propagation
- **Fixpoint iteration**: Multiple optimization passes

### Module 07: Join Planning
- **Join graph construction**: Build graph from query atoms
- **Maximum spanning tree**: Kruskal's algorithm
- **Cost model**: Edge weights based on selectivity
- **Rooted tree generation**: Optimal join order

### Module 08: SIP Rewriting (Sideways Information Passing)
- **Adornment computation**: Track bound/free variables
- **Semijoin filter insertion**: Reduce intermediate results
- **Magic sets transformation**: (disabled by default)
- **Self-join detection**: Avoid circular dependencies

### Module 09: Subplan Sharing
- **Common subexpression detection**: Hash-based comparison
- **Canonical form normalization**: Variable renaming
- **Shared view materialization**: Avoid redundant computation

### Module 10: Boolean Specialization
- **Semiring analysis**: Determine appropriate algebra
- **Set vs bag semantics**: Automatic selection
- **Distinct insertion**: When set semantics required

### Module 11: Code Generation
- IR → Differential Dataflow execution
- **Operators supported**:
  - Scan (relation access)
  - Map (projection)
  - Filter (selection)
  - Join (equijoin)
  - Antijoin (negation)
  - Union (disjunction)
  - Distinct (deduplication)
  - Recursive (fixpoint iteration)

## Execution Engine

### Differential Dataflow Backend
- **Incremental computation**: Only recompute what changed
- **Timely Dataflow foundation**: High-performance streaming
- **Automatic parallelism**: Multi-threaded execution
- **Progress tracking**: Know when computation complete

### Multi-Worker Execution
- **Rayon integration**: Thread pool management
- **Parallel query execution**: Multiple queries concurrently
- **Cross-database queries**: Federated execution
- **Configurable thread count**: Via configuration file

### Recursion Handling
- **True DD recursion**: Using `SemigroupVariable`
- **Delta iteration**: Efficient fixpoint computation
- **Cycle detection**: Automatic termination
- **Stratified evaluation**: Proper negation handling

## Storage Engine

### Multi-Database Support
- **Namespace isolation**: Separate databases
- **Default database**: Automatic creation
- **Database lifecycle**: Create, drop, switch
- **Database listing**: Enumerate all databases

### Persistence Formats
- **Parquet**: Columnar, compressed, efficient for analytics
  - Automatic schema inference
  - Compression support
  - Efficient column access
- **CSV**: Human-readable, interoperable
  - Custom delimiter support
  - Quote handling
  - Type inference (Int32, Int64, Float64, Bool, String, Null)
  - Header row support

### Metadata Management
- **Database metadata**: Track relations, schemas, tuple counts
- **System metadata**: Cross-database catalog
- **JSON serialization**: Human-readable metadata files

## Production Hardening

### Query Timeout
- **Configurable timeout**: Per-query time limits
- **Cooperative cancellation**: Check flag periodically
- **Cancel handles**: External cancellation support
- **Remaining time tracking**: Know time budget

### Resource Limits
- **Memory limits**: Cap memory usage
- **Result size limits**: Maximum tuples returned
- **Intermediate result limits**: Prevent explosion
- **Row width limits**: Maximum tuple arity
- **Recursion depth limits**: Maximum fixpoint iterations

### Query Caching
- **Compiled query cache**: Reuse parsed/optimized IR
- **Result cache**: TTL-based result caching
- **LRU eviction**: Automatic cache management
- **Cache statistics**: Hit rate, evictions, expirations
- **Cache invalidation**: Clear on data changes

## Configuration System

### TOML Configuration
```toml
[storage]
data_dir = "./data"
default_database = "default"
auto_create_databases = true

[storage.persistence]
format = "parquet"
compression = "snappy"
auto_save_interval = 300

[storage.performance]
num_threads = 4

[optimization]
enable_join_planning = true
enable_sip_rewriting = false
enable_subplan_sharing = true
enable_boolean_specialization = true

[logging]
level = "info"
format = "json"
```

### Environment Variable Support
- Override any config value via environment
- Format: `FLOWLOG_<section>_<key>=<value>`

## Value System

### Supported Types
- **Int32**: 32-bit signed integers
- **Int64**: 64-bit signed integers
- **Float64**: 64-bit floating point
- **String**: UTF-8 strings (Arc<str> for efficiency)
- **Bool**: Boolean values
- **Null**: Nullable values

### Tuple Operations
- **Arbitrary arity**: N-tuple support (not limited to pairs)
- **Projection**: Select columns by index
- **Concatenation**: Combine tuples
- **Comparison**: Lexicographic ordering

### Arrow Integration
- **RecordBatch conversion**: For Parquet I/O
- **Schema inference**: Automatic type detection
- **Efficient serialization**: Zero-copy where possible

## API Surface

### DatalogEngine
```rust
let mut engine = DatalogEngine::new();
engine.add_fact("edge", vec![(1, 2), (2, 3)]);
let results = engine.execute("path(x,y) :- edge(x,y).")?;
```

### StorageEngine
```rust
let mut storage = StorageEngine::new(config)?;
storage.create_database("analytics")?;
storage.use_database("analytics")?;
storage.insert("edge", vec![(1, 2)])?;
let results = storage.execute_query("path(x,y) :- edge(x,y).")?;
storage.save_database("analytics")?;
```

### Parallel Execution
```rust
% Multiple queries on same database
let results = storage.execute_parallel_queries_on_database(
    "mydb",
    vec!["q1(x,y) :- edge(x,y).", "q2(x) :- node(x)."]
)?;

% Same query across databases
let results = storage.execute_query_on_multiple_databases(
    vec!["db1", "db2", "db3"],
    "result(x,y) :- edge(x,y)."
)?;
```

### Execution Control
```rust
let config = ExecutionConfig::default()
    .with_timeout(Duration::from_secs(30))
    .with_max_results(100_000)
    .with_memory_limit(1024 * 1024 * 100);
```

## Testing

### Test Coverage
- **221 unit tests** across all modules
- **Integration tests** for end-to-end scenarios
- **Advanced tests** for complex queries
- **Performance tests** for large datasets

### Test Categories
- Parser tests
- IR builder tests
- Optimizer tests
- Code generator tests
- Storage tests
- Execution tests
- Configuration tests

## Performance Characteristics

### Optimizations
- Delta iteration for recursion
- Join ordering optimization
- Filter pushdown
- Common subexpression elimination
- Compiled query caching

### Scalability
- Multi-threaded execution
- Incremental computation
- Memory-efficient tuple representation
- Lazy evaluation where possible

## Limitations

### Current Limitations
- SIP rewriting disabled by default (edge cases with self-joins)
- Aggregations partially implemented
- No distributed execution (single-node only)
- Limited query planning statistics

### Not Implemented
- Full magic sets transformation
- Worst-case optimal joins
- External function calls
- User-defined aggregates
