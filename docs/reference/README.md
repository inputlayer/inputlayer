# InputLayer Reference

Quick lookup documentation in Redis-style format. Each entry includes syntax, description, examples, and notes.

## Contents

| Document | Description |
|----------|-------------|
| [Commands](commands.md) | All meta commands (`.kg`, `.rule`, `.rel`, `.load`, etc.) |
| [Functions](functions.md) | All 55 builtin functions |
| [Syntax Cheatsheet](syntax-cheatsheet.md) | One-page syntax reference |

---

## Quick Reference

### Meta Commands

| Command | Description |
|---------|-------------|
| `.kg` | Knowledge graph operations |
| `.kg list` | List all knowledge graphs |
| `.kg use <name>` | Switch to knowledge graph |
| `.kg create <name>` | Create knowledge graph |
| `.kg drop <name>` | Delete knowledge graph |
| `.rel` | List all relations |
| `.rel <name>` | Show relation schema |
| `.rule` | List all rules |
| `.rule def <name>` | Show rule definition |
| `.rule remove <name> <idx>` | Remove rule clause |
| `.rule drop <name>` | Remove entire rule |
| `.session` | Show session state |
| `.session clear` | Clear session |
| `.load <file>` | Load script file |
| `.status` | Show system status |
| `.compact` | Compact storage |
| `.help` | Show help |
| `.quit` | Exit |

### Aggregations

| Aggregation | Syntax | Description |
|-------------|--------|-------------|
| COUNT | `count<X>` | Count distinct values |
| SUM | `sum<X>` | Sum numeric values |
| MIN | `min<X>` | Minimum value |
| MAX | `max<X>` | Maximum value |
| AVG | `avg<X>` | Average value |
| COUNT_DISTINCT | `count_distinct<X>` | Count distinct values |
| TOP_K | `top_k<K, ..., OrderVar:desc>` | Top K by ordering |

### Comparison Operators

| Operator | Meaning |
|----------|---------|
| `=` | Equal |
| `!=` | Not equal |
| `<` | Less than |
| `<=` | Less than or equal |
| `>` | Greater than |
| `>=` | Greater than or equal |

### Arithmetic Operators

| Operator | Meaning |
|----------|---------|
| `+` | Addition |
| `-` | Subtraction |
| `*` | Multiplication |
| `/` | Division |
| `%` | Modulo |

---

## Function Categories

| Category | Count | Examples |
|----------|-------|----------|
| Distance | 4 | `euclidean`, `cosine`, `dot`, `manhattan` |
| Vector Ops | 4 | `normalize`, `vec_dim`, `vec_add`, `vec_scale` |
| LSH | 3 | `lsh_bucket`, `lsh_probes`, `lsh_multi_probe` |
| Quantization | 4 | `quantize_linear`, `dequantize` |
| Int8 Distance | 4 | `euclidean_int8`, `cosine_int8` |
| Temporal | 14 | `time_now`, `time_diff`, `time_decay` |
| Math | 13 | `abs`, `sqrt`, `pow`, `log`, `sin`, `cos` |
| String | 7 | `len`, `upper`, `lower`, `substr`, `concat` |
| Scalar Min/Max | 2 | `min_val`, `max_val` |

See [Functions Reference](functions.md) for complete documentation.

---

## Syntax Quick Reference

```datalog
% Facts
+relation(value1, value2).     % Insert persistent fact
relation(value1, value2).      % Insert session fact
-relation(value1, value2).     % Delete fact

% Rules
+head(X, Y) :- body(X, Z), other(Z, Y).     % Persistent rule
head(X, Y) :- body(X, Z), other(Z, Y).      % Session rule

% Negation
result(X) :- source(X), !excluded(X).

% Queries
?- relation(X, Y), X > 10.

% Aggregations
?- count<X> :- relation(X, _).
?- sum<V> : G :- data(G, V).  % Group by G

% Computed values
result(X, Y) :- input(X), Y = X * 2 + 1.

% Comments
% Single line comment
/* Multi-line
   comment */
```

See [Syntax Cheatsheet](syntax-cheatsheet.md) for more.
