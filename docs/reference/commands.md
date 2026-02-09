# Meta Commands Reference

Meta commands control the InputLayer environment. They start with a `.` prefix.

## Knowledge Graph Commands

### `.kg`

Show current knowledge graph.

```
.kg
```

**Output:**
```
Current knowledge graph: mykg
```

### `.kg list`

List all knowledge graphs.

```
.kg list
```

**Output:**
```
Knowledge Graphs:
  default
  mykg (current)
  analytics
```

### `.kg create <name>`

Create a new knowledge graph.

```
.kg create analytics
```

### `.kg use <name>`

Switch to a knowledge graph.

```
.kg use analytics
```

**Note:** Switching knowledge graphs clears session rules and transient data.

### `.kg drop <name>`

Delete a knowledge graph and all its data.

```
.kg drop old_knowledge_graph
```

**Warning:** This permanently deletes all relations, rules, and data.

## Relation Commands

### `.rel`

List all relations with row counts.

```
.rel
```

**Output:**
```
Relations:
  edge (150 rows)
  person (25 rows)
  department (5 rows)
```

### `.rel <name>`

Describe a relation's schema and show sample data.

```
.rel person
```

**Output:**
```
Relation: person
Schema: (id: int, name: string, age: int)

Sample data (first 10 rows):
  (1, "alice", 30)
  (2, "bob", 25)
  ...
```

## Rule Commands

### `.rule`

List all persistent rules.

```
.rule
```

**Output:**
```
Rules:
  reachable (2 clauses)
  can_access (3 clauses)
```

### `.rule list`

Same as `.rule` - list all persistent rules.

```
.rule list
```

### `.rule <name>`

Query a rule and show computed results.

```
.rule reachable
```

**Output:**
```
Computed 150 tuples for 'reachable':
  (1, 2)
  (1, 3)
  ...
```

### `.rule def <name>`

Show the definition of a rule.

```
.rule def reachable
```

**Output:**
```
Rule: reachable
Clauses:
  1. reachable(X, Y) :- edge(X, Y).
  2. reachable(X, Z) :- reachable(X, Y), edge(Y, Z).
```

### `.rule drop <name>`

Delete a rule entirely (removes all clauses).

```
.rule drop reachable
```

### `.rule remove <name> <index>`

Remove a specific clause from a rule by index (1-based).

```
.rule remove reachable 2
```

**Output:**
```
Clause 2 removed from rule 'reachable'.
```

**Note:** If the last clause is removed, the entire rule is deleted:
```
Clause 1 removed from rule 'simple'. Rule completely deleted (no clauses remaining).
```

**Errors:**
- If clause index is out of bounds: `Clause index 5 out of bounds. Rule 'reachable' has 2 clause(s).`
- If rule doesn't exist: `Rule 'nonexistent' does not exist`

### `.rule clear <name>`

Clear all clauses from a rule for re-registration.

```
.rule clear reachable
```

### `.rule edit <name> <index> <clause>`

Edit a specific clause in a rule.

```
.rule edit reachable 2 +reachable(X, Z) :- edge(X, Y), reachable(Y, Z).
```

**Note:** Index is 1-based.

## Session Commands

Session rules are transient and not persisted.

### `.session`

List current session rules.

```
.session
```

**Output:**
```
Session rules:
  1. temp(X) :- edge(X, _).
  2. filtered(X, Y) :- temp(X), edge(X, Y).
```

### `.session clear`

Clear all session rules.

```
.session clear
```

### `.session drop <n>`

Remove a specific session rule by index.

```
.session drop 1
```

**Note:** Index is 1-based.

## Load Command

The `.load` command executes statements from a file.

### `.load <file>`

Load and execute a file in strict mode.

```
.load schema.dl
```

**Behavior:**
- Parses all statements in the file
- Fails if any relation or rule already exists
- Use for initial setup or clean loads

### `.load <file> --replace`

Atomically replace existing definitions.

```
.load views/access_control.dl --replace
```

**Behavior:**
1. Parse and validate the entire file
2. Delete all existing rules/relations that will be created
3. Execute the file statements
4. Re-materialize dependent views

**Use case:** Updating rule definitions during development.

### `.load <file> --merge`

Merge with existing definitions.

```
.load additional_rules.dl --merge
```

**Behavior:**
- Add new rules to existing views
- Keep existing data
- Error on schema conflicts

**Use case:** Adding rules incrementally.

### Load Mode Comparison

| Mode | Existing Schema | Existing View | Existing Data |
|------|-----------------|---------------|---------------|
| Default | Error | Error | N/A |
| `--replace` | Delete | Delete | Delete |
| `--merge` | Check compat | Add rules | Keep |

### Supported File Formats

| Extension | Description |
|-----------|-------------|
| `.dl` | Datalog script (statements) |

## System Commands

### `.status`

Show system status.

```
.status
```

**Output:**
```
Knowledge graph: mykg
Relations: 5
Rules: 3
Session rules: 2
Data directory: ./data
```

### `.compact`

Compact storage by consolidating WAL and batch files.

```
.compact
```

### `.help`

Show help message.

```
.help
```

### `.quit` / `.exit` / `.q`

Exit the REPL.

```
.quit
```

---

## Index Commands

Commands for managing HNSW (Hierarchical Navigable Small World) indexes for fast vector similarity search.

### `.index` / `.index list`

List all indexes in the current knowledge graph.

```
.index
.index list
```

**Output:**
```
Indexes:
  embeddings_idx on embeddings(vector) [hnsw, cosine]
  docs_idx on documents(embedding) [hnsw, euclidean]
```

### `.index create`

Create a new HNSW index on a vector column.

**Syntax:**
```
.index create <name> on <relation>(<column>) [type <index_type>] [metric <distance_metric>] [m <max_connections>] [ef_construction <beam_width>] [ef_search <search_beam>]
```

**Parameters:**
- `name` - Unique name for the index
- `relation` - Relation containing the vector column
- `column` - Column name containing vectors
- `type` - Index type (default: `hnsw`)
- `metric` - Distance metric: `cosine`, `euclidean`, `dot_product`, `manhattan` (default: `cosine`)
- `m` - Max connections per node (default: 16, higher = better recall, more memory)
- `ef_construction` - Beam width during construction (default: 100, higher = better quality, slower build)
- `ef_search` - Beam width during search (default: 50, higher = better recall, slower search)

**Examples:**

Basic index with defaults:
```
.index create my_idx on embeddings(vector)
```

Index with cosine distance:
```
.index create doc_idx on documents(embedding) metric cosine
```

Tuned index for high recall:
```
.index create high_recall_idx on items(vec) metric euclidean m 32 ef_construction 200 ef_search 100
```

### `.index drop`

Delete an index.

```
.index drop <name>
```

**Example:**
```
.index drop embeddings_idx
```

### `.index stats`

Show statistics for an index including size, build time, and configuration.

```
.index stats <name>
```

**Example:**
```
.index stats embeddings_idx
```

**Output:**
```
Index: embeddings_idx
  Relation: embeddings
  Column: vector
  Type: hnsw
  Metric: cosine
  Vectors: 10,000
  Dimensions: 384
  M: 16
  ef_construction: 100
  ef_search: 50
  Memory: 15.2 MB
```

### `.index rebuild`

Force rebuild an index. Useful after bulk inserts or if index becomes stale.

```
.index rebuild <name>
```

**Example:**
```
.index rebuild embeddings_idx
```

**Note:** Rebuilding large indexes may take significant time. The index remains available during rebuild.

---

## Error Handling

### Invalid Command

```
.foo
```

**Output:**
```
Unknown meta command: .foo
```

### Missing Arguments

```
.kg create
```

**Output:**
```
Usage: .kg create <name>
```

### Rule Not Found

```
.rule nonexistent
```

**Output:**
```
Rule 'nonexistent' not found.
```
