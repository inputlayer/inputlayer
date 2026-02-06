# Indexing Guide

InputLayer provides HNSW (Hierarchical Navigable Small World) indexes for fast approximate nearest neighbor search on vector data.

## Why Use Indexes?

Without an index, vector similarity queries perform a linear scan:
- **10K vectors**: ~10ms
- **100K vectors**: ~100ms
- **1M vectors**: ~1s

With an HNSW index:
- **10K vectors**: ~1ms
- **100K vectors**: ~5ms
- **1M vectors**: ~10ms

**Trade-off**: Indexes use memory and may return approximate (not exact) results.

---

## Creating an Index

### Basic Syntax

```
.index create <name> on <relation>(<column>) [options]
```

### Simple Example

```datalog
% Create a documents table with embeddings
+documents(id: int, title: string, embedding: vector).

% Insert some documents
+documents(1, "Introduction to ML", [0.1, 0.2, 0.3, 0.4]).
+documents(2, "Vector Databases", [0.15, 0.25, 0.28, 0.42]).
+documents(3, "Graph Theory", [0.8, 0.1, 0.05, 0.05]).
```

```
.index create doc_emb_idx on documents(embedding)
```

### With Options

```
.index create doc_emb_idx on documents(embedding) metric cosine m 16 ef_search 50
```

---

## Index Options

### Distance Metrics

| Metric | Aliases | Use Case |
|--------|---------|----------|
| `cosine` | `cos` | Text embeddings (most common) |
| `euclidean` | `l2`, `euclid` | Image embeddings |
| `dot` | `dotproduct`, `inner` | When vectors have meaningful magnitude |
| `manhattan` | `l1`, `taxicab` | Sparse vectors |

**Default**: `cosine`

```
.index create my_idx on vectors(embedding) metric l2
.index create my_idx on vectors(embedding) metric cosine
.index create my_idx on vectors(embedding) metric dot
```

### HNSW Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `m` | 16 | Max connections per node (higher = better recall, more memory) |
| `ef_construction` | 200 | Construction-time ef (higher = better quality, slower build) |
| `ef_search` | 50 | Search-time ef (higher = better recall, slower search) |

```
.index create my_idx on vectors(embedding) m 32 ef_search 100
```

### Parameter Tuning

**For higher recall (more accurate results):**
```
.index create my_idx on vectors(embedding) m 32 ef_construction 400 ef_search 100
```

**For faster search (lower recall):**
```
.index create my_idx on vectors(embedding) m 8 ef_search 20
```

**For large datasets (millions of vectors):**
```
.index create my_idx on vectors(embedding) m 48 ef_construction 500 ef_search 200
```

---

## Managing Indexes

### List All Indexes

```
.index
```

or

```
.index list
```

**Output:**
```
┌──────────────┬───────────┬───────────┬────────┬────────┬───────┐
│ Name         │ Relation  │ Column    │ Type   │ Metric │ Valid │
├──────────────┼───────────┼───────────┼────────┼────────┼───────┤
│ doc_emb_idx  │ documents │ embedding │ hnsw   │ cosine │ yes   │
└──────────────┴───────────┴───────────┴────────┴────────┴───────┘
```

### View Index Statistics

```
.index stats doc_emb_idx
```

**Output:**
```
Index: doc_emb_idx
  Relation:   documents
  Column:     embedding
  Type:       hnsw
  Metric:     cosine
  Vectors:    10000
  Dimension:  768
  Valid:      yes
  Tombstones: 0
  Built:      2024-01-15 10:30:00
```

### Rebuild an Index

After many insertions/deletions, an index may become fragmented. Rebuild to optimize:

```
.index rebuild doc_emb_idx
```

### Drop an Index

```
.index drop doc_emb_idx
```

---

## Using Indexes in Queries

Indexes are used automatically when you perform vector similarity searches. The query optimizer detects when an index can accelerate a query.

### Automatic Index Usage

```datalog
% Query vector (from your embedding model)
query_vec([0.11, 0.21, 0.29]).

% Find similar documents - index is used automatically
+similar(Id, Title, top_k<10, Dist>) :-
    query_vec(QV),
    documents(Id, Title, V),
    Dist = cosine(QV, V).

?- similar(Id, Title, Dist).
```

### Index Selection

If multiple indexes exist on the same column, the one matching the distance function is preferred:

```
.index create idx_cosine on docs(emb) metric cosine
.index create idx_l2 on docs(emb) metric l2
```

```datalog
% Uses idx_cosine (matches cosine distance function)
?- docs(Id, _, V), D = cosine([0.1, 0.2], V).

% Uses idx_l2 (matches euclidean distance function)
?- docs(Id, _, V), D = euclidean([0.1, 0.2], V).
```

---

## Index Lifecycle

### Build Phase

When you create an index, vectors are inserted incrementally:

1. Index is registered with metadata
2. Existing vectors are added to the HNSW structure
3. Index is marked as valid

### Invalidation

Indexes are automatically invalidated when:
- Base relation is modified (insert/delete)
- Schema changes

```
.index stats my_idx
  Valid: no  ← Index needs rebuild
```

### Rebuild

Invalid indexes are rebuilt on:
- Explicit `.index rebuild` command
- Next query that uses the index

---

## Index Architecture

### HNSW Structure

```
Layer 3:  ●─────────────●
          │             │
Layer 2:  ●───●─────●───●───●
          │   │     │   │   │
Layer 1:  ●─●─●─●─●─●─●─●─●─●─●
          │ │ │ │ │ │ │ │ │ │ │
Layer 0:  ●●●●●●●●●●●●●●●●●●●●● (all nodes)
```

Each layer has fewer nodes. Search starts at top layer and descends:
1. Find nearest nodes in current layer
2. Use those as entry points for next layer
3. Repeat until layer 0
4. Return k nearest neighbors

### Memory Usage

HNSW indexes use approximately:

```
memory ≈ n × (d × 4 + m × 8) bytes
```

Where:
- `n` = number of vectors
- `d` = vector dimension
- `m` = max connections parameter

**Example**: 1M vectors × 768 dimensions × m=16:
```
1M × (768 × 4 + 16 × 8) = ~3.2 GB
```

---

## Tombstones and Compaction

When vectors are deleted, they're marked with a tombstone rather than removed immediately:

```
.index stats my_idx
  Vectors:    10000
  Tombstones: 500  ← Deleted entries not yet cleaned up
```

### Automatic Compaction

When tombstone ratio exceeds 30%, the index is automatically rebuilt during the next query.

### Manual Compaction

Force a rebuild to remove tombstones:

```
.index rebuild my_idx
```

---

## Best Practices

### 1. Choose the Right Metric

| Embedding Type | Recommended Metric |
|---------------|-------------------|
| OpenAI embeddings | `cosine` |
| BERT/Sentence-BERT | `cosine` |
| Image embeddings (CLIP) | `cosine` |
| Raw feature vectors | `euclidean` |
| Pre-normalized vectors | `dot` (fastest) |

### 2. Tune Parameters for Your Use Case

**Discovery/Exploration** (higher recall matters):
```
.index create my_idx on docs(emb) m 32 ef_search 100
```

**Production/Speed** (latency matters):
```
.index create my_idx on docs(emb) m 16 ef_search 30
```

### 3. Monitor Index Health

Regularly check:
```
.index stats my_idx
```

Rebuild if:
- Tombstone ratio > 30%
- Search quality degrades
- After bulk insertions

### 4. Create Indexes Before Bulk Load

For large initial loads, create the index first:

```
.index create my_idx on docs(emb)

% Then bulk insert
+docs[(1, "...", [0.1, ...]),
      (2, "...", [0.2, ...]),
      ...].
```

### 5. Use Appropriate Vector Dimensions

Common dimensions:
- OpenAI text-embedding-3-small: 1536
- Cohere embed-english-v3: 1024
- all-MiniLM-L6-v2: 384
- CLIP ViT-B/32: 512

Higher dimensions = more memory, slower search.

---

## Troubleshooting

### Index Shows "Invalid"

**Cause**: Base relation was modified.

**Solution**:
```
.index rebuild my_idx
```

### Search Returns No Results

**Possible causes**:
1. Index not yet built
2. Query vector dimension mismatch
3. No vectors in relation

**Debug**:
```
.index stats my_idx
?- documents(Id, _, V).  % Check if data exists
```

### Poor Search Quality

**Causes**:
1. Wrong distance metric for embedding type
2. ef_search too low
3. Many tombstones

**Solutions**:
```
% Check metric matches embedding type
.index stats my_idx

% Increase ef_search
.index create my_idx on docs(emb) ef_search 100

% Rebuild to remove tombstones
.index rebuild my_idx
```

### High Memory Usage

**Solutions**:
1. Reduce `m` parameter (trades recall for memory)
2. Use vector quantization (see [Vectors Guide](vectors.md))
3. Consider approximate embeddings with lower dimensions

---

## Next Steps

- [Vector Search Tutorial](vectors.md) - Distance functions and semantic search
- [Aggregations Reference](../reference/aggregations.md) - TopK and WithinRadius
- [Configuration Guide](configuration.md) - Index persistence settings
