# Vector Search Tutorial

Store embeddings alongside structured data. Query by cosine, euclidean, or dot-product similarity - no external vector DB needed.

## Vector Basics

### Storing Vectors

Vectors are arrays of floating-point numbers, typically embeddings from ML models:

```datalog
// Store document embeddings
+document(1, "Introduction to Datalog", [0.1, 0.2, 0.3, 0.4])
+document(2, "Vector Similarity", [0.15, 0.25, 0.28, 0.42])
+document(3, "Graph Databases", [0.8, 0.1, 0.05, 0.05])
```

### Schema with Vectors

Declare vector columns in your schema:

```datalog
+document(id: int, title: string, embedding: vector)
```

---

## Distance Functions

InputLayer supports 4 distance metrics:

### Euclidean Distance

L2 distance - straight-line distance in vector space:

```datalog
?document(Id1, _, V1), document(Id2, _, V2),
   Id1 < Id2,
   Dist = euclidean(V1, V2)
```

### Cosine Distance

Angular distance (1 - cosine similarity) - ignores magnitude:

```datalog
?document(Id1, _, V1), document(Id2, _, V2),
   Id1 < Id2,
   Dist = cosine(V1, V2)
```

**Use cosine for:** Text embeddings, normalized vectors

### Dot Product

Inner product - higher is more similar:

```datalog
?document(Id1, _, V1), document(Id2, _, V2),
   Id1 < Id2,
   Score = dot(V1, V2)
```

**Use dot product for:** When vectors have meaningful magnitude

### Manhattan Distance

L1 distance - sum of absolute differences:

```datalog
?document(Id1, _, V1), document(Id2, _, V2),
   Id1 < Id2,
   Dist = manhattan(V1, V2)
```

---

## Semantic Search Example

Build a document search system:

```datalog
// Store documents with embeddings
+docs[(1, "Introduction to Datalog", [0.1, 0.2, 0.3]),
      (2, "Vector Databases", [0.12, 0.22, 0.31]),
      (3, "Graph Theory", [0.8, 0.1, 0.05]),
      (4, "Machine Learning", [0.15, 0.18, 0.28])]

// Query vector (from your embedding model)
query_vec([0.11, 0.21, 0.29])

// Find similar documents
+similar(Id, Title, top_k<3, Dist>) <-
    query_vec(QV),
    docs(Id, Title, V),
    Dist = cosine(QV, V)

?similar(Id, Title, Dist)
```

**Output:**
```
┌────┬─────────────────────────┬───────┐
│ Id │ Title                   │ Dist  │
├────┼─────────────────────────┼───────┤
│ 1  │ Introduction to Datalog │ 0.002 │
│ 2  │ Vector Databases        │ 0.003 │
│ 4  │ Machine Learning        │ 0.015 │
└────┴─────────────────────────┴───────┘
```

---

## Vector Operations

### Normalize

Convert to unit vector (length 1):

```datalog
?document(Id, _, V),
   NormV = normalize(V)
```

### Get Dimension

Get the number of elements:

```datalog
?document(Id, _, V),
   Dim = vec_dim(V)
```

### Add Vectors

Element-wise addition:

```datalog
?v1([1.0, 2.0, 3.0]), v2([0.5, 0.5, 0.5]),
   Sum = vec_add(V1, V2)  // [1.5, 2.5, 3.5]
```

### Scale Vector

Multiply by scalar:

```datalog
?v([1.0, 2.0, 3.0]),
   Scaled = vec_scale(V, 2.0)  // [2.0, 4.0, 6.0]
```

---

## LSH (Locality Sensitive Hashing)

For approximate nearest neighbor search on large datasets:

### Basic LSH Bucket

Hash vectors into buckets:

```datalog
?document(Id, _, V),
   Bucket = lsh_bucket(V, 0, 8)  // table 0, 8 hyperplanes
```

### LSH Probes

Get multiple candidate buckets to check:

```datalog
?query_vec(QV),
   Buckets = lsh_probes(QV, 0, 8, 3)  // 3 probe levels
```

### LSH Multi-Probe Search

Full multi-probe search:

```datalog
+candidates(Id, Dist) <-
    query_vec(QV),
    Probes = lsh_multi_probe(QV, 0, 8, 3),
    member(Bucket, Probes),
    document(Id, _, V),
    lsh_bucket(V, 0, 8) = Bucket,
    Dist = cosine(QV, V)
```

---

## Int8 Quantization

Reduce memory by 75% using 8-bit quantization:

### Quantize Vectors

```datalog
// Linear quantization (uniform distribution)
+quantized(Id, quantize_linear(V)) <- document(Id, _, V)

// Symmetric quantization (centered at 0)
+quantized(Id, quantize_symmetric(V)) <- document(Id, _, V)
```

### Dequantize

Convert back to float:

```datalog
?quantized(Id, QV),
   V = dequantize(QV)
```

### Int8 Distance Functions

Direct computation on quantized vectors:

```datalog
?quantized(Id1, QV1), quantized(Id2, QV2),
   Dist = euclidean_int8(QV1, QV2)

// Also available: cosine_int8, dot_int8, manhattan_int8
```

---

## Building a Recommendation System

Complete example for item recommendations:

```datalog
// Item embeddings (from your ML model)
+items[(1, "Blue T-Shirt", [0.2, 0.8, 0.1, 0.3]),
       (2, "Red Dress", [0.25, 0.75, 0.15, 0.35]),
       (3, "Running Shoes", [0.9, 0.1, 0.8, 0.2]),
       (4, "Hiking Boots", [0.85, 0.15, 0.75, 0.25]),
       (5, "Formal Shoes", [0.4, 0.6, 0.3, 0.5])]

// User purchase history (for creating user profile)
+purchases[(101, 1), (101, 2),    // User 101 bought shirts & dresses
           (102, 3), (102, 4)]   // User 102 bought athletic footwear

// Compute user profile as average of purchased item embeddings
+user_profile(UserId, avg<V>) <-
    purchases(UserId, ItemId),
    items(ItemId, _, V)

// Recommend items similar to user profile, excluding already purchased
+recommendations(UserId, ItemId, Name, top_k<3, Dist>) <-
    user_profile(UserId, Profile),
    items(ItemId, Name, V),
    !purchases(UserId, ItemId),  // Exclude already purchased
    Dist = cosine(Profile, V)

// Get recommendations for user 101
?recommendations(101, ItemId, Name, Dist)
```

---

## Performance Tips

### 1. Use Appropriate Distance Metric

| Embedding Type | Recommended Metric |
|---------------|-------------------|
| Text (BERT, etc.) | `cosine` |
| Images | `euclidean` or `cosine` |
| Normalized | `dot` (fastest) |

### 2. Create HNSW Indexes

For large datasets (>10K vectors):

```
.index create doc_idx on documents(embedding) metric cosine m 16 ef_search 50
```

### 3. Use Quantization for Memory

For millions of vectors, quantize to Int8:

```datalog
+docs_quantized(Id, Title, quantize_symmetric(V)) <- docs(Id, Title, V)
```

### 4. Filter Before Distance Computation

```datalog
// Filter first, then compute distances
+similar(Id, Title, Dist) <-
    query_vec(QV),
    docs(Id, Title, V, Category),
    Category = "technology",     // Filter first
    Dist = cosine(QV, V)        // Then compute distance
```

---

## Next Steps

- [Indexing Guide](indexing.md) - Create HNSW indexes for fast search
- [Aggregations](../reference/aggregations.md) - TopK and WithinRadius for ranking
- [Temporal Functions](temporal.md) - Add time-decay to recommendations
