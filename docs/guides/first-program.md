# Your First Program

Write a social-network reachability query in under 5 minutes.

## What You'll Build

A simple social network that tracks who follows whom, and computes who can reach whom through any chain of follows.

## Step 1: Start the REPL

```bash
inputlayer-client
```

You'll see the InputLayer prompt:
```
InputLayer v0.1.0
Type .help for commands, .quit to exit

inputlayer>
```

## Step 2: Create a Knowledge Graph

Every InputLayer program runs in a knowledge graph. Let's create one:

```iql
.kg create social
```

Output:
```
Knowledge graph 'social' created
Switched to knowledge graph: social
```

You can verify with:
```iql
.kg
```

Output:
```
Current knowledge graph: social
```

## Step 3: Add Facts

Facts are the base data in your knowledge graph. Let's add some "follows" relationships:

```iql
+follows(1, 2)
```

Output:
```
Inserted 1 fact into 'follows'
```

Let's add more facts using bulk insert:

```iql
+follows[(2, 3), (3, 4), (1, 3)]
```

Output:
```
Inserted 3 facts into 'follows'
```

## Step 4: Query Facts

Use `?` to query data:

```iql
?follows(1, X)
```

Output:
```
Results: 2 rows
  (1, 2)
  (1, 3)
```

This shows everyone that user 1 directly follows.

## Step 5: Define a Rule

Rules derive new data from existing facts. Let's define "reachable" - who can you reach through any chain of follows?

```iql
+reachable(X, Y) <- follows(X, Y)
```

Output:
```
Rule 'reachable' registered
```

This says: "X can reach Y if X follows Y directly."

But we also want transitive reachability. Add another clause:

```iql
+reachable(X, Z) <- follows(X, Y), reachable(Y, Z)
```

Output:
```
Rule added to 'reachable' (2 rules total)
```

This says: "X can reach Z if X follows someone Y who can reach Z."

## Step 6: Query the Rule

Now query the derived relation:

```iql
?reachable(1, X)
```

Output:
```
Results: 3 rows
  (1, 2)
  (1, 3)
  (1, 4)
```

User 1 can reach users 2, 3, and 4! Even though user 1 doesn't directly follow user 4, they can reach them through the chain: 1 → 2 → 3 → 4.

## Step 7: View Your Rules

See what rules are defined:

```iql
.rule
```

Output:
```
Rules:
  reachable
```

See the rule definition:

```iql
.rule def reachable
```

Output:
```
Rule: reachable
Clauses:
  1. reachable(X, Y) <- follows(X, Y)
  2. reachable(X, Z) <- follows(X, Y), reachable(Y, Z)
```

## Step 8: Add More Data and See Incremental Updates

Add a new follows relationship:

```iql
+follows(4, 5)
```

Now query reachable again:

```iql
?reachable(1, X)
```

Output:
```
Results: 4 rows
  (1, 2)
  (1, 3)
  (1, 4)
  (1, 5)
```

User 1 can now reach user 5! InputLayer automatically recomputed the derived relation when you added new data.

## Complete Program

Here's everything we did in one script:

```iql
// Create and use knowledge graph
.kg create social
.kg use social

// Add base facts
+follows[(1, 2), (2, 3), (3, 4), (1, 3)]

// Define transitive reachability
+reachable(X, Y) <- follows(X, Y)
+reachable(X, Z) <- follows(X, Y), reachable(Y, Z)

// Query: who can user 1 reach?
?reachable(1, X)
```

You can save this to a file `social.iql` and run it:

```bash
inputlayer-client < social.iql
```

Or use the `.load` command:
```iql
.load social.iql
```

## Key Takeaways

1. **Facts** (`+relation(...)`) are base data you insert
2. **Rules** (`+head(...) <- body`) derive new data from existing data
3. **Queries** (`? pattern`) ask questions about your data
4. **Incremental** - When you add/remove facts, derived data updates automatically
5. **Persistent** - Facts and rules are saved to disk

## What's Different from SQL?

| SQL | InputLayer (IQL) |
|-----|---------------------|
| `INSERT INTO follows VALUES (1, 2)` | `+follows(1, 2)` |
| `CREATE VIEW` (limited recursion) | `+rule(...) <- body` (full recursion) |
| `SELECT * FROM follows WHERE a = 1` | `? follows(1, X)` |
| Explicit JOINs | Implicit joins via shared variables |

## Next Steps

- **[Core Concepts](core-concepts.md)** - Deeper understanding of facts, rules, and queries
- **[REPL Guide](repl.md)** - All the commands available
- **[Recursion](recursion.md)** - Learn about recursive queries and graph traversal
