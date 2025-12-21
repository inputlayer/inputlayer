# Your First Program

This tutorial walks you through your first InputLayer program step by step.

## What We'll Build

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

## Step 2: Create a Database

Every InputLayer program runs in a database. Let's create one:

```datalog
.db create social
```

Output:
```
Database 'social' created.
Switched to database: social
```

You can verify with:
```datalog
.db
```

Output:
```
Current database: social
```

## Step 3: Add Facts

Facts are the base data in your database. Let's add some "follows" relationships:

```datalog
+follows(1, 2).
```

Output:
```
Inserted 1 fact into 'follows'.
```

Let's add more facts using bulk insert:

```datalog
+follows[(2, 3), (3, 4), (1, 3)].
```

Output:
```
Inserted 3 facts into 'follows'.
```

## Step 4: Query Facts

Use `?-` to query data:

```datalog
?- follows(1, X).
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

```datalog
+reachable(X, Y) :- follows(X, Y).
```

Output:
```
Rule 'reachable' registered.
```

This says: "X can reach Y if X follows Y directly."

But we also want transitive reachability. Add another clause:

```datalog
+reachable(X, Z) :- follows(X, Y), reachable(Y, Z).
```

Output:
```
Rule added to 'reachable' (2 rules total).
```

This says: "X can reach Z if X follows someone Y who can reach Z."

## Step 6: Query the Rule

Now query the derived relation:

```datalog
?- reachable(1, X).
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

```datalog
.rule
```

Output:
```
Rules:
  reachable
```

See the rule definition:

```datalog
.rule def reachable
```

Output:
```
Rule: reachable
Clauses:
  1. reachable(X, Y) :- follows(X, Y).
  2. reachable(X, Z) :- follows(X, Y), reachable(Y, Z).
```

## Step 8: Add More Data and See Incremental Updates

Add a new follows relationship:

```datalog
+follows(4, 5).
```

Now query reachable again:

```datalog
?- reachable(1, X).
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

```datalog
// Create and use database
.db create social
.db use social

// Add base facts
+follows[(1, 2), (2, 3), (3, 4), (1, 3)].

// Define transitive reachability
+reachable(X, Y) :- follows(X, Y).
+reachable(X, Z) :- follows(X, Y), reachable(Y, Z).

// Query: who can user 1 reach?
?- reachable(1, X).
```

You can save this to a file `social.dl` and run it:

```bash
inputlayer-client < social.dl
```

Or use the `.load` command:
```datalog
.load social.dl
```

## Key Takeaways

1. **Facts** (`+relation(...)`) are base data you insert
2. **Rules** (`+head(...) :- body.`) derive new data from existing data
3. **Queries** (`?- pattern.`) ask questions about your data
4. **Incremental** - When you add/remove facts, derived data updates automatically
5. **Persistent** - Facts and rules are saved to disk

## What's Different from SQL?

| SQL | InputLayer (Datalog) |
|-----|---------------------|
| `INSERT INTO follows VALUES (1, 2)` | `+follows(1, 2).` |
| `CREATE VIEW` (limited recursion) | `+rule(...) :- body.` (full recursion) |
| `SELECT * FROM follows WHERE a = 1` | `?- follows(1, X).` |
| Explicit JOINs | Implicit joins via shared variables |

## Next Steps

- **[Core Concepts](03-core-concepts.md)** - Deeper understanding of facts, rules, and queries
- **[REPL Guide](04-repl-guide.md)** - All the commands available
- **[Basic Queries Tutorial](../tutorials/01-basic-queries.md)** - More query patterns
