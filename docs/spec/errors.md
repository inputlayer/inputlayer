# Error Reference

This section documents common errors you may encounter when using InputLayer.

## Syntax Errors

### Invalid Statement

```
Error: Invalid statement: unexpected token
```

Check that your statement ends with `.` and follows correct syntax.

### Parse Error

```
Error: Parse error at line N
```

Common causes:
- Missing period at end of statement
- Unbalanced parentheses
- Invalid characters

## Relation Errors

### Undefined Relation

```
Error: Undefined relation 'foo'
```

The relation doesn't exist. Insert facts or check spelling:

```iql
+foo(1, 2)        // Creates relation
?foo(X, Y)      // Now works
```

### Arity Mismatch

```
Error: Arity mismatch for 'edge': expected 2, got 3
```

You provided the wrong number of columns:

```iql
+edge(1, 2)       // edge has 2 columns
+edge(1, 2, 3)    // ERROR: 3 values for 2-column relation
```

### Type Mismatch

```
Error: Type mismatch: expected int, got string
```

Value types don't match the schema:

```iql
+person(name: string, age: int)
+person(30, "alice")   // ERROR: reversed types
```

### Insert into View

```
Error: Cannot insert into view 'path'
```

You can't insert facts into a derived relation (view):

```iql
+path(X, Y) <- edge(X, Y)   // path is a view
+path(1, 2)                  // ERROR: can't insert into view
```

## Rule Errors

### Unsafe Variable

```
Error: Unsafe variable 'X' in rule head
```

All head variables must appear in a positive body literal:

```iql
// BAD: X not bound in body
+bad(X) <- edge(A, B)

// GOOD: X bound by edge
+good(X) <- edge(X, _)
```

### Unsafe Negation Variable

```
Error: Variable 'X' in negation not bound by positive literal
```

Variables in negations must also appear in positive literals:

```iql
// BAD: X only in negation
+bad(X) <- !excluded(X)

// GOOD: X bound first
+good(X) <- items(X), !excluded(X)
```

### Unstratifiable Negation

```
Error: Circular negation detected
```

A relation can't negatively depend on itself:

```iql
// BAD: a depends on !a
+a(X) <- b(X), !a(X)

// GOOD: negate a different relation
+a(X) <- b(X), !c(X)
```

## Arithmetic Errors

### Division by Zero

```
Error: Division by zero
```

Check your data for zero divisors:

```iql
// May error if Y = 0
+ratio(X, R) <- data(X, Y), R = X / Y

// Safe: filter out zeros
+ratio(X, R) <- data(X, Y), Y != 0, R = X / Y
```

### Overflow

```
Error: Integer overflow
```

Result exceeds 64-bit integer range.

## Knowledge Graph Errors

### Knowledge Graph Not Found

```
Error: Knowledge graph 'foo' not found
```

The knowledge graph doesn't exist:

```
.kg create foo    // Create it first
.kg use foo       // Then use it
```

### Cannot Drop Current

```
Error: Cannot drop current knowledge graph
```

Switch to a different knowledge graph first:

```
.kg use other     // Switch away
.kg drop target   // Now drop works
```

### Permission Denied

```
Error: Permission denied: you have viewer access to this knowledge graph
```

Code `access_denied`. The caller may not run the statement, and nothing ran. Every permission refusal carries this code, whatever refused it:

- The caller's role: a global `viewer` creating a knowledge graph, or a `viewer` of a knowledge graph writing to it
- A write grant: a `writer` or `decider` writing a relation it was not granted, or changing a rule or schema
- An admin-only command (`.compact`, `.backup`, `.user`, `.apikey`)
- An API key's scope: a key scoped to one knowledge graph used on another
- No access to the knowledge graph (`Access denied`), including the system knowledge graph
- A credential that was revoked or has expired

The code comes on whichever frame refuses: an `error` answering a request, the `auth_error` refusing a session on a knowledge graph, or the `subscription_error` pushed when a live subscription loses read access.

Ask an owner of the knowledge graph for the role or grant (`.kg acl grant`), or use a credential that has it.

## Aggregation Errors

### Invalid Aggregation Variable

```
Error: Aggregation variable 'X' not found in body
```

The aggregated variable must appear in the rule body:

```iql
// BAD: Z not in body
+bad(sum<Z>) <- data(X, Y)

// GOOD: X appears in body
+good(sum<X>) <- data(X, _)
```

## Connection Errors

### Server Unavailable

```
Error: Could not connect to server at http://127.0.0.1:8080
```

The InputLayer server isn't running. Start it with:

```bash
inputlayer-server
```

### Request Deadline Exceeded

```
Error: Request deadline exceeded before it began committing; nothing was applied
```

Code `deadline_exceeded`. The request's deadline (`storage.performance.query_timeout_ms`, or the request's `timeout_ms`) covers queueing and computing together, and passed before the request began committing, so nothing was applied. Try:
- Adding filters to reduce data
- Breaking into smaller queries
- Increasing the timeout in config, or the request's `timeout_ms`

A request cancelled by the client fails the same way with code `cancelled`. Code `outcome_unknown` means a commit failed in a way that may have applied it: read the state back before retrying.

### Memory Limit Exceeded

```
Error: Request exceeded the per-query memory limit (storage.performance.max_query_memory_bytes) before it began committing; nothing was applied. Narrow the query or bind more of its arguments
```

Code `resource_exhausted`. The query's computation held more than `storage.performance.max_query_memory_bytes` and was stopped instead of growing the server, so nothing was applied. A recursive rule queried with no argument bound (the whole transitive closure of a large graph) is the usual cause. Try:
- Binding an argument (`?reach("a", Y)` rather than `?reach(X, Y)`)
- Adding filters to reduce data
- Raising the limit in config, if the server has the memory for it

A query also fails with the same code when it grows while the queries running on the server together hold `storage.performance.max_total_query_memory_bytes`, the budget that keeps concurrent queries inside the server's memory:

```
Error: Request stopped: the queries running on the server hold their whole memory budget (storage.performance.max_total_query_memory_bytes); it was stopped before it began committing and nothing was applied. Retry later
```

Nothing was applied, and the query may succeed once the others finish: retry it later.

A write fails with the same code when it would grow its knowledge graph's facts past `storage.performance.max_graph_memory_bytes`. Nothing is applied; deleting facts is always allowed, so free space and retry.

### Not Confirmed on a Replica

```
Error: committed on the primary, but no replica confirmed it within 5000 ms: it is applied here and could be lost only with this server. Do not retry it as a failed write.
```

Code `replica_unconfirmed`. The primary ships synchronously (`replication.mode = "sync"` with `on_follower_loss = "block"`) and no follower confirmed the write within `replication.sync_timeout_ms`. The write **is** committed on the primary: reads there see it, and a follower receives it once one catches up. Until then it is not protected against losing the primary. Do not resend it as if it had failed. Check the follower (`GET /v1/replication/status`); see [Replication](../guides/replication#synchronous-shipping).

### Query Too Complex

```
Error: Query execution failed: Query too complex: it joins 30000 rows with 30000 rows on no shared variable, a cross product of about 900000000 rows, over the limit of 100000000 (storage.performance.max_query_cost). Join them on a shared variable, order the rule body so each atom shares a variable with one before it, or filter a side with a constant or comparison so fewer of its rows meet
```

Code `validation`. Before a query runs, each join of its plan is estimated from the sizes of the relations it reads. Atoms that share no variable (`?a(X), b(Y)`) pair every row of one with every row of the other, a cross product; one estimated past `storage.performance.max_query_cost`, even counting only the rows its filters keep of stored relations, is refused before it runs, so nothing was applied. Try:
- Joining the atoms on a shared variable
- Ordering the rule body so each atom shares a variable with one before it: the planner reorders joins in most rules, but not in every rule (rules with negation keep their order)
- Filtering each side with a constant or comparison (`?a(X, 7), b(Y)`) so fewer of its rows meet, or raising the limit in config

### Recursion Did Not Reach a Fixpoint

```
Error: Query execution failed: Recursion did not reach a fixpoint within 100000 iterations (storage.performance.max_recursion_iterations): a recursive rule keeps deriving new facts, such as a counter without an upper bound. Bound the recursion, for example with N < 1000
```

Code `validation`. Each iteration of a recursive rule extends its paths by one step. A rule that keeps deriving new values (`nat(N) <- nat(M), N = M + 1`) never reaches a fixpoint. When the optional `storage.performance.max_recursion_iterations` is set (it is off by default, leaving the deadline and memory limits to stop such a rule), it is stopped after that many iterations whatever the deadline. Try:
- Bounding the derived value (`N < 1000`)
- Raising the limit, if a path really is longer than that many steps
