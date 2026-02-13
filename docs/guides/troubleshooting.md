# Troubleshooting: Common Errors

This guide covers common errors and how to resolve them.

## Parse Errors

### "Expected '.'" or "Unexpected token"

**Cause**: Missing period at end of statement.

```datalog
// Wrong
+edge(1, 2)

// Correct
+edge(1, 2)
```

### "Invalid relation name"

**Cause**: Relation names must start with lowercase.

```datalog
// Wrong
+Edge(1, 2)

// Correct
+edge(1, 2)
```

### "Expected ':-'"

**Cause**: Rule syntax error.

```datalog
// Wrong - missing :-
+path(X, Y) edge(X, Y)

// Correct
+path(X, Y) <- edge(X, Y)
```

### "Unclosed parenthesis"

**Cause**: Mismatched parentheses.

```datalog
// Wrong
+edge((1, 2)

// Correct
+edge(1, 2)
```

## Type Errors

### "Type mismatch"

**Cause**: Value doesn't match schema.

```datalog
// If schema is: +person(id: int, name: string)

// Wrong
+person("alice", 1)

// Correct
+person(1, "alice")
```

### "Expected int, got float"

**Cause**: Using 1.0 where 1 is expected.

```datalog
// Wrong (if relation expects int)
+score(1.0)

// Correct
+score(1)
```

## Query Errors

### "Unknown relation"

**Cause**: Querying a relation that doesn't exist.

```datalog
?nonexistent(X)
// Error: Unknown relation 'nonexistent'
```

**Solutions**:
1. Check spelling
2. Use `.rel` to list existing relations
3. Create the relation first with `+relation(...)`

### "Unbound variable in head"

**Cause**: Variable in rule head not used in body.

```datalog
// Wrong - Z is not in the body
+path(X, Z) <- edge(X, Y)

// Correct
+path(X, Y) <- edge(X, Y)
```

### "Unsafe variable"

**Cause**: Variable only appears in negation or constraint.

```datalog
// Wrong - X only appears in negation
+orphan(X) <- !parent(_, X)

// Correct - X must appear positively
+orphan(X) <- person(X), !parent(_, X)
```

## Recursion Errors

### "Non-stratifiable program"

**Cause**: Negation through recursion.

```datalog
// Wrong - circular negation
+a(X) <- b(X)
+b(X) <- !a(X)  // Error: a depends on not-a
```

**Solution**: Restructure to avoid negation in recursive cycles.

### "Recursion timeout"

**Cause**: Infinite recursion or very deep recursion.

```datalog
// Potential issue - unbounded generation
+nums(0)
+nums(N) <- nums(M), N = M + 1  // Never terminates!
```

**Solution**: Add termination conditions.

```datalog
+nums(0)
+nums(N) <- nums(M), N = M + 1, N < 100  // Bounded
```

## Knowledge Graph Errors

### "Knowledge graph not found"

**Cause**: Trying to use a non-existent knowledge graph.

```datalog
.kg use nonexistent
// Error: Knowledge graph 'nonexistent' not found
```

**Solution**: Create it first.

```datalog
.kg create mykg
.kg use mykg
```

### "Cannot drop current knowledge graph"

**Cause**: Trying to drop the knowledge graph you're in.

**Solution**: Switch to another knowledge graph first.

```datalog
.kg use default
.kg drop mykg
```

### "No current knowledge graph"

**Cause**: Operating without selecting a knowledge graph.

**Solution**: Use `.kg use <name>` first.

## Rule Errors

### "Rule not found"

**Cause**: Trying to query/drop a rule that doesn't exist.

```datalog
.rule drop nonexistent
// Error: Rule 'nonexistent' not found
```

**Solution**: Check `.rule` to list existing rules.

### "Duplicate rule clause"

**Cause**: Adding the exact same rule twice.

**Note**: This is usually a warning, not an error. The duplicate is ignored.

## Aggregation Errors

### "Aggregation variable must be grouped"

**Cause**: Using a variable in the head without aggregating or grouping.

```datalog
// Wrong - Name appears but isn't grouped
+total(Name, sum<Amount>) <- purchase(_, Amount)

// Correct - Name is in the body
+total(Name, sum<Amount>) <- purchase(Name, Amount)
```

### "Cannot aggregate over empty set"

**Cause**: Aggregating with no matching facts.

**Note**: This returns empty results, not an error. Check your filters.

## Storage Errors

### "Permission denied"

**Cause**: Cannot write to data directory.

**Solution**:
```bash
mkdir -p ~/.inputlayer/data
chmod 755 ~/.inputlayer/data
```

### "Disk full"

**Cause**: No space for data files.

**Solutions**:
1. Free up disk space
2. Run `.compact` to consolidate storage
3. Move data directory to larger disk

### "WAL corruption"

**Cause**: Crash during write operation.

**Solution**: InputLayer should recover automatically. If not:
1. Check for `.wal` files in data directory
2. Remove corrupt WAL files (will lose uncommitted data)
3. Restart

## Performance Issues

### "Query taking too long"

**Possible causes**:
1. Very large datasets
2. Cartesian product (joining without shared variables)
3. Deep recursion

**Solutions**:

1. **Add constraints early**:
   ```datalog
   // Slow - filters after join
   ?huge_table1(X, Y), huge_table2(Y, Z), X < 10

   // Fast - filter first
   ?huge_table1(X, Y), X < 10, huge_table2(Y, Z)
   ```

2. **Check for Cartesian products**:
   ```datalog
   // Bad - no shared variables = cross product
   ?table1(X), table2(Y)

   // Good - joined on Y
   ?table1(X, Y), table2(Y, Z)
   ```

3. **Limit results**:
   ```datalog
   // For exploration, check small sample first
   ?huge_relation(X, Y), X < 10
   ```

### "High memory usage"

**Solutions**:
1. Run `.compact` to consolidate storage
2. Reduce intermediate result sizes with filters
3. Break large operations into smaller chunks

## Getting More Help

### Debug Mode

Set environment variable for verbose output:
```bash
RUST_LOG=debug inputlayer-client
```

### Check System Status

```datalog
.status
```

Shows memory usage, database info, and other diagnostics.

### File Locations

Find data files:
```bash
ls -la ~/.inputlayer/data/
```

Check logs (if configured):
```bash
tail -f ~/.inputlayer/logs/inputlayer.log
```

## Error Message Quick Reference

| Error | Likely Cause | Solution |
|-------|--------------|----------|
| "Expected '.'" | Missing period | Add `.` at end |
| "Unknown relation" | Typo or missing data | Check spelling, `.rel` |
| "Type mismatch" | Wrong value type | Check schema |
| "Unbound variable" | Variable not in body | Add to body predicate |
| "Non-stratifiable" | Negation cycle | Restructure rules |
| "Knowledge graph not found" | Wrong name | `.kg list` to check |
| "Permission denied" | File permissions | Check data dir |

## Still Stuck?

1. Check the [Cheatsheet](../reference/syntax-cheatsheet.md) for correct syntax
2. Look at [Examples](../../examples/datalog/README.md) for working patterns
3. Report issues at: https://github.com/inputlayer/inputlayer/issues
