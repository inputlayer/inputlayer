# InputLayer User Guides

Guides from basics to advanced features.

## Learning Path

### Getting Started (15-30 minutes)

| Guide | Description | Time |
|-------|-------------|------|
| [Quick Start](quickstart.md) | Get InputLayer running and execute your first query | 5 min |
| [Installation](installation.md) | Detailed installation options | 10 min |
| [First Program](first-program.md) | Your first InputLayer program with facts and rules | 15 min |

### Core Concepts (30-60 minutes)

| Guide | Description | Time |
|-------|-------------|------|
| [Core Concepts](core-concepts.md) | Data modeling, identity semantics, update patterns | 20 min |
| [REPL Guide](repl.md) | Interactive usage with the REPL | 15 min |

### Client SDKs

| Guide | Description | Time |
|-------|-------------|------|
| [Python SDK](python-sdk.md) | Python OLM - define schemas, queries, rules, and migrations in pure Python | 30 min |

### Advanced Features (60+ minutes)

| Guide | Description | Time |
|-------|-------------|------|
| [Recursion](recursion.md) | Recursive rules and transitive closure | 20 min |
| [Troubleshooting](troubleshooting.md) | Common errors and solutions | Reference |

---

## Quick Examples

### Hello World
```datalog
// Add some facts
+person("alice")
+person("bob")

// Query all persons
?person(X)
```

### Simple Rule
```datalog
// Facts
+parent("alice", "bob")
+parent("bob", "charlie")

// Rule: grandparent relationship
+grandparent(X, Z) <- parent(X, Y), parent(Y, Z)

// Query
?grandparent(X, Y)
```

### Aggregation
```datalog
+score("alice", 95)
+score("bob", 87)
+score("charlie", 92)

// Average score
?avg<S> <- score(_, S)
```

---

## What's Next?

After completing the guides:

1. **[Reference](../reference/)** - Quick lookup for commands and functions
2. **[Specification](../spec/)** - Authoritative language specification
3. **[Internals](../internals/)** - For contributors

---

## Need Help?

- Check [Troubleshooting](troubleshooting.md) for common issues
- See [Commands Reference](../reference/commands.md) for `.help` command
- Report issues at [GitHub](https://github.com/inputlayer/inputlayer/issues)
