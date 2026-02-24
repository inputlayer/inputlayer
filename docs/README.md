# InputLayer Documentation

Welcome to the InputLayer documentation. InputLayer is an incremental Datalog database engine built on Differential Dataflow.

## Documentation Sections

### [Guides](guides/)
**Start here if you're new to InputLayer.**

Progressive tutorials from installation to advanced features:
- [Quick Start](guides/quickstart.md) - Get running in 5 minutes
- [Installation](guides/installation.md) - Detailed setup instructions
- [First Program](guides/first-program.md) - Your first Datalog queries
- [Core Concepts](guides/core-concepts.md) - Facts, rules, and data modeling
- [REPL Guide](guides/repl.md) - Interactive usage
- [Recursion](guides/recursion.md) - Recursive queries and transitive closure
- [Python SDK](guides/python-sdk.md) - Python OLM client (no Datalog required)
- [Troubleshooting](guides/troubleshooting.md) - Common errors and solutions

### [Reference](reference/)
**Quick lookup for commands, functions, and syntax.**

Redis-style reference documentation:
- [Commands](reference/commands.md) - All meta commands (`.kg`, `.rule`, `.load`, etc.)
- [Functions](reference/functions.md) - All 55 builtin functions
- [Syntax Cheatsheet](reference/syntax-cheatsheet.md) - One-page syntax reference

### [Specification](spec/)
**Authoritative language specification.**

Complete InputLayer Datalog specification:
- [Syntax](spec/syntax.md) - Complete grammar and EBNF
- [Types](spec/types.md) - Type system (9 value types)
- [Rules](spec/rules.md) - Persistent and session rules
- [Queries](spec/queries.md) - Query syntax and semantics
- [Errors](spec/errors.md) - Error code reference

### [Internals](internals/)
**For contributors and developers.**

Architecture and implementation details:
- [Architecture](internals/architecture.md) - System design overview
- [Coding Standards](internals/coding-standards.md) - Code style and patterns
- [Type System](internals/type-system.md) - Value types and coercion
- [Validation](internals/validation.md) - Validation layer design
- [Roadmap](internals/roadmap.md) - Feature roadmap

---

## Quick Links

| Task | Go to |
|------|-------|
| Install InputLayer | [Installation Guide](guides/installation.md) |
| Use the Python SDK | [Python SDK Guide](guides/python-sdk.md) |
| Learn the basics | [First Program](guides/first-program.md) |
| Look up a function | [Function Reference](reference/functions.md) |
| Find a command | [Commands Reference](reference/commands.md) |
| Understand the architecture | [Architecture](internals/architecture.md) |
| Report a bug | [GitHub Issues](https://github.com/inputlayer/inputlayer/issues) |

---

## Test Coverage

InputLayer is thoroughly tested:
- **1435 unit tests** across all modules
- **1107 snapshot tests** for end-to-end validation
- All tests passing continuously

---

## Version

**Current**: v0.1.0 (Production-Ready)

See [Roadmap](internals/roadmap.md) for version history and planned features.
