# Quick Start

Get InputLayer running and execute your first query in 5 minutes.

## 1. Install

```bash
# Clone the repository
git clone https://github.com/inputlayer/inputlayer.git
cd inputlayer

# Build (requires Rust 1.75+)
cargo build --release
```

## 2. Start the REPL

```bash
./target/release/inputlayer
```

You should see:
```
InputLayer v0.1.0
Type .help for commands, .quit to exit.
>
```

## 3. Add Some Data

```datalog
> +person("alice", 30).
OK

> +person("bob", 25).
OK

> +person("charlie", 35).
OK
```

## 4. Query the Data

```datalog
> ?- person(Name, Age).
┌─────────┬─────┐
│ Name    │ Age │
├─────────┼─────┤
│ alice   │ 30  │
│ bob     │ 25  │
│ charlie │ 35  │
└─────────┴─────┘
3 rows
```

## 5. Add a Filter

```datalog
> ?- person(Name, Age), Age > 28.
┌─────────┬─────┐
│ Name    │ Age │
├─────────┼─────┤
│ alice   │ 30  │
│ charlie │ 35  │
└─────────┴─────┘
2 rows
```

## 6. Create a Rule

```datalog
> +senior(Name) :- person(Name, Age), Age >= 30.
OK

> ?- senior(X).
┌─────────┐
│ X       │
├─────────┤
│ alice   │
│ charlie │
└─────────┘
2 rows
```

## 7. Use Aggregation

```datalog
> ?- avg<Age> :- person(_, Age).
┌─────┐
│ avg │
├─────┤
│ 30  │
└─────┘
1 row
```

## What's Next?

- [First Program](first-program.md) - Learn the basics in depth
- [Core Concepts](core-concepts.md) - Understand data modeling
- [REPL Guide](repl.md) - Master the interactive environment
- [Commands Reference](../reference/commands.md) - All available commands

## Common Commands

| Command | Description |
|---------|-------------|
| `.help` | Show all commands |
| `.rel` | List all relations |
| `.rule` | List all rules |
| `.quit` | Exit the REPL |
