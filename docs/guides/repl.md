# REPL Guide

The REPL is where you define facts, rules, and queries interactively.

## Starting the REPL

```bash
inputlayer-client
```

You'll see:
```
InputLayer v0.1.0
Type .help for commands, .quit to exit

inputlayer>
```

## Command Categories

### Knowledge Graph Commands (`.kg`)

Manage your knowledge graphs:

| Command | Description |
|---------|-------------|
| `.kg` | Show current knowledge graph |
| `.kg list` | List all knowledge graphs |
| `.kg create <name>` | Create a new knowledge graph |
| `.kg use <name>` | Switch to a knowledge graph |
| `.kg drop <name>` | Delete a knowledge graph (cannot drop current) |

**Examples:**
```datalog
.kg create myproject
.kg use myproject
.kg list
.kg
```

### Relation Commands (`.rel`)

Inspect base facts:

| Command | Description |
|---------|-------------|
| `.rel` | List all relations with data |
| `.rel <name>` | Show schema and sample data for a relation |

**Examples:**
```datalog
.rel
.rel edge
.rel employee
```

### Rule Commands (`.rule`)

Manage persistent rules:

| Command | Description |
|---------|-------------|
| `.rule` | List all defined rules |
| `.rule <name>` | Query a rule (show computed results) |
| `.rule def <name>` | Show the rule definition (clauses) |
| `.rule drop <name>` | Delete a rule |
| `.rule clear <name>` | Clear clauses for re-registration |
| `.rule edit <name> <n> <clause>` | Replace clause #n |

**Examples:**
```datalog
.rule                           // List all rules
.rule path                      // Query the 'path' rule
.rule def path                  // Show path's definition
.rule drop path                 // Delete the path rule
.rule clear path                // Clear for re-definition
.rule edit path 1 +path(X,Y) <- edge(X,Y)  // Edit clause 1
```

### Session Commands (`.session`)

Manage transient session rules:

| Command | Description |
|---------|-------------|
| `.session` | List session rules |
| `.session clear` | Clear all session rules |
| `.session drop <n>` | Remove session rule #n |

**Examples:**
```datalog
temp(X) <- edge(1, X)    // Add session rule
.session                   // List session rules
.session drop 1            // Remove first rule
.session clear             // Clear all
```

### File Commands (`.load`)

Load and execute InputLayer files:

| Command | Description |
|---------|-------------|
| `.load <file>` | Execute a .idl file |
| `.load <file> --replace` | Replace existing rules |
| `.load <file> --merge` | Merge with existing rules |

**Examples:**
```datalog
.load schema.idl
.load rules.idl --replace
.load additional_data.idl --merge
```

### System Commands

| Command | Description |
|---------|-------------|
| `.status` | Show system status |
| `.compact` | Compact WAL and consolidate storage |
| `.help` | Show help message |
| `.quit` or `.exit` | Exit the REPL |

## Statement Types

### Insert Facts (`+`)

```datalog
// Single fact
+edge(1, 2)

// Bulk insert
+edge[(1, 2), (2, 3), (3, 4)]

// With different types
+person("alice", 30, "engineering")
```

### Delete Facts (`-`)

```datalog
// Single fact
-edge(1, 2)

// Conditional delete (must reference relation in body)
-edge(X, Y) <- edge(X, Y), X > 10

// Delete all from a relation
-edge(X, Y) <- edge(X, Y)
```

### Updates (Delete then Insert)

```datalog
// First delete old value
-counter(1, 0)
// Then insert new value
+counter(1, 5)
```

### Persistent Rules (`+head <- body`)

```datalog
// Simple rule
+adult(Name, Age) <- person(Name, Age), Age >= 18

// Recursive rule
+path(X, Y) <- edge(X, Y)
+path(X, Z) <- path(X, Y), edge(Y, Z)

// With aggregation
+dept_count(Dept, count<Id>) <- employee(Id, Dept)
```

### Session Rules (`head <- body`)

```datalog
// Transient rule (no + prefix)
temp_result(X, Y) <- edge(X, Y), X < Y
```

### Queries (`?`)

```datalog
// Simple query
?edge(1, X)

// With constraints
?person(Name, Age), Age > 25

// Query derived data
?path(1, X)
```

### Schema Declarations

```datalog
// Typed schema
+employee(id: int, name: string, dept: string)
+user(id: int, name: string, email: string)
```

## Tips and Tricks

### Multi-line Statements

Statements can span multiple lines. They're executed when you type the final `.`:

```datalog
+complex_rule(X, Y, Z) <-
  first_condition(X, A),
  second_condition(A, Y),
  third_condition(Y, Z),
  X < Y,
  Y < Z
```

### Comments

```datalog
// Single line comment (Prolog style - preferred)
+edge(1, 2)  // Inline comment

/*
   Multi-line
   block comment
*/
```

### Viewing Results

Query results are displayed with row counts:

```
inputlayer> ?edge(X, Y)
Results: 5 rows
  (1, 2)
  (2, 3)
  (3, 4)
  (4, 5)
  (5, 6)
```

### Using Wildcards

Use `_` to ignore columns:

```datalog
// Get all source nodes (ignore target)
?edge(X, _)

// Count unique sources
temp(count<X>) <- edge(X, _)
```

## Common Workflows

### 1. Exploratory Analysis

```datalog
.kg create exploration
.kg use exploration
.load data.idl
.rel                          // See what data exists
?some_relation(X, Y)       // Explore
temp(X) <- complex_query...   // Session rule for analysis
.session clear                // Clean up when done
```

### 2. Building a Schema

```datalog
.kg create production
.kg use production

// Define schemas first
+user(id: int, name: string, email: string)
+order(id: int, user_id: int, amount: float)

// Load data
.load users.idl
.load orders.idl

// Verify
.rel user
.rel order
```

### 3. Defining Business Rules

```datalog
// Define persistent rules
+high_value_customer(UserId) <-
  order(_, UserId, Amount),
  Amount > 1000

// Aggregate total spend per customer
+customer_spend(UserId, sum<Amount>) <-
  order(_, UserId, Amount)

// VIPs have high total spend
+vip(UserId, Total) <-
  high_value_customer(UserId),
  customer_spend(UserId, Total),
  Total > 5000

// Query
?vip(User, Spend)
```

### 4. Iterating on Rules

```datalog
// First attempt
+path(X, Y) <- edge(X, Y)

// Check results
.rule path

// Not right? Clear and redefine
.rule clear path
+path(X, Y) <- edge(X, Y)
+path(X, Z) <- path(X, Y), edge(Y, Z)

// Verify
.rule def path
.rule path
```

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl+C` | Cancel current input |
| `Ctrl+D` | Exit REPL (same as `.quit`) |
| `↑` / `↓` | Navigate command history |
| `Ctrl+R` | Search command history |

## Error Handling

When something goes wrong, InputLayer provides helpful error messages:

```
inputlayer> +edge(1, "two")
Error: Type mismatch in relation 'edge'
  Expected: (int, int)
  Got: (int, string)
  Hint: Check your data types match the schema
```

```
inputlayer> ?undefined_relation(X)
Error: Unknown relation 'undefined_relation'
  Available relations: edge, node, path
```

## Next Steps

- **[Basic Queries Tutorial](../tutorials/01-basic-queries.md)** - Query patterns in depth
- **[Cheatsheet](../reference/syntax-cheatsheet.md)** - Quick reference
- **[Troubleshooting](../troubleshooting/common-errors.md)** - Common issues and solutions
