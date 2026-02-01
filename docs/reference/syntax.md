# Syntax Reference

Complete syntax reference for InputLayer's Datalog dialect.

## Grammar Overview

```
program     ::= statement*
statement   ::= fact | rule | query | meta_command | schema_decl

fact        ::= '+' atom '.'
              | '+' relation '[' tuple_list ']' '.'
              | '-' atom '.'

rule        ::= '+' atom ':-' body '.'           // Persistent rule
              | atom ':-' body '.'               // Session rule

query       ::= '?-' body '.'

body        ::= goal (',' goal)*
goal        ::= atom | constraint | negated_atom

atom        ::= relation '(' term_list ')'
negated_atom::= '!' atom

term        ::= variable | constant | expression
term_list   ::= term (',' term)*

constraint  ::= term op term
op          ::= '=' | '!=' | '<' | '<=' | '>' | '>='
```

## Lexical Elements

### Identifiers

| Type | Pattern | Examples |
|------|---------|----------|
| Relation | `[a-z][a-z0-9_]*` | `edge`, `my_relation`, `path2` |
| Variable | `[A-Z][a-zA-Z0-9_]*` | `X`, `Name`, `PersonId` |
| Placeholder | `_` | Matches any value, discarded |

### Literals

#### Integers
```datalog
42        % Decimal
-17       % Negative
0         % Zero
```

#### Floats
```datalog
3.14      % Decimal float
-0.5      % Negative float
1.0e10    % Scientific notation
```

#### Strings
```datalog
"hello"           % Basic string
"hello world"     % With spaces
"line1\nline2"    % Escape sequences
"say \"hi\""      % Escaped quotes
```

**Escape sequences:**
| Sequence | Meaning |
|----------|---------|
| `\n` | Newline |
| `\t` | Tab |
| `\\` | Backslash |
| `\"` | Quote |

#### Vectors
```datalog
[1.0, 2.0, 3.0]           % Float vector
[0.1, 0.2, 0.3, 0.4]      % Embedding
```

### Comments

```datalog
% Single line comment (Prolog style - preferred)

/* Multi-line
   block comment */

+edge(1, 2).  % Inline comment
```

## Statements

### Fact Insertion (`+`)

Insert base data into relations.

**Single fact:**
```datalog
+edge(1, 2).
+person("alice", 30).
+location("NYC", 40.7, -74.0).
```

**Bulk insert:**
```datalog
+edge[(1, 2), (2, 3), (3, 4), (4, 5)].
+person[("alice", 30), ("bob", 25), ("carol", 35)].
```

### Fact Deletion (`-`)

Remove base data from relations.

**Single fact:**
```datalog
-edge(1, 2).
```

**Conditional delete (based on another relation):**
```datalog
% Delete all edges where source node is in the 'banned' relation
-edge(X, Y) :- banned(X).

% Delete all edges where X is greater than 5
-edge(X, Y) :- edge(X, Y), X > 5.

% Delete edges that form triangles
-edge(X, Y) :- edge(X, Y), edge(Y, Z), edge(Z, X).
```

**Note:** Conditional delete finds all tuples matching the condition and removes them from the target relation. The target relation is automatically included in the query body to bind all head variables.

### Persistent Rules (`+head :- body`)

Define derived relations that persist across sessions.

```datalog
% Simple derivation
+adult(Name, Age) :- person(Name, Age), Age >= 18.

% Join
+works_in(Name, Building) :-
  employee(Name, Dept),
  department(Dept, Building).

% Recursive
+path(X, Y) :- edge(X, Y).
+path(X, Z) :- path(X, Y), edge(Y, Z).

% With negation
+orphan(X) :- person(X), !parent(_, X).

% With aggregation
+dept_size(Dept, count<Emp>) :- employee(Emp, Dept).
```

### Session Rules (no `+` prefix)

Transient rules that exist only for the current session.

```datalog
% Not persisted
temp(X, Y) :- edge(X, Y), X < Y.
debug(X) :- some_complex_condition(X).
```

### Queries (`?-`)

Ask questions about the data.

```datalog
% Simple query
?- edge(X, Y).

% With constants
?- edge(1, X).

% With constraints
?- person(Name, Age), Age > 25.

% Join query
?- person(Id, Name, _, _), purchase(Id, Item, _).
```

### Schema Declarations

Define typed schemas for relations.

```datalog
% Basic schema
+employee(id: int, name: string, dept: string).

% All types
+example(
  a: int,
  b: float,
  c: string,
  d: vector
).
```

**Supported types:**
| Type | Description | Example Values |
|------|-------------|----------------|
| `int` | 64-bit integer | `42`, `-17`, `0` |
| `float` | 64-bit float | `3.14`, `-0.5` |
| `string` | UTF-8 string | `"hello"`, `"world"` |
| `vector` | Float array | `[0.1, 0.2, 0.3]` |

## Expressions

### Arithmetic

```datalog
+computed(X, Y) :- input(A, B), X = A + B, Y = A * B.
```

| Operator | Description |
|----------|-------------|
| `+` | Addition |
| `-` | Subtraction |
| `*` | Multiplication |
| `/` | Division |
| `%` | Modulo |

### Comparison

```datalog
?- person(Name, Age), Age >= 18, Age < 65.
```

| Operator | Description |
|----------|-------------|
| `=` | Equal |
| `!=` | Not equal |
| `<` | Less than |
| `<=` | Less or equal |
| `>` | Greater than |
| `>=` | Greater or equal |

### Built-in Functions

**Vector functions:**

InputLayer provides built-in vector distance and similarity functions:

```datalog
% Compute cosine similarity between embeddings
?- embedding(Id1, V1), embedding(Id2, V2), Id1 < Id2,
   Sim = cosine(V1, V2), Sim > 0.9.

% Compute Euclidean distance between points
?- point(Id1, V1), point(Id2, V2), Id1 < Id2,
   Dist = euclidean(V1, V2), Dist < 1.0.
```

| Function | Description |
|----------|-------------|
| `euclidean(v1, v2)` | Euclidean distance |
| `cosine(v1, v2)` | Cosine similarity |
| `dot(v1, v2)` | Dot product |
| `manhattan(v1, v2)` | Manhattan distance |

**Note**: Vector functions are used in query bodies (filters/constraints), not in rule heads.

## Aggregations

Compute aggregate values over groups.

### Syntax

```datalog
+result(GroupBy1, GroupBy2, agg<AggColumn>) :-
  source(GroupBy1, GroupBy2, AggColumn, _).
```

Variables in the head that are not aggregated become group-by columns.

### Aggregate Functions

| Function | Description | Example |
|----------|-------------|---------|
| `count<X>` | Count rows | `count<Id>` |
| `sum<X>` | Sum values | `sum<Amount>` |
| `min<X>` | Minimum | `min<Age>` |
| `max<X>` | Maximum | `max<Score>` |
| `avg<X>` | Average | `avg<Salary>` |

### Examples

```datalog
% Count per group
+city_count(City, count<Id>) :- person(Id, _, _, City).

% Sum
+total_sales(Product, sum<Amount>) :- sale(_, Product, Amount).

% Multiple aggregates (separate rules)
+stats_min(min<Age>) :- person(_, _, Age, _).
+stats_max(max<Age>) :- person(_, _, Age, _).

% Global aggregate (no group-by)
+total(sum<Amount>) :- purchase(_, _, Amount).
```

## Negation

Express "does not exist" conditions.

### Syntax

```datalog
!atom(args)     % Negated atom
```

### Rules

1. **Safety**: Variables in negation must appear positively elsewhere
2. **Stratification**: No circular dependencies through negation

### Examples

```datalog
% People without purchases
+non_buyer(Id, Name) :-
  person(Id, Name, _, _),
  !purchase(Id, _, _).

% Nodes with no outgoing edges
+sink(X) :- node(X), !edge(X, _).

% Set difference
+only_in_a(X) :- a(X), !b(X).
```

### Invalid (Unsafe)

```datalog
% WRONG - X only appears in negation
+bad(X) :- !some_rel(X).

% CORRECT - X appears positively
+good(X) :- domain(X), !some_rel(X).
```

## Meta Commands

Commands that control the REPL environment.

### Knowledge Graph Commands

```datalog
.kg                     % Show current knowledge graph
.kg list                % List all knowledge graphs
.kg create <name>       % Create knowledge graph
.kg use <name>          % Switch to knowledge graph
.kg drop <name>         % Delete knowledge graph
```

### Relation Commands

```datalog
.rel                    % List relations with data
.rel <name>             % Show schema and sample data
```

### Rule Commands

```datalog
.rule                   % List all rules
.rule <name>            % Query a rule
.rule def <name>        % Show rule definition
.rule drop <name>       % Delete all clauses of a rule
.rule remove <name> <n> % Remove clause #n (1-based)
.rule clear <name>      % Clear for re-registration
.rule edit <name> <n> <clause>  % Edit clause
```

### Session Commands

```datalog
.session                % List session rules
.session clear          % Clear all session rules
.session drop <n>       % Remove session rule #n
```

### File Commands

```datalog
.load <file>            % Execute a .dl file
.load <file> --replace  % Replace existing rules
.load <file> --merge    % Merge with existing
```

### System Commands

```datalog
.status                 % System status
.compact                % Compact storage
.help                   % Help message
.quit                   % Exit REPL
.exit                   % Exit REPL (alias)
```

## Recursion

### Basic Pattern

```datalog
% Base case
+derived(X, Y) :- base(X, Y).

% Recursive case
+derived(X, Z) :- derived(X, Y), base(Y, Z).
```

### Transitive Closure

```datalog
+reachable(X, Y) :- edge(X, Y).
+reachable(X, Z) :- reachable(X, Y), edge(Y, Z).
```

### Mutual Recursion

```datalog
+odd(X) :- edge(Start, X), start(Start).
+odd(X) :- even(Y), edge(Y, X).
+even(X) :- odd(Y), edge(Y, X).
```

### Restrictions

- Negation through recursion is not allowed (non-stratifiable)
- Must have a base case that terminates

## Complete Examples

### Social Network

```datalog
% Schema
+person(id: int, name: string, age: int).
+follows(follower: int, followed: int).

% Data
+person[(1, "alice", 30), (2, "bob", 25), (3, "carol", 35)].
+follows[(1, 2), (2, 3), (1, 3)].

% Rules
+mutual_follow(A, B) :-
  follows(A, B),
  follows(B, A),
  A < B.

+influencer(Id, count<Follower>) :-
  follows(Follower, Id).

% Query
?- influencer(Id, Count), Count > 10.
```

### Graph Analysis

```datalog
% Transitive closure
+path(X, Y) :- edge(X, Y).
+path(X, Z) :- path(X, Y), edge(Y, Z).

% Cycle detection
+in_cycle(X) :- path(X, X).

% Connected components (undirected)
+bidir(X, Y) :- edge(X, Y).
+bidir(X, Y) :- edge(Y, X).
+connected(X, Y) :- bidir(X, Y).
+connected(X, Z) :- connected(X, Y), bidir(Y, Z).

% Sink nodes (no outgoing)
+sink(X) :- node(X), !edge(X, _).

% Source nodes (no incoming)
+source(X) :- node(X), !edge(_, X).
```

### Bill of Materials

```datalog
% Part hierarchy
+contains(assembly: int, part: int, qty: int).

% All parts needed (recursive)
+requires(Asm, Part) :- contains(Asm, Part, _).
+requires(Asm, Part) :-
  contains(Asm, Sub, _),
  requires(Sub, Part).

% Total quantity calculation
+total_qty(Asm, Part, sum<Qty>) :-
  contains(Asm, Part, Qty).
```

## Reserved Words

The following are reserved and cannot be used as relation names:

```
true, false, null
count, sum, min, max, avg
int, float, string, vector
```

## File Format

InputLayer files use the `.dl` extension and contain valid Datalog statements:

```datalog
% my_program.dl

% Schema declarations
+node(id: int, label: string).
+edge(src: int, dst: int, weight: float).

% Data
+node[(1, "a"), (2, "b"), (3, "c")].
+edge[(1, 2, 1.0), (2, 3, 2.0)].

% Rules
+path(X, Y, W) :- edge(X, Y, W).
+path(X, Z, W) :- path(X, Y, W1), edge(Y, Z, W2), W = W1 + W2.
```

Load with:
```datalog
.load my_program.dl
```
