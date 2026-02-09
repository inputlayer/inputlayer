# Data Types

InputLayer supports several data types for constants.

## Type Summary

| Type | Syntax | Examples |
|------|--------|----------|
| Integer | digits | `1`, `-42`, `1000000` |
| Float | digits with decimal | `3.14`, `-0.5`, `1e10` |
| String | double quotes | `"hello"`, `"alice"` |
| Boolean | true/false | `true`, `false` |
| Vector | brackets | `[1.0, 2.0, 3.0]` |
| Timestamp | Unix milliseconds | `1704067200000` |
| Symbol | schema keyword | `symbol` type in schemas |

## Integers

64-bit signed integers:

```datalog
42
-17
0
1000000
-9223372036854775808   % Minimum
9223372036854775807    % Maximum
```

### Grammar

```ebnf
integer ::= "-"? [0-9]+ ;
```

## Floats

64-bit floating-point numbers:

```datalog
3.14
-0.5
0.0
1.23e10
-4.56e-7
```

### Grammar

```ebnf
float ::= "-"? [0-9]+ "." [0-9]+ ( ("e" | "E") "-"? [0-9]+ )? ;
```

## Strings

UTF-8 text enclosed in double quotes:

```datalog
"hello"
"Alice Smith"
"with spaces and punctuation!"
""                    % Empty string
```

### Escape Sequences

| Sequence | Meaning |
|----------|---------|
| `\"` | Double quote |
| `\\` | Backslash |
| `\n` | Newline |
| `\t` | Tab |
| `\r` | Carriage return |

```datalog
+"She said \"hello\""
+"Line 1\nLine 2"
+"Path: C:\\Users\\alice"
```

### Grammar

```ebnf
string ::= '"' ( [^"\\] | escape )* '"' ;
escape ::= '\\' ( '"' | '\\' | 'n' | 't' | 'r' ) ;
```

## Booleans

Boolean true or false:

```datalog
true
false
```

### Grammar

```ebnf
boolean ::= "true" | "false" ;
```

## Vectors

Arrays of floating-point numbers:

```datalog
[1.0, 2.0, 3.0]
[0.5, -0.5]
[]                  % Empty vector
```

Vectors are used for embeddings and similarity search:

```datalog
+vectors(1, [1.0, 0.0, 0.0]).
+vectors(2, [0.0, 1.0, 0.0]).

?- vectors(Id1, V1), vectors(Id2, V2),
   Dist = euclidean(V1, V2).
```

### Grammar

```ebnf
vector ::= "[" ( float ( "," float )* )? "]" ;
```

## Timestamps

Unix timestamps in milliseconds since epoch (1970-01-01 00:00:00 UTC):

```datalog
1704067200000           % 2024-01-01 00:00:00 UTC
```

Timestamps are used with temporal functions:

```datalog
+events(1, 1704067200000).
+events(2, 1704153600000).

% Find events from last 24 hours
?- events(Id, Ts),
   Now = time_now(),
   within_last(Ts, Now, 86400000).
```

Timestamps are stored as 64-bit integers internally.

### Schema Declaration

```datalog
+event_log(id: int, occurred_at: timestamp, message: string).
```

Aliases: `timestamp`, `time`, `datetime`

## Symbols

Symbols are interned strings optimized for frequent comparisons (like identifiers or tags):

```datalog
+user(1, "alice", "admin").     % "admin" as string
```

### Schema Declaration

Use the `symbol` type when a column contains a small set of repeated values:

```datalog
+user(id: int, name: string, role: symbol).
```

Symbols provide better performance for equality comparisons and use less memory when values repeat frequently.

**Note**: At the data level, symbols appear as strings. The `symbol` type is a schema hint for optimization.

## Type Coercion

InputLayer performs limited automatic type coercion:

| From | To | Automatic? |
|------|-----|------------|
| Integer | Float | Yes (in arithmetic) |
| Others | Any | No |

```datalog
% Integer + Float = Float
?- data(X, Y), Z = X + 3.14.  % X (int) coerced to float
```

## Type in Schemas

When declaring schemas, use these type names:

```datalog
+employee(
    id: int,
    name: string,
    salary: float,
    active: bool
).
```

| Type Keyword | Aliases | Accepts |
|--------------|---------|---------|
| `int` | `integer`, `int64` | Integers |
| `float` | `float64`, `double` | Floats |
| `string` | `text`, `varchar` | Strings |
| `bool` | `boolean` | true, false |
| `vector` | `embedding`, `vec` | Vector arrays |
| `timestamp` | `time`, `datetime` | Unix milliseconds |
| `symbol` | - | Interned strings |

## Examples

### Mixed Types

```datalog
+product(1, "Widget", 19.99, true).
+product(2, "Gadget", 29.99, false).
```

### Vectors for Similarity

```datalog
+embedding[(1, [0.1, 0.2, 0.3]), (2, [0.2, 0.3, 0.4])].

?- embedding(Id1, V1), embedding(Id2, V2),
   Id1 < Id2,
   Sim = cosine(V1, V2),
   Sim > 0.9.
```

### String Comparisons

```datalog
+person[("alice", "engineering"), ("bob", "sales")].

?- person(Name, Dept), Dept = "engineering".
```
