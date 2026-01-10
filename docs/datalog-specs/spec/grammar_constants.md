# Data Types

InputLayer supports several data types for constants.

## Type Summary

| Type | Syntax | Examples |
|------|--------|----------|
| Integer | digits | `1`, `-42`, `1000000` |
| Float | digits with decimal | `3.14`, `-0.5`, `1e10` |
| String | double quotes | `"hello"`, `"alice"` |
| Atom | unquoted lowercase | `alice`, `bob` |
| Boolean | true/false | `true`, `false` |
| Vector | brackets | `[1.0, 2.0, 3.0]` |

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

## Atoms

Unquoted lowercase identifiers are treated as strings:

```datalog
alice              % Same as "alice"
bob                % Same as "bob"
my_identifier      % Same as "my_identifier"
```

Atoms provide a shorter syntax for simple string values.

### Grammar

```ebnf
atom_value ::= [a-z][a-zA-Z0-9_]* ;
```

**Note**: Atoms must start with lowercase. Uppercase identifiers are variables.

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

## Type Coercion

InputLayer performs limited automatic type coercion:

| From | To | Automatic? |
|------|-----|------------|
| Integer | Float | Yes (in arithmetic) |
| Atom | String | Yes (always) |
| Others | Any | No |

```datalog
% Integer + Float = Float
?- data(X, Y), Z = X + 3.14.  % X (int) coerced to float

% Atom is a string
+person(alice, 30).           % alice = "alice"
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

| Type Keyword | Accepts |
|--------------|---------|
| `int` | Integers |
| `float` | Floats |
| `string` | Strings, atoms |
| `bool` | true, false |

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
