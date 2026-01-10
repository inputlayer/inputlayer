# EBNF Notation

This specification uses Extended Backus-Naur Form (EBNF) to describe grammar rules.

## Syntax

```ebnf
symbol ::= expression ;
```

## Operators

| Operator | Meaning | Example |
|----------|---------|---------|
| `"text"` | Literal text | `":-"` matches `:-` |
| `A B` | Sequence | `A B` matches A then B |
| `A \| B` | Alternative | `A \| B` matches A or B |
| `A?` | Optional | `A?` matches zero or one A |
| `A*` | Repetition | `A*` matches zero or more A |
| `A+` | One or more | `A+` matches one or more A |
| `( )` | Grouping | `(A B)?` makes sequence optional |
| `[a-z]` | Character range | `[a-z]` matches lowercase letter |
| `[^x]` | Negation | `[^"]` matches any char except `"` |

## Examples

### Simple Rule

```ebnf
predicate ::= [a-z][a-zA-Z0-9_]* ;
```

Matches: `edge`, `my_relation`, `foo123`

### Optional Element

```ebnf
integer ::= "-"? [0-9]+ ;
```

Matches: `42`, `-17`, `0`

### Alternative

```ebnf
boolean ::= "true" | "false" ;
```

Matches: `true` or `false`

### Repetition

```ebnf
terms ::= term ("," term)* ;
```

Matches: `X` or `X, Y` or `X, Y, Z`

### Grouping

```ebnf
fact ::= ("+" | "-") atom "." ;
```

Matches: `+edge(1, 2).` or `-edge(1, 2).`
