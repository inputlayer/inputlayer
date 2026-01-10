# Grammar Reference

This section describes the complete syntax of InputLayer Datalog.

## Character Encoding

InputLayer programs are UTF-8 encoded text. You can use Unicode characters in string literals.

## Whitespace

Spaces, tabs, and newlines separate tokens. Extra whitespace is ignored:

```datalog
+edge(1, 2).           % Fine
+edge( 1 , 2 ).        % Also fine
+edge(1,2).            % Also fine
```

Whitespace inside strings is preserved:

```datalog
+message("hello world").  % String contains space
```

## Comments

Two styles of comments are supported:

```datalog
% Line comment: everything after % is ignored

/* Block comment:
   spans multiple lines
   until closing */

+edge(1, 2).  % Inline comment after statement
```

Comments cannot be nested:
```datalog
/* outer /* inner */ still in outer? */  % Error!
```

## Statement Types

Every statement ends with a period `.`

| Statement | Syntax | Purpose |
|-----------|--------|---------|
| Insert fact | `+relation(args).` | Add data |
| Bulk insert | `+relation[(tuple), ...].` | Add multiple facts |
| Delete fact | `-relation(args).` | Remove data |
| Conditional delete | `-relation(vars) :- body.` | Remove matching data |
| Persistent rule | `+head(vars) :- body.` | Define saved rule |
| Session rule | `head(vars) :- body.` | Define temporary rule |
| Query | `?- pattern.` | Retrieve data |
| Schema | `+relation(col: type, ...).` | Declare structure |
| Meta command | `.command args` | System operations |

## Grammar Sections

The grammar is organized into these sections:

- **[Facts and Data](grammar_facts.md)** - Inserting, deleting, and bulk operations
- **[Rules](grammar_rules.md)** - Session and persistent rules
- **[Queries](grammar_queries.md)** - Querying data
- **[Atoms and Terms](grammar_atoms.md)** - Variables, constants, predicates
- **[Data Types](grammar_constants.md)** - Integers, strings, floats, vectors
- **[Literals and Filters](grammar_literals.md)** - Negation, comparisons, arithmetic
- **[Comments](grammar_comments.md)** - Comment syntax

## EBNF Notation

This document uses EBNF notation for grammar rules:

```ebnf
symbol ::= expression ;
```

| Notation | Meaning |
|----------|---------|
| `"text"` | Literal text |
| `A B` | A followed by B |
| `A \| B` | A or B |
| `A?` | Optional (zero or one) |
| `A*` | Zero or more |
| `A+` | One or more |
| `( A B )` | Grouping |
| `[a-z]` | Character range |

## Quick Syntax Reference

### Facts

```ebnf
insert_fact  ::= "+" predicate "(" term ("," term)* ")" "." ;
bulk_insert  ::= "+" predicate "[" tuple ("," tuple)* "]" "." ;
delete_fact  ::= "-" predicate "(" term ("," term)* ")" "." ;
tuple        ::= "(" term ("," term)* ")" ;
```

### Rules

```ebnf
persistent_rule ::= "+" head ":-" body "." ;
session_rule    ::= head ":-" body "." ;
head            ::= predicate "(" term ("," term)* ")" ;
body            ::= literal ("," literal)* ;
```

### Queries

```ebnf
query ::= "?-" body "." ;
```

### Terms

```ebnf
term      ::= variable | constant | placeholder ;
variable  ::= [A-Z] [a-zA-Z0-9_]* ;
placeholder ::= "_" ;
constant  ::= integer | float | string | boolean | vector ;
```

### Predicates

```ebnf
predicate ::= [a-z] [a-zA-Z0-9_]* ;
```

Predicates (relation names) must start with a lowercase letter.

## Extensions

InputLayer extends standard Datalog with:

- **[Aggregations](extensions.md#aggregations)** - count, sum, min, max, avg
- **[Vectors](extensions.md#vectors)** - Array literals and distance functions
- **[Meta Commands](extensions.md#meta-commands)** - System operations (.kg, .rel, .rule)
- **[Schema Constraints](extensions.md#schemas)** - @key, @unique, @not_empty, @range
