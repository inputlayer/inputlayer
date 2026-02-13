# Type System Specification

This document specifies InputLayer's type system for developers implementing or extending the language.

## Overview

A program is a sequence of declarations:

```ebnf
Decl       ::= TypeDecl | SchemaDecl | RuleDecl | FactDecl
```

- `type` - defines **value types** (aliases, refinements, record types)
- `+name(col: type, ...)` - defines **relation schemas** with typed columns
- Facts (`+name[(...)]`) - provide **base tuples** for relations
- Rules (`+name(...) <- body`) - define **derived tuples** for relations

## 1. Type Declarations (`type`)

### 1.1 Grammar

```ebnf
TypeDecl   ::= "type" TypeName ":" TypeExpr "."
TypeName   ::= UIdent          // capitalized, e.g. Email, User

TypeExpr   ::= SimpleType
             | RecordType

SimpleType ::= BaseType [Refinements]
BaseType   ::= "int" | "string" | "bool"
             | "list" "[" TypeExpr "]"
             | TypeName         // previously declared type

RecordType ::= "{" FieldList "}"
FieldList  ::= Field ("," Field)*
Field      ::= FieldName ":" TypeExpr

FieldName  ::= LIdent

Refinements ::= "(" Refinement ("," Refinement)* ")"
Refinement  ::= Ident "(" ... ")"   // opaque to the core language
```

### 1.2 Semantics

A `type` declaration introduces a **value type**. Types describe the **shape and constraints of values** but **do not define relations** by themselves.

### 1.3 Aliases and Refinements

```datalog
type Email: string(pattern("^[^@]+@[^@]+$"))
type Id:    int(range(1, 1000000))
type Tags:  list[string](not_empty)
```

Interpretation as refinement types:

- `Email ⊆ string`
- `Id ⊆ int`
- `Tags ⊆ list[string]`

Typing judgment:

```
Γ ⊢ v : Email  ⇒  Γ ⊢ v : string  and  v satisfies pattern("^[^@]+@[^@]+$")
```

The semantics of refinements (`pattern`, `range`, `not_empty`, …) are implementation-defined.

### 1.4 Record Types

```datalog
type User: {
    id:      Id,
    name:    string(not_empty),
    email:   Email,
    my_tags: Tags
}
```

A **record type** `{ f₁ : τ₁, …, fₙ : τₙ }` is the type of records with named fields `fᵢ` of types `τᵢ`.

Important:
- `User` is a **value type**
- A single `User` value is a record with 4 fields
- This is *not* yet a relation; it's just a value type

## 2. Schema Declarations

A **relation schema** declares the structure of a relation (like a table).

### 2.1 Grammar

```ebnf
SchemaDecl ::= "+" RelName "(" ParamList ")" "."

RelName    ::= LIdent           // e.g. user, high_spender

ParamList  ::= Param ("," Param)*
Param      ::= ParamName ":" TypeExpr [Annotations]
ParamName  ::= LIdent

Annotations ::= Annotation+
Annotation  ::= "@" AnnotName
AnnotName   ::= "key" | "unique" | "not_empty" | ...
```

### 2.2 Schema Declaration

```datalog
+user(
    id:      int,
    name:    string,
    email:   string
)
```

This declares a 3-ary relation:

```
user : int × string × string → Bool
```

i.e. `user ⊆ int × string × string`.

Each parameter is a **column** in the relation.

### 2.3 Schema with Type References

```datalog
type Id:    int
type Email: string

+user(id: Id, name: string, email: Email)
```

## 3. Terms and Expressions

### 3.1 Grammar

```ebnf
Term       ::= Var
             | Constant

Var        ::= LIdent | UIdent
```

Constants are implementation-defined (ints, strings, bools, lists, …).

## 4. Atoms, Facts, and Rules

### 4.1 Atoms

```ebnf
Atom        ::= RelName "(" ArgList? ")"
ArgList     ::= Arg ("," Arg)*
Arg         ::= Term
```

Example (positional):

```datalog
user(1, "Alice", "alice@example.com")
```

### 4.2 Facts

```ebnf
FactDecl    ::= "+" Atom "."
              | "+" RelName "[" TupleList "]" "."
```

Example:

```datalog
+user(1, "Alice", "alice@example.com")
+user[(2, "Bob", "bob@example.com"), (3, "Charlie", "charlie@example.com")]
```

### 4.3 Rules

```ebnf
RuleDecl    ::= ["+" ] HeadAtom "<-" Body "."
HeadAtom    ::= RelName "(" HeadArgList? ")"
HeadArgList ::= HeadArg ("," HeadArg)*
HeadArg     ::= Term

Body        ::= BodyAtom ("," BodyAtom)*
BodyAtom    ::= Atom | Condition

Condition   ::= Term RelOp Term
RelOp       ::= "=" | "!=" | "<" | ">" | "<=" | ">="
```

### 4.4 Persistent vs Session Rules

```datalog
// Persistent rule (with + prefix) - stored and incrementally maintained
+admin_email(Email) <-
    user(_, _, Email),
    admin(Email)

// Session rule (no + prefix) - computed on-demand, not stored
temp_result(X, Y) <- source(X, Y), X > 10
```

## 5. Base vs Derived Relations

- **Base data (EDB)**: Relations populated via **facts only**
- **Derived data (IDB/views)**: Relations populated via **rules**

Both are still just **relations**:
- Schema gives the structure
- Facts and rules together define the relation's **extension**

SQL analogy:
- `+user(...) + facts` ≈ `CREATE TABLE user (...) + INSERT INTO user .`
- `+admin_email(...) + rules` ≈ `CREATE VIEW admin_email AS SELECT .`

## 6. Persistent Rules and the `+` Prefix

### 6.1 Design Principles

Clear separation of concerns:

| Syntax | Purpose | DD Materialization | Type Checking |
|--------|---------|-------------------|---------------|
| `type` | Value type definitions | No | N/A |
| `+name(col: type, ...)` | Schema declaration | **No** | **Yes** |
| `+name(...) <- body` | Persistent rule (DD view) | **Yes** | Only if schema exists |
| `name(...) <- body` | Session rule (transient) | **No** | Only if schema exists |
| `+`/`-` (facts) | Base data manipulation | No | Only if schema exists |

### 6.2 Persistent Rule Grammar

```ebnf
PersistentRule ::= "+" RuleName "(" ParamList ")" "<-" Body "."
```

### 6.3 Session Rules

Rules without `+` prefix are **session rules**:

```datalog
temp_result(X, Y) <- source(X, Y), X > 10
```

- Computed on-demand during evaluation
- NOT persisted or incrementally maintained
- Useful for ad-hoc queries

## 7. Implementation Notes

### 7.1 Type Persistence

All typing information needs to be persisted on a database level. The server implements multiple databases where same-named types could have different semantic meanings.

### 7.2 Naming Conventions

To avoid confusion between types and variables:

- **Types**: `UIdent` (capitalized) - `Email`, `User`, `Purchase`
- **Variables**: `LIdent` or `UIdent` - `e`, `Email`, `user_id`
- **Relations**: `LIdent` (lowercase) - `user`, `purchase`, `admin_email`

### 7.3 Command vs Keyword Distinction

Do NOT confuse:
- `+name(col: type)` - Schema declaration syntax
- `.rel` meta command - REPL command to list/describe relations

## 8. Complete Example

```datalog
// Type definitions
type Id: int(range(1, 1000000))
type Email: string(pattern("^[^@]+@[^@]+$"))

// Schema declarations
+user(id: Id, name: string, email: Email)
+purchase(user_id: Id, amount: int)

// Base data
+user[(1, "Alice", "alice@example.com"), (2, "Bob", "bob@example.com")]
+purchase[(1, 1500), (1, 200), (2, 300)]

// Persistent rule (explicit DD materialization)
+high_spender(UserId) <-
    purchase(UserId, Amount),
    Amount > 1000

// Session rule (not materialized, just computed on query)
temp(Id) <- user(Id, _, _), high_spender(Id)

// Query
?high_spender(X)
```

## 9. Type Checking Algorithm

### 9.1 Schema Lookup

Given a rule:

```datalog
+admin_email(Email) <- user(_, _, Email), admin(Email)
```

Type checking steps:
1. Lookup schema: if `+admin_email(email: Email)` exists, check arity matches
2. Check that variable types are consistent across the rule
3. Report type errors if mismatches found

### 9.2 Variable Type Inference

Variables get their types from:
1. Their position in atoms with declared schemas
2. Comparison with constants
3. Aggregation context
