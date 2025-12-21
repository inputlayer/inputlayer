# Type System Specification

This document specifies InputLayer's type system for developers implementing or extending the language.

## Overview

A program is a sequence of declarations:

```ebnf
Decl       ::= TypeDecl | RelDecl | RuleDecl | FactDecl
```

- `type` — defines **value types** (aliases, refinements, record types)
- `rel` — defines **relations** (tables) over those value types
- Facts (`FactDecl`) — provide **base tuples** for relations
- Rules (`RuleDecl`) — define **derived tuples** for relations

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
type Email: string(pattern("^[^@]+@[^@]+$")).
type Id:    int(range(1, 1000000)).
type Tags:  list[string](not_empty).
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
}.
```

A **record type** `{ f₁ : τ₁, …, fₙ : τₙ }` is the type of records with named fields `fᵢ` of types `τᵢ`.

Important:
- `User` is a **value type**
- A single `User` value is a record with 4 fields
- This is *not* yet a relation; it's just a value type

## 2. Relation Declarations (`rel`)

A **relation** is like a table: a set of tuples over value types.

### 2.1 Grammar

```ebnf
RelDecl    ::= "rel" RelName "(" ParamList? ")" "."
             | "rel" RelName ":" TypeName "."

RelName    ::= LIdent           // e.g. user, high_spender

ParamList  ::= Param ("," Param)*
Param      ::= ParamName ":" TypeExpr
ParamName  ::= LIdent
```

### 2.2 Core Form

```datalog
rel user(
    id:      Id,
    name:    string(not_empty),
    email:   Email,
    my_tags: Tags
).
```

This declares a 4-ary relation:

```
user : Id × string(not_empty) × Email × Tags → Bool
```

i.e. `user ⊆ Id × string(not_empty) × Email × Tags`.

Each parameter is a **column** in the relation.

### 2.3 Record-Type Sugar: `rel r: T`

When `T` is a record type:

```datalog
type User: {
    id:      Id,
    name:    string(not_empty),
    email:   Email,
    my_tags: Tags
}.

rel user: User.
```

**Desugaring rule**: If `type T: { f1: τ1, f2: τ2, ..., fn: τn }.` then `rel r: T.` desugars to:

```datalog
rel r(
    f1: τ1,
    f2: τ2,
    ...,
    fn: τn
).
```

Key points:
- `user` is a **4-ary** relation, not unary
- The record type is used as a **schema template**

### 2.4 Static Error: Non-Record Type in `rel r: T`

If `T` is **not** a record type, `rel r: T.` is a **static error**:

```datalog
type Email: string(pattern("^[^@]+@[^@]+$")).

-- ❌ Illegal: Email is a simple (non-record) type
rel admin_email: Email.
```

Reason: a relation needs named columns. We do not invent implicit column names.

Correct form:

```datalog
-- ✅ Legal, with explicit column name
rel admin_email(email: Email).
```

Or wrap in a record:

```datalog
type AdminEmailRow: { email: Email }.
rel admin_email: AdminEmailRow.  // OK, desugars to rel admin_email(email: Email).
```

### 2.5 Anonymous Record Types

Anonymous record types are also supported:

```datalog
rel user: { id: Id, name: string, email: Email }.
```

This is equivalent to declaring a named type and using it.

## 3. Terms and Expressions

### 3.1 Grammar

```ebnf
Term       ::= Var
             | TypedVar
             | Constant
             | RecordLiteral
             | Term "." FieldName

Var        ::= LIdent
TypedVar   ::= Var ":" TypeExpr
```

Constants are implementation-defined (ints, strings, bools, lists, …).

### 3.2 Record Literals

```ebnf
RecordLiteral    ::= "{" RecordFieldList "}"
RecordFieldList  ::= RecordField ("," RecordField)*
RecordField      ::= FieldName "=" Term
```

Example:

```datalog
{ id = 1, name = "Alice", email = "alice@example.com", my_tags = ["admin"] }
```

Typing rule:

> If each `tᵢ` has type `τᵢ` in context `Γ`,
> then `Γ ⊢ { f₁ = t₁, …, fₙ = tₙ } : { f₁ : τ₁, …, fₙ : τₙ }`.

### 3.3 Field Access

If `t` has a record type `{ f₁: τ₁, …, fk: τk, … }`, then `t.fk` is a term of type `τk`.

Typing rule:

> If `Γ ⊢ t : { f₁ : τ₁, …, fk : τk, … }`
> then `Γ ⊢ t.fk : τk`.

### 3.4 Typed Variables

`x: τ` is a term standing for a variable `x` with declared type `τ`.

Typing rule: if `x: τ` appears, add `x : τ` to the environment `Γ`.

## 4. Atoms, Facts, and Rules

### 4.1 Atoms

```ebnf
Atom        ::= RelName "(" ArgList? ")"
ArgList     ::= Arg ("," Arg)*
Arg         ::= Term
```

Example (positional):

```datalog
user(1, "Alice", "alice@example.com", ["admin", "beta"]).
```

### 4.2 Facts

```ebnf
FactDecl    ::= Atom "."
```

Example:

```datalog
rel admin_email(email: Email).

admin_email("alice@example.com").
admin_email("bob@example.com").
```

Meaning:
- `rel admin_email(email: Email).` declares schema: `admin_email : Email → Bool`
- Each `admin_email("...").` declares a tuple in that relation

### 4.3 Rules

```ebnf
RuleDecl    ::= HeadAtom ":-" Body "."
HeadAtom    ::= RelName "(" HeadArgList? ")"
HeadArgList ::= HeadArg ("," HeadArg)*
HeadArg     ::= Term             // may be a typed variable

Body        ::= BodyAtom ("," BodyAtom)*
BodyAtom    ::= Atom | Condition

Condition   ::= Term RelOp Term
RelOp       ::= "=" | "!=" | "<" | ">" | "<=" | ">="
```

Typed variables are allowed in:
- the rule head (`admin_email(e: Email)`)
- the rule body (`purchase(p: Purchase)`, `e: Email = P.email`)

## 5. Base vs Derived Relations

- **Base data (EDB)**: Relations populated via **facts only**
- **Derived data (IDB/views)**: Relations populated via **rules**

Both are still just **relations**:
- `rel` gives the schema
- Facts and rules together define the relation's **extension**

SQL analogy:
- `rel user(...) + facts` ≈ `CREATE TABLE user (...) + INSERT INTO user ...`
- `rel admin_email(...) + rules` ≈ `CREATE VIEW admin_email AS SELECT ...`

## 6. Persistent Rules and the `+` Prefix

### 6.1 Design Principles

Clear separation of concerns:

| Syntax | Purpose | DD Materialization | Type Checking |
|--------|---------|-------------------|---------------|
| `type` | Value type definitions | No | N/A |
| `rel` | Relation schema declaration | **No** | **Yes** |
| `+name(...) :- body.` | Persistent rule (DD view) | **Yes** | Only if `rel` exists |
| `name(...) :- body.` | Session rule (transient) | **No** | Only if `rel` exists |
| `+`/`-` (facts) | Base data manipulation | No | Only if `rel` exists |

### 6.2 Invalid Syntax

The `:=` operator is **not valid syntax**. Use the `+` prefix for persistent rules:

```datalog
+emp_dept(EmpId, DeptName) :-
    employee(EmpId, DeptId),
    department(DeptId, DeptName).
```

With typing + materialization:

```datalog
rel emp_dept(emp_id: Id, dept_name: string).

+emp_dept(EmpId, DeptName) :-
    employee(EmpId, DeptId),
    department(DeptId, DeptName).
```

### 6.3 Persistent Rule Grammar

```ebnf
PersistentRule ::= "+" RuleName "(" ParamList ")" ":-" Body "."
```

### 6.4 Session Rules

Rules without `+` prefix are **session rules**:

```datalog
temp_result(X, Y) :- source(X, Y), X > 10.
```

- Computed on-demand during evaluation
- NOT persisted or incrementally maintained
- Useful for ad-hoc queries

## 7. Implementation Notes

### 7.1 Type Persistence

All typing information needs to be persisted on a database level. The server implements multiple databases where same-named types could have different semantic meanings.

### 7.2 Naming Conventions

To avoid confusion between types and variables:

- **Types**: `UIdent` (capitalized) — `Email`, `User`, `Purchase`
- **Variables**: `LIdent` (lowercase) — `e`, `u`, `p`, `tags`
- **Relations**: `LIdent` (lowercase) — `user`, `purchase`, `admin_email`

### 7.3 Command vs Keyword Distinction

Do NOT confuse:
- `rel` keyword (schema declaration in Datalog)
- `.rel` meta command (server management command)

## 8. Complete Example

```datalog
// Type definitions
type Id: int(range(1, 1000000)).
type Email: string(pattern("^[^@]+@[^@]+$")).

type User: {
    id: Id,
    name: string,
    email: Email
}.

// Relation schemas (typing only)
rel user: User.
rel purchase(user_id: Id, amount: int).

// Base data
+user[(1, "Alice", "alice@example.com"), (2, "Bob", "bob@example.com")].
+purchase[(1, 1500), (1, 200), (2, 300)].

// Persistent rule (explicit DD materialization)
+high_spender(UserId) :-
    user(U),
    purchase(U.id, Amount),
    Amount > 1000,
    UserId = U.id.

// Session rule (not materialized, just computed on query)
rel temp(id: Id).
temp(U.id) :- user(U), high_spender(U.id).

// Query
?- high_spender(X).
```

## 9. Type Checking Algorithm

### 9.1 Rule Head Type Checking

Given:

```datalog
rel admin_email(email: Email).
admin_email(e: Email) :- Body.
```

Type checking steps:
1. Lookup schema: `admin_email` has 1 argument of type `Email`
2. The head argument is `e: Email`
3. Check that annotated type `Email` is compatible with schema type `Email`
4. Add `e : Email` to the environment for checking the body

Type error example:

```datalog
admin_email(e: string) :- ...  // ERROR: string ≠ Email
```

### 9.2 Body Type Checking

Typed variables in the body:

```datalog
high_spender(id: Id) :-
    user(U: User),
    purchase(P: Purchase),
    P.user_id = U.id,
    P.amount  > 1000,
    id = U.id.
```

- `user(U: User)` → expands to `user(U.id, U.name, U.email, U.my_tags)` with `U : User`
- `purchase(P: Purchase)` similarly if `Purchase` is a record type
