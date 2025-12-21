# Record Syntax and Desugaring

This document specifies the record-related syntactic sugar in InputLayer and their desugaring rules.

## Overview

InputLayer supports several surface forms for working with record types. All forms desugar to positional n-ary relations.

## 1. Relation Declaration Desugaring

### 1.1 `rel r: T` Desugaring

Given a record type:

```datalog
type User: {
    id:      Id,
    name:    string(not_empty),
    email:   Email,
    my_tags: Tags
}.
```

The declaration:

```datalog
rel user: User.
```

Desugars to:

```datalog
rel user(
    id:      Id,
    name:    string(not_empty),
    email:   Email,
    my_tags: Tags
).
```

**Key point**: `user` becomes a 4-ary relation, not unary.

## 2. Atom Forms and Desugaring

Given `rel user: User.`, the following surface forms are supported:

### 2.1 Positional Atom (Core Form)

```datalog
user(1, "Alice", "alice@example.com", ["admin", "beta"]).
```

This is the underlying core representation. All other forms desugar to this.

### 2.2 Named-Field Atom

```datalog
user(id = 1,
     name = "Alice",
     email = "alice@example.com",
     my_tags = ["admin", "beta"]).
```

**Desugaring**: Use field order from the record type.

```
user(id = a, name = b, email = c, my_tags = d)
  ⟶ user(a, b, c, d)
```

### 2.3 Record-Literal Atom

```datalog
user({ id = 1,
       name = "Alice",
       email = "alice@example.com",
       my_tags = ["admin", "beta"] }).
```

**Desugaring**:

```
user({ id = a, name = b, email = c, my_tags = d })
  ⟶ user(a, b, c, d)
```

### 2.4 Record-Variable Atom

If `U` is a term of type `User`:

```datalog
user(U).
```

**Desugaring**: Expand to field access.

```
user(U)
  ⟶ user(U.id, U.name, U.email, U.my_tags)
```

Here `.id`, `.name` etc. are field access on the record `U`.

## 3. Record Destructuring Patterns

You can pattern-match records directly in rule bodies.

### 3.1 Grammar

```ebnf
RecordPattern    ::= "{" PatternFieldList "}"
PatternFieldList ::= PatternField ("," PatternField)*
PatternField     ::= FieldName ":" VarPattern
VarPattern       ::= Var | TypedVar
```

### 3.2 Basic Pattern

```datalog
admin_email(e: Email) :-
    user({ id:      id,
           name:    name,
           email:   e,
           my_tags: tags }),
    "admin" in tags.
```

**Desugaring**:

```
user({ id: Id, name: Name, email: E, my_tags: Tags })
  ⟶ user(Id, Name, E, Tags)
```

The body:

```datalog
user({ id: Id, name: Name, email: E, my_tags: Tags }),
"admin" in Tags
```

Desugars to:

```datalog
user(Id, Name, E, Tags),
"admin" in Tags
```

Plus bindings for the variables.

### 3.3 Wildcard Patterns

Use `_` to ignore fields:

```datalog
admin_email(e: Email) :-
    user({ id:      _,
           name:    _,
           email:   e,
           my_tags: tags }),
    "admin" in tags.
```

## 4. Typed Variables in Rules

### 4.1 Motivation

Instead of:

```datalog
admin_email(e) :-
    user({ id: _, name: _, email: e, my_tags: tags }),
    "admin" in tags.
```

You can write:

```datalog
admin_email(e: Email) :-
    user({ id: _, name: _, email: e, my_tags: tags }),
    "admin" in tags.
```

This makes the type of `e` explicit and enables type checking.

### 4.2 Semantics

Typed variables **do not** redefine the relation's schema. They are type annotations checked against the schema.

### 4.3 Typed Variables in Body

```datalog
high_spender(id: Id) :-
    user(U: User),
    purchase(P: Purchase),
    P.user_id = U.id,
    P.amount  > 1000,
    id = U.id.
```

**Desugaring**:

- `user(U: User)` → `user(U.id, U.name, U.email, U.my_tags)` with `U : User`
- `purchase(P: Purchase)` → similarly if `Purchase` is a record type

You don't need to type every variable; it's optional.

## 5. Complete Desugaring Example

### Source

```datalog
type Id:    int(range(1, 1000000)).
type Email: string(pattern("^[^@]+@[^@]+$")).
type Tags:  list[string](not_empty).

type User: {
    id:      Id,
    name:    string(not_empty),
    email:   Email,
    my_tags: Tags
}.

type Purchase: {
    user_id: Id,
    amount:  int
}.

rel user:     User.
rel purchase: Purchase.
rel high_spender(id: Id).

user(id = 1, name = "Alice", email = "alice@example.com", my_tags = ["admin"]).

high_spender(id: Id) :-
    user(U: User),
    purchase(P: Purchase),
    P.user_id = U.id,
    P.amount  > 1000,
    id = U.id.
```

### After Desugaring

```datalog
// Type declarations remain
type Id:    int(range(1, 1000000)).
type Email: string(pattern("^[^@]+@[^@]+$")).
type Tags:  list[string](not_empty).

type User: { ... }.
type Purchase: { ... }.

// rel user: User. desugars to:
rel user(id: Id, name: string(not_empty), email: Email, my_tags: Tags).

// rel purchase: Purchase. desugars to:
rel purchase(user_id: Id, amount: int).

rel high_spender(id: Id).

// Named-field fact desugars to positional:
user(1, "Alice", "alice@example.com", ["admin"]).

// Rule with record variables desugars:
high_spender(id) :-
    user(U_id, U_name, U_email, U_my_tags),  // U: User expanded
    purchase(P_user_id, P_amount),            // P: Purchase expanded
    P_user_id = U_id,
    P_amount > 1000,
    id = U_id.
```

## 6. Desugaring Algorithm

### 6.1 `rel r: T` Desugaring

```
desugar_rel_decl(rel r: T):
    if T is not a record type:
        error("rel r: T requires T to be a record type")

    let { f1: τ1, f2: τ2, ..., fn: τn } = lookup_type(T)
    return rel r(f1: τ1, f2: τ2, ..., fn: τn)
```

### 6.2 Atom Desugaring

```
desugar_atom(r(args), rel_schema):
    if args is a single record-typed variable V:
        // r(V) where V: RecordType
        let fields = get_record_fields(type_of(V))
        return r(V.f1, V.f2, ..., V.fn)

    if args is a record literal { f1=e1, f2=e2, ..., fn=en }:
        let ordered = order_by_schema(rel_schema, [(f1,e1), (f2,e2), ...])
        return r(ordered[0], ordered[1], ..., ordered[n-1])

    if args is named-field (f1=e1, f2=e2, ..., fn=en):
        let ordered = order_by_schema(rel_schema, [(f1,e1), (f2,e2), ...])
        return r(ordered[0], ordered[1], ..., ordered[n-1])

    // Already positional
    return r(args)
```

### 6.3 Record Pattern Desugaring

```
desugar_record_pattern(r({ f1: v1, f2: v2, ..., fn: vn }), rel_schema):
    let ordered = order_by_schema(rel_schema, [(f1,v1), (f2,v2), ...])
    return r(ordered[0], ordered[1], ..., ordered[n-1])
```

## 7. Error Cases

### 7.1 Non-Record Type in `rel r: T`

```datalog
type Email: string(pattern("^[^@]+@[^@]+$")).
rel admin_email: Email.  // ❌ ERROR: Email is not a record type
```

### 7.2 Field Name Mismatch

```datalog
type User: { id: Id, name: string }.
rel user: User.

user(id = 1, unknown_field = "x").  // ❌ ERROR: unknown field
```

### 7.3 Missing Required Fields

```datalog
type User: { id: Id, name: string, email: Email }.
rel user: User.

user(id = 1, name = "Alice").  // ❌ ERROR: missing 'email' field
```

### 7.4 Type Annotation Mismatch

```datalog
rel admin_email(email: Email).

admin_email(e: string) :- ...  // ❌ ERROR: string ≠ Email
```

## 8. Style Guidelines

### 8.1 Naming Conventions

- **Types**: Capitalized (`Email`, `User`, `Purchase`)
- **Variables**: Lowercase (`e`, `u`, `p`, `tags`)
- **Relations**: Lowercase (`user`, `purchase`, `admin_email`)

### 8.2 Idiomatic Usage

Prefer explicit type annotations for clarity:

```datalog
rel admin_email(email: Email).

admin_email(e: Email) :-
    user({ id: _, name: _, email: e, my_tags: tags }),
    "admin" in tags.
```

### 8.3 When to Use Each Form

| Form | Use Case |
|------|----------|
| Positional | Simple relations, performance-critical code |
| Named-field | Complex relations, self-documenting facts |
| Record-literal | When constructing from record values |
| Record-variable | When passing entire records between rules |
| Destructuring | When extracting specific fields in rule bodies |
