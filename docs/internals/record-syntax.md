# Record Syntax and Desugaring

This document specifies the record-related syntactic sugar in InputLayer and their desugaring rules.

## Overview

InputLayer supports record types as a way to document the structure of data. Record types serve as documentation; the desugaring features described here are planned for future implementation.

## 1. Type Declarations

### 1.1 Simple Type Aliases

```datalog
type Email: string.
type UserId: int.
type Score: int.
```

These declare type aliases that can be used in schema declarations for documentation.

### 1.2 Record Types

```datalog
type User: {
    id:      int,
    name:    string,
    email:   string
}.
```

A **record type** `{ f₁ : τ₁, …, fₙ : τₙ }` documents the structure of a relation.

## 2. Schema Declarations

Schemas are declared using the `+` prefix with typed columns:

```datalog
+user(id: int, name: string, email: string).
```

This declares a 3-ary relation with typed columns. The schema enables:
- Column name documentation
- Type validation (when implemented)
- Constraint declarations

### 2.1 Schema with Type References

You can reference declared types in schemas:

```datalog
type Email: string.
type UserId: int.

+user(id: UserId, name: string, email: Email).
```

## 3. Working with Data

### 3.1 Inserting Facts

```datalog
% Single fact
+user(1, "Alice", "alice@example.com").

% Bulk insert
+user[(1, "Alice", "alice@example.com"), (2, "Bob", "bob@example.com")].
```

### 3.2 Persistent Rules

```datalog
+admin_email(Email) :-
    user(_, _, Email),
    admin(Email).
```

### 3.3 Queries

```datalog
?- user(Id, Name, Email).
?- user(1, Name, _).
```

## 4. Planned Features (Not Yet Implemented)

The following features are documented for future implementation:

### 4.1 Record-Based Schema Sugar

**Planned syntax** (not yet implemented):

```datalog
type User: { id: int, name: string, email: string }.

% Would desugar to: +user(id: int, name: string, email: string).
+user: User.
```

### 4.2 Named-Field Atoms

**Planned syntax** (not yet implemented):

```datalog
+user(id = 1, name = "Alice", email = "alice@example.com").
```

Would desugar to positional form based on schema order.

### 4.3 Record Literal Atoms

**Planned syntax** (not yet implemented):

```datalog
+user({ id = 1, name = "Alice", email = "alice@example.com" }).
```

### 4.4 Record Destructuring Patterns

**Planned syntax** (not yet implemented):

```datalog
admin_email(e) :-
    user({ id: _, name: _, email: e }),
    admin(e).
```

## 5. Style Guidelines

### 5.1 Naming Conventions

- **Types**: Capitalized (`Email`, `User`, `Purchase`)
- **Variables**: Lowercase or capitalized (`e`, `Email`, `user_id`)
- **Relations**: Lowercase (`user`, `purchase`, `admin_email`)

### 5.2 Current Best Practices

Use explicit positional schemas and facts:

```datalog
% Declare schema
+user(id: int, name: string, email: string).

% Insert facts positionally
+user[(1, "Alice", "alice@example.com")].

% Query with positional variables
?- user(Id, Name, Email).
```
