Here’s the full mini-spec in one place, with the `rel r: T` edge case spelled out (only allowed when `T` is a **record** type; otherwise it’s a static error).

---

# 0. Top-level overview

A program is a sequence of declarations:

```ebnf
Decl       ::= TypeDecl | RelDecl | RuleDecl | FactDecl
```

* `type` → defines **value types** (aliases, refinements, record types).
* `rel`  → defines **relations** (tables) over those value types.
* Facts (`FactDecl`) provide **base tuples** for relations.
* Rules (`RuleDecl`) define **derived tuples** for relations.

---

# 1. Types (`type`)

## 1.1. Type declarations

```ebnf
TypeDecl   ::= "type" TypeName ":" TypeExpr "."
TypeName   ::= UIdent          // capitalized, e.g. Email, User
```

```ebnf
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

**Spec wording:**

> A `type` declaration introduces a **value type**.
> Types describe the **shape and constraints of values** but **do not define relations** by themselves.

## 1.2. Aliases and refinements

Examples:

```datalog
type Email: string(pattern("^[^@]+@[^@]+$")).
type Id:    int(range(1, 1000000)).
type Tags:  list[string](not_empty).
```

Interpretation as refinement types:

* `Email ⊆ string`
* `Id ⊆ int`
* `Tags ⊆ list[string]`

Informally:

```text
Γ ⊢ v : Email  ⇒  Γ ⊢ v : string  and  v satisfies pattern("^[^@]+@[^@]+$")
```

The semantics of refinements (`pattern`, `range`, `not_empty`, …) are left to the implementation.

## 1.3. Record types

Example:

```datalog
type User: {
    id:      Id,
    name:    string(not_empty),
    email:   Email,
    my_tags: Tags
}.
```

Spec statement:

> A **record type** `{ f₁ : τ₁, …, fₙ : τₙ }` is the type of records with named fields
> `fᵢ` of types `τᵢ`.

Important:

* `User` is a **value type**.
* A single `User` value is a record with 4 fields.
* This is *not* yet a relation; it’s just a value type.

---

# 2. Relations (`rel`)

A **relation** is like a table: a set of tuples over value types.

## 2.1. Relational schema declarations (core form)

```ebnf
RelDecl    ::= "rel" RelName "(" ParamList? ")" "."
             | "rel" RelName ":" TypeName "."

RelName    ::= LIdent           // e.g. user, high_spender

ParamList  ::= Param ("," Param)*
Param      ::= ParamName ":" TypeExpr
ParamName  ::= LIdent
```

Example:

```datalog
rel user(
    id:      Id,
    name:    string(not_empty),
    email:   Email,
    my_tags: Tags
).
```

Spec wording:

> This declares a 4-ary relation
>
> ```text
> user : Id × string(not_empty) × Email × Tags → Bool
> ```
>
> i.e. `user ⊆ Id × string(not_empty) × Email × Tags`.

Each parameter is a **column** in the relation.

## 2.2. Record-type schema sugar: `rel r: T` (record types only)

We want:

```datalog
type User: {
    id:      Id,
    name:    string(not_empty),
    email:   Email,
    my_tags: Tags
}.

rel user: User.
```

to mean:

> "`user` is a relation whose columns are exactly the fields of the `User` record type."

**Desugaring rule**

If we have:

```datalog
type T: { f1: τ1, f2: τ2, ..., fn: τn }.
```

then

```datalog
rel r: T.
```

is syntactic sugar for:

```datalog
rel r(
    f1: τ1,
    f2: τ2,
    ...,
    fn: τn
).
```

So your example:

```datalog
rel user: User.
```

desugars to:

```datalog
rel user(
    id:      Id,
    name:    string(not_empty),
    email:   Email,
    my_tags: Tags
).
```

Mathematically:

```text
user ⊆ Id × string(not_empty) × Email × Tags
```

**Key point:**

* `user` is a **4-ary** relation, not unary.
* The record type is used as a **schema template**.

## 2.3. Edge case: `rel r: T` with non-record `T` is a static error

If `T` is not a record type (i.e. `T` does **not** have the form `{ f₁: τ₁, …, fₙ: τₙ }`), then:

```datalog
rel r: T.
```

is **not allowed**.

Example:

```datalog
type Email: string(pattern("^[^@]+@[^@]+$")).

-- ❌ Illegal: Email is a simple (non-record) type
rel admin_email: Email.
```

Reason: a relation needs named columns, and we do **not** invent implicit column names like `value` or `_1`.

Instead you must explicitly name the parameter:

```datalog
-- ✅ Legal, with explicit column name
rel admin_email(email: Email).
```

If you really want to use the record sugar, you can wrap it:

```datalog
type AdminEmailRow: { email: Email }.

rel admin_email: AdminEmailRow.  // OK, desugars to rel admin_email(email: Email).
```

Static rule:

> For `rel r: T.` to be well-typed sugar, `T` must be a previously declared **record type**.
> If `T` is a simple type (base type, alias, list, etc.), the declaration is a **static error**.

---

# 3. Terms, records, and field access

We need term syntax for rules and sugar.

## 3.1. Terms and variables (with types)

We extend terms with:

* variables (`VarName`)
* typed variables (`VarName ":" TypeExpr`)
* record literals
* field access

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

## 3.2. Record literals

```ebnf
RecordLiteral    ::= "{" RecordFieldList "}"
RecordFieldList  ::= RecordField ("," RecordField)*
RecordField      ::= FieldName "=" Term
```

Example:

```datalog
{ id = 1, name = "Alice", email = "alice@example.com", my_tags = ["admin"] }
```

Typing rule (informal):

> If each `tᵢ` has type `τᵢ` in context `Γ`,
> then `Γ ⊢ { f₁ = t₁, …, fₙ = tₙ } : { f₁ : τ₁, …, fₙ : τₙ }`.

## 3.3. Field access (`.`)

Field access:

* If `t` has a record type `{ f₁: τ₁, …, fk: τk, … }`,
* then `t.fk` is a term of type `τk`.

Typing rule (informal):

> If `Γ ⊢ t : { f₁ : τ₁, …, fk : τk, … }`
> then `Γ ⊢ t.fk : τk`.

**Typed variables**

* `x: τ` is a term standing for a variable `x` with declared type `τ`.
* Typing rule: if `x: τ` appears, add `x : τ` to the environment `Γ`.

---

# 4. Atoms, facts, rules

## 4.1. Atoms

Atoms are predicate applications:

```ebnf
Atom        ::= RelName "(" ArgList? ")"
ArgList     ::= Arg ("," Arg)*
Arg         ::= Term
```

Example (positional):

```datalog
user(1, "Alice", "alice@example.com", ["admin", "beta"]).
```

## 4.2. Facts

A fact is just an atom ending with a period:

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

* `rel admin_email(email: Email).` declares schema: `admin_email : Email → Bool`.
* Each `admin_email("...").` declares a tuple in that relation.

Mathematically, with those facts only:

```text
admin_email = { "alice@example.com", "bob@example.com" }
```

## 4.3. Rules (with typed variables)

Rule syntax:

```ebnf
RuleDecl    ::= HeadAtom ":-" Body "."
HeadAtom    ::= RelName "(" HeadArgList? ")"
HeadArgList ::= HeadArg ("," HeadArg)*
HeadArg     ::= Term             // may be a typed variable

Body        ::= BodyAtom ("," BodyAtom)*
BodyAtom    ::= Atom | Condition

Condition   ::= Term RelOp Term
             | PredicateTerm

RelOp       ::= "=" | "!=" | "<" | ">" | "<=" | ">="
PredicateTerm ::= ...            // e.g. "admin" in tags, implementation-defined
```

Typed variables are allowed in:

* the rule head (`admin_email(e: Email)`)
* the rule body (`purchase(p: Purchase)`, `e: Email = P.email`)

They are just variables with explicit type annotations, checked against:

* relation schemas (`rel` declarations),
* type declarations (`type`),
* and record fields.

---

# 5. Base vs derived relations (facts vs rules)

Important semantic concept:

* **Base data (EDB)**:

  * Relations populated via **facts only**.
  * Example: `user(...)`, `purchase(...)` facts.

* **Derived data (IDB / views)**:

  * Relations populated via **rules** (and possibly no facts).
  * Example: `high_spender`, `admin_email`.

Both are still just **relations**:

* `rel` gives the schema.
* Facts and rules together define the relation’s **extension** (its set of tuples).

Analogy with SQL:

* `rel user(...)`  + facts ≈ `CREATE TABLE user (...)` + `INSERT INTO user ...`.
* `rel admin_email(...)` + rules ≈ `CREATE VIEW admin_email AS SELECT ...`.

---

# 6. Record-related sugar for relations

Assume:

```datalog
type User: {
    id:      Id,
    name:    string(not_empty),
    email:   Email,
    my_tags: Tags
}.

rel user: User.  // desugars to 4-ary schema
```

## 6.1. Allowed atom forms

Given `rel user: User.`, we support several **surface** forms.

### (1) Positional atom (core)

```datalog
user(1, "Alice", "alice@example.com", ["admin", "beta"]).
```

This is the underlying core; all other forms desugar to it.

### (2) Named-field atom

```datalog
user(id = 1,
     name = "Alice",
     email = "alice@example.com",
     my_tags = ["admin", "beta"]).
```

**Desugaring:**

* Use the field order from the record type `User`.
* `user(id = a, name = b, email = c, my_tags = d)`
  ⟶ `user(a, b, c, d)`.

### (3) Record-literal atom

```datalog
user({ id = 1,
       name = "Alice",
       email = "alice@example.com",
       my_tags = ["admin", "beta"] }).
```

**Desugaring:**

* `user({ id = a, name = b, email = c, my_tags = d })`
  ⟶ `user(a, b, c, d)`.

### (4) Record-variable atom

If `U` is a term of type `User`:

```datalog
user(U).
```

**Desugaring:**

* If `User = { id: Id, name: ..., email: ..., my_tags: ... }`:
* `user(U)` ⟶ `user(U.id, U.name, U.email, U.my_tags)`.

Here `.id`, `.name` etc. are just **field access** on the record `U`.

---

# 7. Record destructuring patterns in rule bodies

You can also **pattern-match** records directly when using `rel r: T` in a rule body.

## 7.1. Record patterns

Grammar:

```ebnf
RecordPattern    ::= "{" PatternFieldList "}"
PatternFieldList ::= PatternField ("," PatternField)*
PatternField     ::= FieldName ":" VarPattern
VarPattern       ::= Var | TypedVar
```

Example:

```datalog
rel admin_email(email: Email).

admin_email(e: Email) :-
    user({ id: id: Id,
           name: name,
           email: e,
           my_tags: tags }),
    "admin" in tags.
```

More readable (often omitting nested type annotations):

```datalog
admin_email(e: Email) :-
    user({ id:      id,
           name:    name,
           email:   e,
           my_tags: tags }),
    "admin" in tags.
```

**Desugaring:**

* `user({ id: Id, name: Name, email: E, my_tags: Tags })`
  ⟶ `user(Id, Name, E, Tags)`.

So the body:

```datalog
user({ id: Id, name: Name, email: E, my_tags: Tags }),
"admin" in Tags
```

desugars to:

```datalog
user(Id, Name, E, Tags),
"admin" in Tags
```

plus bindings for the variables.

You can use `_` to ignore fields:

```datalog
admin_email(e: Email) :-
    user({ id: _,
           name: _,
           email: e,
           my_tags: tags }),
    "admin" in tags.
```

---

# 8. Typed variables in rules (e.g. `e: Email`)

## 8.1. Motivation

Instead of:

```datalog
rel admin_email(email: Email).

admin_email(e) :-
    user({ id: _, name: _, email: e, my_tags: tags }),
    "admin" in tags.
```

you can write:

```datalog
rel admin_email(email: Email).

admin_email(e: Email) :-
    user({ id: _, name: _, email: e, my_tags: tags }),
    "admin" in tags.
```

to make the type of `e` explicit.

This **does not** redefine the relation’s schema. It’s a type annotation checked against the schema.

## 8.2. Typing rule for rule heads

Given:

```datalog
rel admin_email(email: Email).
```

and rule:

```datalog
admin_email(e: Email) :- Body.
```

Type checking:

1. Lookup schema: `admin_email` has 1 argument of type `Email`.
2. The head argument is `e: Email`.
3. Check that annotated type `Email` is compatible with the schema type `Email`.
4. Add `e : Email` to the environment for checking the body.

If you wrote:

```datalog
admin_email(e: string) :- ...
```

this is a **type error**, because it disagrees with:

```datalog
rel admin_email(email: Email).
```

You can define “compatible” as “equal” for simplicity.

## 8.3. Typed variables in the body

Typed vars are also allowed in the body:

```datalog
high_spender(id: Id) :-
    user(U: User),
    purchase(P: Purchase),
    P.user_id = U.id,
    P.amount  > 1000,
    id = U.id.
```

Desugaring:

* `user(U: User)` → `user(U.id, U.name, U.email, U.my_tags)` with `U : User`.
* `purchase(P: Purchase)` similarly if `Purchase` is a record type.

You **don’t need** to type every variable; it’s optional and mainly for clarity and better error messages.

## 8.4. Style guidelines

To avoid confusion between types and variables:

* **Types**: `UIdent` (capitalized) — `Email`, `User`, `Purchase`.
* **Variables**: `LIdent` (lowercase) — `e`, `u`, `p`, `tags`.

Idiomatic:

```datalog
rel admin_email(email: Email).

admin_email(e: Email) :-
    user({ id: _, name: _, email: e, my_tags: tags }),
    "admin" in tags.
```

---

# 9. Examples putting it all together

## 9.1. User and purchase with a derived `high_spender` relation

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
user(id = 2, name = "Bob",   email = "bob@example.com",   my_tags = ["user"]).

purchase(user_id = 1, amount = 1500).
purchase(user_id = 1, amount = 200).
purchase(user_id = 2, amount = 300).

high_spender(id: Id) :-
    user(U: User),
    purchase(P: Purchase),
    P.user_id = U.id,
    P.amount  > 1000,
    id = U.id.
```

Desugaring (schematically):

* `rel user: User.` → 4-ary `user(id, name, email, my_tags)`.
* `user(U: User)` → `user(U.id, U.name, U.email, U.my_tags)` with `U : User`.
* `purchase(P: Purchase)` → `purchase(P.user_id, P.amount)` with `P : Purchase`.

Result:

* `high_spender(1)` will be in the relation; `high_spender(2)` will not.

NOTE: The following case where we define a relation and a anonymous record type: 
```rel user: { id: Id, name: string, email: Email }.``` 
Should also be fully supported. 

NOTE: All typing information needs to be persisted on a database level. Server will implemented multiple databases where same named types could technically have different semantic meaning.

NOTE: DO NOT MIX and mistake the rel keyword from .rel server management command.

## 9.2. Admin email as a derived relation (view)

```datalog
type Email: string(pattern("^[^@]+@[^@]+$")).

rel admin_email(email: Email).

admin_email(e: Email) :-
    user({ id: _,
           name: _,
           email: e,
           my_tags: tags }),
    "admin" in tags.
```

Interpretation:

* Schema (from `rel`): `admin_email : Email → Bool`.
* Rule: for every `user` row whose `my_tags` contains `"admin"`, take its `email` field and put it into `admin_email`.

With facts:

```datalog
user(id = 1, name = "Alice", email = "alice@example.com", my_tags = ["admin"]).
user(id = 2, name = "Bob",   email = "bob@example.com",   my_tags = ["user"]).
```

The **meaning** of `admin_email` is:

```datalog
admin_email("alice@example.com").
```

even though that fact is **not** written explicitly; it’s derived.

---

# 10. Conceptual summary

* `type` introduces **value types** (primitive, refined, record).
* `rel` introduces a **relation schema** (arity and column types).

  * `rel r(…)` is the core form with explicit parameter names.
  * `rel r: T.` is **record sugar** that expands **only** when `T` is a record type.
  * `rel r: T.` with non-record `T` is a **static error**.
* A relation’s **extension** (set of tuples) comes from:

  * **facts** (base rows, EDB),
  * and **rules** (derived/view rows, IDB).
* Record and named-field constructs are **surface syntax** over n-ary relations:

  * `rel r: T.` expands a record type into positional columns.
  * Named-field atoms, record-literal atoms, and `r(x)` with record variables all desugar to positional atoms.
* `.` is field access on record-typed terms.
* Record destructuring in rule bodies (`r({ f: x, ... })`) is sugar that binds variables to positional arguments.
* Typed variables (`x: τ`) are allowed in heads and bodies:

  * Heads: annotations must match the `rel` schema.
  * Bodies: annotations must match term/record types.
  * They don’t change semantics; they just make types explicit and checked.


Here’s a clean extra section you can tack onto the spec.

---

# 11. Views, removal of `:=`, and separation of concerns

This section introduces the explicit `view` keyword for materialized views and clarifies the separation between `rel` (typing) and `view` (DD materialization).

## 11.1. Design Principles

We introduce a clear separation of concerns:

* **`type`** — Defines **value types** (aliases, refinements, records)
* **`rel`** — Defines **relation schemas** for type enforcement only (no materialization)
* **`view`** — **Explicitly** creates DD materialized views with incremental maintenance
* **Rules `:-`** — Define derived data; type-checked against schemas
* **Facts** — Base data inserted into relations

This design makes the intent **explicit**:
- If you want type checking, use `rel`
- If you want DD materialization, use `view`
- Both can be combined when you want both typing AND materialization

## 11.2. Removal of the `:=` operator

The legacy syntax for persistent views:

```datalog
emp_dept(EmpId, DeptName) :=
    employee(EmpId, DeptId),
    department(DeptId, DeptName).
```

is **no longer part of the language**.

* `:=` does not appear in the grammar.
* Any program using `:=` is **rejected** by the parser/typechecker.
* All shipped examples (e.g. in `examples/datalog`) MUST be updated to use the `view` keyword and MUST NOT use `:=`.

Migration:

* Old:

  ```datalog
  emp_dept(EmpId, DeptName) :=
      employee(EmpId, DeptId),
      department(DeptId, DeptName).
  ```

* New (using explicit `view` keyword):

  ```datalog
  view emp_dept(emp_id: int, dept_name: string) :-
      employee(emp_id, dept_id),
      department(dept_id, dept_name).
  ```

* New (with typing + materialization):

  ```datalog
  rel emp_dept(emp_id: Id, dept_name: string).

  view emp_dept :-
      employee(emp_id, dept_id),
      department(dept_id, dept_name).
  ```

## 11.3. The `rel` keyword — Typing Enforcement Only

The `rel` keyword declares a relation schema for **type checking purposes only**. It does NOT create a DD materialized view.

```ebnf
RelDecl    ::= "rel" RelName "(" ParamList? ")" "."
             | "rel" RelName ":" TypeName "."
```

### 11.3.1. Purpose of `rel`

* Declares the **arity and types** of a relation's columns
* Enables **type checking** for facts and rules that reference this relation
* Does NOT trigger DD materialization — it's purely declarative typing

### 11.3.2. Base relations with `rel`

```datalog
rel employee(emp_id: Id, dept_id: Id).

+employee[(1, 10), (2, 20)].
```

* `rel` declares the schema
* Facts are inserted using `+` operator
* Type checking ensures inserted facts match the schema
* This is a **base relation** (EDB), not a materialized view

### 11.3.3. Rules type-checked against `rel`

```datalog
rel employee(emp_id: int, dept_id: int).
rel high_earner(emp_id: int).

// Type-checked rule (session/query rule, not materialized)
high_earner(E) :- employee(E, _), salary(E, S), S > 100000.
```

* Both relations have schemas declared via `rel`
* Rules referencing them are type-checked
* But the rule itself is NOT materialized (it's a session rule)

## 11.4. The `view` keyword — Explicit DD Materialization

The `view` keyword explicitly creates a DD (Differential Dataflow) materialized view.

```ebnf
ViewDecl   ::= "view" ViewName "(" ParamList? ")" ":-" Body "."
             | "view" ViewName ":-" Body "."       // uses schema from prior `rel`
```

### 11.4.1. Standalone view (with inline schema)

```datalog
view emp_dept(emp_id: int, dept_name: string) :-
    employee(emp_id, dept_id),
    department(dept_id, dept_name).
```

* Creates a DD materialized view named `emp_dept`
* Schema is specified inline: `(emp_id: int, dept_name: string)`
* Incremental maintenance: when `employee` or `department` changes, `emp_dept` updates

### 11.4.2. View with prior `rel` schema

```datalog
rel emp_dept(emp_id: Id, dept_name: string).

view emp_dept :-
    employee(emp_id, dept_id),
    department(dept_id, dept_name).
```

* `rel` declares the typed schema
* `view` references the same name, using the `rel` schema
* Both type checking AND DD materialization are enabled

### 11.4.3. Multiple rules for one view

```datalog
view reachable(x: int, y: int) :-
    edge(x, y).

view reachable(x: int, y: int) :-
    edge(x, z),
    reachable(z, y).
```

* Multiple `view` declarations with the same name define multiple rules
* All rules contribute to the same materialized view
* Supports recursion (transitive closure, etc.)

## 11.5. Session Rules — Query-Only (No Persistence)

Rules without a `view` declaration are **session rules**:

```datalog
// Just a rule, no 'view' keyword
temp_result(X, Y) :- source(X, Y), X > 10.
```

* Computed on-demand during evaluation
* NOT persisted or incrementally maintained
* Useful for ad-hoc queries in the REPL or scripts

### 11.5.1. Session rules with `rel` typing

```datalog
rel temp_result(x: int, y: int).

temp_result(X, Y) :- source(X, Y), X > 10.
```

* `rel` provides type checking
* But without `view`, it's still a session rule (not materialized)

## 11.6. Summary: Keywords and Their Purposes

| Keyword | Purpose | DD Materialization | Type Checking |
|---------|---------|-------------------|---------------|
| `type`  | Value type definitions | No | N/A |
| `rel`   | Relation schema declaration | **No** | **Yes** |
| `view`  | DD materialized view creation | **Yes** | Only if `rel` exists |
| `:-` (rule) | Derived data definition | Only if `view` | Only if `rel` exists |
| `+`/`-` (facts) | Base data manipulation | No | Only if `rel` exists |

## 11.7. Full Example

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

// Materialized view (explicit DD)
view high_spender(user_id: Id) :-
    user(U),
    purchase(U.id, Amount),
    Amount > 1000.

// Session rule (not materialized, just computed on query)
rel temp(id: Id).
temp(U.id) :- user(U), high_spender(U.id).

// Query
?- high_spender(X).
```

## 11.8. Migration Guide

| Old Syntax | New Syntax |
|------------|------------|
| `r(X, Y) := body.` | `view r(x: type, y: type) :- body.` |
| Multiple `:=` rules | Multiple `view r(...) :- ...` declarations |
| No typing | Add `rel` declaration for type checking |

---

**Key Takeaways:**

* `:=` is **removed** from the language
* `rel` = typing enforcement (schema declaration)
* `view` = explicit DD materialization (replaces `:=`)
* Rules `:-` without `view` = session rules (not persisted)
* Combine `rel` + `view` for both typing and materialization


