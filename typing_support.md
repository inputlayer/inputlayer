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

# 11. Persistent views, removal of `:=`, and “rule-only” queries

This section replaces the legacy `:=` operator and clarifies when a relation is **persisted/materialized** vs. just a **query**.

## 11.1. Removal of the `:=` operator

The legacy syntax for persistent views:

```datalog
emp_dept(EmpId, DeptName) :=
    employee(EmpId, DeptId),
    department(DeptId, DeptName).
```

is **no longer part of the language**.

* `:=` does not appear in the grammar.
* Any program using `:=` is **rejected** by the parser/typechecker.
* All shipped examples (e.g. in `examples/datalog`) MUST be updated to the new `rel` + `:-` style and MUST NOT use `:=`.

Migration:

* Old:

  ```datalog
  emp_dept(EmpId, DeptName) :=
      employee(EmpId, DeptId),
      department(DeptId, DeptName).
  ```

* New (persistent/materialized):

  ```datalog
  rel emp_dept(emp_id: Id, dept_name: string).

  emp_dept(emp_id: Id, dept_name: string) :-
      employee(emp_id, dept_id),
      department(dept_id, dept_name).
  ```

The new form has the **same logical meaning** and **same materialized behavior** as the old `:=`.

## 11.2. When a relation is a persistent/materialized view

We distinguish:

* **Base relations**: `rel` + facts only.
* **Derived relations**: `rel` + rules (persistent/materialed view implemented with differential dataflow).
* **Rule-only queries**: rules without a preceding `rel`.

### 11.2.1. Base relations

```datalog
rel employee(emp_id: Id, dept_id: Id).

employee(1, 10).
employee(2, 20).
```

*Extension is exactly the set of facts provided.*

### 11.2.2. Persistent (materialized) views

**Definition (persistent view):**

Let there be a declaration:

```datalog
rel r(p1: τ1, ..., pn: τn).
```

and at least one rule with `r` in the head:

```datalog
r(t1₁, ..., t1ₙ) :- Body1.
...
r(tm₁, ..., tmₙ) :- Bodym.
```

Then:

1. The **logical extension** of `r` is the least fixed point of these rules over the database, as in standard Datalog.
2. The implementation MUST treat `r` as a **persistent/materialized view**:

   * The tuples of `r` are stored as a materialized relation.
   * Changes to underlying base relations are propagated so that `r`’s stored contents always match the least fixed point.

This is the **direct replacement** for the old `r(...) := Body` semantics.

Example:

```datalog
rel emp_dept(emp_id: Id, dept_name: string).

emp_dept(emp_id: Id, dept_name: string) :-
    employee(emp_id, dept_id),
    department(dept_id, dept_name).
```

* `emp_dept` is declared via `rel`.
* It has a rule head.
* Therefore it is a **persistent/materialized view**.

(If you also add facts for `emp_dept`, the extension is `facts ∪ derived-tuples`, still maintained persistently.)

## 11.3. Rule-only definitions: queries that are *not* persisted

Rules do **not** require a prior `rel` declaration in the grammar:

```ebnf
RuleDecl ::= HeadAtom ":-" Body "."
```

So you can still write things like:

```datalog
emp_dept(emp_id: Id, dept_name: string) :-
    employee(emp_id, dept_id),
    department(dept_id, dept_name).
```

even if there is **no** preceding:

```datalog
rel emp_dept(...).
```

### 11.3.1. Semantics of rule-only heads

If a predicate `p` appears in rule heads but has **no** `rel` declaration:

```datalog
p(... ) :- Body.
```

then:

1. `p` is treated as a **query-only derived relation**:

   * It exists only for the duration/scope of evaluation.
   * The engine may compute its extension on demand.
2. The system MUST **not** treat `p` as a persistent/materialized view:

   * No stored table is created for `p`.
   * No incremental maintenance is performed as other relations change.
3. Types for the head arguments are inferred from the body and local annotations (e.g., `x: τ`), but there is no global schema for `p`.

This is the “rule as query” mode:

* You can quickly define ad-hoc views in code or at the REPL.
* But if you want the result to be **persisted/materialized** (like the old `:=`), you **must** add a `rel` declaration first.

### 11.3.2. Example: query vs persistent view

**Query-only (not persisted):**

```datalog
-- No 'rel emp_dept(...)' here

emp_dept(emp_id: Id, dept_name: string) :-
    employee(emp_id, dept_id),
    department(dept_id, dept_name).
```

* `emp_dept` is just a query-defined predicate.
* Engine can compute `emp_dept` results and return them.
* No table for `emp_dept` is stored in the database; it’s not maintained incrementally.

**Persistent/materialized:**

```datalog
rel emp_dept(emp_id: Id, dept_name: string).

emp_dept(emp_id: Id, dept_name: string) :-
    employee(emp_id, dept_id),
    department(dept_id, dept_name).
```

* Same rule body.
* But now `emp_dept` is declared via `rel`, so:

  * It is a **named relation** with a fixed schema.
  * It is **materialized and kept up to date**, exactly as the old `emp_dept(...) := ...` used to be.

---

**Summary of this section**

* `:=` is removed from the language; examples must be rewritten to `rel + rule`.
* A predicate in rule heads **without** a prior `rel` declaration is allowed, but:

  * It defines a **query-only** derived relation.
  * It is **not persisted/materialized**.
* A predicate with a `rel` declaration **and** at least one rule head:

  * Is a **persistent/materialized view**.
  * Has semantics equivalent to the legacy `r(...) := Body`.


