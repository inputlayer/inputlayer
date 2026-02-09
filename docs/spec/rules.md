# Rules

Rules define derived relations (views) by computing new facts from existing data. InputLayer has two types of rules:

| Type | Syntax | Persistence |
|------|--------|-------------|
| **Session rule** | `head :- body.` | Temporary, cleared on disconnect |
| **Persistent rule** | `+head :- body.` | Saved to disk, survives restarts |

## Session Rules

Session rules exist only for your current connection:

```datalog
% Define a session rule
ancestor(X, Y) :- parent(X, Y).
ancestor(X, Z) :- ancestor(X, Y), parent(Y, Z).

% Use it immediately
?- ancestor("alice", X).

% Rule is gone when you disconnect
```

Session rules are useful for:
- Ad-hoc analysis
- Experimenting with rule definitions
- One-time computations

### Managing Session Rules

```
.session              % List all session rules
.session clear        % Clear all session rules
.session drop 1       % Drop session rule #1
```

## Persistent Rules

Persistent rules are saved to disk and automatically loaded:

```datalog
% Create a persistent rule (saved)
+path(X, Y) :- edge(X, Y).
+path(X, Z) :- path(X, Y), edge(Y, Z).

% Works immediately and after restart
?- path(1, X).
```

Persistent rules are stored in `{data_dir}/{kg}/rules/catalog.json`.

### Multi-Clause Rules

A view can be defined by multiple rules (clauses):

```datalog
% First clause: base case
+ancestor(X, Y) :- parent(X, Y).

% Second clause: recursive case
+ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).
```

Both clauses define `ancestor`. Results are the union of all matching clauses.

### Deleting Persistent Rules

Remove a persistent rule with `-`:

```datalog
-path(X, Y).    % Removes all rules defining 'path'
```

Or use meta commands:

```
.rule drop path        % Drop 'path' rule
.rule clear path       % Clear for re-registration
```

## Rule Structure

A rule has two parts:

```
head(Variables) :- body.
     ↑              ↑
  What to         What must
  produce         be true
```

### Head

The head is an atom that defines what the rule produces:

```datalog
+path(X, Y) :- edge(X, Y).
% ^^^^^^^^
% Head: produces facts for 'path' relation
```

The head must use a **lowercase** predicate name.

### Body

The body specifies conditions using literals separated by commas (AND):

```datalog
+employee_in_dept(Name, Dept) :- employee(Id, Name), works_in(Id, Dept).
%                                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
%                                Body: both conditions must match
```

## Grammar

```ebnf
session_rule    ::= head ":-" body "." ;
persistent_rule ::= "+" head ":-" body "." ;
delete_rule     ::= "-" predicate "(" variables ")" "." ;
head            ::= predicate "(" term ("," term)* ")" ;
body            ::= literal ("," literal)* ;
```

## Body Literals

The body can contain:

| Literal | Example | Meaning |
|---------|---------|---------|
| Positive atom | `edge(X, Y)` | Relation must have matching fact |
| Negated atom | `!edge(X, Y)` | Relation must NOT have matching fact |
| Comparison | `X > 10` | Condition must be true |
| Arithmetic | `Z = X + Y` | Compute and bind value |
| Aggregation | `count<X>` | Aggregate over variable |

## Recursion

Rules can reference themselves (recursion):

```datalog
% Transitive closure - find all reachable nodes
+reach(X, Y) :- edge(X, Y).
+reach(X, Z) :- reach(X, Y), edge(Y, Z).
```

InputLayer handles recursion automatically. It keeps computing until no new facts are found (fixpoint).

### Mutual Recursion

Rules can reference each other:

```datalog
+even(0).
+odd(X) :- even(Y), succ(Y, X).
+even(X) :- odd(Y), succ(Y, X).
```

## Variable Safety Rules

All variables in the **head** must appear in a **positive** body literal:

```datalog
% GOOD: X appears in edge(X, Y)
+start_nodes(X) :- edge(X, Y).

% BAD: X only in head, not in positive body literal
+broken(X) :- edge(A, B).   % ERROR: X is unbound
```

Variables in **negations** or **comparisons** must also appear in a positive literal:

```datalog
% GOOD: X is bound by edge, then filtered
+filtered(X) :- edge(X, Y), X > 5.

% BAD: X only in comparison
+broken(X) :- Y > 5.   % ERROR: X unbound, Y only in comparison
```

## Common Patterns

### Filter

```datalog
+adults(Name, Age) :- person(Name, Age), Age >= 18.
```

### Join

```datalog
+employee_dept(Name, DeptName) :-
    employee(Id, Name),
    works_in(Id, DeptId),
    department(DeptId, DeptName).
```

### Set Difference

```datalog
+not_in_b(X) :- a(X), !b(X).
```

### Aggregation

```datalog
+dept_count(Dept, count<Id>) :- works_in(Id, Dept).
```

## Examples

### Social Network

```datalog
% Facts
+follows[(1, 2), (2, 3), (3, 4), (2, 4)].

% Find who user 1 can reach (transitive)
+can_reach(X, Y) :- follows(X, Y).
+can_reach(X, Z) :- can_reach(X, Y), follows(Y, Z).

% Query
?- can_reach(1, X).
```

### Access Control

```datalog
% Facts
+user_role[("alice", "admin"), ("bob", "viewer")].
+role_perm[("admin", "read"), ("admin", "write"), ("viewer", "read")].

% Rule: user has permission via role
+has_perm(User, Perm) :- user_role(User, Role), role_perm(Role, Perm).

% Query
?- has_perm("alice", Perm).
```

### Graph Analysis

```datalog
% Facts
+edge[(1, 2), (2, 3), (3, 1), (4, 5)].

% Nodes in same strongly connected component
+same_scc(X, Y) :- reach(X, Y), reach(Y, X).
+reach(X, Y) :- edge(X, Y).
+reach(X, Z) :- reach(X, Y), edge(Y, Z).

% Query
?- same_scc(1, X).
```
