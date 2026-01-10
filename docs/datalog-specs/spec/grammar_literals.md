# Literals and Filters

Literals are the building blocks of rule bodies and query conditions. InputLayer supports three types:

| Type | Example | Purpose |
|------|---------|---------|
| **Positive** | `edge(X, Y)` | Match facts |
| **Negative** | `!edge(X, Y)` | Exclude facts |
| **Arithmetic** | `X > 10` | Filter or compute |

## Positive Literals

A positive literal matches facts in a relation:

```datalog
+path(X, Y) :- edge(X, Y).
%               ^^^^^^^^
%               Positive literal
```

All facts in `edge` that match the pattern are included.

## Negative Literals (Negation)

A negative literal excludes matching facts:

```datalog
% Find people who are not managers
+non_manager(X) :- employee(X), !manager(X).
```

### Syntax

Use `!` or `NOT`:

```datalog
alive(X) :- person(X), !dead(X).
alive(X) :- person(X), NOT dead(X).
```

### Safety Rule

Variables in negated literals must also appear in a positive literal:

```datalog
% GOOD: X is bound by employee
+non_manager(X) :- employee(X), !manager(X).

% BAD: X is only in negation
+invalid(X) :- !manager(X).   % ERROR: X is not bound
```

### Stratification

Negation must be **stratifiable**: you can't negate a relation that depends on the relation being defined:

```datalog
% BAD: circular negation
+a(X) :- b(X), !a(X).   % ERROR: a depends on !a
```

## Comparison Operators

Compare values in rule bodies:

| Operator | Meaning |
|----------|---------|
| `=` | Equal |
| `!=` | Not equal |
| `<` | Less than |
| `<=` | Less than or equal |
| `>` | Greater than |
| `>=` | Greater than or equal |

### Examples

```datalog
% Filter by value
+adults(Name) :- person(Name, Age), Age >= 18.

% Range filter
+working_age(Name) :- person(Name, Age), Age >= 18, Age <= 65.

% Inequality
+different(X, Y) :- pair(X, Y), X != Y.
```

### String Comparison

Strings are compared lexicographically:

```datalog
% Names starting before "m"
?- person(Name, _), Name < "m".
```

## Arithmetic Operators

Compute values in rule bodies:

| Operator | Meaning |
|----------|---------|
| `+` | Addition |
| `-` | Subtraction |
| `*` | Multiplication |
| `/` | Division |
| `%` | Modulo |

### Syntax

Use `=` to bind a computed value:

```datalog
+total_cost(Item, Total) :-
    item(Item, Price, Qty),
    Total = Price * Qty.
```

### Examples

```datalog
% Increment
+next(X, Y) :- number(X), Y = X + 1.

% Calculate difference
+gap(X, Y, Diff) :- pair(X, Y), Diff = Y - X.

% Percentage
+profit_margin(Product, Margin) :-
    product(Product, Cost, Price),
    Margin = (Price - Cost) / Price * 100.
```

### Division by Zero

Division by zero returns an error:

```datalog
+ratio(X, Y, R) :- data(X, Y), R = X / Y.
% If Y = 0, this produces an error
```

## Combining Literals

Literals are combined with `,` (AND):

```datalog
+qualified(Name) :-
    employee(Id, Name),        % Positive literal
    !terminated(Id),           % Negative literal
    years_of_service(Id, Y),   % Positive literal
    Y >= 5.                    % Comparison
```

All conditions must be true for a result to be produced.

## Safety Rules Summary

All variables must be "bound" (appear in a positive literal) before use:

| Context | Rule |
|---------|------|
| **Head variables** | Must appear in a positive body literal |
| **Negation variables** | Must appear in a positive body literal |
| **Comparison variables** | Must appear in a positive body literal |
| **Arithmetic variables** | Must appear in a positive body literal |

```datalog
% GOOD: All variables bound
+result(X, Y, Z) :-
    source(X, Y),    % X, Y bound here
    !exclude(X),     % X already bound
    Z = X + Y.       % X, Y already bound

% BAD: Z not bound
+broken(X, Z) :- source(X, _), Z > 0.   % ERROR: Z not bound
```

## Examples

### Finding Differences

```datalog
+in_a_not_b(X) :- a(X), !b(X).
+in_b_not_a(X) :- b(X), !a(X).
```

### Complex Filtering

```datalog
+premium_customer(Id, Name, Spent) :-
    customer(Id, Name),
    orders(Id, Spent),
    Spent > 1000,
    !blacklisted(Id).
```

### Computed Columns

```datalog
+employee_stats(Name, Annual) :-
    employee(_, Name, Monthly),
    Annual = Monthly * 12.
```

### Avoiding Duplicates

```datalog
% Find pairs without duplicates (A < B ensures each pair once)
+unique_pairs(A, B) :-
    items(A), items(B), A < B.
```
