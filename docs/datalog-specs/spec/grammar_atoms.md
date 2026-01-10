# Atoms and Terms

Atoms are the building blocks of facts, rules, and queries.

## Atoms

An atom is a relation name (predicate) followed by arguments in parentheses:

```datalog
edge(1, 2)
person("alice", 30)
has_permission(User, "read")
```

### Grammar

```ebnf
atom      ::= predicate "(" term ("," term)* ")" ;
predicate ::= [a-z][a-zA-Z0-9_]* ;
```

### Predicate Names

Predicates (relation names) must:
- Start with a **lowercase letter**
- Contain only letters, digits, and underscores

```datalog
edge          % Valid
my_relation   % Valid
edge2         % Valid
Edge          % Invalid - starts with uppercase
2edge         % Invalid - starts with digit
```

## Terms

A term is a single value or variable within an atom:

```datalog
edge(X, 2)
%    ^  ^
%    |  +-- constant term (value)
%    +----- variable term
```

### Grammar

```ebnf
term ::= variable | constant | placeholder | aggregate ;
```

## Variables

Variables start with an **uppercase letter** and match any value:

```datalog
X
Name
MyVariable
Variable_1
```

### Grammar

```ebnf
variable ::= [A-Z][a-zA-Z0-9_]* ;
```

### Variable Binding

When a variable appears multiple times, all occurrences must have the same value:

```datalog
% X must be the same in both positions
?- edge(X, Y), edge(Y, X).   % Finds bidirectional edges

% Same variable name = same value
?- edge(X, X).               % Finds self-loops
```

## Placeholder (Wildcard)

The underscore `_` is a special placeholder that matches any value but doesn't bind:

```datalog
% Get source nodes, ignore destination
?- edge(X, _).

% Multiple underscores are independent
?- triple(_, X, _).  % Only care about middle column
```

Each `_` is independent - they don't need to match the same value.

## Constants

Constants are literal values. See [Data Types](grammar_constants.md) for details.

```datalog
1                  % Integer
3.14               % Float
"hello"            % String
true               % Boolean
[1.0, 2.0, 3.0]    % Vector
alice              % Atom (unquoted string)
```

## Aggregates

Aggregate terms compute values over groups:

```datalog
count<X>           % Count occurrences of X
sum<Amount>        % Sum of Amount values
min<Age>           % Minimum Age
max<Score>         % Maximum Score
avg<Price>         % Average Price
```

See [Extensions](extensions.md#aggregations) for details.

## Examples

### Atom Types in Context

```datalog
% Fact: all terms are constants
+person("alice", 30).

% Rule head: terms can be variables
+adult(Name) :- person(Name, Age), Age >= 18.

% Query: mix of constants and variables
?- person("alice", Age).
```

### Variable Usage

```datalog
% Same variable binds same value
+edge[(1, 2), (2, 3), (2, 1)].
?- edge(X, Y), edge(Y, X).   % X=1,Y=2 and X=2,Y=1

% Different variables can have same or different values
?- edge(X, Y).               % All edges

% Underscore ignores value
?- edge(X, _).               % All source nodes
```

### Ground vs Non-Ground Atoms

A **ground atom** has all constants (no variables):
```datalog
edge(1, 2)           % Ground
person("alice", 30)  % Ground
```

A **non-ground atom** has variables:
```datalog
edge(X, Y)           % Non-ground
person(Name, 30)     % Non-ground
```

Facts must be ground. Rule heads and bodies can be non-ground.
