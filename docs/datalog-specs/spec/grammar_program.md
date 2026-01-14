# Program Structure

An InputLayer program consists of:

1. **Facts** - Base data stored in relations
2. **Rules** - Definitions that derive new data from existing data
3. **Queries** - Questions that retrieve data

## Grammar

```ebnf
program   ::= statement* ;
statement ::= fact | rule | query | meta_command ;
```

## Example Program

Here's a complete example demonstrating all statement types:

```datalog
% 1. Insert facts (base data)
+human("socrates").
+human("plato").

% 2. Define a rule (derived data)
+mortal(X) :- human(X).

% 3. Query the result
?- mortal("socrates").
```

Output:
```
1 row:
  ()
```

The empty tuple means "yes, Socrates is mortal."

## Statement Types

### Facts

Facts are ground data stored in relations:

```datalog
+edge(1, 2).                           % Single fact
+edge[(1, 2), (2, 3), (3, 4)].         % Bulk insert
-edge(1, 2).                           % Delete fact
```

### Rules

Rules define how to compute derived data:

```datalog
% Session rule (temporary)
path(X, Y) :- edge(X, Y).

% Persistent rule (saved to disk)
+path(X, Y) :- edge(X, Y).
+path(X, Z) :- path(X, Y), edge(Y, Z).
```

### Queries

Queries retrieve data:

```datalog
?- edge(X, Y).              % Get all edges
?- path(1, X).              % Get nodes reachable from 1
?- mortal("socrates").      % Check if fact exists
```

### Meta Commands

System commands for managing the database:

```
.kg create mydb             % Create knowledge graph
.kg use mydb                % Switch to knowledge graph
.rel                        % List relations
.rule                       % List rules
.load file.dl               % Load program from file
```

## Execution Order

Statements execute in order:

1. Facts are inserted/deleted immediately
2. Rules are registered (persistent) or added to session
3. Queries execute against current state

```datalog
+edge(1, 2).                % Insert happens first
+path(X, Y) :- edge(X, Y).  % Rule registered second
?- path(1, X).              % Query runs against current state
% Result: (2)
```

## File Programs

A `.dl` file contains statements that execute sequentially:

```datalog
% graph.dl

% Setup
.kg create example
.kg use example

% Data
+edge[(1, 2), (2, 3), (3, 4)].

% Rules
+path(X, Y) :- edge(X, Y).
+path(X, Z) :- path(X, Y), edge(Y, Z).

% Query
?- path(1, X).
```

Load with:
```
.load graph.dl
```
