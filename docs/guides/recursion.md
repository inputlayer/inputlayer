# Tutorial: Recursion

Recursive rules let you express transitive closure, reachability, and graph algorithms that would require loops or CTEs in SQL.

## Prerequisites

- Completed [Basic Queries](01-basic-queries.md)
- Understanding of [Core Concepts](core-concepts.md)

## What is Recursion?

A recursive rule references itself in its body. This allows computing things that require an unknown number of steps, like:

- All nodes reachable from a starting point
- Shortest paths in a graph
- Transitive relationships (ancestor/descendant)
- Fixed-point computations

## Setup

```datalog
.kg create recursion_tutorial
.kg use recursion_tutorial

% A simple directed graph
%     1 → 2 → 3 → 4
%     ↓   ↓
%     5 → 6 → 7

+edge[(1, 2), (2, 3), (3, 4),
      (1, 5), (2, 6), (5, 6), (6, 7)].
```

## Basic Recursion: Transitive Closure

### The Problem

Given edges, find all pairs (X, Y) where you can get from X to Y through any path.

### Non-Recursive Attempt (Limited)

You could try explicit path lengths:

```datalog
% Direct edges (length 1)
+path1(X, Y) :- edge(X, Y).

% Length 2
+path2(X, Y) :- edge(X, Z), edge(Z, Y).

% Length 3
+path3(X, Y) :- edge(X, Z), path2(Z, Y).
```

But this only works for paths up to length 3. What about longer paths?

### Recursive Solution

```datalog
% Base case: direct edges are paths
+reachable(X, Y) :- edge(X, Y).

% Recursive case: if X can reach Z, and Z has an edge to Y, then X can reach Y
+reachable(X, Z) :- reachable(X, Y), edge(Y, Z).
```

Query:
```datalog
?- reachable(1, X).
```

Result:
```
Results: 6 rows
  (1, 2)
  (1, 3)
  (1, 4)
  (1, 5)
  (1, 6)
  (1, 7)
```

Node 1 can reach all other nodes!

### How Recursion Works

InputLayer evaluates recursive rules using *fixpoint iteration*:

1. **Iteration 0**: Compute base case (direct edges)
   - reachable = {(1,2), (2,3), (3,4), (1,5), (2,6), (5,6), (6,7)}

2. **Iteration 1**: Apply recursive rule
   - From (1,2) and edge(2,3): add (1,3)
   - From (1,2) and edge(2,6): add (1,6)
   - From (2,3) and edge(3,4): add (2,4)
   - ... and so on

3. **Iteration 2**: Continue until no new facts
   - From (1,3) and edge(3,4): add (1,4)
   - ...

4. **Fixpoint**: When no new facts are derived, we're done

## Left vs Right Recursion

### Left Recursion (Preferred)

```datalog
+reach_left(X, Y) :- edge(X, Y).
+reach_left(X, Z) :- reach_left(X, Y), edge(Y, Z).
```

The recursive call is on the **left** side of the join.

### Right Recursion

```datalog
+reach_right(X, Y) :- edge(X, Y).
+reach_right(X, Z) :- edge(X, Y), reach_right(Y, Z).
```

The recursive call is on the **right** side.

**Both work**, but left recursion often performs better in Differential Dataflow.

## Mutual Recursion

Rules can reference each other:

```datalog
% Odd-length paths from node 1
+odd_path(X) :- edge(1, X).
+odd_path(X) :- even_path(Y), edge(Y, X).

% Even-length paths from node 1
+even_path(X) :- odd_path(Y), edge(Y, X).
```

These rules depend on each other and are computed together.

## Practical Examples

### Example 1: Ancestor/Descendant

```datalog
% Family tree
+parent[(1, 2), (1, 3), (2, 4), (2, 5), (3, 6)].
% 1 is parent of 2, 3; 2 is parent of 4, 5; 3 is parent of 6

% Ancestor relationship
+ancestor(X, Y) :- parent(X, Y).
+ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).
```

Query: Who are person 1's descendants?
```datalog
?- ancestor(1, Desc).
```

Result: 4, 5, 6 (grandchildren) plus 2, 3 (children)

### Example 2: Same Generation

Find all pairs at the same generation level:

```datalog
% Root nodes (no parents)
+root(X) :- node(X), !parent(_, X).

% Generation level
+generation(X, 0) :- root(X).
+generation(X, N) :- parent(P, X), generation(P, M), N = M + 1.

% Same generation
+same_gen(X, Y) :- generation(X, N), generation(Y, N), X != Y.
```

### Example 3: Shortest Path (with Distance)

Compute shortest path distances:

```datalog
% Direct edges have distance 1
+distance(X, Y, 1) :- edge(X, Y).

% Extend shortest paths
+distance(X, Z, D) :-
  distance(X, Y, D1),
  edge(Y, Z),
  D = D1 + 1,
  !shorter_path(X, Z, D).  % Only keep if no shorter path exists

% Helper: there's a shorter path
+shorter_path(X, Z, D) :-
  distance(X, Z, D2),
  D2 < D.
```

**Note**: This pattern requires stratified negation.

### Example 4: Connected Components

Find which nodes are in the same connected component:

```datalog
% Symmetric edges for undirected graph
+sym_edge(X, Y) :- edge(X, Y).
+sym_edge(X, Y) :- edge(Y, X).

% Same component (connected)
+same_component(X, X) :- node(X).  % Reflexive
+same_component(X, Y) :- sym_edge(X, Y).
+same_component(X, Z) :- same_component(X, Y), sym_edge(Y, Z).
```

### Example 5: Bill of Materials

Classic manufacturing example - compute all parts needed:

```datalog
% Part containment: component(assembly, part, quantity)
+component[(1, 2, 1), (1, 3, 2), (2, 4, 3), (3, 4, 1), (3, 5, 2)].

% All parts required for an assembly (including nested)
+requires(Assembly, Part) :- component(Assembly, Part, _).
+requires(Assembly, Part) :-
  component(Assembly, SubAsm, _),
  requires(SubAsm, Part).
```

## Recursion with Aggregation

### Counting Path Lengths

```datalog
% Count number of paths of each length
+path_count(Src, Dst, Len, count<Path>) :-
  path_with_length(Src, Dst, Len, Path).
```

### Recursive Sum

Total quantity of each part needed (with multipliers):

```datalog
% Direct quantity needed
+qty(Asm, Part, Qty) :- component(Asm, Part, Qty).

% Transitive quantity (multiplied through levels)
+qty(Asm, Part, TotalQty) :-
  component(Asm, Sub, SubQty),
  qty(Sub, Part, PartQty),
  TotalQty = SubQty * PartQty.

% Sum all quantities per part
+total_qty(Asm, Part, sum<Qty>) :- qty(Asm, Part, Qty).
```

## Common Patterns

### Pattern 1: Transitive Closure

```datalog
+closure(X, Y) :- base_relation(X, Y).
+closure(X, Z) :- closure(X, Y), base_relation(Y, Z).
```

### Pattern 2: Reflexive-Transitive Closure

```datalog
+rt_closure(X, X) :- domain(X).  % Reflexive: X reaches itself
+rt_closure(X, Y) :- base_relation(X, Y).
+rt_closure(X, Z) :- rt_closure(X, Y), base_relation(Y, Z).
```

### Pattern 3: Inductive Definition

```datalog
% Base case
+inductive(0, "base").

% Inductive step
+inductive(N, "derived") :-
  inductive(M, _),
  N = M + 1,
  N < 10.
```

### Pattern 4: Graph Algorithms

```datalog
% Node with maximum reachability
+reach_count(X, count<Y>) :- reachable(X, Y).
+max_reach(max<Count>) :- reach_count(_, Count).
+most_connected(X) :- reach_count(X, C), max_reach(C).
```

## Debugging Recursion

### Check Intermediate Results

```datalog
% Query the recursive relation directly
?- reachable(X, Y).

% See how many facts are derived
count_reachable(count<X>) :- reachable(X, _).
?- count_reachable(N).
```

### Limit Depth for Testing

For debugging, you can create bounded versions:

```datalog
+reach1(X, Y) :- edge(X, Y).
+reach2(X, Y) :- reach1(X, Y).
+reach2(X, Z) :- reach1(X, Y), edge(Y, Z).
% etc.
```

## Performance Considerations

1. **Left recursion** often performs better than right recursion
2. **Minimize joins** in recursive rules when possible
3. **Use constraints early** to prune the search space
4. **Indexes** are automatically created on join columns

## Exercises

1. Write a rule to find all cycles in the graph (nodes that can reach themselves)
2. Compute the length of the longest path from node 1
3. Find nodes that have exactly 2 ancestors
4. Write rules to detect if a graph is a DAG (directed acyclic graph)

## Next Steps

- **[Negation Tutorial](03-negation.md)** - Stratified negation patterns
- **[Aggregations Tutorial](04-aggregations.md)** - Combining recursion with aggregates
- **[Graph Patterns](../patterns/graph-algorithms.md)** - More graph algorithms
