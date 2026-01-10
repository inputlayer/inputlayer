# Comments

InputLayer supports two comment styles, following Prolog conventions:

1. **Line comments** -- Begin with `%` and continue to the end of the line
2. **Block comments** -- Begin with `/*` and end with `*/`

```ebnf
comment ::= line-comment | block-comment ;
```

## Line Comments

Line comments start with `%` and extend to the end of the line:

```ebnf
line-comment
        ::= "%" [^\r\n]* EOL ;
```

```datalog
% This is a line comment
+edge(1, 2).  % This is an inline comment
```

## Block Comments

Block comments can span multiple lines:

```ebnf
block-comment
        ::= '/*' ( [^*] | '*'+ [^*/] )* '*'* '*/' ;
```

```datalog
/* This is a
   multi-line comment */
+edge(1, 2).

path(X /* source */, Y /* target */) :- edge(X, Y).
```

Block comments cannot be nested.

## Examples

```datalog
% Define edges in a graph
+edge(1, 2).
+edge(2, 3).  % Node 2 connects to node 3

/* Compute transitive closure
   of the edge relation */
+path(X, Y) :- edge(X, Y).
+path(X, Z) :- edge(X, Y), path(Y, Z).

?- path(1, X).  % Find all nodes reachable from node 1
```
