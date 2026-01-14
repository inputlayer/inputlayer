# Lexical Elements

Basic character classes used throughout the grammar.

## Character Classes

```ebnf
WHITESPACE  ::= " " | "\t" | "\n" | "\r" ;
LC_ALPHA    ::= [a-z] ;
UC_ALPHA    ::= [A-Z] ;
ALPHA       ::= LC_ALPHA | UC_ALPHA ;
DIGIT       ::= [0-9] ;
```

## Identifiers

**Predicate names** start with a lowercase letter:

```ebnf
predicate   ::= LC_ALPHA ( ALPHA | DIGIT | "_" )* ;
```

Examples: `edge`, `my_relation`, `user123`

**Variable names** start with an uppercase letter:

```ebnf
variable    ::= UC_ALPHA ( ALPHA | DIGIT | "_" )* ;
```

Examples: `X`, `Name`, `User_Id`

## Comments

```ebnf
line_comment  ::= "%" [^\n]* ;
block_comment ::= "/*" .* "*/" ;
```

Example:
```datalog
% This is a line comment
/* This is a
   block comment */
```
