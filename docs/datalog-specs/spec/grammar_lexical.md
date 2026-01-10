# Terminal/Lexical

The following are lexical rules that can be assumed by the non-terminal rules in previous sections.

```ebnf
EOL     ::= "\n" | "\r" "\n"? ;

DQUOTE  ::= #x22 ;

UNDERSCORE
        ::= "_" ;

SPACE_SEP
        ::= ? corresponds to the Unicode category 'Zs' ? ;

WHITESPACE
        ::= SPACE_SEP | "\t" | EOL ;

LC_ALPHA
        ::= ? corresponds to the Unicode category 'Ll' ? ;

UC_ALPHA
        ::= ? corresponds to the Unicode category 'Lu' ? ;

TC_ALPHA
        ::= ? corresponds to the Unicode category 'Lt' ? ;

ALPHA   ::= LC_ALPHA | UC_ALPHA | TC_ALPHA ;

DIGIT   ::= ? corresponds to the Unicode category 'Nd' (decimal number) ? ;

HEXDIGIT
        ::= [0-9a-fA-F] ;
```
