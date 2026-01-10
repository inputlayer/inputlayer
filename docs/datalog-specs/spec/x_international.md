# Appendix: Internationalization Considerations

Unicode provides a mechanism for signaling direction within a string (see [Unicode Bidirectional Algorithm](https://www.unicode.org/reports/tr9/tr9-42.html) <span class="bibref inline">[UAX9](x_references.md#UAX9)</span>), however, when a string has an overall base direction which cannot be determined by the beginning of the string, an external indicator is required, such as the <span class="bibref inline">[HTML](x_references.md#HTML)</span> `dir` attribute, which currently has no counterpart for Datalog literals.

Until a more comprehensive solution can be addressed in a future version of this specification, programmers should consider this issue when representing strings where the base direction of the string cannot otherwise be correctly inferred based on the content of the string. See <span class="bibref inline">[string-meta](x_references.md#string-meta)</span> for a discussion best practices for identifying language and base direction for strings used on the Web.


## Identifier Characters

The characters allowed in labeling relations, attributes, facts, atoms, and variables are taken from a broad set of Unicode by category rather than by codepoint range. The following are the definitions of the lexical productions, showing their relation to defined categories:

```ebnf
SPACE_SEP
        ::= ? corresponds to the Unicode category 'Zs' ? ;
LC_ALPHA
        ::= ? corresponds to the Unicode category 'Ll' ? ;
UC_ALPHA
        ::= ? corresponds to the Unicode category 'Lu' ? ;
TC_ALPHA
        ::= ? corresponds to the Unicode category 'Lt' ? ;
ALPHA   ::= LC_ALPHA | UC_ALPHA | TC_ALPHA ;
```

The following productions are the key _identifier_-like values, built almost entirely from the character productions above with the addition of the single "_" underscore.

```ebnf
predicate
        ::= LC_ALPHA  ( ALPHA | DIGIT | "_" )* ;
named-variable
        ::= UC_ALPHA  ( ALPHA | DIGIT | "_" )* ;
identifier-string
        ::= predicate ( ":" ALPHA ( ALPHA | DIGIT | "_" )* )? ;
```

### Example

The following is a perfectly valid version of the Socratic syllogism from the example in [§&nbsp;Program](grammar_program.md).

```datalog
ανθρώπινο("Σωκράτης").

θνητός(Χ) :- ανθρώπινο(Χ).

?- θνητός("Σωκράτης").
```
## Numerical Values

Numerical values use the following lexical production that allows a number of language representations of the digits 0 to 9. For example, "123", "١٢٣" (Arabic-Indic), or "१२३" (Devangari).

```ebnf
DIGIT   ::= ? corresponds to the Unicode category 'Nd' (decimal number) ? ;
```

The only exception to this broad inclusive approach is the definition of the `HEXDIGIT` production where the Anglo-centric approach is commonly understood and alternate forms may cause confusion.

```ebnf
HEXDIGIT
        ::= [0-9a-fA-F] ;
```


## Unicode Operators

The following operators, or syntax symbols, are defined in this specification. Where Unicode symbols are defined for the symbol these are described by codepoint value and assigned name.

| Symbol                 | Primary | Alternate | Unicode | Codepoint | Name                     |
|------------------------|---------|-----------|---------|-----------|--------------------------|
| Material implication   | `:-`    | `<-`      | `←`     | U+E28690  | LEFTWARDS ARROW          |
| Conjunction            | `,`     | `&`, `AND` | `∧`     | U+E288A7  | LOGICAL AND              |
| True (boolean)         | `true`  |           | `⊤`     | U+E28AA4  | DOWN TACK                |
| False (boolean)        | `false` |           | `⊥`     | U+E28AA5  | UP TACK                  |
| Logical negation       | `!`     | `NOT`     | `￢`     | U+EFBFA2  | FULLWIDTH NOT SIGN       |
| Disjunction            | `;`     | <code>&#124;</code>, `OR`  | `∨` | U+E2BBA8  | LOGICAL OR  |
| Tautology              | N/A     |           | `⊤`     | U+E28AA4  | DOWN TACK                |
| Absurdity              |         |           | `⊥`     | U+E28AA5  | UP TACK                  |
| Equal                  | `=`     |           |         | U+3D      | EQUALS SIGN              |
| Not equal              | `!=`    | `/=`      | `≠`     | U+E289A0  | NOT EQUAL TO             |
| Less Than              | `<`     |           |         | U+3C      | LESS-THAN SIGN           |
| Less than, or equal    | `<=`    |           | `≤`     | U+E289A4  | LESS-THAN OR EQUAL TO    |
| Greater than           | `>`     |           |         | U+3E      | GREATER-THAN SIGN        |
| Greater than, or equal | `>=`    |           | `≥`     | U+E289A5  | GREATER-THAN OR EQUAL TO |
| String match           | `*=`    | `MATCH`   | `≛`     | U+E2899B  | STAR EQUALS              |
| Functional dependency  | `-->`   |           | `⟶`     | U+E29FB6  | LONG RIGHTWARDS ARROW    |

The following table describes the symbols introduced by different Datalog languages. Note that only the symbols material implication, conjunction, true, and false, are required by the core language $\small\text{Datalog}$.

| Language                             | Introduces                                   | Symbols                                                  |
|--------------------------------------|----------------------------------------------|----------------------------------------------------------|
| $\small\text{Datalog}^{\lnot}$       | negation of literals in rule bodies          | `!`, `NOT`, `￢`                                          |
| $\small\text{Datalog}^{\lor}$        | disjunction in rule heads                    | `;`, <code>&#124;</code>, `OR`, `∨`                      |
| $\small\text{Datalog}^{\Leftarrow}$  | rules as constraints, i.e. no body           | `⊤`                                                      |
| $\small\text{Datalog}^{\Gamma}$      | typed attributes for relations               | N/A                                                      |
| $\small\text{Datalog}^{\theta}$      | arithmetic literals in rule bodies           | `=`, `!=`, `≠`, <`, `<=`, `≤`, `>`, `>=`, `≥`, `*=`, `≛` |
| $\small\text{Datalog}^{\rightarrow}$ | functional dependency processing instruction | `-->`, `⟶`                                               |
