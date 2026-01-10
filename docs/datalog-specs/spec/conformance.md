# Conformance

As well as sections marked as non-normative, all authoring guidelines, diagrams, examples, and notes in this specification are non-normative. Everything else in this specification is normative.

The keywords _MAY_, _MUST_, _MUST NOT_, _RECOMMENDED_, _SHOULD_, and _SHOULD NOT_ in this document are to be interpreted as described in [BCP 14](https://tools.ietf.org/html/bcp14) <span class="bibref inline">[RFC2119](x_references.md#RFC2119)</span> <span class="bibref inline">[RFC8174](x_references.md#RFC8174)</span> when, and only when, they appear in all capitals, as shown here.

A conforming DATALOG-TEXT _resource_ is a Unicode string that MUST conform to the grammar and additional constraints defined in [§&nbsp;Datalog Text Grammar](grammar.md), starting with the `program` production. 

A conforming DATALOG-TEXT _parser_ MUST ensure the resource it is provided is a conforming DATALOG-TEXT resource, signalling error conditions specified herein. Additionally, a conforming _resolver_ MUST be able to convert paths to URIs <span class="bibref inline">[RFC3986](x_references.md#RFC3986)</span> and retrieve the identified resource on behalf of the parser.

## Strict vs Lax Processing

Two modes of operation for a conforming DATALOG-TEXT processor are described,
a **strict** processing mode that, while more verbose, relies less on
inference of a program's semantics. For example, when strict processing is
enabled all relations both extensional and intensional MUST BE declared by
processing instructions. A **lax** processing mode allows more flexibility in
the source and is especially useful in dealing with Datalog written in
different dialects.

Strict processing support is RECOMMENDED, and may be enabled by the [`strict` pragma](pragmas.md#pragma-strict). Where behavior is required by strict processing it will be highlighted in the text. This specification does not define whether a conforming DATALOG-TEXT processor is required to be strict by default.

## Media Type and Content Encoding

The media type of DATALOG-TEXT resource is `application/vnd.datalog`. The content encoding of a DATALOG-TEXT document is always UTF-8. For complete details, see appendix [§&nbsp;IANA Considerations](x_iana.md).

### `features` Parameter

This parameter is used to indicate the language features the resource requires. The list of supported feature identifiers is in [§&nbsp;Language Features](#language-features).

The content of this parameter is a comma separated list of feature identifiers. This is used as a hint to a conforming DATALOG-TEXT processor to determine if they can process the resource. 

While it is clearly of benefit for the set of features listed in this parameter to be **exactly** the same as the set of language pragmas used in the document it is impossible to guarantee. Therefore, processors MAY choose to proceed parsing the resource or signal an error if they find that these feature lists do not match. 

#### Errors

* [`ERR_UNSUPPORTED_FEATURE`](errors.md#ERR_UNSUPPORTED_FEATURE) -- a feature identifier was not recognized, or supported, by the processor, even if it may be valid in some other version of this specification.

#### Example

The media type `application/vnd.datalog;features=negation,constraints` denotes a document with the following feature pragmas.

```datalog
.pragma negation.
.pragma constraints.
```

### `dialect` Parameter

This parameter is used to indicate the language dialect the resource conforms to. The list of supported dialect identifiers is in [§&nbsp;Dialect Identifiers](#dialect-identifiers).

The content of this parameter is a single dialect identifier.

#### Errors

* `ERR_UNSUPPORTED_DIALECT` -- a dialog identifier was not recognized, or supported, by the processor, even if it may be valid in some other version of this specification.

## Language Features

As mentioned in the [introduction](introduction.md) the language described by DATALOG-TEXT with no additional language features enabled is $\small\text{Datalog}^{\Gamma}$, the language $\small\text{Datalog}$ with typed relation attributes. All the language features defined in this specification MUST BE supported by a conforming DATALOG-TEXT processor. These features are: `arithmetic_literals`, `constraints`, `disjunction`, `extended_numerics`, `functional_dependencies`, and `negation`.


| Identifier               | Source                  | Date          | 
|--------------------------|-------------------------|---------------|
| `arithmetic_literals`    | This specification      | XX March 2022 |
| `constraints`            | This specification      | XX March 2022 |
| `disjunction`            | This specification      | XX March 2022 |
| `extended_numerics`      | This specification      | XX March 2022 |
| `negation`               | This specification      | XX March 2022 |

This specification DOES NOT cover the behavior of a DATALOG-TEXT evaluator and
so support of these language features only ensures that a DATALOG-TEXT
resource can be parsed and validated, not that it can be evaluated. In the
case that an evaluator detects a language feature it cannot support it MUST
signal the error [`ERR_UNSUPPORTED_FEATURE`](errors.md#ERR_UNSUPPORTED_FEATURE).

## Dialect Identifiers

The purpose of this parameter is to allow for the identification of Datalog
representations that are produced by applications that are not DATALOG-TEXT
conformant. These applications may use an alternate syntax for grammar
specified by DATALOG-TEXT, or they may extend the language with non-standard 
features. 

| Identifier       | Source                                           | Date          | 
|------------------|--------------------------------------------------|---------------|
| `core` (default) | This specification                               | XX March 2022 |
| `mitre`          | MITRE Datalog [implementation](http://datalog.sourceforge.net/) | 01 June 2016 (v2.6) |

### MITRE Datalog

This dialect only allows ASCII, Prolog style, operators; `:-` for material
implication, `,` for conjunction, and `!` for negation. The dialect does not
support the features `constraints`, `disjunction`, or `extended_numerics`.

The use of other syntaxed defined in this specification MUST signal the error
[`ERR_UNSUPPORTED_SYNTAX`](errors.md#ERR_UNSUPPORTED_SYNTAX).

### Example

Note that the absence of the `dialect` parameter on the MIME type is the same
as using the value `core`. The following values are therefore equivalent.

```text
application/vnd.datalog
application/vnd.datalog;dialect=core
```
