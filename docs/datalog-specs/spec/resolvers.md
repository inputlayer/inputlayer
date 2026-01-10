# Resolvers

Resolvers are used to download and parse content referenced from an initial
DATALOG-TEXT resource. In general these are datasets that are external to the
Datalog source and often in alternate representations. 

## URI References

Relative URIs are resolved with base URIs as per _Uniform Resource Identifier
(URI): Generic Syntax_ <span class="bibref
inline">[RFC3986](x_references.md#RFC3986)</span> using only the basic
algorithm in section 5.2. Neither Syntax-Based Normalization nor Scheme-Based
Normalization (described in sections 6.2.2 and 6.2.3 of RFC3986) are
performed.

The [`base` pragma](pragmas.md#pragma-base) defines the Base URI used to
resolve relative URIs per RFC3986 §&nbsp;5.1.1, "_Base URI Embedded in
Content_". §&nbsp;5.1.2, "_Base URI from the Encapsulating Entity_" defines
how the In-Scope Base URI may come from an encapsulating document, such as a
SOAP envelope with an `xml:base` directive or a mime multipart document with a
`Content-Location` header. The "_Retrieval URI_" identified in §&nbsp;5.1.3, Base
"_URI from the Retrieval URI_", is the URL from which a particular
DATALOG-TEXT resource was retrieved. If none of the above specifies the Base
URI, the default Base URI (§&nbsp;5.1.4, "_Default Base URI_") is used.

In the case that multiple `base` pragmas exist, each replaces the previous value.

### Content Negotiation

This section applies to URI schemes, such as `http` and `https` that provide content negotiation.

The resolver MAY use a provided media type to construct an HTTP `Accept`
header (see <span class="bibref
inline">[RFC7231](x_references.md#RFC7231)</span> §&nbsp;5.3.2) for URIs with `http` or `https` schemes, see <span class="bibref
inline">[RFC2616](x_references.md#RFC2616), §&nbsp;14.1</span>.

In the following example no media type is provided.

```datalog
.assert human(name: string).
.input human(uri="http://example.com/data/humans").
```

In this case the HTTP request contains a list of acceptable response types as
well as language and encoding preferences. In this case, not only does the
client accept CSV and TSV representations, it indicates its preference for CSV
over TSV.

```http
GET /data/humans HTTP/1.1
Host: example.com
Accept: text/csv; q=0.8, text/tab-separated-values; q=0.4
Accept-Language: en-us
Accept-Encoding: gzip, deflate
```

While `Accept-Encoding` <span class="bibref
inline">[RFC7231](x_references.md#RFC7231)</span> §&nbsp;5.3.4 and
`Accept-Language` <span class="bibref
inline">[RFC7231](x_references.md#RFC7231)</span> §&nbsp;5.3.5 are RECOMMENDED, 
`Accept-Charset` <span class="bibref
inline">[RFC7231](x_references.md#RFC7231)</span> §&nbsp;5.3.3 if present MAY only
take the value `UTF-8`. 

### Examples

Given the following program, the input file is provided as an absolute URI and
can be fetched directly, the media type and parameters are also provided. 
 
```datalog
.assert human(name: string).
.input human(
    uri="http://example.com/data/humans.csv", 
    type="text/csv", 
    header=present).
```

The following is the resulting HTTP request.

```http
GET /data/humans.csv HTTP/1.1
Host: example.com
Accept: text/csv;header=present
```

Given a relative URI, as shown below, the resolver requires a _base URI_ to determine the absolute URI to fetch. An explicit base URI may be provided using the `base` pragma, or some other means such as processor command-line parameters, environment variables, or the current working directory of the host process[^1].

```datalog
.pragma base="https://example.com/datalog/".
.assert human(name: string).
.input human(uri="data/humans.csv", type="text/csv", header=present).
```

```datalog
.assert human(name: string).
.input human(uri="data/humans.csv", type="text/csv", header=present).
```

## Dataset Media Types

The following media types are documented here, however only `text/csv` and `text/tab-separated-values` MUST be supported by a conforming DATALOG-TEXT processor.

| Media Type                | Short   | Parameters | Conform | Expectations                                                                      |
|---------------------------|---------|------------|---------|-----------------------------------------------------------------------------------|
| text/csv                  | csv     | header     | MUST    | See <span class="bibref inline">[RFC4180](x_references.md#RFC4180)</span>         |
| text/tab-separated-values | tsv     | N/A        | MUST    | See <span class="bibref inline">[IANA-TSV](x_references.md#IANA-TSV)</span>       |
| application/json          | json    | N/A        | N/A     | See <span class="bibref inline">[RFC4627](x_references.md#RFC4627)</span>         |
| application/vnd.sqlite3   | sqlite3 | N/A        | N/A     | See <span class="bibref inline">[IANA-SQLITE3](x_references.md#IANA-SQLITE3)</span> |
| application/vnd.datalog          | datalog | N/A        | N/A     | Used on output only to write DATALOG-TEXT from an in-memory representation        |

### Common Parameters

* `uri` -- this MUST BE interpreted as a URI <span class="bibref inline">[RFC3986](x_references.md#RFC3986)</span> and handled as per [§&nbsp;URI References](#uri-references) above.
* `type` -- this string value MUST BE either the type name and subtype name of a supported media type (i.e. `text/csv`) or one of the supported media type short form identifiers described in the [§&nbsp;Dataset Media Types](resolvers.md#dataset-media-types) table.

### Common Errors

* [`ERR_IO_INSTRUCTION_PARAMETER`](errors.md#ERR_IO_INSTRUCTION_PARAMETER) --
  one of the specified parameters was incorrectly specified.

### text/csv Parameters

* `header` -- one of the following values `{present, absent}` from <span class="bibref inline">[RFC4180](x_references.md#RFC4180)</span>.
* `columns` -- a comma-separated list of either positive integers (the first
  column is column `1`)) or inclusive range specifiers of the form `[min:max]`
  where min and max are optional positive integers. If min is not present it
  is assumed to be the initial attribute index of `1`, if max is not present
  it is assumed to the last attribute index $\small j$ in
  $\small\(\alpha_1,\ldots,\alpha_j\)$.

#### Errors

* [`ERR_INVALID_ATTRIBUTE_INDEX`](errors.md#ERR_INVALID_ATTRIBUTE_INDEX) -- a
  value in the `columns` parameter is not a valid attribute index in this relation.
  
#### Example

```datalog
.assert human(name: string).
.input human( 
    uri="http://example.com/data/humans.csv", 
    type="text/csv", 
    columns="2",
    header=present).
```

### text/tsv Parameters

* `columns` -- see [§&nbsp;Text/CSV Parameters](#textcsv-parameters).

According to <span class="bibref
inline">[IANA-TSV](x_references.md#IANA-TSV)</span> in the `tsv` production
(copied below) the name line MUST BE present and so no `header` parameter is
exposed for this type. 

```ebnf
tsv      ::= nameline record+ ;
```

#### Example

```datalog
.assert car(make: string, model: string, year: integer).
.input car( 
    uri="http://example.com/data/cars.tsv", 
    type="text/tab-separated-values", 
    columns="[1:2],4").
```

### application/json Parameters

_This section is non-normative._

The JSON data interchange syntax <span class="bibref
inline">[ECMA-JSON](x_references.md#ECMA-JSON)</span> is a well-defined and
simple interchange format and can be used to represent datasets, given the
following structural restrictions.

1. The JSON root value MUST be an array.
2. Each member of this array MUST be _either_:
   1. An array where each value being MUST be an atomic value (string, number,
      boolean).
   2. An object where each member value MUST be an atomic value (string, number,
      boolean).
3. All members of this array MUST be homogenous.

Additional, more complex structures MAY be supported by a DATALOG-TEXT
processor.

* `columns` -- this MUST only be used when each member of the JSON array is
  also an array. See [§&nbsp;Text/CSV Parameters](#textcsv-parameters).
* `members` -- this MUST only be used when each member of the JSON array is
  an object and is a comma-separated list of member names.

#### Errors

* [`ERR_INVALID_ATTRIBUTE_LABEL`](errors.md#ERR_INVALID_ATTRIBUTE_LABEL) -- a
  value in the `members` parameter is not a valid attribute label in this relation.

#### Examples

Given the following, array-shaped, JSON value.

```json
[
  ["ford", "fiesta", "uk", 2010],
  ["ford", "escort", "uk", 2008]
]
```

The input processing instruction for the relation `car` specifies columns 1, 2, and
4 to skip the unused third column. By default an array-shaped input will match
by position and there MUST be the same number of values in each array as there are
attributes in the relation.

```datalog
.assert car(make: string, model: string, year: integer).
.input car( 
    uri="http://example.com/data/cars.json",
    type="application/vnd.sqlite3",
    columns="1,2,4").
```

Given the following, object-shaped, JSON value.

```json
[
  {"make": "ford", "model": "fiesta", "geo": "uk", "year": 2010},
  {"make": "ford", "model": "escort", "geo": "uk", "year": 2008}
]
```

The input processing instruction for the relation `car` specifies the members
make, model, and year but not the geo value. By default an object-shaped input
is matched by label **IFF** all attributes in the relation are named, or else by
position. If matched by label the object may have more members than the
relation has attributes; if matched by position the length of object and
relation MUST be equal.

```datalog
.assert car(make: string, model: string, year: integer).
.input car( 
    uri="http://example.com/data/cars.json",
    type="application/vnd.sqlite3",
    members="make,model,year").
```

### application/vnd.sqlite3 Parameters

_This section is non-normative._

* `table` -- the name of the table in the database to read from, or write to. If missing the processor MAY use the relation name as the table name, or MAY signal the error [`ERR_IO_INSTRUCTION_PARAMETER`](errors.md#ERR_IO_INSTRUCTION_PARAMETER).
* `columns` -- a comma-separated list of column names in the table, these are matched positionally with relation attributes. If missing the processor MAY use the attribute names as column names, or MAY signal the error [`ERR_IO_INSTRUCTION_PARAMETER`](errors.md#ERR_IO_INSTRUCTION_PARAMETER).

According to <span class="bibref inline">[SQLITE3-URI](x_references.md#SQLITE3-URI)</span> the URI specified for the database location MUST have the scheme `file` <span class="bibref inline">[RFC8089](x_references.md#RFC8089)</span>.

A conforming DATALOG-TEXT processor MAY choose to validate these parameters at parse time or MAY wait until the actual input/output operation is required. In either case if the processor determines that the table or columns specified in these parameters does not match the schema of the relation the processor MUST signal the error [`ERR_IO_INSTRUCTION_PARAMETER`](errors.md#ERR_IO_INSTRUCTION_PARAMETER).

#### Example

In the following the intensional relation `mortal` is output to a SQLite database.

```datalog
.infer mortal(name: string).
.output mortal( 
    uri="file:///var/db/results.sql?cache=private&mode=rwc", 
    type="application/vnd.sqlite3",
    table="mortals",
    columns="short_name").
```

### Text/Datalog

_This section is non-normative._

In either the input or output cases the representation is a list of facts that
corresponds to the grammar production `assertion`, see [§&nbsp;Relations &amp; Facts](grammar_facts.md).

<p>&nbsp;</p>

----------

[^1]: See The Open Group [man page](https://pubs.opengroup.org/onlinepubs/007904975/functions/getcwd.html) for the `getcwd()` function.
