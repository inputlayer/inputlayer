# InputLayer IDL - VS Code Extension

Syntax highlighting for InputLayer Datalog (`.idl` / `.dl`) files.

## Features

- Semantic syntax highlighting with head/body distinction in rules
- Schema column names highlighted differently from relation names
- `<-` renders as `←` arrow with ligature fonts (Fira Code, JetBrains Mono, Cascadia Code)
- Line (`//`) and block (`/* */`) comment support with nesting
- Comment toggling with `Cmd+/` / `Ctrl+/`
- Bracket matching, auto-closing pairs, bracket pair colorization
- File associations: `.idl`, `.dl`

### Semantic highlighting

Rules distinguish **head** (definition) from **body** (references):

```
+triangle(A, B, C) <- edge(A, B), edge(B, C), edge(C, A)
│         │            │    │
│         │            │    └─ Variable (light blue)
│         │            └────── Body relation (teal/green - distinct from head)
│         └─────────────────── Variable (light blue)
└───────────────────────────── Head relation (yellow - definition)
```

Schema declarations distinguish column names from relation names:

```
+employee(emp_id: int, name: string, embedding: vector)
│         │       │
│         │       └─ Type (blue - keyword color)
│         └───────── Column name (member color - distinct from relation)
└─────────────────── Relation name (yellow - head)
```

### Arrow ligature

The extension enables font ligatures for `.idl` files. With a ligature font, `<-` displays as `←`:

**Recommended fonts:** Fira Code, JetBrains Mono, Cascadia Code, Iosevka

### All highlighted constructs

| Construct | Examples | Scope |
|-----------|----------|-------|
| Comments | `// ...`, `/* ... */` | `comment` |
| Strings | `"hello"` | `string` |
| Meta commands | `.kg create`, `.rule list` | `keyword.control` |
| Query marker | `?relation(X)` | `keyword.control` |
| Rule arrow | `<-` | `keyword.control` |
| Mutation prefix | `+fact(...)`, `-fact(...)` | `keyword.control` |
| Negation | `!relation(X)` | `keyword.control` |
| Comparison ops | `=`, `!=`, `<`, `>`, `<=`, `>=` | `keyword.operator` |
| Numbers | `42`, `3.14`, `1.5e-3` | `constant.numeric` |
| Aggregates | `count`, `sum`, `avg`, `min`, `max`, `top_k` | `support.function` |
| Built-in functions | `euclidean`, `cosine`, `abs`, `concat` | `support.function` |
| Type keywords | `type`, `int`, `string`, `bool`, `float`, `list` | `storage.type` |
| Booleans | `true`, `false` | `constant.language` |
| Schema types | `vector`, `timestamp`, `embedding`, `symbol`, `any` | `storage.type` |
| Sort order | `:desc`, `:asc` | `keyword.other` |
| Variables | `X`, `Y`, `_Tmp` | `variable.parameter` |
| Wildcard | `_` | `variable.language` |
| Head relations | `path(X, Y) <- ...` | `entity.name.function` |
| Body relations | `... <- edge(X, Y)` | `entity.name.tag` |
| Column names | `name: string` | `variable.other.member` |

## Installation

### Manual install

```bash
cp -r packages/inputlayer-vscode ~/.vscode/extensions/inputlayer-idl-0.1.0
```

Then quit and reopen VS Code.

### Development mode

```bash
code --extensionDevelopmentPath=$(pwd)/packages/inputlayer-vscode
```

## Development

The TextMate grammar (`syntaxes/idl.tmLanguage.json`) is derived from:
- PEG tokenizer: `src/syntax/datalog.pest` (token definitions)
- Schema types: `src/schema/mod.rs` (`SchemaType::from_str`)
- Built-in functions: `src/vector_ops.rs`

When adding new syntax constructs, update the TextMate grammar to match.
