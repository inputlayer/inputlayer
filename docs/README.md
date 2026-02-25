# InputLayer Documentation

Welcome to the InputLayer documentation. InputLayer is a reasoning engine for AI agents — a modern database that stores facts, defines rules, and derives everything that logically follows.

## Documentation Structure

All documentation content lives in **`docs/content/`** as MDX files — this is the single source of truth.

### Authoring

Edit files in `docs/content/`. Navigation is controlled by `_meta.json` files in each directory.

### Viewing

| Method | URL |
|--------|-----|
| **GUI (InputLayer Studio)** | Navigate to `/docs` in the GUI — works without a server connection |
| **GitHub Pages** | Deployed automatically on push to `main` |
| **Local dev** | `cd docs/site && npm install && npm run dev` |

### Content Map

```
docs/content/
├── index.mdx                    # Landing page
└── docs/
    ├── guides/                  # Step-by-step tutorials (18 pages)
    │   ├── quickstart.mdx
    │   ├── installation.mdx
    │   ├── first-program.mdx
    │   ├── python-sdk.mdx
    │   ├── deployment.mdx
    │   ├── authentication.mdx
    │   ├── websocket-api.mdx
    │   ├── migrations.mdx
    │   └── ...
    ├── reference/               # API reference (6 pages)
    │   ├── commands.mdx
    │   ├── functions.mdx
    │   ├── syntax.mdx
    │   └── ...
    ├── spec/                    # Formal specification (7 pages)
    │   ├── types.mdx
    │   ├── rules.mdx
    │   ├── queries.mdx
    │   └── ...
    └── internals/               # Architecture docs (7 pages)
        ├── architecture.mdx
        ├── coding-standards.mdx
        └── ...
```

### Renderers

- **Nextra site** (`docs/site/`) — Static site for GitHub Pages. Copies content at build time.
- **GUI docs viewer** (`gui/scripts/bundle-docs.mjs`) — Bundles content into the GUI at build time.

### Syntax Highlighting

Code blocks with ` ```datalog ` get syntax highlighting via a TextMate grammar at `docs/grammars/datalog.tmLanguage.json`.

## Quick Links

| Task | Go to |
|------|-------|
| Install InputLayer | `docs/content/docs/guides/installation.mdx` |
| Use the Python SDK | `docs/content/docs/guides/python-sdk.mdx` |
| Learn the basics | `docs/content/docs/guides/first-program.mdx` |
| Look up a function | `docs/content/docs/reference/functions.mdx` |
| Find a command | `docs/content/docs/reference/commands.mdx` |
| Deploy in production | `docs/content/docs/guides/deployment.mdx` |

## Test Coverage

- **3,107 unit tests** across all modules
- **1,121 snapshot tests** for end-to-end validation
- 0 failures, 0 ignored
