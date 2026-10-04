# InputLayer Documentation

Welcome to the InputLayer documentation. InputLayer is the live rules engine for AI agents: declare your facts and rules once, and InputLayer keeps every conclusion current as facts change, tells your agents what changed, and records an agent's action only while the rules allow it. Any row can be explained with a proof, on request. A knowledge graph is what the engine holds: the facts and rule-derived conclusions of one domain.

The project's [README](../README.md) has the first screen: what InputLayer replaces in an agent stack, what it keeps, and where it sits. The guides [What InputLayer Replaces](content/docs/guides/what-it-replaces.mdx) and [Where It Sits](content/docs/guides/where-it-sits.mdx) cover both in depth, with examples that run on today's engine.

## Documentation Structure

All documentation content lives in **`docs/content/`** as MDX files - this is the single source of truth.

### Authoring

Edit files in `docs/content/`. Navigation is controlled by `_meta.json` files in each directory.

### Viewing

| Method | URL |
|--------|-----|
| **GUI (InputLayer Studio)** | Navigate to `/docs` in the GUI - works without a server connection |
| **GitHub Pages** | See the [CI deployment policy](../CONTRIBUTING#continuous-integration) |
| **Local dev** | `cd front && npm install && npm run dev`, then open `/docs` |

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

- **Website** (`front/`) - Next.js site deployed to GitHub Pages. `front/scripts/bundle-docs.mjs` bundles content at build time.
- **GUI docs viewer** (`gui/scripts/bundle-docs.mjs`) - Bundles content into the GUI at build time.

### Syntax Highlighting

Code blocks with ` ```iql ` get syntax highlighting via a TextMate grammar at `docs/grammars/iql.tmLanguage.json`.

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
