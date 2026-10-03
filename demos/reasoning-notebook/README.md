# Reasoning Notebook

A self-contained demo: Obsidian-like note editor backed by InputLayer for reasoning, LangChain for extraction, and LangGraph for ontology consolidation.

Write notes, drop images, watch the knowledge graph grow, ask questions across everything you've written.

## Architecture

```
Frontend (Vite + React)  -->  Backend (FastAPI)  -->  InputLayer (Rust)
       :5173              REST      :8000          WebSocket  :8080
                            |
                     LangChain (extraction)
                     LangGraph (ontology agent)
                     ChatOpenAI (Q&A)
                     Vision LLM (image analysis)
```

Three processes:
- **InputLayer server** (Rust) — reasoning engine, stores facts and rules, incremental maintenance, provenance
- **FastAPI backend** (Python) — note CRUD, LLM extraction pipeline, chat, ontology consolidation, image analysis
- **Vite frontend** (React) — markdown editor, force-directed graph, chat panel, provenance viewer

## Prerequisites

- **Rust toolchain** — to build the InputLayer server
- **Python 3.10+** and **uv** — for the backend
- **Bun** — for the frontend
- **LLM** — one of:
  - LM Studio running locally (free, default)
  - OpenAI API key
  - Anthropic API key

## Quick start

```bash
# From the repository root:

# 1. Build the engine (first time only)
cargo build --release --bin inputlayer-server

# 2. Install backend dependencies (first time only)
cd demos/reasoning-notebook/backend
uv sync

# 3. Install frontend dependencies (first time only)
cd ../frontend
bun install

# 4. Start everything (from the repository root)
cd ../..
./demos/reasoning-notebook/start.sh
```

Open http://localhost:5173

## Manual start (three terminals)

If `start.sh` doesn't work or you want more control, start each process separately.

**Terminal 1 — InputLayer server** (from repository root):
```bash
./target/release/inputlayer-server
```
Wait for "INITIAL ADMIN CREDENTIALS CREATED" message.

**Terminal 2 — Backend** (from repository root):
```bash
cd demos/reasoning-notebook/backend
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```
Wait for "Schema and rules deployed" message.

**Terminal 3 — Frontend** (from repository root):
```bash
cd demos/reasoning-notebook/frontend
bun run dev
```

Open http://localhost:5173

## LLM setup

The extraction pipeline, chat, and image analysis need an LLM.

### LM Studio (default, free)

1. Open LM Studio
2. Load a model (recommended: `mistralai/ministral-3-3b` for multimodal, or any model that supports structured output)
3. Go to the "Developer" tab and start the local server

The backend auto-connects to `localhost:1234`. No configuration needed.

### OpenAI

Set the API key before starting the backend:
```bash
OPENAI_API_KEY=sk-... uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### Anthropic

```bash
ANTHROPIC_API_KEY=sk-ant-... uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## Features

### Editor (Cmd+E)
- Create, edit, delete markdown notes
- Auto-save with debounce or Cmd+S
- Markdown preview toggle
- Drag-and-drop or paste images into notes
- Notes persisted in InputLayer KG

### Extraction
- Manual "Extract" button for text entity extraction
- Image upload triggers multimodal analysis (scene, objects, people, emotion, aesthetic, caption)
- Entity tags and relationship list shown in collapsible panel below editor
- Rich image scene analysis displayed when expanded

### Graph (Cmd+G)
- Force-directed visualization of all entities and relationships
- Nodes colored by kind (person, organization, technology, concept, scene, object, emotion, etc.)
- Image scene data shown as grouped nodes (scene hub with objects, emotion, event, cultural context)
- Click a node to see detail panel: description, source notes, relationships, "Why?" button
- Edge labels shown only on selected node for readability
- "Consolidate Ontology" normalizes synonymous predicates and entity names
- "Resolve Entities" merges near-duplicate entities via HNSW vector similarity

### Chat (Cmd+K)
- Ask questions across all your notes
- LLM uses the full knowledge graph (notes, entities, relationships) as context
- Conversation history within the session

### Provenance
- Click "Why?" on any entity or relationship in the graph
- Shows the InputLayer proof tree explaining how the fact was derived
- For base facts: shows the source (edb = extensional database)
- For derived facts: shows the rule chain back to base facts

## Keyboard shortcuts

| Shortcut | Action |
|----------|--------|
| Cmd+N | New note |
| Cmd+S | Save note |
| Cmd+E | Switch to Editor |
| Cmd+G | Switch to Graph |
| Cmd+K | Switch to Chat |
| Enter | Send chat message |

## Jupyter Notebook

A query patterns notebook demonstrates InputLayer's retrieval capabilities:

```bash
cd demos/reasoning-notebook/backend
uv run jupyter notebook ../notebooks/query_patterns.ipynb
```

The notebook covers:
1. **Semantic retrieval** — HNSW vector search over entity embeddings
2. **Structured retrieval** — multi-hop IQL rules (connected, two_hop, same_note)
3. **Hybrid queries** — vector similarity seeding into rule-based traversal
4. **Multimodal queries** — querying across text and image-extracted entities

Requires the demo to be running (InputLayer server + notes with extracted entities).

## Benchmarks

Compare extraction quality and speed across multiple LLMs (local and cloud).

### Setup

Place test inputs in `benchmarks/inputs/`:
- Text files: `.txt` (two samples included)
- Images: `.jpg`, `.jpeg`, `.png` (add your own)

Configure models in `benchmarks/config.py`.

### Running benchmarks

All commands run from the `backend/` directory:

```bash
cd demos/reasoning-notebook/backend

# All enabled models, all inputs
uv run python ../benchmarks/run_benchmark.py

# Local models only (requires LM Studio running with model loaded)
uv run python ../benchmarks/run_benchmark.py --models local

# Cloud models only
OPENAI_API_KEY=sk-... ANTHROPIC_API_KEY=sk-ant-... \
  uv run python ../benchmarks/run_benchmark.py --models cloud

# Text inputs only
uv run python ../benchmarks/run_benchmark.py --text-only

# Image inputs only
uv run python ../benchmarks/run_benchmark.py --image-only
```

### Viewing results

```bash
# Terminal summary (latest results)
uv run python ../benchmarks/compare.py

# HTML report with charts (opens in browser)
uv run python ../benchmarks/compare.py --html

# Specific results file
uv run python ../benchmarks/compare.py ../benchmarks/results/benchmark_20260430.json --html
```

The HTML report includes:
- Per-input comparison tables
- Entity extraction comparison (tags per model)
- Image scene analysis comparison
- Chart.js visualizations: entities per input, extraction time, entities vs relationships, speed vs quality scatter

### Notes

- LM Studio only keeps one model in memory at a time. When benchmarking multiple local models, only the currently loaded model will succeed.
- Cloud models require API keys set as environment variables.
- Results are saved as JSON in `benchmarks/results/` for later comparison.

## Configuration

Copy `.env.example` and adjust as needed. All settings have sensible defaults for local development.

| Variable | Default | Description |
|----------|---------|-------------|
| `INPUTLAYER_URL` | `ws://localhost:8080/ws` | Engine WebSocket URL |
| `INPUTLAYER_USER` | `admin` | Engine username |
| `KG_NAME` | `reasoning_notebook` | Knowledge graph name |
| `LLM_BASE_URL` | `http://localhost:1234/v1` | LLM API endpoint |
| `LLM_MODEL` | `gpt-4o-mini` | Model name (LM Studio ignores this) |
| `OPENAI_API_KEY` | — | OpenAI API key |
| `EXTRACTION_MAX_CHARS` | `4000` | Max content chars sent to LLM |
| `FRONTEND_ORIGIN` | `http://localhost:5173` | CORS origin |

## Resetting data

To start fresh, delete the engine's data directory and credentials:

```bash
# From the repository root
rm -rf data/ .inputlayer-credentials.toml
```

Then restart the demo. New credentials will be auto-generated.

## Project structure

```
demos/reasoning-notebook/
  backend/
    main.py           FastAPI app, CRUD, extraction, chat, provenance, image endpoints
    config.py          Environment configuration
    schemas.py         Pydantic request/response models
    extraction.py      LangChain entity/relationship extraction pipeline
    ontology.py        LangGraph ontology consolidation agent
    resolution.py      Entity resolution via HNSW vector similarity
    chat.py            Chat agent using KG context
    images.py          Image upload, storage, and multimodal extraction
  frontend/
    src/
      App.tsx          Main layout, routing, keyboard shortcuts
      api.ts           Typed fetch wrapper for all backend endpoints
      types.ts         TypeScript interfaces
      components/
        Sidebar.tsx          Note list with create/delete
        Editor.tsx           Markdown editor with preview and image drop
        ExtractionPanel.tsx  Entity/relationship display + image scene analysis
        GraphView.tsx        Force-directed graph + detail panel + provenance
        ChatPanel.tsx        Chat interface
        ProvenanceTree.tsx   Proof tree viewer modal
  notebooks/
    query_patterns.ipynb   Jupyter notebook: semantic, structured, hybrid, multimodal queries
  benchmarks/
    config.py          Model definitions (local + cloud)
    run_benchmark.py   Run extraction across models, save JSON results
    compare.py         Generate terminal + HTML comparison reports
    inputs/            Test text and images
    results/           Benchmark output (JSON + HTML)
  start.sh             Launch all three processes
  docker-compose.yml   Containerized deployment
  .env.example         Configuration template
```
