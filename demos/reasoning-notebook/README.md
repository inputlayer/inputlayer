# Reasoning Notebook

A self-contained demo: Obsidian-like note editor backed by InputLayer for reasoning, LangChain for extraction, and LangGraph for ontology consolidation.

Write notes, watch the knowledge graph grow, ask questions across everything you've written.

## Architecture

```
Frontend (Vite + React)  -->  Backend (FastAPI)  -->  InputLayer (Rust)
       :5173              REST      :8000          WebSocket  :8080
                            |
                     LangChain (extraction)
                     LangGraph (ontology agent)
                     ChatOpenAI (Q&A)
```

Three processes:
- **InputLayer server** (Rust) — reasoning engine, stores facts and rules, incremental maintenance, provenance
- **FastAPI backend** (Python) — note CRUD, LLM extraction pipeline, chat, ontology consolidation
- **Vite frontend** (React) — markdown editor, force-directed graph, chat panel, provenance viewer

## Quick start

Prerequisites: Rust toolchain (for InputLayer), Python 3.10+, uv, Bun, and an LLM (LM Studio or OpenAI API key).

```bash
# 1. Build the engine (if not already built)
cargo build --release --bin inputlayer-server

# 2. Start everything
./start.sh
```

Open http://localhost:5173

### Manual start (three terminals)

```bash
# Terminal 1: Engine
./target/release/inputlayer-server

# Terminal 2: Backend
cd demos/reasoning-notebook/backend
uv sync
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Terminal 3: Frontend
cd demos/reasoning-notebook/frontend
bun install
bun run dev
```

### LLM setup

The extraction pipeline and chat need an LLM. Either:

**LM Studio** (default): Open LM Studio, load a model (Qwen 2.5 7B or similar), start the local server. The backend auto-connects to `localhost:1234`.

**OpenAI**: Set the API key before starting the backend:
```bash
OPENAI_API_KEY=sk-... uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## Features

### Editor (Cmd+E)
- Create, edit, delete markdown notes
- Auto-save with 800ms debounce or Cmd+S
- Notes persisted in InputLayer KG

### Extraction
- On each save, LLM extracts entities and relationships from note content
- Manual "Extract" button for on-demand extraction
- Entity tags and relationship list shown below the editor

### Graph (Cmd+G)
- Force-directed visualization of all entities and relationships
- Nodes colored by kind (person, organization, technology, concept, etc.)
- Click a node to see detail panel: description, source notes, relationships
- "Why?" button shows provenance proof tree
- "Consolidate Ontology" normalizes synonymous predicates and entity names

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

## Configuration

Copy `.env.example` and adjust as needed. All settings have sensible defaults for local development.

## Project structure

```
demos/reasoning-notebook/
  backend/
    main.py           FastAPI app, CRUD, extraction, chat, provenance endpoints
    config.py          Environment configuration
    schemas.py         Pydantic request/response models
    extraction.py      LangChain entity/relationship extraction pipeline
    ontology.py        LangGraph ontology consolidation agent
    chat.py            Chat agent using KG context
  frontend/
    src/
      App.tsx          Main layout, routing, keyboard shortcuts
      api.ts           Typed fetch wrapper for all backend endpoints
      types.ts         TypeScript interfaces
      components/
        Sidebar.tsx          Note list with create/delete
        Editor.tsx           Markdown editor with auto-save
        ExtractionPanel.tsx  Entity/relationship display + extract button
        GraphView.tsx        Force-directed graph + detail panel
        ChatPanel.tsx        Chat interface
        ProvenanceTree.tsx   Proof tree viewer modal
  start.sh             Launch all three processes
  docker-compose.yml   Containerized deployment
  .env.example         Configuration template
```
