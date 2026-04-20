from __future__ import annotations

import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

from fastapi import BackgroundTasks
from inputlayer import InputLayer, Relation

from config import (
    FRONTEND_ORIGIN,
    INPUTLAYER_PASSWORD,
    INPUTLAYER_URL,
    INPUTLAYER_USER,
    KG_NAME,
)
from chat import chat as chat_fn
from extraction import Entity, Relationship, extract_from_note
from ontology import consolidate_ontology
from schemas import ChatRequest, ChatResponse, NoteCreate, NoteResponse, NoteUpdate

logger = logging.getLogger("reasoning_notebook")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")


# ── KG Schema ──────────────────────────────────────────────────────


class Note(Relation):
    """A markdown note stored in the knowledge graph."""

    id: str
    title: str
    content: str
    created_at: int
    updated_at: int


# ── App setup ──────────────────────────────────────────────────────


async def _connect(app: FastAPI) -> None:
    """Connect to InputLayer and deploy schema. Used at startup and for reconnect."""
    il = InputLayer(INPUTLAYER_URL, username=INPUTLAYER_USER, password=INPUTLAYER_PASSWORD)
    await il.connect()
    logger.info("Connected to InputLayer at %s", INPUTLAYER_URL)

    kg = il.knowledge_graph(KG_NAME)
    await kg.define(Note, Entity, Relationship)
    logger.info("Schema deployed: Note, Entity, Relationship")

    app.state.il = il
    app.state.kg = kg


@asynccontextmanager
async def lifespan(app: FastAPI):
    await _connect(app)
    yield
    await app.state.il.close()
    logger.info("Disconnected from InputLayer")


app = FastAPI(title="Reasoning Notebook", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def get_kg(request: Request) -> Any:
    """Get the KG handle, reconnecting if the WebSocket dropped."""
    try:
        il = request.app.state.il
        conn = il._conn
        ws = conn._ws
        if ws is None or not conn._connected or (hasattr(ws, "close_code") and ws.close_code is not None):
            raise ConnectionError("stale connection")
    except (AttributeError, ConnectionError):
        logger.warning("WebSocket disconnected, reconnecting...")
        await _connect(request.app)
    return request.app.state.kg


# ── Health ─────────────────────────────────────────────────────────


@app.get("/health")
async def health(request: Request):
    kg = await get_kg(request)
    try:
        result = await kg.execute("?__health(1)")
    except Exception:
        result = None
    return {
        "status": "ok",
        "engine": "connected" if result is not None else "error",
        "kg": KG_NAME,
    }


# ── Note CRUD ──────────────────────────────────────────────────────


def _row_to_note(columns: list[str], row: list[Any]) -> NoteResponse:
    data = dict(zip(columns, row, strict=True))
    return NoteResponse(**data)


@app.post("/notes", status_code=201)
async def create_note(body: NoteCreate, request: Request) -> NoteResponse:
    kg = await get_kg(request)
    now = int(time.time())
    note = Note(
        id=uuid.uuid4().hex[:12],
        title=body.title,
        content=body.content,
        created_at=now,
        updated_at=now,
    )
    await kg.insert(note)
    return NoteResponse(
        id=note.id,
        title=note.title,
        content=note.content,
        created_at=note.created_at,
        updated_at=note.updated_at,
    )


@app.get("/notes")
async def list_notes(request: Request) -> list[NoteResponse]:
    kg = await get_kg(request)
    result = await kg.execute("?note(Id, Title, Content, CreatedAt, UpdatedAt)")
    if not result.rows or result.columns == ["error"]:
        return []
    return [_row_to_note(result.columns, row) for row in result.rows]


@app.get("/notes/{note_id}")
async def get_note(note_id: str, request: Request) -> NoteResponse:
    kg = await get_kg(request)
    result = await kg.execute(
        f'?note("{note_id}", Title, Content, CreatedAt, UpdatedAt)'
    )
    if not result.rows or result.columns == ["error"]:
        raise HTTPException(status_code=404, detail="Note not found")
    return _row_to_note(result.columns, result.rows[0])


@app.put("/notes/{note_id}")
async def update_note(
    note_id: str,
    body: NoteUpdate,
    request: Request,
    bg: BackgroundTasks,
) -> NoteResponse:
    kg = await get_kg(request)

    existing = await kg.execute(
        f'?note("{note_id}", Title, Content, CreatedAt, UpdatedAt)'
    )
    if not existing.rows or existing.columns == ["error"]:
        raise HTTPException(status_code=404, detail="Note not found")

    old_data = dict(zip(existing.columns, existing.rows[0], strict=True))
    new_title = body.title if body.title is not None else old_data["title"]
    new_content = body.content if body.content is not None else old_data["content"]
    created_at = old_data["created_at"]
    now = int(time.time())

    await kg.execute(
        f'-note(Id, T, C, Ca, Ua) <- note(Id, T, C, Ca, Ua), Id = "{note_id}"'
    )
    note = Note(
        id=note_id,
        title=new_title,
        content=new_content,
        created_at=created_at,
        updated_at=now,
    )
    await kg.insert(note)

    bg.add_task(extract_from_note, kg, note_id, new_title, new_content)

    return NoteResponse(
        id=note_id,
        title=new_title,
        content=new_content,
        created_at=created_at,
        updated_at=now,
    )


@app.delete("/notes/{note_id}", status_code=204)
async def delete_note(note_id: str, request: Request):
    kg = await get_kg(request)
    await kg.execute(
        f'-note(Id, T, C, Ca, Ua) <- note(Id, T, C, Ca, Ua), Id = "{note_id}"'
    )
    await kg.execute(
        f'-entity(Id, N, K, D, Src) <- entity(Id, N, K, D, Src), Src = "{note_id}"'
    )
    await kg.execute(
        f'-relationship(Id, S, P, O, Src) <- relationship(Id, S, P, O, Src), Src = "{note_id}"'
    )


# ── Extraction ─────────────────────────────────────────────────────


@app.post("/notes/{note_id}/extract")
async def trigger_extraction(note_id: str, request: Request):
    kg = await get_kg(request)
    result = await kg.execute(
        f'?note("{note_id}", Title, Content, CreatedAt, UpdatedAt)'
    )
    if not result.rows or result.columns == ["error"]:
        raise HTTPException(status_code=404, detail="Note not found")
    data = dict(zip(result.columns, result.rows[0], strict=True))
    counts = await extract_from_note(kg, note_id, data["title"], data["content"])
    return counts


@app.get("/notes/{note_id}/entities")
async def get_note_entities(note_id: str, request: Request):
    kg = await get_kg(request)
    entities = await kg.execute(
        f'?entity(Id, Name, Kind, Desc, "{note_id}")'
    )
    relationships = await kg.execute(
        f'?relationship(Id, Subject, Predicate, Object, "{note_id}")'
    )
    return {
        "entities": [
            dict(zip(entities.columns, row, strict=True))
            for row in (entities.rows or [])
            if entities.columns != ["error"]
        ],
        "relationships": [
            dict(zip(relationships.columns, row, strict=True))
            for row in (relationships.rows or [])
            if relationships.columns != ["error"]
        ],
    }


@app.get("/graph")
async def get_graph(request: Request):
    kg = await get_kg(request)
    ent_result = await kg.execute("?entity(Id, Name, Kind, Desc, SourceNoteId)")
    rel_result = await kg.execute("?relationship(Id, Subject, Predicate, Object, SourceNoteId)")

    nodes = []
    if ent_result.rows and ent_result.columns != ["error"]:
        for row in ent_result.rows:
            data = dict(zip(ent_result.columns, row, strict=True))
            nodes.append(data)

    edges = []
    if rel_result.rows and rel_result.columns != ["error"]:
        for row in rel_result.rows:
            data = dict(zip(rel_result.columns, row, strict=True))
            edges.append(data)

    return {"nodes": nodes, "edges": edges}


# ── Ontology ───────────────────────────────────────────────────────


@app.get("/ontology/predicates")
async def list_predicates(request: Request):
    kg = await get_kg(request)
    result = await kg.execute("?relationship(_, _, Predicate, _, _)")
    predicates = sorted({row[0] for row in (result.rows or [])}) if result.rows else []
    return {"predicates": predicates}


@app.post("/ontology/consolidate")
async def consolidate(request: Request):
    kg = await get_kg(request)
    return await consolidate_ontology(kg)


# ── Chat ───────────────────────────────────────────────────────────


@app.post("/chat")
async def chat_endpoint(body: ChatRequest, request: Request) -> ChatResponse:
    kg = await get_kg(request)
    reply = await chat_fn(kg, body.message, body.history)
    return ChatResponse(reply=reply)


# ── Provenance ─────────────────────────────────────────────────────


def _proof_tree_to_dict(tree) -> dict[str, Any]:
    """Serialize a ProofTree to a JSON-safe dict."""
    nodes = {}
    for nid, node in tree.nodes.items():
        nodes[nid] = {
            "kind": node.kind,
            "conclusion": {"pred": node.conclusion.pred, "args": node.conclusion.args},
            "children": node.children,
            "source": node.source,
            "rule_id": node.rule_id,
            "bindings": node.bindings,
        }
    return {"roots": tree.roots, "nodes": nodes, "query": tree.query}


@app.post("/why")
async def why_endpoint(request: Request):
    body = await request.json()
    kg = await get_kg(request)
    query = body.get("query", "")
    if not query:
        raise HTTPException(status_code=400, detail="query is required")

    result = await kg.execute(f".why {query}")
    raw_trees = getattr(result, "proof_trees", None) or []

    from inputlayer.knowledge_graph import ProofTree

    trees = []
    for t in raw_trees:
        if isinstance(t, dict):
            trees.append(t)
        elif isinstance(t, ProofTree):
            trees.append(_proof_tree_to_dict(t))

    return {
        "columns": result.columns,
        "rows": result.rows or [],
        "proof_trees": trees,
    }


@app.post("/why_not")
async def why_not_endpoint(request: Request):
    body = await request.json()
    kg = await get_kg(request)
    query = body.get("query", "")
    if not query:
        raise HTTPException(status_code=400, detail="query is required")

    result = await kg.execute(f".why_not {query}")
    text = "\n".join(str(row[0]) for row in (result.rows or []))
    raw_trees = getattr(result, "proof_trees", None) or []

    from inputlayer.knowledge_graph import ProofTree

    tree = None
    if raw_trees:
        t = raw_trees[0]
        if isinstance(t, dict):
            tree = t
        elif isinstance(t, ProofTree):
            tree = _proof_tree_to_dict(t)

    return {"text": text, "proof_tree": tree}
