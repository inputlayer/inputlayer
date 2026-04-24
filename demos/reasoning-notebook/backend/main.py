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
from images import Image, extract_from_image, get_image_path, save_image
from ontology import consolidate_ontology
from resolution import EntityEmbedding, resolve_entities
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
    await kg.define(Note, Entity, Relationship, EntityEmbedding, Image)

    # Derived rules — these create inferred facts from extracted data
    rules = [
        # Two people mentioned in the same note are colleagues
        '+colleague(A, B) <- entity(_, A, "person", _, S), entity(_, B, "person", _, S), A != B',
        # Direct connection via any relationship (bidirectional)
        "+connected(A, B) <- relationship(_, A, _, B, _)",
        "+connected(A, B) <- relationship(_, B, _, A, _)",
    ]
    for rule in rules:
        try:
            await kg.execute(rule)
        except Exception:
            pass  # rule may already exist
    logger.info("Schema and rules deployed")

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


def _unescape_iql_string(s: Any) -> Any:
    """Unescape IQL string literals returned by the engine."""
    if not isinstance(s, str):
        return s
    return (
        s.replace("\\n", "\n")
        .replace("\\r", "\r")
        .replace("\\t", "\t")
        .replace("\\0", "\x00")
        .replace('\\"', '"')
        .replace("\\\\", "\\")
    )


def _row_to_note(columns: list[str], row: list[Any]) -> NoteResponse:
    data = {k: _unescape_iql_string(v) for k, v in zip(columns, row, strict=True)}
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
    img_source = f"img:{note_id}"
    await kg.execute(
        f'-note(Id, T, C, Ca, Ua) <- note(Id, T, C, Ca, Ua), Id = "{note_id}"'
    )
    # Delete text-extracted entities
    await kg.execute(
        f'-entity(Id, N, K, D, Src) <- entity(Id, N, K, D, Src), Src = "{note_id}"'
    )
    await kg.execute(
        f'-relationship(Id, S, P, O, Src) <- relationship(Id, S, P, O, Src), Src = "{note_id}"'
    )
    # Delete image-extracted entities
    await kg.execute(
        f'-entity(Id, N, K, D, Src) <- entity(Id, N, K, D, Src), Src = "{img_source}"'
    )
    await kg.execute(
        f'-relationship(Id, S, P, O, Src) <- relationship(Id, S, P, O, Src), Src = "{img_source}"'
    )


# ── Extraction ─────────────────────────────────────────────────────


@app.post("/notes/{note_id}/extract")
async def trigger_extraction(note_id: str, request: Request):
    kg = await get_kg(request)
    result = await kg.execute(
        f'?note("{note_id}", Title, Content, CreatedAt, UpdatedAt)'
    )
    logger.info(
        "Extract lookup note_id=%s columns=%s rows=%d",
        note_id, result.columns, len(result.rows or []),
    )
    if not result.rows or result.columns == ["error"]:
        raise HTTPException(status_code=404, detail="Note not found")
    data = dict(zip(result.columns, result.rows[0], strict=True))
    counts = await extract_from_note(kg, note_id, data["title"], data["content"])
    return counts


@app.get("/notes/{note_id}/entities")
async def get_note_entities(note_id: str, request: Request):
    kg = await get_kg(request)
    img_source = f"img:{note_id}"

    # Fetch both text-extracted and image-extracted entities
    text_ents = await kg.execute(f'?entity(Id, Name, Kind, Desc, "{note_id}")')
    img_ents = await kg.execute(f'?entity(Id, Name, Kind, Desc, "{img_source}")')
    text_rels = await kg.execute(f'?relationship(Id, Subject, Predicate, Object, "{note_id}")')
    img_rels = await kg.execute(f'?relationship(Id, Subject, Predicate, Object, "{img_source}")')

    def collect_rows(result):
        if not result.rows or result.columns == ["error"]:
            return []
        return [dict(zip(result.columns, row, strict=True)) for row in result.rows]

    return {
        "entities": collect_rows(text_ents) + collect_rows(img_ents),
        "relationships": collect_rows(text_rels) + collect_rows(img_rels),
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

    # Add derived edges from rules
    derived_queries = [
        ("colleague", "?colleague(A, B)"),
    ]
    entity_names = {n["name"] for n in nodes}
    seen_edges = {(e["subject"], e["predicate"], e["object"]) for e in edges}

    for predicate, query in derived_queries:
        try:
            result = await kg.execute(query)
            if result.rows and result.columns != ["error"]:
                for row in result.rows:
                    a, b = row[0], row[1]
                    if a in entity_names and b in entity_names and (a, predicate, b) not in seen_edges:
                        edges.append({
                            "id": f"derived_{predicate}_{a}_{b}",
                            "subject": a,
                            "predicate": predicate,
                            "object": b,
                            "source_note_id": "derived",
                            "derived": True,
                        })
                        seen_edges.add((a, predicate, b))
        except Exception:
            pass

    return {"nodes": nodes, "edges": edges}


# ── Ontology ───────────────────────────────────────────────────────


@app.get("/ontology/predicates")
async def list_predicates(request: Request):
    kg = await get_kg(request)
    result = await kg.execute("?relationship(_, _, Predicate, _, _)")
    predicates = sorted({row[0] for row in (result.rows or [])}) if result.rows else []
    return {"predicates": predicates}


@app.post("/ontology/cleanup")
async def cleanup_orphans(request: Request):
    """Remove entities and relationships whose source note no longer exists."""
    kg = await get_kg(request)
    notes = await kg.execute("?note(Id, Title, Content, Ca, Ua)")
    note_ids = {row[0] for row in (notes.rows or [])}

    def _is_orphan(source: str) -> bool:
        clean = source.replace("img:", "")
        return clean not in note_ids

    removed = 0
    ents = await kg.execute("?entity(Id, Name, Kind, Desc, Source)")
    for row in (ents.rows or []):
        if _is_orphan(row[4]):
            from inputlayer.integrations.langchain.params import iql_literal
            await kg.execute(
                f"-entity({iql_literal(row[0])}, {iql_literal(row[1])}, "
                f"{iql_literal(row[2])}, {iql_literal(row[3])}, {iql_literal(row[4])})"
            )
            removed += 1

    rels = await kg.execute("?relationship(Id, Subject, Predicate, Object, Source)")
    for row in (rels.rows or []):
        if _is_orphan(row[4]):
            from inputlayer.integrations.langchain.params import iql_literal
            await kg.execute(
                f"-relationship({iql_literal(row[0])}, {iql_literal(row[1])}, "
                f"{iql_literal(row[2])}, {iql_literal(row[3])}, {iql_literal(row[4])})"
            )
            removed += 1

    return {"removed": removed}


@app.post("/ontology/consolidate")
async def consolidate(request: Request):
    kg = await get_kg(request)
    return await consolidate_ontology(kg)


@app.post("/ontology/resolve")
async def resolve(request: Request):
    kg = await get_kg(request)
    return await resolve_entities(kg)


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
            "children": node.children or [],
            "source": node.source,
            "rule_id": node.rule_id,
            "bindings": node.bindings,
        }
    return {"roots": tree.roots, "nodes": nodes, "query": tree.query}


def _raw_tree_to_dict(tree: dict[str, Any]) -> dict[str, Any]:
    """Normalize a raw wire-format proof tree dict."""
    nodes = {}
    for nid, node in tree.get("nodes", {}).items():
        conc = node.get("conclusion", {})
        nodes[nid] = {
            "kind": node.get("kind", "unknown"),
            "conclusion": {"pred": conc.get("pred", ""), "args": conc.get("args", [])},
            "children": node.get("children", []),
            "source": node.get("source"),
            "rule_id": node.get("rule_id"),
            "bindings": node.get("bindings"),
        }
    return {"roots": tree.get("roots", []), "nodes": nodes, "query": tree.get("query")}


@app.post("/why")
async def why_endpoint(request: Request):
    body = await request.json()
    kg = await get_kg(request)
    query = body.get("query", "")
    if not query:
        raise HTTPException(status_code=400, detail="query is required")

    # Use _execute() to get the raw ResultResponse which includes proof_trees
    result = await kg._execute(f".why {query}")
    raw_trees = getattr(result, "proof_trees", None) or []

    from inputlayer.knowledge_graph import ProofTree

    trees = []
    for t in raw_trees:
        if isinstance(t, dict):
            trees.append(_raw_tree_to_dict(t))
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

    result = await kg._execute(f".why_not {query}")
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


# ── Images ─────────────────────────────────────────────────────────


@app.post("/notes/{note_id}/images")
async def upload_image(note_id: str, request: Request):
    from fastapi import UploadFile, File

    kg = await get_kg(request)

    # Read multipart form data
    form = await request.form()
    file = form.get("file")
    if file is None:
        raise HTTPException(status_code=400, detail="No file uploaded")

    data = await file.read()
    filename_orig = getattr(file, "filename", "image.jpg") or "image.jpg"
    image_id, filename = save_image(data, filename_orig)

    # Extract in foreground so we can return the description
    result = await extract_from_image(kg, image_id, note_id, filename)

    return {
        "image_id": image_id,
        "filename": filename,
        "url": f"/images/{filename}",
        "description": result.get("description", ""),
        "entities": result.get("entities", 0),
        "relationships": result.get("relationships", 0),
    }


@app.get("/images/{filename}")
async def serve_image(filename: str):
    from fastapi.responses import FileResponse

    path = get_image_path(filename)
    if path is None:
        raise HTTPException(status_code=404, detail="Image not found")
    return FileResponse(path)
