"""LangChain extraction pipeline: note -> entities + relationships."""

from __future__ import annotations

import logging
import os
from typing import Any

from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from inputlayer import Relation
from inputlayer.integrations.langchain.params import iql_literal

from config import LLM_API_KEY, LLM_BASE_URL, LLM_MODEL

logger = logging.getLogger("reasoning_notebook.extraction")


# ── KG Schema ──────────────────────────────────────────────────────


class Entity(Relation):
    """An entity extracted from a note."""

    id: str
    name: str
    kind: str
    description: str
    source_note_id: str


class Relationship(Relation):
    """A relationship between two entities extracted from a note."""

    id: str
    subject: str
    predicate: str
    object: str
    source_note_id: str


# ── Extraction models (Pydantic for structured output) ─────────────


class ExtractedEntity(BaseModel):
    name: str = Field(description="Entity name, normalized to lowercase")
    kind: str = Field(
        description="Type: person, organization, technology, concept, place, event, role"
    )
    description: str = Field(description="One-sentence description of the entity")


class ExtractedRelationship(BaseModel):
    subject: str = Field(description="Source entity name (must match an entity name)")
    predicate: str = Field(
        description="Relationship type, e.g. works_at, uses, created_by, part_of, collaborates_with"
    )
    object: str = Field(description="Target entity name (must match an entity name)")


class Extraction(BaseModel):
    entities: list[ExtractedEntity] = Field(description="Entities found in the text")
    relationships: list[ExtractedRelationship] = Field(
        description="Relationships between extracted entities"
    )


# ── Pipeline ───────────────────────────────────────────────────────


def _get_llm() -> ChatOpenAI:
    return ChatOpenAI(
        base_url=LLM_BASE_URL,
        api_key=LLM_API_KEY,
        model=LLM_MODEL,
        temperature=0,
    )


EXTRACTION_PROMPT = (
    "Extract all entities (people, organizations, technologies, concepts, "
    "places, events, roles) and their relationships from the following note.\n\n"
    "Rules:\n"
    "- Normalize all entity names to lowercase\n"
    "- Use short, consistent predicate names (works_at, uses, part_of, "
    "collaborates_with, manages, created_by, reports_to, located_in)\n"
    "- Every relationship's subject and object must match an entity name exactly\n"
    "- If the text is empty or has no extractable entities, return empty lists\n\n"
    "Note title: {title}\n\n"
    "Note content:\n{content}"
)


async def extract_from_note(kg: Any, note_id: str, title: str, content: str) -> dict[str, Any]:
    """Extract entities and relationships from a note and store in the KG.

    Returns counts: {"entities": N, "relationships": M}.
    """
    if not content.strip():
        return {"entities": 0, "relationships": 0}

    # Truncate very long content to avoid exceeding model context limits
    max_chars = int(os.environ.get("EXTRACTION_MAX_CHARS", "4000"))
    truncated_content = content[:max_chars]

    llm = _get_llm()
    extractor = llm.with_structured_output(Extraction)

    prompt = EXTRACTION_PROMPT.format(title=title, content=truncated_content)

    try:
        result = await extractor.ainvoke(prompt)
    except Exception as exc:
        logger.exception("Extraction failed for note %s", note_id)
        return {"entities": 0, "relationships": 0, "error": str(exc)}

    # Retract ALL old extractions for this note, then re-insert
    # (both text and image entities share the same source_note_id)
    await kg.execute(
        f'-entity(Id, N, K, D, Src) <- entity(Id, N, K, D, Src), Src = "{note_id}"'
    )
    await kg.execute(
        f'-relationship(Id, S, P, O, Src) <- relationship(Id, S, P, O, Src), Src = "{note_id}"'
    )

    # Insert new entities with text prefix
    for i, e in enumerate(result.entities):
        eid = f"t_{note_id}_e{i}"
        await kg.execute(
            f"+entity({iql_literal(eid)}, {iql_literal(e.name)}, "
            f"{iql_literal(e.kind)}, {iql_literal(e.description)}, "
            f"{iql_literal(note_id)})"
        )

    # Insert new relationships with text prefix
    for i, r in enumerate(result.relationships):
        rid = f"t_{note_id}_r{i}"
        await kg.execute(
            f"+relationship({iql_literal(rid)}, {iql_literal(r.subject)}, "
            f"{iql_literal(r.predicate)}, {iql_literal(r.object)}, "
            f"{iql_literal(note_id)})"
        )

    logger.info(
        "Extracted %d entities, %d relationships from note %s",
        len(result.entities),
        len(result.relationships),
        note_id,
    )
    return {"entities": len(result.entities), "relationships": len(result.relationships)}
