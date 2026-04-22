"""Image upload, storage, and multimodal extraction."""

from __future__ import annotations

import base64
import logging
import os
import uuid
from pathlib import Path
from typing import Any

from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from inputlayer import Relation
from inputlayer.integrations.langchain.params import iql_literal

from config import LLM_API_KEY, LLM_BASE_URL, LLM_MODEL

logger = logging.getLogger("reasoning_notebook.images")

UPLOAD_DIR = Path(__file__).parent / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)


# ── KG Schema ──────────────────────────────────────────────────────


class Image(Relation):
    """An image attached to a note."""

    id: str
    note_id: str
    filename: str
    description: str


# ── Structured extraction model ────────────────────────────────────


class ImageExtraction(BaseModel):
    scene: str = Field(description="Brief description of the scene")
    objects: list[str] = Field(description="List of objects visible")
    cultural_context: str = Field(default="", description="Cultural or historical context if applicable")
    visible_text: str = Field(default="", description="Any text visible in the image")
    mood: str = Field(default="", description="Emotional quality of the image")
    entities: list[ImageEntity] = Field(default_factory=list, description="Entities found")
    relationships: list[ImageRelationship] = Field(default_factory=list, description="Relationships between entities")


class ImageEntity(BaseModel):
    name: str = Field(description="Entity name, lowercase")
    kind: str = Field(description="Type: person, place, object, building, artwork, animal, concept")
    description: str = Field(description="One-sentence description")


class ImageRelationship(BaseModel):
    subject: str = Field(description="Source entity name")
    predicate: str = Field(description="Relationship type")
    object: str = Field(description="Target entity name")


# Fix forward reference
ImageExtraction.model_rebuild()


# ── Storage ────────────────────────────────────────────────────────


def save_image(data: bytes, original_filename: str) -> tuple[str, str]:
    """Save image to disk. Returns (image_id, filepath)."""
    image_id = uuid.uuid4().hex[:12]
    ext = Path(original_filename).suffix or ".jpg"
    filename = f"{image_id}{ext}"
    filepath = UPLOAD_DIR / filename
    filepath.write_bytes(data)
    return image_id, filename


def get_image_path(filename: str) -> Path | None:
    """Get the full path to an uploaded image."""
    path = UPLOAD_DIR / filename
    return path if path.is_file() else None


# ── Multimodal extraction ──────────────────────────────────────────


VISION_PROMPT = (
    "Analyze this image and extract structured information.\n"
    "Identify: the scene, objects, any cultural context, visible text, "
    "mood, and notable entities and their relationships.\n"
    "For entities, use lowercase names and categorize them "
    "(person, place, object, building, artwork, animal, concept).\n"
    "For relationships, describe how entities relate to each other."
)


async def extract_from_image(
    kg: Any, image_id: str, note_id: str, filename: str
) -> dict[str, Any]:
    """Send image to vision LLM and extract entities into the KG."""
    filepath = UPLOAD_DIR / filename
    if not filepath.is_file():
        return {"error": "Image file not found"}

    img_bytes = filepath.read_bytes()
    img_b64 = base64.b64encode(img_bytes).decode()

    # Detect mime type
    ext = filepath.suffix.lower()
    mime = {"jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
            ".gif": "image/gif", ".webp": "image/webp"}.get(ext, "image/jpeg")

    llm = ChatOpenAI(
        base_url=LLM_BASE_URL,
        api_key=LLM_API_KEY,
        model=LLM_MODEL,
        temperature=0,
        max_tokens=800,
    )

    # Step 1: Get a text description (plain completion, more reliable)
    try:
        from langchain_core.messages import HumanMessage

        description_resp = await llm.ainvoke([
            HumanMessage(content=[
                {"type": "text", "text": "Describe this image in 2-3 sentences. Be specific about what you see."},
                {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{img_b64}"}},
            ])
        ])
        description = description_resp.content.strip()
    except Exception:
        logger.exception("Vision description failed for image %s", image_id)
        description = ""

    # Step 2: Structured extraction
    entities_count = 0
    relationships_count = 0
    try:
        extractor = llm.with_structured_output(ImageExtraction)
        extraction = await extractor.ainvoke([
            HumanMessage(content=[
                {"type": "text", "text": VISION_PROMPT},
                {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{img_b64}"}},
            ])
        ])

        # Store image record with description
        await kg.execute(
            f"+image({iql_literal(image_id)}, {iql_literal(note_id)}, "
            f"{iql_literal(filename)}, {iql_literal(description)})"
        )

        # Store extracted entities
        for i, e in enumerate(extraction.entities):
            eid = f"{image_id}_e{i}"
            await kg.execute(
                f"+entity({iql_literal(eid)}, {iql_literal(e.name)}, "
                f"{iql_literal(e.kind)}, {iql_literal(e.description)}, "
                f"{iql_literal(note_id)})"
            )
            entities_count += 1

        # Store extracted relationships
        for i, r in enumerate(extraction.relationships):
            rid = f"{image_id}_r{i}"
            await kg.execute(
                f"+relationship({iql_literal(rid)}, {iql_literal(r.subject)}, "
                f"{iql_literal(r.predicate)}, {iql_literal(r.object)}, "
                f"{iql_literal(note_id)})"
            )
            relationships_count += 1

    except Exception:
        logger.exception("Vision structured extraction failed for image %s", image_id)

    logger.info(
        "Image %s: description=%d chars, entities=%d, relationships=%d",
        image_id, len(description), entities_count, relationships_count,
    )

    return {
        "image_id": image_id,
        "description": description,
        "entities": entities_count,
        "relationships": relationships_count,
    }
