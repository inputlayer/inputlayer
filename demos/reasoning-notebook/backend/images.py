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


class ImageEntity(BaseModel):
    name: str = Field(description="Entity name, lowercase")
    kind: str = Field(description="Type: person, place, object, building, artwork, animal, concept")
    description: str = Field(description="One-sentence description")


class ImageRelationship(BaseModel):
    subject: str = Field(description="Source entity name")
    predicate: str = Field(description="Relationship type")
    object: str = Field(description="Target entity name")


class ImageAnalysis(BaseModel):
    scene: str = Field(description="Brief scene description, e.g. 'birthday party, indoors'")
    objects: list[str] = Field(description="List of objects visible, e.g. ['cake', 'candles', 'balloons']")
    people: str = Field(default="none", description="People count and description, e.g. '5 (2 children, 3 adults)' or 'none'")
    emotion: str = Field(default="neutral", description="Emotional quality, e.g. 'joyful, celebratory'")
    event_type: str = Field(default="", description="Type of event if applicable, e.g. 'birthday', 'ceremony', 'travel'")
    aesthetic: str = Field(default="", description="Visual style, e.g. 'warm lighting, candid' or 'dramatic, high contrast'")
    caption_seed: str = Field(default="", description="A short phrase that could caption this image, e.g. 'blowing out the candles'")
    cultural_context: str = Field(default="", description="Cultural or historical context if applicable")
    visible_text: str = Field(default="", description="Any text visible in the image")
    entities: list[ImageEntity] = Field(default_factory=list, description="Notable entities found")
    relationships: list[ImageRelationship] = Field(default_factory=list, description="Relationships between entities")


# ── KG Schema for image analysis ───────────────────────────────────


class ImageScene(Relation):
    """Scene-level analysis of an image."""

    image_id: str
    note_id: str
    scene: str
    objects: str
    people: str
    emotion: str
    event_type: str
    aesthetic: str
    caption_seed: str
    cultural_context: str
    visible_text: str


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
    "Analyze this image and extract structured information.\n\n"
    "Provide:\n"
    "- scene: brief description (e.g. 'birthday party, indoors')\n"
    "- objects: list of visible objects (e.g. ['cake', 'candles'])\n"
    "- people: count and description (e.g. '5 (2 children, 3 adults)') or 'none'\n"
    "- emotion: emotional quality (e.g. 'joyful, celebratory')\n"
    "- event_type: type of event (e.g. 'birthday', 'ceremony', 'travel')\n"
    "- aesthetic: visual style (e.g. 'warm lighting, candid')\n"
    "- caption_seed: a short caption phrase (e.g. 'blowing out the candles')\n"
    "- cultural_context: cultural or historical context if any\n"
    "- visible_text: any text visible in the image\n"
    "- entities: notable entities with name, kind, description\n"
    "- relationships: how entities relate to each other"
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

    # Step 2: Structured extraction with rich schema
    entities_count = 0
    relationships_count = 0
    analysis = None
    try:
        extractor = llm.with_structured_output(ImageAnalysis)
        analysis = await extractor.ainvoke([
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

        # Store scene-level analysis
        await kg.execute(
            f"+image_scene({iql_literal(image_id)}, {iql_literal(note_id)}, "
            f"{iql_literal(analysis.scene)}, {iql_literal(', '.join(analysis.objects))}, "
            f"{iql_literal(analysis.people)}, {iql_literal(analysis.emotion)}, "
            f"{iql_literal(analysis.event_type)}, {iql_literal(analysis.aesthetic)}, "
            f"{iql_literal(analysis.caption_seed)}, {iql_literal(analysis.cultural_context)}, "
            f"{iql_literal(analysis.visible_text)})"
        )

        # Store extracted entities with img: source prefix
        img_source = f"img:{note_id}"
        for i, e in enumerate(analysis.entities):
            eid = f"i_{image_id}_e{i}"
            await kg.execute(
                f"+entity({iql_literal(eid)}, {iql_literal(e.name)}, "
                f"{iql_literal(e.kind)}, {iql_literal(e.description)}, "
                f"{iql_literal(img_source)})"
            )
            entities_count += 1

        # Store extracted relationships with img: source prefix
        for i, r in enumerate(analysis.relationships):
            rid = f"i_{image_id}_r{i}"
            await kg.execute(
                f"+relationship({iql_literal(rid)}, {iql_literal(r.subject)}, "
                f"{iql_literal(r.predicate)}, {iql_literal(r.object)}, "
                f"{iql_literal(img_source)})"
            )
            relationships_count += 1

    except Exception:
        logger.exception("Vision structured extraction failed for image %s", image_id)

    logger.info(
        "Image %s: description=%d chars, entities=%d, relationships=%d",
        image_id, len(description), entities_count, relationships_count,
    )

    result: dict[str, Any] = {
        "image_id": image_id,
        "description": description,
        "entities": entities_count,
        "relationships": relationships_count,
    }
    if analysis:
        result["analysis"] = {
            "scene": analysis.scene,
            "objects": analysis.objects,
            "people": analysis.people,
            "emotion": analysis.emotion,
            "event_type": analysis.event_type,
            "aesthetic": analysis.aesthetic,
            "caption_seed": analysis.caption_seed,
            "cultural_context": analysis.cultural_context,
            "visible_text": analysis.visible_text,
        }
    return result
