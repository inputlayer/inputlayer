"""Entity resolution via vector similarity.

Embeds entity names + descriptions into a simple vector space,
builds an HNSW index, and finds near-duplicates for merging.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

from inputlayer import HnswIndex, Relation, Vector
from inputlayer.integrations.langchain.params import iql_literal

logger = logging.getLogger("reasoning_notebook.resolution")

EMBED_DIM = 32


# ── KG Schema ──────────────────────────────────────────────────────


class EntityEmbedding(Relation):
    """Entity embedding for similarity search."""

    id: str
    entity_name: str
    embedding: Vector


# ── Simple character n-gram embedder (no external service needed) ──


def _char_ngram_embed(text: str, dim: int = EMBED_DIM) -> list[float]:
    """Deterministic character n-gram embedding.

    Hashes character trigrams into a fixed-size vector.
    Not production quality, but works for demonstrating
    the HNSW similarity search pattern.
    """
    text = text.lower().strip()
    vec = [0.0] * dim
    for i in range(len(text) - 2):
        trigram = text[i : i + 3]
        h = int(hashlib.md5(trigram.encode()).hexdigest(), 16)
        idx = h % dim
        vec[idx] += 1.0

    # Normalize
    norm = sum(v * v for v in vec) ** 0.5
    if norm > 0:
        vec = [v / norm for v in vec]
    else:
        vec = [1.0 / dim**0.5] * dim
    return vec


# ── Resolution pipeline ───────────────────────────────────────────


async def resolve_entities(kg: Any, threshold: float = 0.85) -> dict[str, Any]:
    """Find and merge near-duplicate entities using vector similarity.

    1. Get all unique entity names
    2. Embed each name + description
    3. Store embeddings and build HNSW index
    4. For each entity, find nearest neighbors above threshold
    5. Merge duplicates (keep the longer/more common name)

    Returns summary of merges performed.
    """

    # Step 1: Get all entities
    ent_result = await kg.execute("?entity(Id, Name, Kind, Desc, Source)")
    if not ent_result.rows or ent_result.columns == ["error"]:
        return {"status": "no_entities"}

    # Group by name
    name_info: dict[str, dict[str, Any]] = {}
    for row in ent_result.rows:
        data = dict(zip(ent_result.columns, row, strict=True))
        name = data["name"]
        if name not in name_info:
            name_info[name] = {
                "kind": data["kind"],
                "description": data["description"],
                "count": 0,
            }
        name_info[name]["count"] += 1

    if len(name_info) < 2:
        return {"status": "too_few_entities", "count": len(name_info)}

    # Step 2: Define schema and create embeddings
    await kg.define(EntityEmbedding)

    # Clear old embeddings
    try:
        await kg.execute("-entity_embedding(I, N, E) <- entity_embedding(I, N, E)")
    except Exception:
        pass

    # Insert embeddings
    for name, info in name_info.items():
        embed_text = f"{name} {info['kind']} {info['description']}"
        vec = _char_ngram_embed(embed_text)
        emb = EntityEmbedding(id=f"emb_{name}", entity_name=name, embedding=vec)
        await kg.insert(emb)

    # Step 3: Create HNSW index
    try:
        await kg.execute(".index drop entity_name_idx")
    except Exception:
        pass

    await kg.create_index(
        HnswIndex(
            name="entity_name_idx",
            relation=EntityEmbedding,
            column="embedding",
            metric="cosine",
        )
    )

    # Step 4: Find near-duplicates
    merges: list[dict[str, str]] = []
    merged_away: set[str] = set()

    names = sorted(name_info.keys())
    for name in names:
        if name in merged_away:
            continue

        vec = _char_ngram_embed(f"{name} {name_info[name]['kind']} {name_info[name]['description']}")

        try:
            result = await kg.vector_search(
                EntityEmbedding, vec, k=5, metric="cosine"
            )
        except Exception:
            continue

        for row in (result.rows or []):
            match_data = dict(zip(result.columns, row, strict=True))
            match_name = match_data["entity_name"]
            if match_name == name or match_name in merged_away:
                continue

            # Compute similarity (cosine distance → similarity)
            match_vec = _char_ngram_embed(
                f"{match_name} {name_info[match_name]['kind']} {name_info[match_name]['description']}"
            )
            sim = sum(a * b for a, b in zip(vec, match_vec))

            if sim >= threshold:
                # Keep the longer name as canonical
                canonical = name if len(name) >= len(match_name) else match_name
                variant = match_name if canonical == name else name
                merges.append({"canonical": canonical, "variant": variant, "similarity": round(sim, 3)})
                merged_away.add(variant)

    # Step 5: Apply merges
    applied = 0
    for merge in merges:
        old_name = merge["variant"]
        new_name = merge["canonical"]

        # Update entities
        ent_rows = await kg.execute(f"?entity(Id, {iql_literal(old_name)}, Kind, Desc, Source)")
        for row in (ent_rows.rows or []):
            eid, kind, desc, source = row[0], row[2], row[3], row[4]
            await kg.execute(
                f"-entity({iql_literal(eid)}, {iql_literal(old_name)}, "
                f"{iql_literal(kind)}, {iql_literal(desc)}, {iql_literal(source)})"
            )
            await kg.execute(
                f"+entity({iql_literal(eid)}, {iql_literal(new_name)}, "
                f"{iql_literal(kind)}, {iql_literal(desc)}, {iql_literal(source)})"
            )
            applied += 1

        # Update relationships (subject)
        subj_rows = await kg.execute(f"?relationship(Id, {iql_literal(old_name)}, Pred, Obj, Source)")
        for row in (subj_rows.rows or []):
            rid, pred, obj, source = row[0], row[2], row[3], row[4]
            await kg.execute(
                f"-relationship({iql_literal(rid)}, {iql_literal(old_name)}, "
                f"{iql_literal(pred)}, {iql_literal(obj)}, {iql_literal(source)})"
            )
            await kg.execute(
                f"+relationship({iql_literal(rid)}, {iql_literal(new_name)}, "
                f"{iql_literal(pred)}, {iql_literal(obj)}, {iql_literal(source)})"
            )

        # Update relationships (object)
        obj_rows = await kg.execute(f"?relationship(Id, Subj, Pred, {iql_literal(old_name)}, Source)")
        for row in (obj_rows.rows or []):
            rid, subj, pred, source = row[0], row[1], row[2], row[4]
            await kg.execute(
                f"-relationship({iql_literal(rid)}, {iql_literal(subj)}, "
                f"{iql_literal(pred)}, {iql_literal(old_name)}, {iql_literal(source)})"
            )
            await kg.execute(
                f"+relationship({iql_literal(rid)}, {iql_literal(subj)}, "
                f"{iql_literal(pred)}, {iql_literal(new_name)}, {iql_literal(source)})"
            )

    logger.info("Entity resolution: %d merges applied", applied)

    return {
        "status": "done",
        "merges": merges,
        "entities_renamed": applied,
    }
