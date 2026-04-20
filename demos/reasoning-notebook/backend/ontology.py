"""LangGraph ontology consolidation agent.

Scans all predicates and entity names, proposes normalizations
(e.g. "works for" / "employed by" -> "works_at"), and applies
the merges as IQL retract+insert operations.
"""

from __future__ import annotations

import logging
from typing import Any

from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from inputlayer.integrations.langchain.params import iql_literal
from inputlayer.integrations.langgraph import InputLayerState

from config import LLM_API_KEY, LLM_BASE_URL, LLM_MODEL

logger = logging.getLogger("reasoning_notebook.ontology")


# ── LLM structured output models ──────────────────────────────────


class PredicateMerge(BaseModel):
    variants: list[str] = Field(description="Predicate names that mean the same thing")
    canonical: str = Field(description="Single canonical predicate name to keep (snake_case)")


class EntityMerge(BaseModel):
    variants: list[str] = Field(description="Entity names that refer to the same thing")
    canonical: str = Field(description="Single canonical entity name to keep (lowercase)")


class MergeProposal(BaseModel):
    predicate_merges: list[PredicateMerge] = Field(
        default_factory=list,
        description="Groups of synonymous predicates to unify",
    )
    entity_merges: list[EntityMerge] = Field(
        default_factory=list,
        description="Groups of entity names that refer to the same thing",
    )


# ── State ──────────────────────────────────────────────────────────


class OntologyState(InputLayerState):
    predicates: list[str]
    entity_names: list[str]
    proposal: dict[str, Any]
    applied: dict[str, int]
    iteration: int
    max_iterations: int
    status: str


# ── Pipeline (no LangGraph graph needed for this simple flow) ──────


def _get_llm() -> ChatOpenAI:
    return ChatOpenAI(
        base_url=LLM_BASE_URL,
        api_key=LLM_API_KEY,
        model=LLM_MODEL,
        temperature=0,
    )


CONSOLIDATION_PROMPT = (
    "You are an ontology normalization agent. Given these predicates and entity "
    "names from a knowledge graph, identify groups that should be merged.\n\n"
    "Rules:\n"
    "- Only merge predicates that truly mean the same relationship\n"
    "- Pick a canonical name in snake_case (e.g. works_at, reports_to)\n"
    "- Only merge entities that clearly refer to the same real-world thing\n"
    "- Pick the most complete/common name as canonical (lowercase)\n"
    "- If nothing needs merging, return empty lists\n"
    "- Do NOT merge predicates that are merely related (e.g. manages vs reports_to)\n\n"
    "Predicates: {predicates}\n\n"
    "Entity names: {entities}"
)


async def consolidate_ontology(kg: Any) -> dict[str, Any]:
    """Run one round of ontology consolidation. Returns summary of changes."""

    # Step 1: Scan distinct predicates and entity names
    pred_result = await kg.execute("?relationship(_, _, Predicate, _, _)")
    predicates = sorted({row[0] for row in (pred_result.rows or [])})

    ent_result = await kg.execute("?entity(_, Name, _, _, _)")
    entity_names = sorted({row[0] for row in (ent_result.rows or [])})

    if len(predicates) < 2 and len(entity_names) < 2:
        return {"status": "nothing_to_consolidate", "predicates": predicates, "entities": entity_names}

    logger.info("Scanning: %d predicates, %d entities", len(predicates), len(entity_names))

    # Step 2: LLM proposes merges
    llm = _get_llm()
    proposer = llm.with_structured_output(MergeProposal)

    try:
        proposal = await proposer.ainvoke(
            CONSOLIDATION_PROMPT.format(
                predicates=", ".join(predicates),
                entities=", ".join(entity_names),
            )
        )
    except Exception:
        logger.exception("Ontology consolidation LLM call failed")
        return {"status": "llm_error"}

    pred_merges = 0
    entity_merges = 0

    # Step 3: Apply predicate merges
    for merge in proposal.predicate_merges:
        variants_to_replace = [v for v in merge.variants if v != merge.canonical]
        for old_pred in variants_to_replace:
            # Find all relationships with the old predicate
            rows = await kg.execute(
                f"?relationship(Id, Subject, {iql_literal(old_pred)}, Object, Source)"
            )
            if not rows.rows:
                continue
            for row in rows.rows:
                rid, subject, obj, source = row[0], row[1], row[3], row[4]
                # Retract old
                await kg.execute(
                    f"-relationship({iql_literal(rid)}, {iql_literal(subject)}, "
                    f"{iql_literal(old_pred)}, {iql_literal(obj)}, {iql_literal(source)})"
                )
                # Insert with canonical predicate
                await kg.execute(
                    f"+relationship({iql_literal(rid)}, {iql_literal(subject)}, "
                    f"{iql_literal(merge.canonical)}, {iql_literal(obj)}, {iql_literal(source)})"
                )
                pred_merges += 1

    # Step 4: Apply entity merges
    for merge in proposal.entity_merges:
        variants_to_replace = [v for v in merge.variants if v != merge.canonical]
        for old_name in variants_to_replace:
            # Update entities
            ent_rows = await kg.execute(
                f"?entity(Id, {iql_literal(old_name)}, Kind, Desc, Source)"
            )
            for row in (ent_rows.rows or []):
                eid, kind, desc, source = row[0], row[2], row[3], row[4]
                await kg.execute(
                    f"-entity({iql_literal(eid)}, {iql_literal(old_name)}, "
                    f"{iql_literal(kind)}, {iql_literal(desc)}, {iql_literal(source)})"
                )
                await kg.execute(
                    f"+entity({iql_literal(eid)}, {iql_literal(merge.canonical)}, "
                    f"{iql_literal(kind)}, {iql_literal(desc)}, {iql_literal(source)})"
                )
                entity_merges += 1

            # Update relationships referencing old entity name (as subject)
            subj_rows = await kg.execute(
                f"?relationship(Id, {iql_literal(old_name)}, Pred, Obj, Source)"
            )
            for row in (subj_rows.rows or []):
                rid, pred, obj, source = row[0], row[2], row[3], row[4]
                await kg.execute(
                    f"-relationship({iql_literal(rid)}, {iql_literal(old_name)}, "
                    f"{iql_literal(pred)}, {iql_literal(obj)}, {iql_literal(source)})"
                )
                await kg.execute(
                    f"+relationship({iql_literal(rid)}, {iql_literal(merge.canonical)}, "
                    f"{iql_literal(pred)}, {iql_literal(obj)}, {iql_literal(source)})"
                )

            # Update relationships referencing old entity name (as object)
            obj_rows = await kg.execute(
                f"?relationship(Id, Subj, Pred, {iql_literal(old_name)}, Source)"
            )
            for row in (obj_rows.rows or []):
                rid, subj, pred, source = row[0], row[1], row[2], row[4]
                await kg.execute(
                    f"-relationship({iql_literal(rid)}, {iql_literal(subj)}, "
                    f"{iql_literal(pred)}, {iql_literal(old_name)}, {iql_literal(source)})"
                )
                await kg.execute(
                    f"+relationship({iql_literal(rid)}, {iql_literal(subj)}, "
                    f"{iql_literal(pred)}, {iql_literal(merge.canonical)}, {iql_literal(source)})"
                )

    logger.info(
        "Consolidated: %d predicate renames, %d entity renames",
        pred_merges,
        entity_merges,
    )

    return {
        "status": "done",
        "predicate_merges": [
            {"variants": m.variants, "canonical": m.canonical}
            for m in proposal.predicate_merges
        ],
        "entity_merges": [
            {"variants": m.variants, "canonical": m.canonical}
            for m in proposal.entity_merges
        ],
        "predicates_renamed": pred_merges,
        "entities_renamed": entity_merges,
    }
