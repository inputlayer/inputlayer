"""Chat agent: answer questions across notes using the knowledge graph."""

from __future__ import annotations

import logging
from typing import Any

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

from inputlayer.integrations.langchain.params import iql_literal

from config import LLM_API_KEY, LLM_BASE_URL, LLM_MODEL

logger = logging.getLogger("reasoning_notebook.chat")


def _get_llm() -> ChatOpenAI:
    return ChatOpenAI(
        base_url=LLM_BASE_URL,
        api_key=LLM_API_KEY,
        model=LLM_MODEL,
        temperature=0.3,
    )


SYSTEM_PROMPT = (
    "You are a helpful assistant that answers questions using a personal knowledge graph.\n"
    "You have access to the user's notes, extracted entities, and their relationships.\n\n"
    "When answering:\n"
    "- Cite specific notes by title when referencing information\n"
    "- If the knowledge graph contains relevant relationships, explain the chain of reasoning\n"
    "- If you don't have enough information, say so honestly\n"
    "- Keep answers concise and direct\n"
)

QA_TEMPLATE = (
    "{system}\n\n"
    "=== Notes in the knowledge graph ===\n{notes_context}\n\n"
    "=== Entities ===\n{entities_context}\n\n"
    "=== Relationships ===\n{relationships_context}\n\n"
    "User question: {question}"
)


async def _gather_context(kg: Any, question: str) -> dict[str, str]:
    """Query the KG for relevant notes, entities, and relationships."""

    # Get all notes (titles + content snippets)
    notes_result = await kg.execute("?note(Id, Title, Content, CreatedAt, UpdatedAt)")
    notes_lines = []
    if notes_result.rows and notes_result.columns != ["error"]:
        for row in notes_result.rows:
            data = dict(zip(notes_result.columns, row, strict=True))
            snippet = str(data["content"])[:300]
            notes_lines.append(f'- "{data["title"]}": {snippet}')

    # Get all entities
    ent_result = await kg.execute("?entity(Id, Name, Kind, Description, SourceNoteId)")
    ent_lines = []
    if ent_result.rows and ent_result.columns != ["error"]:
        for row in ent_result.rows:
            data = dict(zip(ent_result.columns, row, strict=True))
            ent_lines.append(f'- {data["name"]} ({data["kind"]}): {data["description"]}')

    # Get all relationships
    rel_result = await kg.execute("?relationship(Id, Subject, Predicate, Object, SourceNoteId)")
    rel_lines = []
    if rel_result.rows and rel_result.columns != ["error"]:
        for row in rel_result.rows:
            data = dict(zip(rel_result.columns, row, strict=True))
            rel_lines.append(f'- {data["subject"]} --{data["predicate"]}--> {data["object"]}')

    return {
        "notes_context": "\n".join(notes_lines) if notes_lines else "(no notes yet)",
        "entities_context": "\n".join(ent_lines) if ent_lines else "(no entities extracted yet)",
        "relationships_context": "\n".join(rel_lines) if rel_lines else "(no relationships yet)",
    }


async def chat(kg: Any, question: str, history: list[dict[str, str]]) -> str:
    """Answer a question using the knowledge graph as context.

    Args:
        kg: KnowledgeGraph handle.
        question: The user's question.
        history: List of {"role": "user"|"assistant", "content": "..."} messages.

    Returns:
        The assistant's response text.
    """
    context = await _gather_context(kg, question)

    llm = _get_llm()
    prompt = ChatPromptTemplate.from_template(QA_TEMPLATE)
    chain = prompt | llm | StrOutputParser()

    answer = await chain.ainvoke({
        "system": SYSTEM_PROMPT,
        "question": question,
        **context,
    })

    return answer.strip()
