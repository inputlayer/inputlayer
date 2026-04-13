"""Branching pipeline: route documents to specialized processors.

Different document types (email, report, code review, support ticket)
get classified and routed to different processing nodes. Each node
extracts different facts. Rules aggregate results across all branches.

Shows kg_router as an intelligent dispatcher for heterogeneous inputs.
"""

import asyncio
from typing import Any

from examples.langgraph._common import (
    CYAN,
    DIM,
    GREEN,
    MAGENTA,
    RED,
    RESET,
    WHITE,
    YELLOW,
    header,
    os,
    step,
    success,
)

from inputlayer import InputLayer
from inputlayer.integrations.langgraph import InputLayerState
from langgraph.graph import END, StateGraph

# ── State ────────────────────────────────────────────────────────────


class PipelineState(InputLayerState):
    documents: list[dict[str, Any]]
    doc_index: int
    current_doc: dict[str, Any]
    processed: int


# ── Document corpus ──────────────────────────────────────────────────

DOCUMENTS = [
    {
        "id": 1,
        "type": "email",
        "subject": "Q1 Revenue Numbers",
        "body": "Hi team, Q1 revenue hit $4.2M, up 15% from last quarter. "
        "Key drivers: enterprise deals (+30%) and APAC expansion.",
        "from": "cfo@company.com",
    },
    {
        "id": 2,
        "type": "code_review",
        "subject": "PR #342: Add rate limiting",
        "body": "Added token bucket rate limiter to API gateway. "
        "100 req/s per user. Includes Redis backend and fallback.",
        "author": "alice",
    },
    {
        "id": 3,
        "type": "support_ticket",
        "subject": "Cannot login after password reset",
        "body": "User reports login failure after password reset. "
        "Error: 'Invalid token'. Affects 3 users since March 1.",
        "priority": "high",
    },
    {
        "id": 4,
        "type": "report",
        "subject": "Infrastructure Cost Analysis",
        "body": "Monthly cloud spend: $45K (AWS $30K, GCP $15K). "
        "Recommendations: consolidate to single provider, "
        "use reserved instances for 40% savings.",
        "author": "devops-team",
    },
    {
        "id": 5,
        "type": "email",
        "subject": "Partnership Proposal from TechCorp",
        "body": "TechCorp proposes joint ML platform development. "
        "They contribute GPU infra, we contribute the software. "
        "Potential $2M annual value.",
        "from": "partnerships@techcorp.com",
    },
    {
        "id": 6,
        "type": "support_ticket",
        "subject": "API response time degradation",
        "body": "P95 latency increased from 200ms to 800ms since "
        "yesterday's deploy. Affecting /api/search endpoint.",
        "priority": "critical",
    },
]

# ── Node colors per type ─────────────────────────────────────────────

TYPE_COLORS = {
    "email": CYAN,
    "code_review": MAGENTA,
    "support_ticket": RED,
    "report": YELLOW,
}


# ── Graph nodes ──────────────────────────────────────────────────────


async def pick_document(state: dict[str, Any]) -> dict[str, Any]:
    """Pick the next document to process."""
    docs = state.get("documents", [])
    idx = state.get("doc_index", 0)

    if idx >= len(docs):
        return {"current_doc": {}, "doc_index": idx}

    doc = docs[idx]
    color = TYPE_COLORS.get(doc["type"], DIM)
    print(f"\n  {color}[{doc['type']}]{RESET} {WHITE}{doc['subject']}{RESET}")

    return {"current_doc": doc, "doc_index": idx + 1}


async def route_by_type(state: dict[str, Any]) -> str:
    """Route document to the appropriate processor."""
    doc = state.get("current_doc", {})
    if not doc:
        return "summarize"

    doc_type = doc.get("type", "unknown")

    # Store classification in KG
    kg = state["kg"]
    await kg.execute(f'+doc_classification({doc["id"]}, "{doc_type}")')

    routes = {
        "email": "process_email",
        "code_review": "process_code",
        "support_ticket": "process_ticket",
        "report": "process_report",
    }
    return routes.get(doc_type, "process_report")


async def process_email(state: dict[str, Any]) -> dict[str, Any]:
    """Extract facts from an email."""
    doc = state["current_doc"]
    kg = state["kg"]

    # Extract: sender, key numbers, action items
    body = doc["body"]
    sender = doc.get("from", "unknown")

    escaped_subj = doc["subject"].replace('"', '\\"')
    await kg.execute(f'+email_fact({doc["id"]}, "sender", "{sender}")')
    await kg.execute(f'+email_fact({doc["id"]}, "subject", "{escaped_subj}")')

    # Simple number extraction
    import re

    numbers = re.findall(r"\$[\d.]+[MKB]?", body)
    for num in numbers:
        await kg.execute(f'+email_fact({doc["id"]}, "amount_mentioned", "{num}")')

    processed = state.get("processed", 0) + 1
    print(f"    {DIM}Extracted: sender={sender}, {len(numbers)} amounts{RESET}")

    return {"processed": processed}


async def process_code(state: dict[str, Any]) -> dict[str, Any]:
    """Extract facts from a code review."""
    doc = state["current_doc"]
    kg = state["kg"]

    author = doc.get("author", "unknown")
    escaped_subj = doc["subject"].replace('"', '\\"')

    await kg.execute(f'+code_review_fact({doc["id"]}, "author", "{author}")')
    await kg.execute(f'+code_review_fact({doc["id"]}, "title", "{escaped_subj}")')

    # Detect keywords
    keywords = ["rate limit", "security", "performance", "bug fix"]
    body_lower = doc["body"].lower()
    for kw in keywords:
        if kw in body_lower:
            await kg.execute(f'+code_review_fact({doc["id"]}, "topic", "{kw}")')

    processed = state.get("processed", 0) + 1
    print(f"    {DIM}Extracted: author={author}{RESET}")

    return {"processed": processed}


async def process_ticket(state: dict[str, Any]) -> dict[str, Any]:
    """Extract facts from a support ticket."""
    doc = state["current_doc"]
    kg = state["kg"]

    priority = doc.get("priority", "normal")
    escaped_subj = doc["subject"].replace('"', '\\"')

    await kg.execute(f'+ticket_fact({doc["id"]}, "priority", "{priority}")')
    await kg.execute(f'+ticket_fact({doc["id"]}, "subject", "{escaped_subj}")')

    # Flag critical tickets
    if priority in ("high", "critical"):
        await kg.execute(f'+urgent_item({doc["id"]}, "ticket", "{escaped_subj}")')
        print(f"    {RED}URGENT: {priority}{RESET}")

    processed = state.get("processed", 0) + 1

    return {"processed": processed}


async def process_report(state: dict[str, Any]) -> dict[str, Any]:
    """Extract facts from a report."""
    doc = state["current_doc"]
    kg = state["kg"]

    author = doc.get("author", "unknown")
    escaped_subj = doc["subject"].replace('"', '\\"')

    await kg.execute(f'+report_fact({doc["id"]}, "author", "{author}")')
    await kg.execute(f'+report_fact({doc["id"]}, "title", "{escaped_subj}")')

    processed = state.get("processed", 0) + 1
    print(f"    {DIM}Extracted: author={author}{RESET}")

    return {"processed": processed}


async def check_more_docs(state: dict[str, Any]) -> str:
    """Check if there are more documents."""
    idx = state.get("doc_index", 0)
    total = len(state.get("documents", []))
    if idx < total:
        return "next"
    return "summarize"


async def summarize_pipeline(state: dict[str, Any]) -> dict[str, Any]:
    """Summarize all extracted facts."""
    kg = state["kg"]

    # Query facts by type
    print(f"\n{'─' * 50}")

    r = await kg.execute("?doc_classification(Id, Type)")
    type_counts: dict[str, int] = {}
    for row in r.rows:
        type_counts[row[1]] = type_counts.get(row[1], 0) + 1

    print(f"\n  {WHITE}Documents by type:{RESET}")
    for dtype, count in sorted(type_counts.items()):
        color = TYPE_COLORS.get(dtype, DIM)
        print(f"    {color}{dtype}{RESET}: {count}")

    r = await kg.execute("?urgent_item(Id, Type, Subject)")
    if r.rows:
        print(f"\n  {RED}Urgent items:{RESET}")
        for row in r.rows:
            print(f"    {RED}!{RESET} [{row[1]}] {row[2]}")

    r = await kg.execute("?email_fact(Id, Key, Value)")
    amounts = [row[2] for row in r.rows if row[1] == "amount_mentioned"]
    if amounts:
        print(f"\n  {CYAN}Financial amounts mentioned:{RESET}")
        for amt in amounts:
            print(f"    {CYAN}{amt}{RESET}")

    return {}


# ── Main ─────────────────────────────────────────────────────────────


async def run():
    header("Branching pipeline", 4)

    async with InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    ) as il:
        import contextlib

        with contextlib.suppress(Exception):
            await il.drop_knowledge_graph("lg_pipeline")
        kg = il.knowledge_graph("lg_pipeline")

        # ── Schema ───────────────────────────────────────────────────

        await kg.execute("+doc_classification(doc_id: int, doc_type: string)")
        await kg.execute("+email_fact(doc_id: int, key: string, value: string)")
        await kg.execute("+code_review_fact(doc_id: int, key: string, value: string)")
        await kg.execute("+ticket_fact(doc_id: int, key: string, value: string)")
        await kg.execute("+report_fact(doc_id: int, key: string, value: string)")
        await kg.execute("+urgent_item(doc_id: int, item_type: string, subject: string)")

        step(1, "Build the branching pipeline")
        print(f"{DIM}  pick_doc -> classify -> [email|code|ticket|report] -> loop -> summarize{RESET}")

        # ── Build graph ──────────────────────────────────────────────

        graph = StateGraph(PipelineState)
        graph.add_node("pick", pick_document)
        graph.add_node("process_email", process_email)
        graph.add_node("process_code", process_code)
        graph.add_node("process_ticket", process_ticket)
        graph.add_node("process_report", process_report)
        graph.add_node("summarize", summarize_pipeline)

        graph.set_entry_point("pick")
        graph.add_conditional_edges(
            "pick",
            route_by_type,
            {
                "process_email": "process_email",
                "process_code": "process_code",
                "process_ticket": "process_ticket",
                "process_report": "process_report",
                "summarize": "summarize",
            },
        )

        for node_name in [
            "process_email",
            "process_code",
            "process_ticket",
            "process_report",
        ]:
            graph.add_conditional_edges(
                node_name,
                check_more_docs,
                {"next": "pick", "summarize": "summarize"},
            )

        graph.add_edge("summarize", END)

        app = graph.compile()

        # ── Run ──────────────────────────────────────────────────────

        step(2, f"Process {len(DOCUMENTS)} documents")

        result = await app.ainvoke(
            {
                "kg": kg,
                "documents": DOCUMENTS,
                "doc_index": 0,
                "current_doc": {},
                "processed": 0,
                "results": [],
            }
        )

        step(3, "Pipeline complete")
        print(f"  {GREEN}Processed {result['processed']} documents{RESET}")

        await il.drop_knowledge_graph("lg_pipeline")
        success("Done!")


if __name__ == "__main__":
    asyncio.run(run())
