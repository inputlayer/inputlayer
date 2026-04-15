"""Event correlation: rules detect incidents from event patterns.

Simulates a stream of system events (logs, alerts, metrics). Events
are inserted as facts. Rules correlate events across components
and time to detect incidents (e.g., "deploy followed by error spike
on the same service = failed deploy").

Shows pattern matching across accumulated events. As events arrive,
rules derive incidents automatically - each new fact triggers
re-evaluation of all correlations without any manual bookkeeping.
"""

import asyncio
import contextlib
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
    check_llm,
    get_llm,
    header,
    os,
    step,
    success,
)

from inputlayer import InputLayer
from inputlayer.integrations.langgraph import InputLayerState, escape_iql
from langgraph.graph import END, StateGraph

# ── State ────────────────────────────────────────────────────────────


class EventState(InputLayerState):
    event_batch: int
    incidents_found: int
    summary: str


# ── Event stream (simulated) ─────────────────────────────────────────

EVENT_BATCHES = [
    # Batch 1: normal operations
    [
        (1, "api-gateway", "info", "deploy", "Deployed v2.3.1"),
        (2, "api-gateway", "info", "traffic", "Requests: 1200/s"),
        (3, "database", "info", "health", "Replication lag: 2ms"),
        (4, "cache", "info", "health", "Hit rate: 95%"),
    ],
    # Batch 2: trouble starts
    [
        (5, "api-gateway", "warn", "latency", "P99 latency: 500ms"),
        (6, "database", "warn", "connections", "Connection pool 80% full"),
        (7, "api-gateway", "error", "timeout", "Upstream timeout on /api/search"),
        (8, "cache", "error", "eviction", "High eviction rate: 40%"),
    ],
    # Batch 3: incident develops
    [
        (9, "api-gateway", "error", "5xx", "Error rate 15% on /api/search"),
        (10, "database", "error", "timeout", "Query timeout: SELECT on users"),
        (11, "api-gateway", "error", "5xx", "Error rate 25% on /api/users"),
        (12, "monitoring", "alert", "pager", "PagerDuty alert: API error rate > 10%"),
    ],
    # Batch 4: resolution
    [
        (13, "database", "info", "action", "Connection pool expanded to 200"),
        (14, "cache", "info", "action", "Cache TTL increased to 300s"),
        (15, "api-gateway", "info", "recovery", "Error rate back to 0.1%"),
        (16, "monitoring", "info", "resolved", "PagerDuty alert resolved"),
    ],
]

SEVERITY_COLORS = {
    "info": GREEN,
    "warn": YELLOW,
    "error": RED,
    "alert": MAGENTA,
}


# ── Graph nodes ──────────────────────────────────────────────────────


async def ingest_events(state: dict[str, Any]) -> dict[str, Any]:
    """Ingest the next batch of events into the KG."""
    kg = state["kg"]
    batch_idx = state.get("event_batch", 0)

    if batch_idx >= len(EVENT_BATCHES):
        return {"event_batch": batch_idx}

    batch = EVENT_BATCHES[batch_idx]
    print(f"\n  {WHITE}Batch {batch_idx + 1}/{len(EVENT_BATCHES)}:{RESET}")

    for evt_id, component, severity, evt_type, message in batch:
        await kg.execute(
            f'+event({evt_id}, "{escape_iql(component)}", "{escape_iql(severity)}", "{escape_iql(evt_type)}", "{escape_iql(message)}")'
        )
        color = SEVERITY_COLORS.get(severity, DIM)
        print(f"    {color}{severity:5s}{RESET} {CYAN}{component:15s}{RESET} {DIM}{message}{RESET}")

    return {"event_batch": batch_idx + 1}


async def check_patterns(state: dict[str, Any]) -> dict[str, Any]:
    """Check for incidents derived by rules."""
    kg = state["kg"]

    r = await kg.execute("?incident(Type, Component, Description)")
    incidents_found = len(r.rows)

    if r.rows:
        seen = set()
        print(f"\n  {RED}Incidents detected:{RESET}")
        for row in r.rows:
            key = (row[0], row[1])
            if key not in seen:
                seen.add(key)
                print(f"    {RED}!!{RESET} [{row[0]}] {row[1]}: {row[2]}")

    r = await kg.execute("?warning_pattern(Type, Component, Description)")
    if r.rows:
        seen = set()
        print(f"\n  {YELLOW}Warning patterns:{RESET}")
        for row in r.rows:
            key = (row[0], row[1])
            if key not in seen:
                seen.add(key)
                print(f"    {YELLOW}!{RESET} [{row[0]}] {row[1]}: {row[2]}")

    return {"incidents_found": incidents_found}


async def route_events(state: dict[str, Any]) -> str:
    """Continue processing or summarize."""
    batch_idx = state.get("event_batch", 0)
    if batch_idx < len(EVENT_BATCHES):
        return "next_batch"
    return "summarize"


async def summarize_incidents(state: dict[str, Any]) -> dict[str, Any]:
    """Produce final incident summary."""
    kg = state["kg"]

    r_incidents = await kg.execute("?incident(Type, Component, Desc)")
    r_warnings = await kg.execute("?warning_pattern(Type, Component, Desc)")
    r_resolved = await kg.execute("?resolved_incident(Component)")

    print(f"\n{'─' * 55}")
    print(f"\n  {WHITE}Event stream analysis complete:{RESET}")

    total_events = sum(len(b) for b in EVENT_BATCHES)
    print(f"  {DIM}Total events processed: {total_events}{RESET}")

    incident_types = set()
    for row in r_incidents.rows:
        incident_types.add((row[0], row[1]))
    print(f"  {RED}Incidents: {len(incident_types)}{RESET}")

    warning_types = set()
    for row in r_warnings.rows:
        warning_types.add((row[0], row[1]))
    print(f"  {YELLOW}Warnings: {len(warning_types)}{RESET}")

    resolved = {row[0] for row in r_resolved.rows}
    print(f"  {GREEN}Resolved: {len(resolved)}{RESET}")

    # LLM summary if available
    summary = ""
    if check_llm():
        from langchain_core.output_parsers import StrOutputParser
        from langchain_core.prompts import ChatPromptTemplate

        llm = get_llm()

        context = "Incidents:\n"
        for t, c in incident_types:
            context += f"  - [{t}] {c}\n"
        context += "Warnings:\n"
        for t, c in warning_types:
            context += f"  - [{t}] {c}\n"
        context += f"Resolved components: {', '.join(resolved) or 'none'}\n"

        prompt = ChatPromptTemplate.from_template(
            "You are an SRE analyzing an incident. Summarize what "
            "happened based on the correlated events.\n\n"
            "{context}\n\nBrief post-mortem (3 sentences):"
        )
        chain = prompt | llm | StrOutputParser()
        summary = await chain.ainvoke({"context": context})
        print(f"\n  {GREEN}{summary.strip()}{RESET}")
    else:
        summary = (
            f"{len(incident_types)} incidents detected, "
            f"{len(warning_types)} warnings, "
            f"{len(resolved)} resolved."
        )
        print(f"\n  {GREEN}{summary}{RESET}")

    return {"summary": summary}


# ── Main ─────────────────────────────────────────────────────────────


async def run():
    header("Event correlation pipeline", 7)

    async with InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    ) as il:
        with contextlib.suppress(Exception):
            await il.drop_knowledge_graph("lg_events")
        kg = il.knowledge_graph("lg_events")
        try:
            # ── Schema ───────────────────────────────────────────────────

            await kg.execute(
                "+event(id: int, component: string, severity: string, "
                "event_type: string, message: string)"
            )

            # ── Correlation rules ────────────────────────────────────────

            # Rule: deploy followed by errors on same component = failed deploy
            await kg.execute(
                '+incident("failed_deploy", Component, '
                '"Deploy followed by errors") <- '
                'event(IdA, Component, "info", "deploy", MsgA), '
                'event(IdB, Component, "error", TypeB, MsgB), '
                "IdB > IdA"
            )

            # Rule: errors on multiple components within a close time window = cascade
            await kg.execute(
                '+incident("cascade", CompA, '
                '"Errors spreading across components") <- '
                'event(IdA, CompA, "error", TypeA, MsgA), '
                'event(IdB, CompB, "error", TypeB, MsgB), '
                "CompA != CompB, "
                "IdB >= IdA, IdB - IdA <= 4"
            )

            # Rule: warning then error on same component = escalation
            await kg.execute(
                '+warning_pattern("escalation", Component, '
                '"Warning escalated to error") <- '
                'event(IdA, Component, "warn", TypeA, MsgA), '
                'event(IdB, Component, "error", TypeB, MsgB), '
                "IdB > IdA"
            )

            # Rule: pager alert = critical incident
            await kg.execute(
                '+incident("pager", Component, '
                '"PagerDuty alert triggered") <- '
                'event(Id, Component, "alert", "pager", Msg)'
            )

            # Rule: recovery event after error = resolved
            await kg.execute(
                "+resolved_incident(Component) <- "
                'event(IdA, Component, "error", TypeA, MsgA), '
                'event(IdB, Component, "info", "recovery", MsgB), '
                "IdB > IdA"
            )

            step(1, "Correlation rules defined")
            print(f"{DIM}  deploy + error -> failed_deploy{RESET}")
            print(f"{DIM}  multi-component errors -> cascade{RESET}")
            print(f"{DIM}  warn + error -> escalation{RESET}")
            print(f"{DIM}  pager alert -> critical incident{RESET}")
            print(f"{DIM}  error + recovery -> resolved{RESET}")

            # ── Build graph ──────────────────────────────────────────────

            step(2, "Build event processing pipeline")
            print(
                f"{DIM}  ingest_batch -> check_patterns -> [more batches? loop : summarize]{RESET}"
            )

            graph = StateGraph(EventState)
            graph.add_node("ingest", ingest_events)
            graph.add_node("check", check_patterns)
            graph.add_node("summarize", summarize_incidents)

            graph.set_entry_point("ingest")
            graph.add_edge("ingest", "check")
            graph.add_conditional_edges(
                "check",
                route_events,
                {"next_batch": "ingest", "summarize": "summarize"},
            )
            graph.add_edge("summarize", END)

            app = graph.compile()

            # ── Run ──────────────────────────────────────────────────────

            step(3, "Process event stream (4 batches)")

            await app.ainvoke(
                {
                    "kg": kg,
                    "event_batch": 0,
                    "incidents_found": 0,
                    "summary": "",
                }
            )

            success("Done!")
        finally:
            with contextlib.suppress(Exception):
                await il.drop_knowledge_graph("lg_events")


if __name__ == "__main__":
    asyncio.run(run())
