"""Streaming aggregation: metrics arrive, rules detect threshold breaches.

Simulates a monitoring system where metrics stream in continuously.
Rules check each metric against defined thresholds. When
breaches are detected, the router triggers alert/remediation nodes.

Shows: real-time analytics with rule-driven triggers. The KG acts
as a streaming policy engine.
"""

import asyncio
from typing import Any

from examples.langgraph._common import (
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
from inputlayer.integrations.langgraph import InputLayerState
from langgraph.graph import END, StateGraph

# ── State ────────────────────────────────────────────────────────────


class MonitorState(InputLayerState):
    batch_index: int
    total_breaches: int
    alerts_sent: int
    remediations: list[str]


# ── Metric stream (simulated) ────────────────────────────────────────

METRIC_BATCHES = [
    # Batch 1: all normal
    [
        (100, "api", "error_rate", 1),
        (100, "api", "latency_p99", 150),
        (100, "api", "rps", 1200),
        (100, "db", "connections", 45),
        (100, "db", "query_time_ms", 20),
        (100, "cache", "hit_rate", 95),
    ],
    # Batch 2: latency creeping up
    [
        (200, "api", "error_rate", 3),
        (200, "api", "latency_p99", 450),
        (200, "api", "rps", 1100),
        (200, "db", "connections", 65),
        (200, "db", "query_time_ms", 80),
        (200, "cache", "hit_rate", 85),
    ],
    # Batch 3: breaches!
    [
        (300, "api", "error_rate", 12),
        (300, "api", "latency_p99", 900),
        (300, "api", "rps", 800),
        (300, "db", "connections", 95),
        (300, "db", "query_time_ms", 250),
        (300, "cache", "hit_rate", 60),
    ],
    # Batch 4: partial recovery
    [
        (400, "api", "error_rate", 4),
        (400, "api", "latency_p99", 300),
        (400, "api", "rps", 1100),
        (400, "db", "connections", 55),
        (400, "db", "query_time_ms", 40),
        (400, "cache", "hit_rate", 90),
    ],
]


# ── Graph nodes ──────────────────────────────────────────────────────


async def ingest_metrics(state: dict[str, Any]) -> dict[str, Any]:
    """Ingest the next batch of metrics."""
    kg = state["kg"]
    idx = state.get("batch_index", 0)

    if idx >= len(METRIC_BATCHES):
        return {"batch_index": idx}

    batch = METRIC_BATCHES[idx]
    ts = batch[0][0]
    print(f"\n  {WHITE}T={ts}: Ingesting {len(batch)} metrics{RESET}")

    for metric_ts, component, name, value in batch:
        await kg.execute(f'+metric({metric_ts}, "{component}", "{name}", {value})')

    # Show current values
    for _metric_ts, component, name, value in batch:
        # Check if this metric has a threshold
        r = await kg.execute(f'?threshold("{component}", "{name}", Max)')
        if r.rows:
            max_val = r.rows[0][2]
            if value > max_val:
                pct = ((value - max_val) / max_val) * 100
                print(f"    {RED}{component}.{name} = {value} (>{max_val}, +{pct:.0f}%){RESET}")
            elif value > max_val * 0.8:
                print(f"    {YELLOW}{component}.{name} = {value} (approaching {max_val}){RESET}")
            else:
                print(f"    {GREEN}{component}.{name} = {value}{RESET}")
        else:
            print(f"    {DIM}{component}.{name} = {value}{RESET}")

    return {"batch_index": idx + 1}


async def check_breaches(state: dict[str, Any]) -> dict[str, Any]:
    """Check for threshold breaches derived by rules."""
    kg = state["kg"]

    r = await kg.execute("?breach(Ts, Component, Name, Value, Max)")
    breaches = r.rows

    # Get new breaches (from latest timestamp)
    idx = state.get("batch_index", 1)
    if idx <= len(METRIC_BATCHES):
        latest_ts = METRIC_BATCHES[idx - 1][0][0]
        new_breaches = [b for b in breaches if b[0] == latest_ts]
    else:
        new_breaches = []

    total = state.get("total_breaches", 0) + len(new_breaches)

    if new_breaches:
        print(f"\n  {RED}Breaches detected ({len(new_breaches)} new):{RESET}")
        for b in new_breaches:
            print(f"    {RED}!!{RESET} {b[1]}.{b[2]} = {b[3]} (threshold: {b[4]})")

    # Check for cascading failures
    r = await kg.execute("?cascade_alert(CompA, CompB)")
    if r.rows:
        seen = set()
        for row in r.rows:
            key = tuple(sorted([row[0], row[1]]))
            if key not in seen:
                seen.add(key)
                print(f"    {MAGENTA}CASCADE{RESET}: {row[0]} + {row[1]} both breaching")

    return {"total_breaches": total}


async def route_after_check(state: dict[str, Any]) -> str:
    """Route based on breach status."""
    kg = state["kg"]
    idx = state.get("batch_index", 0)

    latest_ts = METRIC_BATCHES[idx - 1][0][0] if idx <= len(METRIC_BATCHES) else 0

    # Check for new breaches at this timestamp
    r = await kg.execute(f"?breach({latest_ts}, Component, Name, Value, Max)")

    if r.rows:
        return "alert"

    if idx < len(METRIC_BATCHES):
        return "next_batch"

    return "report"


async def send_alert(state: dict[str, Any]) -> dict[str, Any]:
    """Send alerts for breaches and trigger remediation."""
    kg = state["kg"]
    alerts = state.get("alerts_sent", 0)
    remediations = list(state.get("remediations", []))

    idx = state.get("batch_index", 1)
    latest_ts = METRIC_BATCHES[idx - 1][0][0] if idx <= len(METRIC_BATCHES) else 0

    r = await kg.execute(f"?breach({latest_ts}, Component, Name, Value, Max)")

    for row in r.rows:
        component, name, value, max_val = row[1], row[2], row[3], row[4]
        alerts += 1

        # Determine remediation based on rules
        if name == "connections" and value > max_val:
            action = f"Expand {component} connection pool to {max_val * 2}"
            remediations.append(action)
            print(f"    {GREEN}REMEDIATE{RESET}: {action}")
        elif name == "error_rate" and value > max_val:
            action = f"Enable circuit breaker on {component}"
            remediations.append(action)
            print(f"    {GREEN}REMEDIATE{RESET}: {action}")
        elif name == "latency_p99" and value > max_val:
            action = f"Scale up {component} replicas"
            remediations.append(action)
            print(f"    {GREEN}REMEDIATE{RESET}: {action}")
        elif name == "hit_rate":
            action = f"Increase {component} TTL and capacity"
            remediations.append(action)
            print(f"    {GREEN}REMEDIATE{RESET}: {action}")

    return {"alerts_sent": alerts, "remediations": remediations}


async def check_more_batches(state: dict[str, Any]) -> str:
    """Check if there are more metric batches."""
    idx = state.get("batch_index", 0)
    if idx < len(METRIC_BATCHES):
        return "next_batch"
    return "report"


async def generate_report(state: dict[str, Any]) -> dict[str, Any]:
    """Generate monitoring report."""
    total_breaches = state.get("total_breaches", 0)
    alerts_sent = state.get("alerts_sent", 0)
    remediations = state.get("remediations", [])

    print(f"\n{'─' * 55}")
    print(f"\n  {WHITE}Monitoring Report:{RESET}")
    print(f"  {DIM}Batches processed: {len(METRIC_BATCHES)}{RESET}")
    print(f"  {DIM}Total metrics: {sum(len(b) for b in METRIC_BATCHES)}{RESET}")

    if total_breaches:
        print(f"  {RED}Total breaches: {total_breaches}{RESET}")
    else:
        print(f"  {GREEN}No breaches{RESET}")

    print(f"  {YELLOW}Alerts sent: {alerts_sent}{RESET}")

    if remediations:
        print(f"\n  {WHITE}Remediations applied:{RESET}")
        for r in remediations:
            print(f"    {GREEN}+{RESET} {r}")

    # LLM summary if available
    if check_llm() and total_breaches > 0:
        from langchain_core.output_parsers import StrOutputParser
        from langchain_core.prompts import ChatPromptTemplate

        llm = get_llm()

        context = f"Breaches: {total_breaches}, Alerts: {alerts_sent}\n"
        context += "Remediations:\n"
        for r in remediations:
            context += f"  - {r}\n"

        prompt = ChatPromptTemplate.from_template(
            "You are an SRE. Summarize this monitoring incident in 2 sentences.\n\n{context}"
        )
        chain = prompt | llm | StrOutputParser()
        summary = await chain.ainvoke({"context": context})
        print(f"\n  {GREEN}{summary.strip()}{RESET}")

    return {}


# ── Main ─────────────────────────────────────────────────────────────


async def run():
    header("Streaming aggregation with threshold alerts", 9)

    async with InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    ) as il:
        import contextlib

        with contextlib.suppress(Exception):
            await il.drop_knowledge_graph("lg_monitor")
        kg = il.knowledge_graph("lg_monitor")

        # ── Schema ───────────────────────────────────────────────────

        await kg.execute("+metric(ts: int, component: string, name: string, value: int)")
        await kg.execute("+threshold(component: string, metric_name: string, max_val: int)")

        # ── Thresholds ───────────────────────────────────────────────

        thresholds = [
            ("api", "error_rate", 5),
            ("api", "latency_p99", 500),
            ("api", "rps", 500),  # minimum, not max, but we'll treat as max for simplicity
            ("db", "connections", 90),
            ("db", "query_time_ms", 100),
            ("cache", "hit_rate", 70),  # inverted: below threshold is bad
        ]

        for comp, name, max_val in thresholds:
            await kg.execute(f'+threshold("{comp}", "{name}", {max_val})')

        # ── Rules ────────────────────────────────────────────────────

        # Breach: metric exceeds threshold
        await kg.execute(
            "+breach(Ts, Component, Name, Value, Max) <- "
            "metric(Ts, Component, Name, Value), "
            "threshold(Component, Name, Max), "
            "Value > Max"
        )

        # Cascade: breaches on multiple components at same timestamp
        await kg.execute(
            "+cascade_alert(CompA, CompB) <- "
            "breach(Ts, CompA, NameA, ValA, MaxA), "
            "breach(Ts, CompB, NameB, ValB, MaxB), "
            "CompA != CompB"
        )

        step(1, "Thresholds and rules defined")
        for comp, name, max_val in thresholds:
            print(f"  {DIM}{comp}.{name} <= {max_val}{RESET}")
        print(f"{DIM}  Rules: breach, cascade_alert{RESET}")

        # ── Build graph ──────────────────────────────────────────────

        step(2, "Build monitoring pipeline")
        print(
            f"{DIM}  ingest -> check_breaches -> "
            f"[breach: alert -> loop | ok: loop | done: report]{RESET}"
        )

        graph = StateGraph(MonitorState)
        graph.add_node("ingest", ingest_metrics)
        graph.add_node("check", check_breaches)
        graph.add_node("alert", send_alert)
        graph.add_node("report", generate_report)

        graph.set_entry_point("ingest")
        graph.add_edge("ingest", "check")
        graph.add_conditional_edges(
            "check",
            route_after_check,
            {
                "alert": "alert",
                "next_batch": "ingest",
                "report": "report",
            },
        )
        graph.add_conditional_edges(
            "alert",
            check_more_batches,
            {"next_batch": "ingest", "report": "report"},
        )
        graph.add_edge("report", END)

        app = graph.compile()

        # ── Run ──────────────────────────────────────────────────────

        step(3, f"Process {len(METRIC_BATCHES)} metric batches")

        await app.ainvoke(
            {
                "kg": kg,
                "batch_index": 0,
                "total_breaches": 0,
                "alerts_sent": 0,
                "remediations": [],
                "results": [],
            }
        )

        await il.drop_knowledge_graph("lg_monitor")
        success("Done!")


if __name__ == "__main__":
    asyncio.run(run())
