"""Multi-step investigation: agent queries KG, identifies gaps, gathers info.

An investigation agent receives a question. It queries the KG for what's
known, identifies missing information, gathers it from sources, inserts
new facts, and lets rules derive conclusions. The router checks
derived conclusions to decide the next step.

Shows incremental fact accumulation with rule-driven decision making.
"""

import asyncio
import contextlib
from typing import Any

from examples.langgraph._common import (
    CYAN,
    DIM,
    GREEN,
    RED,
    RESET,
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


class InvestigationState(InputLayerState):
    suspect: str
    phase: str
    findings: list[str]
    conclusion: str


# ── Simulated evidence database ──────────────────────────────────────

EVIDENCE_DB: dict[str, list[tuple[str, str, str]]] = {
    "financial": [
        ("alice", "large_transaction", "Transferred $50K to offshore account on March 3"),
        ("bob", "expense_report", "Filed $200 lunch expense on March 5"),
        ("carol", "large_transaction", "Received $50K wire from unknown source on March 4"),
    ],
    "access_logs": [
        ("alice", "after_hours_access", "Accessed server room at 2:30 AM on March 3"),
        ("bob", "normal_access", "Badge in at 9:00 AM, out at 5:30 PM daily"),
        ("carol", "after_hours_access", "VPN login at 11 PM on March 3 from unusual IP"),
    ],
    "communications": [
        ("alice", "encrypted_msg", "Sent encrypted messages to external address on March 2"),
        ("carol", "deleted_emails", "Bulk deleted 200 emails on March 5"),
    ],
    "hr_records": [
        ("alice", "performance", "Excellent reviews, 5 years tenure"),
        ("bob", "performance", "Average reviews, 2 years tenure"),
        ("carol", "performance", "Under performance review, filed grievance last month"),
        ("carol", "financial_stress", "Requested salary advance twice in Q1"),
    ],
}


# ── Graph nodes ──────────────────────────────────────────────────────


async def assess_situation(state: dict[str, Any]) -> dict[str, Any]:
    """Initial assessment, check what evidence areas exist."""
    kg = state["kg"]

    r = await kg.execute("?evidence(Person, Type, Detail)")
    existing = len(r.rows)

    print(f"  {DIM}Current evidence: {existing} facts in KG{RESET}")
    print(f"  {DIM}Available sources: financial, access_logs, communications, hr_records{RESET}")

    return {"phase": "gathering"}


async def gather_evidence(state: dict[str, Any]) -> dict[str, Any]:
    """Gather evidence from the next unchecked source."""
    kg = state["kg"]

    # Check which sources we've already consulted
    r = await kg.execute("?source_checked(Source)")
    checked = {row[0] for row in r.rows}

    sources = ["financial", "access_logs", "communications", "hr_records"]
    unchecked = [s for s in sources if s not in checked]

    if not unchecked:
        return {"phase": "all_gathered"}

    source = unchecked[0]
    print(f"\n  {CYAN}Checking {source}...{RESET}")

    # Insert evidence from this source
    for person, etype, detail in EVIDENCE_DB.get(source, []):
        await kg.execute(
            f'+evidence("{escape_iql(person)}", "{escape_iql(etype)}", "{escape_iql(detail)}")'
        )
        flag = (
            RED
            if etype
            in (
                "large_transaction",
                "after_hours_access",
                "encrypted_msg",
                "deleted_emails",
                "financial_stress",
            )
            else GREEN
        )
        print(
            f"    {flag}{'!' if flag == RED else 'ok'}{RESET} {person}: {DIM}{detail[:60]}{RESET}"
        )

    await kg.execute(f'+source_checked("{escape_iql(source)}")')

    return {"phase": "gathering"}


async def analyze_patterns(state: dict[str, Any]) -> dict[str, Any]:
    """Check derived suspicious patterns after new evidence."""
    kg = state["kg"]

    # Check derived rules
    r = await kg.execute("?suspicious_pattern(Person, Reason)")
    patterns = [(row[0], row[1]) for row in r.rows]

    r = await kg.execute("?high_risk(Person)")
    high_risk = [row[0] for row in r.rows]

    findings = []
    if patterns:
        print(f"\n  {YELLOW}Suspicious patterns detected:{RESET}")
        for person, reason in patterns:
            finding = f"{person}: {reason}"
            findings.append(finding)
            print(f"    {YELLOW}!{RESET} {finding}")

    if high_risk:
        print(f"\n  {RED}High-risk individuals:{RESET}")
        for person in high_risk:
            print(f"    {RED}!!{RESET} {person}")

    return {"findings": findings}


async def produce_conclusion(state: dict[str, Any]) -> dict[str, Any]:
    """Generate final investigation conclusion."""
    kg = state["kg"]

    r = await kg.execute("?high_risk(Person)")
    high_risk = [row[0] for row in r.rows]

    r = await kg.execute("?suspicious_pattern(Person, Reason)")
    patterns = [(row[0], row[1]) for row in r.rows]

    if check_llm():
        from langchain_core.output_parsers import StrOutputParser
        from langchain_core.prompts import ChatPromptTemplate

        llm = get_llm()

        context = "High-risk individuals:\n"
        for p in high_risk:
            context += f"  - {p}\n"
        context += "\nSuspicious patterns:\n"
        for person, reason in patterns:
            context += f"  - {person}: {reason}\n"

        # Get all evidence
        r = await kg.execute("?evidence(Person, Type, Detail)")
        context += "\nAll evidence:\n"
        for row in r.rows:
            context += f"  - {row[0]} ({row[1]}): {row[2]}\n"

        prompt = ChatPromptTemplate.from_template(
            "You are a fraud investigator. Based on the evidence and "
            "patterns detected by automated rules, write a brief "
            "investigation summary with your recommendation.\n\n"
            "{context}\n\nConclusion (3-4 sentences):"
        )
        chain = prompt | llm | StrOutputParser()
        conclusion = await chain.ainvoke({"context": context})
    else:
        if high_risk:
            conclusion = (
                f"Investigation complete. {', '.join(high_risk)} flagged as "
                f"high-risk based on {len(patterns)} suspicious patterns. "
                f"Recommend further review."
            )
        else:
            conclusion = "No high-risk individuals identified."

    return {"conclusion": conclusion}


# ── Main ─────────────────────────────────────────────────────────────


async def run():
    header("Multi-step investigation", 2)

    async with InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    ) as il:
        with contextlib.suppress(Exception):
            await il.drop_knowledge_graph("lg_investigation")
        kg = il.knowledge_graph("lg_investigation")
        try:
            # ── Setup schema and rules ───────────────────────────────────

            await kg.execute("+evidence(person: string, evidence_type: string, detail: string)")
            await kg.execute("+source_checked(source: string)")

            # Rule: suspicious patterns
            await kg.execute(
                '+suspicious_pattern(Person, "large_transaction") <- '
                'evidence(Person, "large_transaction", Detail)'
            )
            await kg.execute(
                '+suspicious_pattern(Person, "after_hours_access") <- '
                'evidence(Person, "after_hours_access", Detail)'
            )
            await kg.execute(
                '+suspicious_pattern(Person, "encrypted_communications") <- '
                'evidence(Person, "encrypted_msg", Detail)'
            )
            await kg.execute(
                '+suspicious_pattern(Person, "data_destruction") <- '
                'evidence(Person, "deleted_emails", Detail)'
            )
            await kg.execute(
                '+suspicious_pattern(Person, "financial_stress") <- '
                'evidence(Person, "financial_stress", Detail)'
            )

            # Rule: high-risk = 2+ suspicious patterns
            await kg.execute(
                "+high_risk(Person) <- "
                "suspicious_pattern(Person, A), "
                "suspicious_pattern(Person, B), A != B"
            )

            step(1, "Build the investigation graph")
            print(f"{DIM}  assess -> gather -> analyze -> [more sources? loop : conclude]{RESET}")
            print(f"{DIM}  Rules detect: suspicious_pattern, high_risk{RESET}")

            # ── Build the graph ──────────────────────────────────────────

            # Router: check how many sources we've covered
            async def route_after_analysis(state: dict[str, Any]) -> str:
                kg_handle = state["kg"]
                r = await kg_handle.execute("?source_checked(S)")
                checked = len(r.rows)
                total = 4  # financial, access_logs, communications, hr_records
                print(f"  {DIM}[router] {checked}/{total} sources checked{RESET}")
                if checked >= total:
                    return "conclude"
                return "gather"

            graph = StateGraph(InvestigationState)
            graph.add_node("assess", assess_situation)
            graph.add_node("gather", gather_evidence)
            graph.add_node("analyze", analyze_patterns)
            graph.add_node("conclude", produce_conclusion)

            graph.set_entry_point("assess")
            graph.add_edge("assess", "gather")
            graph.add_edge("gather", "analyze")
            graph.add_conditional_edges(
                "analyze",
                route_after_analysis,
                {"conclude": "conclude", "gather": "gather"},
            )
            graph.add_edge("conclude", END)

            app = graph.compile()

            # ── Run ──────────────────────────────────────────────────────

            step(2, "Execute investigation")

            result = await app.ainvoke(
                {
                    "kg": kg,
                    "suspect": "",
                    "phase": "start",
                    "findings": [],
                    "conclusion": "",
                }
            )

            # ── Results ──────────────────────────────────────────────────

            step(3, "Investigation results")

            r = await kg.execute("?high_risk(Person)")
            if r.rows:
                print(f"\n  {RED}High-risk:{RESET}")
                for row in r.rows:
                    print(f"    {RED}!!{RESET} {row[0]}")

            r = await kg.execute("?suspicious_pattern(Person, Reason)")
            if r.rows:
                print(f"\n  {YELLOW}All suspicious patterns:{RESET}")
                seen = set()
                for row in r.rows:
                    key = (row[0], row[1])
                    if key not in seen:
                        seen.add(key)
                        print(f"    {YELLOW}!{RESET} {row[0]}: {row[1]}")

            step(4, "Conclusion")
            print(f"\n{GREEN}  {result['conclusion'].strip()}{RESET}")

            success("Done!")
        finally:
            with contextlib.suppress(Exception):
                await il.drop_knowledge_graph("lg_investigation")


if __name__ == "__main__":
    asyncio.run(run())
