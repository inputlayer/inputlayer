"""Reasoning loop: search -> accumulate facts -> rules check -> loop or answer.

The KG accumulates facts across iterations. Rules derive when
enough context has been gathered. The kg_router checks these derived
facts to decide whether to loop or produce a final answer.

This is the canonical LangGraph + InputLayer pattern: the graph controls
WHEN things happen, the KG controls WHAT follows from WHAT.
"""

import asyncio
import contextlib
from typing import Any

# Avoid F405 by importing explicitly from _common
from examples.langgraph._common import (
    CYAN,
    DIM,
    GREEN,
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
from inputlayer.integrations.langgraph import InputLayerState, escape_iql, kg_router
from langgraph.graph import END, StateGraph

# ── State ────────────────────────────────────────────────────────────


class ResearchState(InputLayerState):
    question: str
    iteration: int
    max_iterations: int
    search_terms: list[str]
    answer: str


# ── Graph nodes ──────────────────────────────────────────────────────


async def plan_search(state: dict[str, Any]) -> dict[str, Any]:
    """Determine what to search for based on the question and what's known."""
    question = state["question"]
    iteration = state.get("iteration", 0)
    kg = state["kg"]

    # Check what topics we already have facts about
    r = await kg.execute("?research_fact(Topic, Content)")
    known_topics = {row[0] for row in r.rows}

    # Define search strategy based on question
    all_terms = []
    if "python" in question.lower() or "programming" in question.lower():
        all_terms = ["python", "typing", "performance", "ecosystem"]
    elif "ml" in question.lower() or "machine learning" in question.lower():
        all_terms = ["supervised", "unsupervised", "deep_learning", "evaluation"]
    else:
        all_terms = ["overview", "details", "applications", "comparison"]

    # Only search for terms we don't have yet
    new_terms = [t for t in all_terms if t not in known_topics]
    if not new_terms:
        new_terms = all_terms[:1]  # fallback

    print(
        f"  {DIM}[iter {iteration}] Planning search: "
        f"{', '.join(new_terms)} "
        f"(already know: {', '.join(known_topics) or 'nothing'}){RESET}"
    )

    return {
        "search_terms": new_terms[:2],  # search 2 topics per iteration
        "iteration": iteration + 1,
    }


async def gather_facts(state: dict[str, Any]) -> dict[str, Any]:
    """Simulate gathering facts and inserting them into the KG."""
    kg = state["kg"]
    terms = state.get("search_terms", [])

    # Simulated knowledge base (in production, this would be an LLM or API)
    knowledge = {
        "python": "Python is a high-level, dynamically typed language.",
        "typing": "Python supports optional type hints via the typing module.",
        "performance": "Python uses CPython by default; PyPy offers JIT.",
        "ecosystem": "Python has pip, conda, and 400K+ packages on PyPI.",
        "supervised": "Supervised learning uses labeled data for training.",
        "unsupervised": "Unsupervised learning finds patterns without labels.",
        "deep_learning": "Deep learning uses neural networks with many layers.",
        "evaluation": "ML models are evaluated with metrics like accuracy and F1.",
        "overview": "A broad summary of the topic.",
        "details": "In-depth technical information.",
        "applications": "Real-world use cases and applications.",
        "comparison": "How it compares to alternatives.",
    }

    for term in terms:
        content = knowledge.get(term, f"General info about {term}.")
        await kg.execute(f'+research_fact("{escape_iql(term)}", "{escape_iql(content)}")')
        print(f"  {GREEN}+{RESET} {CYAN}{term}{RESET}: {DIM}{content}{RESET}")

    # Rule: we have enough context when we have 4+ facts
    # (this rule is defined in setup, it fires automatically)

    return {}


async def synthesize_answer(state: dict[str, Any]) -> dict[str, Any]:
    """Produce a final answer from all gathered facts."""
    kg = state["kg"]
    question = state["question"]

    r = await kg.execute("?research_fact(Topic, Content)")
    facts = {row[0]: row[1] for row in r.rows}

    if check_llm():
        from langchain_core.output_parsers import StrOutputParser
        from langchain_core.prompts import ChatPromptTemplate

        llm = get_llm()
        context = "\n".join(f"- {t}: {c}" for t, c in facts.items())
        prompt = ChatPromptTemplate.from_template(
            "Based on these researched facts:\n{context}\n\n"
            "Answer this question concisely: {question}"
        )
        chain = prompt | llm | StrOutputParser()
        answer = await chain.ainvoke(
            {
                "context": context,
                "question": question,
            }
        )
    else:
        answer = "Summary: " + "; ".join(f"{t}: {c}" for t, c in list(facts.items())[:4])

    return {"answer": answer}


async def no_more_iterations(state: dict[str, Any]) -> dict[str, Any]:
    """Fallback when max iterations reached."""
    return {"answer": "Reached max iterations. Partial answer available."}


# ── Main ─────────────────────────────────────────────────────────────


async def run():
    header("Reasoning loop with fact accumulation", 1)

    async with InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    ) as il:
        with contextlib.suppress(Exception):
            await il.drop_knowledge_graph("lg_reasoning")
        kg = il.knowledge_graph("lg_reasoning")
        try:
            # ── Setup KG schema and rules ────────────────────────────────

            await kg.execute("+research_fact(topic: string, content: string)")

            # Rule: enough_context when we have 4+ distinct topics.
            # Uses named content variables (Ca, Cb, ...) instead of _
            # because the engine requires named variables when
            # inequality constraints reference sibling atoms.
            await kg.execute(
                '+enough_context("yes") <- '
                "research_fact(A, Ca), research_fact(B, Cb), "
                "research_fact(C, Cc), research_fact(D, Cd), "
                "A != B, B != C, C != D, A != C, A != D, B != D"
            )

            step(1, "Build the reasoning graph")
            print(f"{DIM}  Nodes: plan_search -> gather_facts -> [loop or answer]{RESET}")
            print(f"{DIM}  Router: enough_context(X) -> answer, else -> loop{RESET}")
            print(f"{DIM}  Max 3 iterations as safety limit{RESET}")

            # ── Build the graph ──────────────────────────────────────────

            # Router: check if KG has derived enough_context
            route = kg_router(
                branches={
                    "answer": "?enough_context(Status)",
                },
                default="plan",
            )

            # Safety: don't loop forever
            async def iteration_guard(state: dict[str, Any]) -> str:
                if state.get("iteration", 0) >= state.get("max_iterations", 3):
                    return "max_reached"
                # Otherwise defer to the KG router
                return await route(state)

            graph = StateGraph(ResearchState)
            graph.add_node("plan", plan_search)
            graph.add_node("gather", gather_facts)
            graph.add_node("answer", synthesize_answer)
            graph.add_node("max_reached", no_more_iterations)

            graph.set_entry_point("plan")
            graph.add_edge("plan", "gather")
            graph.add_conditional_edges(
                "gather",
                iteration_guard,
                {
                    "answer": "answer",
                    "plan": "plan",
                    "max_reached": "max_reached",
                },
            )
            graph.add_edge("answer", END)
            graph.add_edge("max_reached", END)

            app = graph.compile()

            # ── Run the graph ────────────────────────────────────────────

            step(2, "Execute: researching 'Python programming'")
            print()

            result = await app.ainvoke(
                {
                    "kg": kg,
                    "question": "What makes Python a good programming language?",
                    "iteration": 0,
                    "max_iterations": 3,
                    "search_terms": [],
                    "answer": "",
                }
            )

            step(3, "Final answer")
            print(f"\n{GREEN}  {result['answer'].strip()}{RESET}")

            # Show what the KG accumulated
            step(4, "Accumulated facts in KG")
            r = await kg.execute("?research_fact(Topic, Content)")
            for row in r.rows:
                print(f"  {CYAN}{row[0]}{RESET}: {DIM}{row[1]}{RESET}")

            r = await kg.execute("?enough_context(Status)")
            if r.rows:
                print(f"\n  {GREEN}enough_context: YES (rule fired){RESET}")
            else:
                print(f"\n  {YELLOW}enough_context: NO (not enough facts){RESET}")

            print(f"  {DIM}Iterations used: {result['iteration']}{RESET}")

            success("Done!")
        finally:
            with contextlib.suppress(Exception):
                await il.drop_knowledge_graph("lg_reasoning")


if __name__ == "__main__":
    asyncio.run(run())
