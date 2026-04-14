"""Tool selection via rules: KG decides which tool to use, not the LLM.

The KG stores tool capabilities and question categories. Rules
match questions to the best tool based on capability scores. The router
dispatches to the selected tool node. Deterministic, explainable,
and editable without changing code.

Shows: KG as an intelligent tool router for complex agent systems.
"""

import asyncio
import contextlib
from typing import Any

from examples.langgraph._common import (
    CYAN,
    DIM,
    GREEN,
    MAGENTA,
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


class ToolState(InputLayerState):
    questions: list[str]
    question_index: int
    current_question: str
    selected_tool: str
    answers: list[dict[str, str]]


# ── Simulated tool implementations ──────────────────────────────────


async def run_calculator(state: dict[str, Any]) -> dict[str, Any]:
    """Simulate calculator tool."""
    q = state["current_question"]
    # Simple eval for demo
    answers = list(state.get("answers", []))
    try:
        import re

        nums = re.findall(r"[\d.]+", q)
        if "+" in q and len(nums) >= 2:
            result = sum(float(n) for n in nums)
        elif "*" in q or "times" in q.lower():
            result = 1
            for n in nums:
                result *= float(n)
        else:
            result = "Cannot compute"
        answer = f"Calculator: {result}"
    except Exception:
        answer = "Calculator: error"

    answers.append({"question": q, "tool": "calculator", "answer": answer})
    print(f"    {CYAN}Calculator{RESET}: {answer}")
    return {"answers": answers}


async def run_search(state: dict[str, Any]) -> dict[str, Any]:
    """Simulate search tool."""
    q = state["current_question"]
    answers = list(state.get("answers", []))

    # Simulated search results
    search_db = {
        "capital": "Paris is the capital of France.",
        "president": "The current US president info would come from search.",
        "weather": "Weather data would come from a weather API.",
        "population": "World population is approximately 8 billion.",
    }

    result = "No results found."
    for keyword, answer in search_db.items():
        if keyword in q.lower():
            result = answer
            break

    answers.append({"question": q, "tool": "search", "answer": result})
    print(f"    {GREEN}Search{RESET}: {result}")
    return {"answers": answers}


async def run_code(state: dict[str, Any]) -> dict[str, Any]:
    """Simulate code execution tool."""
    q = state["current_question"]
    answers = list(state.get("answers", []))

    answer = f"Code runner: Would execute code to answer '{q[:40]}...'"
    answers.append({"question": q, "tool": "code_runner", "answer": answer})
    print(f"    {MAGENTA}Code runner{RESET}: {answer}")
    return {"answers": answers}


async def run_database(state: dict[str, Any]) -> dict[str, Any]:
    """Simulate database query tool."""
    q = state["current_question"]
    answers = list(state.get("answers", []))

    answer = f"Database: Would query structured data for '{q[:40]}...'"
    answers.append({"question": q, "tool": "database", "answer": answer})
    print(f"    {YELLOW}Database{RESET}: {answer}")
    return {"answers": answers}


async def run_default(state: dict[str, Any]) -> dict[str, Any]:
    """Fallback when no tool matches."""
    q = state["current_question"]
    answers = list(state.get("answers", []))

    if check_llm():
        from langchain_core.output_parsers import StrOutputParser
        from langchain_core.prompts import ChatPromptTemplate

        llm = get_llm()
        prompt = ChatPromptTemplate.from_template("Answer briefly: {question}")
        chain = prompt | llm | StrOutputParser()
        answer = await chain.ainvoke({"question": q})
        answer = f"LLM fallback: {answer.strip()[:100]}"
    else:
        answer = f"No matching tool. Would use LLM for: '{q[:40]}...'"

    answers.append({"question": q, "tool": "llm_fallback", "answer": answer})
    print(f"    {DIM}Fallback{RESET}: {answer}")
    return {"answers": answers}


# ── Routing nodes ────────────────────────────────────────────────────


async def classify_and_select(state: dict[str, Any]) -> dict[str, Any]:
    """Classify the question and let KG rules select the best tool."""
    questions = state.get("questions", [])
    idx = state.get("question_index", 0)

    if idx >= len(questions):
        return {"current_question": "", "selected_tool": "done"}

    question = questions[idx]
    kg = state["kg"]

    # Classify the question by keywords (LLM would do this in production)
    categories = []
    q_lower = question.lower()
    if any(w in q_lower for w in ["calculate", "sum", "multiply", "+", "*", "how much"]):
        categories.append("math")
    if any(w in q_lower for w in ["who", "what is", "capital", "president", "when"]):
        categories.append("factual")
    if any(w in q_lower for w in ["weather", "today", "current", "latest", "news"]):
        categories.append("current_events")
    if any(w in q_lower for w in ["code", "function", "algorithm", "implement", "write"]):
        categories.append("code")
    if any(w in q_lower for w in ["average", "total", "count", "sales", "revenue"]):
        categories.append("aggregation")
    if any(w in q_lower for w in ["query", "database", "SQL", "table"]):
        categories.append("structured_query")

    if not categories:
        categories.append("general")

    # Insert classification into KG
    escaped_q = escape_iql(question)
    for cat in categories:
        await kg.execute(f'+question_type("{escaped_q}", "{cat}")')

    # Query KG rules for best tool
    r = await kg.execute(f'?best_tool("{escaped_q}", Tool, Strength)')

    if r.rows:
        # Pick highest strength
        best = max(r.rows, key=lambda row: row[2])
        tool = best[1]
        strength = best[2]
        print(f'\n  {WHITE}Q: "{question}"{RESET}')
        print(
            f"  {DIM}Categories: {', '.join(categories)} -> "
            f"Best tool: {CYAN}{tool}{RESET} "
            f"(strength: {strength}){RESET}"
        )
    else:
        tool = "default"
        print(f'\n  {WHITE}Q: "{question}"{RESET}')
        print(f"  {DIM}No matching tool, using fallback{RESET}")

    return {
        "current_question": question,
        "selected_tool": tool,
        "question_index": idx + 1,
    }


async def route_to_tool(state: dict[str, Any]) -> str:
    """Route to the selected tool node."""
    tool = state.get("selected_tool", "default")
    routes = {
        "calculator": "calculator",
        "search": "search",
        "code_runner": "code",
        "database": "database",
        "done": "summarize",
    }
    return routes.get(tool, "default")


async def check_more_questions(state: dict[str, Any]) -> str:
    """Check if there are more questions."""
    idx = state.get("question_index", 0)
    total = len(state.get("questions", []))
    if idx < total:
        return "next"
    return "summarize"


async def summarize_results(state: dict[str, Any]) -> dict[str, Any]:
    """Show all answers."""
    answers = state.get("answers", [])
    kg = state["kg"]

    print(f"\n{'─' * 55}")
    print(f"\n  {WHITE}Tool usage summary:{RESET}")

    tool_counts: dict[str, int] = {}
    for a in answers:
        tool_counts[a["tool"]] = tool_counts.get(a["tool"], 0) + 1

    tool_colors = {
        "calculator": CYAN,
        "search": GREEN,
        "code_runner": MAGENTA,
        "database": YELLOW,
        "llm_fallback": DIM,
    }

    for tool, count in sorted(tool_counts.items()):
        color = tool_colors.get(tool, DIM)
        print(f"    {color}{tool}{RESET}: {count} question(s)")

    # Show provenance: why was each tool selected?
    print(f"\n  {WHITE}Selection provenance:{RESET}")
    for a in answers:
        escaped_q = escape_iql(a["question"])
        r = await kg.execute(f'?question_type("{escaped_q}", Category)')
        cats = [row[1] for row in r.rows]
        print(
            f'    "{a["question"][:40]}..." -> '
            f"{DIM}{', '.join(cats)}{RESET} -> "
            f"{tool_colors.get(a['tool'], DIM)}{a['tool']}{RESET}"
        )

    return {}


# ── Main ─────────────────────────────────────────────────────────────


async def run():
    header("Tool selection via rules", 8)

    async with InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    ) as il:
        with contextlib.suppress(Exception):
            await il.drop_knowledge_graph("lg_tools")
        kg = il.knowledge_graph("lg_tools")
        try:

            # ── Schema ───────────────────────────────────────────────────

            await kg.execute("+tool_capability(tool: string, capability: string, strength: int)")
            await kg.execute("+question_type(question: string, category: string)")

            # ── Tool registry ────────────────────────────────────────────

            capabilities = [
                ("calculator", "math", 10),
                ("calculator", "conversion", 8),
                ("search", "factual", 9),
                ("search", "current_events", 10),
                ("code_runner", "code", 10),
                ("code_runner", "math", 7),
                ("database", "structured_query", 10),
                ("database", "aggregation", 9),
            ]

            for tool, cap, strength in capabilities:
                await kg.execute(f'+tool_capability("{tool}", "{cap}", {strength})')

            # ── Selection rule ───────────────────────────────────────────

            await kg.execute(
                "+best_tool(Question, Tool, Strength) <- "
                "question_type(Question, Category), "
                "tool_capability(Tool, Category, Strength)"
            )

            step(1, "Tool registry and selection rules")
            print(f"{DIM}  8 capabilities across 4 tools{RESET}")
            print(
                f"{DIM}  Rule: best_tool(Q, Tool, Strength) <- "
                f"question_type(Q, Cat), tool_capability(Tool, Cat, Strength){RESET}"
            )

            # ── Questions to process ─────────────────────────────────────

            questions = [
                "Calculate 15 + 27 + 38",
                "What is the capital of France?",
                "Write a Python function to sort a list",
                "What's the weather today?",
                "What is the total sales revenue for Q1?",
                "Tell me a joke about programming",
            ]

            # ── Build graph ──────────────────────────────────────────────

            step(2, f"Process {len(questions)} questions")

            graph = StateGraph(ToolState)
            graph.add_node("classify", classify_and_select)
            graph.add_node("calculator", run_calculator)
            graph.add_node("search", run_search)
            graph.add_node("code", run_code)
            graph.add_node("database", run_database)
            graph.add_node("default", run_default)
            graph.add_node("summarize", summarize_results)

            graph.set_entry_point("classify")
            graph.add_conditional_edges(
                "classify",
                route_to_tool,
                {
                    "calculator": "calculator",
                    "search": "search",
                    "code": "code",
                    "database": "database",
                    "default": "default",
                    "summarize": "summarize",
                },
            )

            for tool_node in ["calculator", "search", "code", "database", "default"]:
                graph.add_conditional_edges(
                    tool_node,
                    check_more_questions,
                    {"next": "classify", "summarize": "summarize"},
                )

            graph.add_edge("summarize", END)

            app = graph.compile()

            await app.ainvoke(
                {
                    "kg": kg,
                    "questions": questions,
                    "question_index": 0,
                    "current_question": "",
                    "selected_tool": "",
                    "answers": [],
                }
            )

            success("Done!")
        finally:
            with contextlib.suppress(Exception):
                await il.drop_knowledge_graph("lg_tools")


if __name__ == "__main__":
    asyncio.run(run())
