#!/usr/bin/env python3
"""Interactive runner for InputLayer + LangChain examples.

Usage:
    # Run all examples
    uv run python -m examples.langchain.runner

    # Run specific examples by number
    uv run python -m examples.langchain.runner 1 4 6

    # Run a range
    uv run python -m examples.langchain.runner 1-5

    # List available examples
    uv run python -m examples.langchain.runner --list

Prerequisites:
    - InputLayer server running (cargo build --release && ./target/release/inputlayer-server)
    - Set INPUTLAYER_PASSWORD env var
    - For LLM examples: LM Studio or compatible server at localhost:1234
    - Set LLM_MODEL env var (default: deepseek/deepseek-r1-0528-qwen3-8b)
"""

import asyncio
import importlib
import sys
import time

from examples.langchain._common import (
    BLUE,
    BOLD,
    CYAN,
    DIM,
    GREEN,
    RED,
    RESET,
    YELLOW,
    InputLayer,
    check_llm,
    os,
    setup,
    success,
)

# ── Example registry ─────────────────────────────────────────────────

EXAMPLES = [
    ("ex01_retriever", "Retriever with Datalog query"),
    ("ex02_vector", "Retriever with vector search"),
    ("ex03_tool", "Tool for agent queries"),
    ("ex04_lcel_chain", "LCEL chain with LLM"),
    ("ex05_kg_building", "KG building from documents"),
    ("ex06_explainable_rag", "Explainable RAG with .why()"),
    ("ex07_multihop", "Multi-hop reasoning"),
    ("ex08_memory", "Conversational memory as facts"),
    ("ex09_access_control", "Access-controlled RAG"),
    ("ex10_multi_agent", "Multi-agent fact-checking"),
    ("ex11_anomaly", "Anomaly detection"),
    ("ex12_hallucination", "Hallucination detection"),
    ("ex13_guardrails", "Guardrails / output safety"),
    ("ex14_graphrag", "GraphRAG"),
    ("ex15_caching", "Semantic caching"),
    ("ex16_recommendation", "Recommendation engine"),
    ("ex17_lineage", "Data lineage and provenance"),
    ("ex18_vectorstore", "VectorStore (LangChain interface)"),
]

# Examples that require an LLM server
LLM_EXAMPLES = {4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 16, 17}


def print_menu() -> None:
    print(f"\n{BOLD}{BLUE}  InputLayer + LangChain Examples{RESET}")
    print(f"{DIM}  {'─' * 56}{RESET}\n")

    llm_available = check_llm()
    llm_status = f"{GREEN}available{RESET}" if llm_available else f"{RED}not detected{RESET}"
    print(f"  {DIM}LLM server: {llm_status}{DIM}{RESET}\n")

    for i, (_, title) in enumerate(EXAMPLES, 1):
        needs_llm = i in LLM_EXAMPLES
        llm_tag = f" {YELLOW}[LLM]{RESET}" if needs_llm else ""
        blocked = needs_llm and not llm_available
        color = DIM if blocked else ""
        end = RESET if blocked else ""
        print(f"  {color}{CYAN}{i:2d}{RESET} {color}{title}{end}{llm_tag}")

    print(f"\n{DIM}  Usage: runner.py [numbers|ranges|--all|--list]{RESET}")
    print(f"{DIM}  Examples: runner.py 1 4 6  |  runner.py 1-5  |  runner.py --all{RESET}\n")


def parse_selection(args: list[str]) -> list[int]:
    """Parse command-line arguments into a list of example numbers."""
    if not args or "--all" in args:
        return list(range(1, len(EXAMPLES) + 1))

    if "--list" in args:
        return []

    selected: list[int] = []
    for arg in args:
        if "-" in arg and not arg.startswith("-"):
            start, end = arg.split("-", 1)
            selected.extend(range(int(start), int(end) + 1))
        else:
            selected.append(int(arg))

    return [n for n in selected if 1 <= n <= len(EXAMPLES)]


async def run_examples(numbers: list[int]) -> None:
    print(f"\n{BOLD}{BLUE}  InputLayer + LangChain Integration Demo{RESET}")
    print(f"{DIM}  Running {len(numbers)} of {len(EXAMPLES)} examples{RESET}")

    async with InputLayer(
        os.environ.get("INPUTLAYER_URL", "ws://localhost:8080/ws"),
        username=os.environ.get("INPUTLAYER_USER", "admin"),
        password=os.environ.get("INPUTLAYER_PASSWORD", "admin"),
    ) as il:
        kg = await setup(il)

        passed = 0
        failed = 0
        skipped = 0
        t0 = time.time()

        for num in numbers:
            module_name, title = EXAMPLES[num - 1]
            try:
                mod = importlib.import_module(f"examples.langchain.{module_name}")
                await mod.run(kg)
                passed += 1
            except Exception as e:
                failed += 1
                print(f"\n  {RED}ERROR in example {num} ({title}): {e}{RESET}")

        elapsed = time.time() - t0

        # Cleanup
        await il.drop_knowledge_graph("langchain_demo")

        # Summary
        print(f"\n{BOLD}{'━' * 64}{RESET}")
        print(f"  {GREEN}Passed: {passed}{RESET}", end="")
        if failed:
            print(f"  {RED}Failed: {failed}{RESET}", end="")
        if skipped:
            print(f"  {YELLOW}Skipped: {skipped}{RESET}", end="")
        print(f"  {DIM}({elapsed:.1f}s){RESET}")
        success("Done! Knowledge graph cleaned up.")


def main() -> None:
    args = sys.argv[1:]

    if "--list" in args or "-l" in args:
        print_menu()
        return

    if "--help" in args or "-h" in args:
        print(__doc__)
        return

    numbers = parse_selection(args)

    if not numbers:
        print_menu()
        return

    asyncio.run(run_examples(numbers))


if __name__ == "__main__":
    main()
