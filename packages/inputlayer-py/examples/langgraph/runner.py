#!/usr/bin/env python3
"""Interactive runner for InputLayer + LangGraph examples.

Usage:
    uv run python -m examples.langgraph.runner              # list examples
    uv run python -m examples.langgraph.runner 1 5 7        # run specific
    uv run python -m examples.langgraph.runner 1-4          # run range
    uv run python -m examples.langgraph.runner --all        # run all

Prerequisites:
    - InputLayer server running
    - Set INPUTLAYER_PASSWORD env var
    - For LLM examples: LM Studio or compatible server at localhost:1234
"""

import asyncio
import importlib
import sys
import time

from examples.langgraph._common import (
    BLUE,
    BOLD,
    CYAN,
    DIM,
    GREEN,
    RED,
    RESET,
    YELLOW,
    check_llm,
    success,
)

EXAMPLES = [
    ("ex01_reasoning_loop", "Reasoning loop with fact accumulation"),
    ("ex02_investigation", "Multi-step investigation"),
    ("ex03_human_in_loop", "Human-in-the-loop approval"),
    ("ex04_branching_pipeline", "Branching pipeline"),
    ("ex05_self_correcting", "Self-correcting agent"),
    ("ex06_collaborative_planning", "Collaborative planning"),
    ("ex07_event_correlation", "Event correlation pipeline"),
    ("ex08_tool_selection", "Tool selection via rules"),
    ("ex09_streaming_aggregation", "Streaming aggregation with alerts"),
    ("ex10_resumable_graph", "Resumable graph with checkpointer"),
    ("ex11_memory", "Semantic memory with derived context"),
]

LLM_EXAMPLES = {1, 2, 5, 7, 8, 9, 11}


def print_menu() -> None:
    print(f"\n{BOLD}{BLUE}  InputLayer + LangGraph Examples{RESET}")
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
    print(f"{DIM}  Examples: runner.py 1 5 7  |  runner.py 1-4  |  runner.py --all{RESET}\n")


def parse_selection(args: list[str]) -> list[int]:
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
    print(f"\n{BOLD}{BLUE}  InputLayer + LangGraph Integration Demo{RESET}")
    print(f"{DIM}  Running {len(numbers)} of {len(EXAMPLES)} examples{RESET}")

    passed = 0
    failed = 0
    t0 = time.time()

    for num in numbers:
        module_name, title = EXAMPLES[num - 1]
        try:
            mod = importlib.import_module(f"examples.langgraph.{module_name}")
            await mod.run()
            passed += 1
        except Exception as e:
            failed += 1
            print(f"\n  {RED}ERROR in example {num} ({title}): {e}{RESET}")

    elapsed = time.time() - t0

    print(f"\n{BOLD}{'━' * 64}{RESET}")
    print(f"  {GREEN}Passed: {passed}{RESET}", end="")
    if failed:
        print(f"  {RED}Failed: {failed}{RESET}", end="")
    print(f"  {DIM}({elapsed:.1f}s){RESET}")
    success("Done!")


def main() -> None:
    args = sys.argv[1:]

    if "--help" in args or "-h" in args:
        print(__doc__)
        return

    if "--list" in args or "-l" in args:
        print_menu()
        return

    numbers = parse_selection(args)

    if not numbers:
        print_menu()
        return

    asyncio.run(run_examples(numbers))


if __name__ == "__main__":
    main()
